#!/usr/bin/env python3
"""Daily quality report for Order Book collector metrics."""

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def load_metrics(metrics_path: Path, date_filter: str = None, symbol_filter: str = None):
    entries = []
    if not metrics_path.exists():
        return entries
    with open(metrics_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if date_filter:
                ts = entry.get("timestamp", "")
                if not ts.startswith(date_filter):
                    continue
            if symbol_filter and entry.get("symbol") != symbol_filter:
                continue
            entries.append(entry)
    return entries


def compute_symbol_stats(entries):
    stats = {}
    by_symbol = defaultdict(list)
    for e in entries:
        by_symbol[e["symbol"]].append(e)
    for symbol, sym_entries in by_symbol.items():
        sym_entries.sort(key=lambda x: x["timestamp"])
        first = sym_entries[0]
        last = sym_entries[-1]
        try:
            t_first = datetime.fromisoformat(first["timestamp"].replace("Z", "+00:00"))
            t_last = datetime.fromisoformat(last["timestamp"].replace("Z", "+00:00"))
            duration = (t_last - t_first).total_seconds()
        except (ValueError, KeyError):
            duration = 0.0
        total_updates_received = sum(e.get("updates_received", 0) for e in sym_entries)
        total_updates_written = sum(e.get("updates_written", 0) for e in sym_entries)
        total_snapshots = max(e.get("snapshots", 0) for e in sym_entries) if sym_entries else 0
        total_reconnects = max(e.get("reconnects", 0) for e in sym_entries) if sym_entries else 0
        total_jumps = max(e.get("update_id_jumps", 0) for e in sym_entries) if sym_entries else 0
        invalid_secs = sum(e.get("invalid_state_duration_secs", 0) for e in sym_entries)
        rows_written = last.get("rows_written", 0)
        disk_free = last.get("disk_free_gb", -1)
        errors = sum(e.get("errors", 0) for e in sym_entries)
        is_valid = last.get("is_valid", False)
        stats[symbol] = {
            "duration_hours": duration / 3600,
            "updates_received": total_updates_received,
            "updates_written": total_updates_written,
            "snapshots": total_snapshots,
            "reconnects": total_reconnects,
            "update_id_jumps": total_jumps,
            "invalid_state_secs": invalid_secs,
            "rows_written": rows_written,
            "disk_free_gb": disk_free,
            "errors": errors,
            "is_valid": is_valid,
            "avg_updates_per_sec": total_updates_received / max(duration, 1),
        }
    return stats


def format_report(stats, date_filter):
    lines = []
    lines.append(f"Order Book Collector — Daily Quality Report")
    lines.append(f"Date: {date_filter or 'all'}")
    lines.append(f"Symbols: {len(stats)}")
    lines.append("")
    lines.append(f"{'Symbol':<12} {'Hours':>6} {'UpdRec':>10} {'UpdWrt':>10} {'Snaps':>8} {'Reconn':>6} {'Jumps':>6} {'InvSec':>8} {'Rows':>10} {'DiskGB':>7} {'Err':>4} {'Status':>8}")
    lines.append("-" * 115)
    for symbol in sorted(stats.keys()):
        s = stats[symbol]
        status = "OK" if s["is_valid"] else "GAP"
        lines.append(
            f"{symbol:<12} {s['duration_hours']:>6.1f} {s['updates_received']:>10} {s['updates_written']:>10} "
            f"{s['snapshots']:>8} {s['reconnects']:>6} {s['update_id_jumps']:>6} "
            f"{s['invalid_state_secs']:>8.1f} {s['rows_written']:>10} {s['disk_free_gb']:>7.1f} {s['errors']:>4} {status:>8}"
        )
    lines.append("")
    total_received = sum(s["updates_received"] for s in stats.values())
    total_written = sum(s["updates_written"] for s in stats.values())
    total_rows = sum(s["rows_written"] for s in stats.values())
    total_reconnects = sum(s["reconnects"] for s in stats.values())
    total_jumps = sum(s["update_id_jumps"] for s in stats.values())
    total_invalid = sum(s["invalid_state_secs"] for s in stats.values())
    total_errors = sum(s["errors"] for s in stats.values())
    lines.append(f"TOTAL: received={total_received} written={total_written} rows={total_rows} reconn={total_reconnects} jumps={total_jumps} invalid={total_invalid:.1f}s errors={total_errors}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Order Book collector daily quality report")
    parser.add_argument("--date", help="Filter by date (YYYY-MM-DD)")
    parser.add_argument("--symbol", help="Filter by symbol")
    parser.add_argument("--metrics", default="data/market/orderbook/reconstructed/_metrics.jsonl", help="Metrics JSONL path")
    args = parser.parse_args()
    metrics_path = Path(args.metrics)
    entries = load_metrics(metrics_path, args.date, args.symbol)
    if not entries:
        print(f"No metrics found in {metrics_path}" + (f" for date={args.date}" if args.date else "") + (f" symbol={args.symbol}" if args.symbol else ""))
        return
    stats = compute_symbol_stats(entries)
    print(format_report(stats, args.date))


if __name__ == "__main__":
    main()

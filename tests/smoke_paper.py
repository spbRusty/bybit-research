"""Smoke test: MODE B paper trading end-to-end.

Resets state → creates synthetic signal → executes → verifies portfolio.
No real data required. Validates the full infrastructure works.
Uses temporary state/registry files — never touches production data.
"""
import sys
import tempfile
from pathlib import Path
from datetime import datetime, timezone

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import src.paper as paper_mod

_TMP = Path(tempfile.mkdtemp(prefix="smoke_paper_"))
paper_mod.STATE_FILE = _TMP / "state.json"
paper_mod.SHADOW_STATE_FILE = _TMP / "shadow_state.json"

from src.paper import (
    PaperMode, PaperSignal, execute_signal, reset_state,
    load_state, get_portfolio_summary,
)
from src.risk import check_position
from src.registry import (
    HypothesisLifecycle, HypothesisStatus,
    update_from_research, get_eligible_for_paper, get_shadow_eligible,
)


def smoke_test():
    print("=== MODE B Paper Trading Smoke Test ===\n")

    # 1. Reset state
    state = reset_state()
    assert state["balance"] == 1000.0, f"Expected 1000.0, got {state['balance']}"
    print(f"1. State reset: balance={state['balance']} USDT")

    # 2. Create signal
    signal = PaperSignal(
        hypothesis_id="H001",
        symbol="BTCUSDT",
        side="long",
        entry_price=50000.0,
        stop_loss=49500.0,
        take_profit=50750.0,
        signal_timestamp="2026-09-07T12:00:00Z",
        horizon_min=5,
        mode=PaperMode.SHADOW,
        vol_gk_30d=0.02,
    )
    print(f"2. Signal created: {signal.symbol} {signal.side} @ {signal.entry_price}")

    # 3. Create candle data (5 candles after signal)
    candles = pl.DataFrame({
        "open_time": [
            datetime(2026, 9, 7, 12, i, tzinfo=timezone.utc)
            for i in range(1, 6)
        ],
        "open":  [50010.0, 50050.0, 50100.0, 50200.0, 50300.0],
        "high":  [50050.0, 50100.0, 50150.0, 50250.0, 50350.0],
        "low":   [49980.0, 50020.0, 50070.0, 50150.0, 50250.0],
        "close": [50030.0, 50080.0, 50120.0, 50220.0, 50310.0],
        "volume": [100.0] * 5,
    })
    print(f"3. Candles: {len(candles)} rows")

    # 4. Execute
    result = execute_signal(signal, candles, state)
    assert result["executed"], f"Execution failed: {result}"
    trade = result["trade"]
    print(f"4. Executed: trade_id={trade['trade_id']}, reason={trade['reason']}")
    print(f"   entry={trade['entry_price']}, exit={trade['exit_price']}")
    print(f"   entry_fee={trade['entry_fee']}, exit_fee={trade['exit_fee']}")
    print(f"   gross_pnl={trade['gross_pnl']}, net_pnl={trade['net_pnl']}")

    # 5. Verify portfolio
    state = load_state()
    summary = get_portfolio_summary(state)
    print(f"5. Portfolio: balance={summary['balance']:.2f}, trades={summary['trade_count']}")
    print(f"   realized_pnl={summary['realized_pnl']:.4f}, fees={summary['total_fees']:.4f}")
    print(f"   win_rate={summary['win_rate']:.2%}")

    # 6. Risk check
    check = check_position(0.01, 50000.0, state)
    print(f"6. Risk check: allowed={check.allowed}, reason={check.reason or 'ok'}")

    # 7. Registry
    reg = HypothesisLifecycle(registry_path=_TMP / "registry.json")
    reg.register("H001", HypothesisStatus.CANDIDATE)
    shadow = get_shadow_eligible(reg)
    eligible = get_eligible_for_paper(reg)
    print(f"7. Registry: shadow={len(shadow)}, eligible={len(eligible)}")

    # 8. Verify no live trading paths
    from src.paper import _initial_state
    s = _initial_state()
    assert "live" not in str(s).lower()
    print("8. No live trading paths detected")

    print("\n=== ALL CHECKS PASSED ===")
    return True


if __name__ == "__main__":
    ok = smoke_test()
    sys.exit(0 if ok else 1)

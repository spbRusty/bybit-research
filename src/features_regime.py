"""Признаки рыночного режима (ТЗ §20). Режим определяется только данными <= T."""
from __future__ import annotations

import polars as pl

from src.registry import Feature, register


def _mk(fid, name, desc, formula, lookback=1, cost="low", data=("ohlcv", "btc")):
    return Feature(fid, name, "regime", desc, formula, "features_regime",
                   "1m", lookback, data, True, cost)


register(_mk("trend_regime", "Режим тренда", "trend|range по trend_strength",
             "regime(trend)", 60))
register(_mk("high_volatility_regime", "Режим высокой волатильности",
             "volatility_regime in (high, very_high)", "regime(hvol)", 241, cost="medium"))
register(_mk("liquidity_regime", "Режим ликвидности", "high|low по процентилю объёма (240)",
             "regime(liq)", 240, cost="medium"))
register(_mk("volume_regime", "Режим объёма", "high|normal|low", "regime(vol)", 240))
register(_mk("risk_on", "Риск-он", "btc_trend_regime>0 и breadth>0", "risk_on", 60,
             cost="medium", data=("ohlcv", "btc")))
register(_mk("risk_off", "Риск-офф", "btc_trend_regime<0", "risk_off", 60,
             cost="medium", data=("ohlcv", "btc")))
# §20 — дополнительные режимы (производные от уже вычисленных признаков)
register(_mk("range_regime", "Режим боковика", "trend_strength <= 70-процентиль",
             "range", 120))
register(_mk("low_volatility_regime", "Режим низкой волатильности",
             "volatility_regime in (low, very_low)", "lvol", 241, cost="medium"))
register(_mk("btc_trend", "Режим тренда BTC (label)", "sign(SMA20-SMA60) BTC",
             "btc_trend", 60, data=("ohlcv", "btc")))
register(_mk("btc_volatility", "Режим волатильности BTC (label)",
             "high|low BTC", "btc_vol", 120, data=("ohlcv", "btc")))
register(_mk("correlation_regime", "Режим корреляции с BTC",
             "high|low corr по порогу", "corrreg", 60, data=("ohlcv", "btc")))
register(_mk("market_breadth_regime", "Рыночная широта (breadth)",
             "advancing - declining / total", "breadth", 60, cost="medium",
             data=("ohlcv", "breadth")))


def _ema(s: pl.Expr, span: int) -> pl.Expr:
    return s.ewm_mean(alpha=2.0 / (span + 1), min_samples=1)


def add_regime_features(df: pl.DataFrame) -> pl.DataFrame:
    """Комбинированные режимы на основе уже вычисленных признаков."""
    ts = pl.col("trend_strength")
    out = df.with_columns(
        pl.when(ts > ts.rolling_quantile(0.7, interpolation="nearest",
                                         window_size=120, min_samples=1))
        .then(pl.lit("trend")).otherwise(pl.lit("range")).alias("trend_regime"))
    # ликвидность: median объёма 240; порог параметризуем дефолтом 0.7/1.3
    v = pl.col("volume")
    med = v.rolling_median(240, min_samples=1)
    out = out.with_columns(
        pl.when(out["volume"] > med * 1.3).then(pl.lit("high"))
         .when(out["volume"] < med * 0.7).then(pl.lit("low"))
         .otherwise(pl.lit("normal")).alias("liquidity_regime"))
    # volume regime: по процентилю 240
    out = out.with_columns(
        pl.when(v > v.rolling_quantile(0.8, interpolation="nearest",
                                       window_size=240, min_samples=1)).then(pl.lit("high"))
         .when(v < v.rolling_quantile(0.2, interpolation="nearest",
                                      window_size=240, min_samples=1)).then(pl.lit("low"))
         .otherwise(pl.lit("normal")).alias("volume_regime"))
    # risk_on / risk_off (используем наличные cross/trend колонки, если есть)
    if "btc_trend_regime" in out.columns:
        out = out.with_columns(
            (pl.col("btc_trend_regime") > 0).cast(pl.Int8).alias("risk_on"),
            (pl.col("btc_trend_regime") < 0).cast(pl.Int8).alias("risk_off"))
        out = out.with_columns(
            pl.when(pl.col("btc_trend_regime") > 0).then(pl.lit("up"))
             .when(pl.col("btc_trend_regime") < 0).then(pl.lit("down"))
             .otherwise(pl.lit("flat")).alias("btc_trend"))
    # range_regime — производный от trend_regime (inverse)
    if "trend_regime" in out.columns:
        out = out.with_columns(pl.col("trend_regime").alias("range_regime"))
    # low_volatility — производный от volatility_regime, если он есть
    if "volatility_regime" in out.columns:
        out = out.with_columns(
            pl.when(pl.col("volatility_regime").is_in(["low", "very_low"]))
            .then(pl.lit(1)).otherwise(pl.lit(0)).alias("low_volatility_regime"))
        out = out.with_columns(
            pl.when(pl.col("volatility_regime").is_in(["high", "very_high"]))
            .then(pl.lit("high")).otherwise(pl.lit("low")).alias("btc_volatility"))
    # correlation_regime — порог ABS корреляции с BTC
    if "corr_btc_60" in out.columns:
        out = out.with_columns(
            pl.when(pl.col("corr_btc_60").abs() >= 0.5).then(pl.lit("high"))
            .otherwise(pl.lit("low")).alias("correlation_regime"))
    # market_breadth — только если включён §17 (колонка breadth из внешнего пайплайна)
    if "breadth" in out.columns:
        out = out.with_columns(
            ((pl.col("breadth") > 0).cast(pl.Int8)).alias("market_breadth_regime"))
    return out

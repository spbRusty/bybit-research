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
    return out

"""Межрыночные признаки (ТЗ §16). BTC/ETH как фактор рынка, с контрольным таймфреймом.

Для альткоина важна дифференциация: вырос монета из-за рынка или сама по себе.
Bar UTC — каждый признак помечается временем доступности (обычно = времени свечи T).
"""
from __future__ import annotations

import polars as pl

from src.registry import Feature, register


def _mk(fid, name, desc, formula, lookback=1, cost="low", data=("ohlcv", "btc")):
    return Feature(fid, name, "cross", desc, formula, "features_cross",
                   "1m", lookback, data, True, cost)


register(_mk("btc_return", "Доходность BTC", "close_btc/close_btc(-1)-1", "r_btc", 2))
register(_mk("btc_volatility_60", "Волатильность BTC (60)", "std(r_btc,60)", "vol_btc", 60))
register(_mk("btc_relative_volume_60", "Относительный объём BTC (60)",
             "v_btc/median(v_btc,60)", "rv_btc", 60))
register(_mk("btc_roc_20", "ROC BTC (20)", "close_btc/close_btc(-20)-1", "roc_btc", 21))
register(_mk("btc_trend_regime", "Режим тренда BTC", "sign(SMA20_btc-SMA60_btc)",
             "trend_btc", 60, cost="medium"))
register(_mk("btc_volatility_regime", "Режим волатильности BTC",
             "high|low по процентилю", "volreg_btc", 120, cost="medium"))
register(_mk("corr_btc_60", "Корреляция с BTC (60)", "corr(r, r_btc, 60)",
             "corr_btc", 60, cost="medium"))
register(_mk("rolling_beta_btc_60", "Бета к BTC (60)", "cov(r,r_btc)/var(r_btc)",
             "beta_btc", 60, cost="medium"))
register(_mk("residual_return_btc", "Остаточная доходность к BTC",
             "r - beta*r_btc", "resid", 60, cost="medium"))
register(_mk("relative_strength_btc", "Относительная сила к BTC",
             "roc_20 - roc_btc_20", "rs_btc", 21))
# ETH как второй фактор рынка (§16): те же признаки, префикс eth_
for _fid, _name, _desc, _formula, _lb, *_cost in [
    ("eth_return", "Доходность ETH", "close_eth/close_eth(-1)-1", "r_eth", 2),
    ("eth_volatility_60", "Волатильность ETH (60)", "std(r_eth,60)", "vol_eth", 60),
    ("eth_znak", "Направление ETH", "sign(r_eth)", "dir_eth", 2),
    ("eth_roc_20", "ROC ETH (20)", "close_eth/close_eth(-20)-1", "roc_eth", 21),
    ("eth_trend_regime", "Режим тренда ETH", "sign(SMA20_eth-SMA60_eth)",
     "trend_eth", 60, "medium"),
    ("eth_volatility_regime", "Режим волатильности ETH",
     "high|low по процентилю", "volreg_eth", 120, "medium"),
    ("corr_eth_60", "Корреляция с ETH (60)", "corr(r, r_eth, 60)",
     "corr_eth", 60, "medium"),
    ("rolling_beta_eth_60", "Бета к ETH (60)", "cov(r,r_eth)/var(r_eth)",
     "beta_eth", 60, "medium"),
    ("residual_return_eth", "Остаточная доходность к ETH", "r - beta*r_eth",
     "resid_eth", 60, "medium"),
    ("relative_strength_eth", "Относительная сила к ETH", "roc_20 - roc_eth_20",
     "rs_eth", 21),
]:
    register(_mk(_fid, _name, _desc, _formula, _lb, cost=(_cost[0] if _cost else "low")))


def _ema(s: pl.Expr, span: int) -> pl.Expr:
    return s.ewm_mean(alpha=2.0 / (span + 1), min_samples=1)


def add_cross_features(df: pl.DataFrame, market: pl.DataFrame | None,
                       market_sym: str = "btc") -> pl.DataFrame:
    """Признаки относительно рынка (BTC/ETH). market_df — свечи рынка, выровнены asof.

    market_sym -> префикс колонок (btc_ / eth_). Для BTC/ETH обе доступны.
    """
    if market is None:
        # без данных рынка возвращаем df без cross-признаков
        return df
    m = market.sort("open_time")
    m = m.with_columns([
        pl.col("close").pct_change().alias(f"{market_sym}_return"),
        pl.col("close").rolling_mean(20, min_samples=1).alias(f"{market_sym}_sma20"),
        pl.col("close").rolling_mean(60, min_samples=1).alias(f"{market_sym}_sma60"),
    ])
    m = m.select([
        "open_time",
        f"{market_sym}_return",
        (pl.col("close") / pl.col("close").shift(20) - 1).alias(f"{market_sym}_roc_20"),
        pl.col(f"{market_sym}_sma20"),
        pl.col(f"{market_sym}_sma60"),
        # волатильность рынка (rolling std доходности)
        pl.col(f"{market_sym}_return").rolling_std(60, min_samples=1)
        .alias(f"{market_sym}_volatility_60"),
    ])
    out = df.join_asof(m, on="open_time", strategy="backward")
    b = pl.col(f"{market_sym}_return")
    r = pl.col("candle_return_1m")
    sma20 = pl.col(f"{market_sym}_sma20")
    sma60 = pl.col(f"{market_sym}_sma60")
    out = out.with_columns([
        (sma20 - sma60).sign().alias(f"{market_sym}_trend_regime"),
        (pl.col(f"{market_sym}_volatility_60") >
         pl.col(f"{market_sym}_volatility_60")
         .rolling_quantile(0.7, interpolation="nearest", window_size=120, min_samples=1))
        .alias(f"{market_sym}_volatility_regime"),
        # rolling beta и корреляция (60): через cov/var упрощённо
        ((r - r.rolling_mean(60, min_samples=1)) *
         (b - b.rolling_mean(60, min_samples=1))).rolling_mean(60, min_samples=1)
        .alias(f"cov_{market_sym}_60"),
    ])
    beta = (pl.col(f"cov_{market_sym}_60") /
            b.rolling_std(60, min_samples=1).pow(2).clip(1e-12)).alias(f"rolling_beta_{market_sym}_60")
    out = out.with_columns([
        beta,
        pl.col(f"cov_{market_sym}_60").alias("_skip"),
    ])
    # корреляция = cov/(std_r * std_b) ; residual = r - beta*b ; relative strength
    sigma_r = r.rolling_std(60, min_samples=1)
    sigma_b = b.rolling_std(60, min_samples=1)
    out = out.with_columns([
        (pl.col(f"cov_{market_sym}_60") / (sigma_r * sigma_b).clip(1e-12))
        .alias(f"corr_{market_sym}_60"),
        (r - pl.col(f"rolling_beta_{market_sym}_60") * b).alias(f"residual_return_{market_sym}"),
        (pl.col("roc_20") - pl.col(f"{market_sym}_roc_20")).alias(f"relative_strength_{market_sym}"),
    ])
    # рыночный режим волатильности: стринговые и булевы корректно
    out = out.with_columns(
        pl.when(pl.col(f"{market_sym}_volatility_regime"))
        .then(pl.lit("high")).otherwise(pl.lit("low"))
        .alias(f"{market_sym}_vol_regime_label"))
    return out.drop("_skip")

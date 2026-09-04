SL HYPOTHESIS TEST — RESULTS
=============================
Date: 2026-09-04
Status: REJECTED — SL does not pass gates with current cost model

1. SETUP
========
- SL levels tested: 0.03%, 0.05%, 0.10%, 0.20%
- Horizons: 5m, 10m, 30m
- Cost model: 0.20% round-trip on ALL events (strict)
- Conditions: all 8 base hypotheses + "all events" (no condition)
- Periods: discovery (2025-12-25 to 2026-04-30), validation (2026-05-01 to 2026-06-30), OOS (2026-07-01 to 2026-09-02)

2. RESULTS
==========

2.1 SL on ALL events (no condition filter)
-------------------------------------------
SL=0.03% 30m:
  discovery:  mean=-0.001782  t=-993.8  n=1,949,928
  validation: mean=-0.001772  t=-726.7  n=1,054,135
  oos:        mean=-0.001818  t=-747.8  n=1,249,462

SL=0.05% 30m:
  discovery:  mean=-0.001794  t=-884.6  n=1,949,928
  validation: mean=-0.001780  t=-643.4  n=1,054,135
  oos:        mean=-0.001831  t=-676.6  n=1,249,462

SL=0.10% 30m:
  discovery:  mean=-0.001841  t=-743.4  n=1,949,928
  validation: mean=-0.001811  t=-532.5  n=1,054,135
  oos:        mean=-0.001852  t=-558.5  n=1,249,462

2.2 SL on H001 (relative_volume > 3.0 & is_green)
---------------------------------------------------
SL=0.03% 5m:
  mean=-0.001836  SL hits=74.2%  miss_ret=-0.000498

All SL variants on H001: mean_net < 0

2.3 Why SL fails with current cost model
------------------------------------------
- SL exit = intrabar, but cost model charges 0.20% round-trip
- SL hit rate = 74-88% (very high) — most events hit SL
- Each SL hit: return = -SL, net = -SL - 0.002
- SL=0.03%: net per hit = -0.0023, hit rate 87.6%
- Only 12.4% events don't hit SL, their raw return = +0.0039
- Weighted: 87.6% × (-0.0023) + 12.4% × (+0.0019) = -0.00178

2.4 What would make SL work
------------------------------
- SL exit cost ≠ full round-trip cost (intrabar = spread only)
- If SL cost = 0.05% (entry spread only): SL=0.03% net = -0.00035
- Then blended: 87.6% × (-0.00035) + 12.4% × (+0.0019) = +0.000218
- But this requires cost model change, which user prohibits

3. GATES ASSESSMENT
====================
All SL hypotheses FAIL:
- mean_net < 0 on discovery → FAILS min_t_stat gate
- mean_net < 0 on validation → FAILS validation gate
- mean_net < 0 on OOS → FAILS OOS gate
- All cost grid levels fail → FAILS cost stress gate

4. CONCLUSION
==============
SL does not pass any gate with the current strict cost model (0.20% on all trades).

The SL mechanism is sound (miss events have +0.0039 raw return), but the cost structure
assumption eliminates the benefit. SL would require:
- Differentiated cost model (SL exit = lower cost than full horizon exit)
- Or: SL as risk management tool (not standalone strategy)

Status: NOT a strategy. SL infrastructure added to Hypothesis dataclass but
not activated in production pipeline.

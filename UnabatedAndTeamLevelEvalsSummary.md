# Unabated And Team-Level Eval Summary

This note summarizes the Unabated real-lines eval run on March 10, 2026 using:

- `inputs/unabated/unabatedResponseV5.json`
- `outputs/model_projections/pipeline_existing_kp_Tue_Mar_10_10-28-32_AM_saved_2026-03-10_102832.xlsx`

The detailed row-level output is:

- `outputs/evals/unabated_real_lines_eval_pipeline_existing_kp_Tue_Mar_10_10-28-32_AM_saved_2026-03-10_102832_2026-03-10_111637.csv`

Matched sample:

- 461 player/stat rows
- 12 stat categories

## Market-Level Bias Table

```text
+---------------------------+-------+----------+----------------+--------+--------+--------+
| Stat                      | Count | Avg Line | Avg Projection |   MAE  |  RMSE  |  Bias  |
+---------------------------+-------+----------+----------------+--------+--------+--------+
| points_rebounds_assists   |    68 |  21.5110 |        22.2499 | 1.5893 | 1.9484 | 0.7388 |
| points_rebounds           |    66 |  18.7879 |        19.3902 | 1.3766 | 1.7716 | 0.6023 |
| points_assists            |    67 |  16.5672 |        16.6542 | 1.2687 | 1.6436 | 0.0870 |
| points                    |    81 |  13.8542 |        13.8828 | 1.1585 | 1.4958 | 0.0286 |
| rebounds_assists          |    51 |   7.6961 |         8.3560 | 0.9470 | 1.1731 | 0.6599 |
| rebounds                  |    43 |   5.1405 |         5.8852 | 0.8491 | 1.0657 | 0.7447 |
| stocks                    |     5 |   1.9000 |         2.3949 | 0.4949 | 0.5368 | 0.4949 |
| assists                   |    29 |   2.8793 |         3.1051 | 0.3869 | 0.4750 | 0.2258 |
| turnovers                 |    14 |   1.8571 |         2.1423 | 0.3842 | 0.4855 | 0.2851 |
| threes                    |     6 |   1.8333 |         2.1611 | 0.3278 | 0.3374 | 0.3278 |
| blocks                    |     8 |   1.1250 |         1.3361 | 0.3185 | 0.3574 | 0.2111 |
| steals                    |    23 |   0.8478 |         1.0598 | 0.2766 | 0.3320 | 0.2119 |
+---------------------------+-------+----------+----------------+--------+--------+--------+
```

## Team-Context Table

`Avg Team Season` is the average full-team season baseline for the relevant stat across the matched teams in that stat bucket. `Avg Team Projected` is the average full-team projected total for those same teams from the workbook.

```text
+---------------------------+-------+----------+----------------+-----------------+--------------------+--------+--------+--------+
| Stat                      | Count | Avg Line | Avg Projection | Avg Team Season | Avg Team Projected |   MAE  |  RMSE  |  Bias  |
+---------------------------+-------+----------+----------------+-----------------+--------------------+--------+--------+--------+
| points_rebounds_assists   |    68 |  21.5110 |        22.2499 |        125.6306 |           127.0151 | 1.5893 | 1.9484 | 0.7388 |
| points_rebounds           |    66 |  18.7879 |        19.3902 |        111.7085 |           112.8977 | 1.3766 | 1.7716 | 0.6023 |
| points_assists            |    67 |  16.5672 |        16.6542 |         91.2466 |            91.0459 | 1.2687 | 1.6436 | 0.0870 |
| points                    |    81 |  13.8542 |        13.8828 |         76.1857 |            75.6790 | 1.1585 | 1.4958 | 0.0286 |
| rebounds_assists          |    51 |   7.6961 |         8.3560 |         49.7657 |            50.9554 | 0.9470 | 1.1731 | 0.6599 |
| rebounds                  |    43 |   5.1405 |         5.8852 |         35.0005 |            36.2679 | 0.8491 | 1.0657 | 0.7447 |
| stocks                    |     5 |   1.9000 |         2.3949 |         10.6280 |            10.7360 | 0.4949 | 0.5368 | 0.4949 |
| assists                   |    29 |   2.8793 |         3.1051 |         14.7069 |            14.7470 | 0.3869 | 0.4750 | 0.2258 |
| turnovers                 |    14 |   1.8571 |         2.1423 |         11.2914 |            11.0480 | 0.3842 | 0.4855 | 0.2851 |
| threes                    |     6 |   1.8333 |         2.1611 |          8.0733 |             8.3098 | 0.3278 | 0.3374 | 0.3278 |
| blocks                    |     8 |   1.1250 |         1.3361 |          3.9525 |             3.8168 | 0.3185 | 0.3574 | 0.2111 |
| steals                    |    23 |   0.8478 |         1.0598 |          6.4883 |             6.8280 | 0.2766 | 0.3320 | 0.2119 |
+---------------------------+-------+----------+----------------+-----------------+--------------------+--------+--------+--------+
```

## Main Takeaway

The team-level values look broadly reasonable. In most categories, the average team projected totals are close to the average team season baselines. That makes a pure team-total miss less likely as the primary explanation for the player-level bias.

The cleaner explanation is allocation: projections appear to be concentrating too much production on starters and primary rotation players, and not enough on reserve players. If total team rebounds, assists, points, and related combo totals are near fair, but individual-player markets still skew high, that usually means the distribution of those team totals across players is too top-heavy.

This explanation fits the current pattern especially well for rebound-heavy and combo markets. Those stats are more sensitive to role concentration and minute-share assumptions than simple scoring means alone.

## How Big An Issue Is Each Stat?

### High concern

- `rebounds`: meaningful issue. The raw bias is `0.7447` on an average line of `5.1405`, which is a large relative miss. Since team rebound totals look reasonable, this points strongly to rebound share being too concentrated among top players.
- `rebounds_assists`: meaningful issue. The `0.6599` bias is large relative to the line scale and is directionally consistent with the rebound problem.
- `points_rebounds`: moderate-to-meaningful issue. The `0.6023` bias is not catastrophic on an average line near `18.8`, but it is persistent and likely inherits the rebound concentration problem.

### Moderate concern

- `points_rebounds_assists`: moderate issue, not necessarily severe. The `0.7388` bias looks more tolerable in context because the average line is `21.5110`. For a left-skewed discrete distribution, a mean above the line does not automatically imply the under is bad. Still, the direction of the bias is consistent with the rebound/share concentration story, so it should be treated as a calibration warning rather than ignored.
- `stocks`: likely an issue if it persists, but the sample is too small to conclude much from `5` rows.
- `threes`: similar story. `0.3278` bias is noticeable relative to the line, but `6` rows is too small to read confidently.

### Low-to-moderate concern

- `assists`: mild upward bias. Not alarming, but consistent with primary-ballhandler concentration being a little too aggressive.
- `turnovers`: mild upward bias. This is not a top-priority issue.
- `blocks`: mild upward bias with low sample. Worth monitoring, not a clear defect.
- `steals`: mild upward bias. Small issue at most.

### Low concern / acceptable

- `points`: acceptable. Bias is essentially flat at `0.0286`, which is strong given the sample size.
- `points_assists`: acceptable. Error is not tiny, but the mean is very well centered with `0.0870` bias. This looks much better than rebound-including combos.

## Interpreting Combo-Stat Bias

Mean bias should not be treated as identical to betting edge, especially for combo stats. A projection mean that sits above a line can still leave the under near 50% or better if the underlying stat distribution is discrete and left-skewed.

That matters most for `PRA`. A `+0.74` mean bias on a `21.5` line is not enough by itself to call PRA pricing broken. The right conclusion is narrower:

- PRA is probably not a severe issue.
- PRA does share the same upward directional pressure seen in rebound-heavy categories.
- The current mean-based eval is useful for center-of-distribution checks, but not sufficient for judging bet quality.

## Most Likely Model Issue

The current evidence points more toward player allocation than team-total generation:

- Team totals are close to season baselines.
- Player-level rebound and combo markets skew high.
- Scoring alone looks much better centered than rebound-driven categories.

That combination suggests the model is putting too much usage, rebound share, or minute-quality on starters and core rotation players, while reserve players are absorbing too little of the team total.

## Recommended Next Checks

- Compare projected player share of team rebounds, assists, and points against season share for starters vs bench players.
- Review whether minute projections are too sticky to recent starters and too slow to reallocate to reserve usage.
- Backtest selected-side `win_pct` calibration, especially for combo props, instead of relying only on mean-minus-line bias.
- Split evals by likely starters vs reserves to confirm whether the bias is concentrated in top-minute players.

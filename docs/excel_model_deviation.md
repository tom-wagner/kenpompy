# Excel Model Deviation Report

Comparison of `projection_model.py` (Python model) vs `EXISTING_MODEL.xlsx` (Excel model).
Tested on the 168 players / 16 teams in EXISTING_MODEL.xlsx.

---

## Summary Table

| Stat | Team Total | Player Distribution | Player Correlation | Player MAE | Confidence |
|------|-----------|--------------------|--------------------|------------|------------|
| PTS  | 1.00x (identical) | Changed (matchup adj.) | r=1.0000 | 0.030 | 90 |
| AST  | 1.00x (identical) | Changed (ARate fix) | r=0.9971 | 0.074 | 75 |
| REB  | 0.95x | Changed (top-down anchor) | r=0.9986 | 0.247 | 85 |
| 3PM  | 1.00x (identical) | Changed (scoring profile) | r=0.9844 | 0.082 | 80 |
| TO   | 1.00x (identical) | Identical | r=1.0000 | 0.000 | 95 |
| STL  | 1.13x | Changed (top-down calibration) | r=0.9834 | 0.089 | 82 |
| BLK  | 0.60x | Changed (2P FGA fix + anchor) | r=0.9868 | 0.328 | 88 |

Note: "1.00x team total" means the team sum is preserved, but individual player values differ. Only TO is truly identical at both team and player level.

---

## PROJ PTS — Deviation: 1.00x (effectively identical)

**What changed:** Added a matchup adjustment (70% player scoring profile, 30% opponent defensive profile). The adjustment redistributes points among players on the same team based on how each player's scoring mix (2P/3P/FT) interacts with the specific opponent's defensive weaknesses. Team totals remain exactly equal to VEGAS_PTS.

**Player-level deviation:** Mean ratio 1.00, MAE 0.030. Worst case: Koby Brea +0.18 pts. The matchup adjustment slightly shifts points between teammates but the effect is small.

**Why this is better:** The Excel model treats all players on a team as equally affected by opponent defense. A 3-point specialist facing a team that allows many 3s should project higher than a post player on that same team. The matchup adjustment captures this. The effect is intentionally subtle (30% weight) to avoid overfitting.

**Confidence: 90/100.** The team total is provably correct (anchored to VEGAS_PTS). The player redistribution is directionally correct and small in magnitude. The 70/30 weights are not empirically calibrated, which costs some confidence.

---

## PROJ AST — Deviation: 1.00x team total, player redistribution

**What changed:** Fixed the ARate denominator. KenPom defines ARate as `100 * AST / teammate_FGM_while_on_court`. The Excel model uses `team_fgm` (total team FGM including the player's own) in the denominator, inflating assist projections for high-scoring players by ~16%. We subtract the player's per-game FGM to get the correct `teammate_fgm`.

**Player-level deviation:** Mean ratio 1.04, MAE 0.074. Team totals are exactly 1.00x because the identity normalization (sum of ast_unadj) absorbs the correction. The redistribution shifts assists away from high-usage scorers toward pure facilitators.

**Worst cases:** Donovan Dent -0.63, Nique Clifford -0.62, Cooper Flagg -0.43. These are high-scoring players whose inflated ARate contribution was corrected downward.

**Why this is better:** The ARate denominator fix is mathematically provable from KenPom's published formula. Using total team FGM instead of teammate FGM systematically overstates assists for players who score a lot, because their own FGM inflates the base. The fix is not a modeling choice — it's a bug fix.

**Confidence: 75/100.** The ARate fix itself is 100% correct. Confidence is lower because the team-level AST target still lacks an external anchor (identity normalization). The model produces an AST/FGM range of 0.33-0.78 across teams, while real NCAA data shows 0.50-0.61. This is a known weakness not yet addressed. A planned improvement (blending with league average at w=0.50) would raise confidence to ~85.

---

## PROJ REB — Deviation: 0.95x

**What changed:** Two changes:
1. Removed the 1.1x multiplier that the Excel model applies to all rebounds
2. Added a top-down team target using team-level rebounding rates (`Team.Def-OR%`, `Team.Off-OR%`, `Opponent.Off-OR%`, `Opponent.Def-OR%`) with 75/25 team/opponent blending

The Excel model computes rebounds bottom-up (`DR% * DefRebOpp + OR% * OffRebOpp`) and then multiplies by 1.1. Our model computes the same player-level weights but normalizes them to a team target derived from team rebounding rates.

**Team-level deviation:** 0.95x on EXISTING_MODEL data. On the full MAIN_SCRIPT_OUTPUT (289 teams), the model produces 35.6 RPG vs NCAA average ~36 (-1% gap). The old bottom-up model produced 32.2 RPG (-11% gap).

**Validation against known teams (sports-reference):**
- BYU: Old=24.8, New=37.6, Actual=37.6 (exact match)
- Duke: Old=24.0, New=36.1, Actual=38.2
- Alabama: Old=34.7, New=38.6, Actual=42.3
- MAE across 8 known teams: 1.9 RPG

**Why this is better:** The Excel model's 1.1x multiplier is a crude approximation. It inflates all teams equally regardless of their actual rebounding ability. The top-down approach uses KenPom's team-level OR%/DR% data, which captures real differences in rebounding strength. Teams with elite offensive rebounding (high Team.Off-OR%) correctly get more rebounds. The improvement is dramatic for teams where individual DR%/OR% stats don't reconstruct well (BYU went from 24.8 to 37.6).

**Confidence: 85/100.** The top-down anchor is well-grounded in KenPom team-level data. The ~1% gap from NCAA averages is small and structural (some dead-ball rebounds aren't capturable at the individual level). The 0.95x deviation on EXISTING_MODEL data reflects a better calibration, not a regression.

---

## PROJ 3PM — Deviation: 1.00x team total, player redistribution

**What changed:** Replaced the distribution weight from `unadj_3pm` (historical 3PM-per-minute * MINS_PROJ) with `share_3p * simple_pts` (player's 3-point scoring fraction * their projected points contribution). Team totals remain anchored to the same 3P FGM target from point distribution.

**Player-level deviation:** Mean ratio 1.02, MAE 0.082. The redistribution shifts 3PM away from pure shooters with low overall scoring toward players who score more overall.

**Worst cases:** Langston Love -0.60, Chris Youngblood -0.59. These are 3-point specialists whose raw 3PM-per-minute rate gave them a disproportionate share of team 3PM relative to their overall scoring contribution.

**Why this is better:** The Excel model distributes 3PM based on historical 3PM rate per minute, independent of projected points. This creates a fundamental inconsistency: a player can be projected for more 3-point-points (3PM*3) than total points. The old model had 96 players hitting the `3PM*3 > PTS` cap. The new model has 0 violations without the cap. The projected 3PM fraction correlates r=0.990 with actual KenPom player 3P scoring profiles (MAE=0.021), confirming the distribution is accurate.

**Confidence: 80/100.** The scoring-profile approach eliminates a structural impossibility (3PM*3 > PTS) while maintaining r=0.990 fidelity to actual player profiles. Confidence is not higher because the team-level 3P FGM target is driven by VEGAS_PTS (~74) which is below season scoring averages (~77), systematically depressing 3PM by ~9%. This is correct for matchup-specific projections but looks low vs season averages.

---

## PROJ TO — Deviation: 1.00x (identical)

**What changed:** Nothing. The turnover formula is unchanged from the Excel model.

`PROJ TO = PACE * (%Poss/100) * ((TORate*0.7 + Opponent.Def-TO%*0.3)/100) * MINS_PROJ/40`

**Confidence: 95/100.** Produces 10.8 TO/team vs NCAA ~11.5 (-6%), which is reasonable given matchup-specific VEGAS_PTS. The formula is clean, well-calibrated, and correlations are correct (TO-PTS r=0.81, TO-AST r=0.72). Not 100% because it lacks a team-level anchor and the 70/30 blend weights are not empirically calibrated.

---

## PROJ STL — Deviation: 1.13x

**What changed:** Two changes:
1. Fixed the opponent adjustment from additive blending (Stl% ~2% + Off-TO% ~17%, incompatible scales) to multiplicative adjustment
2. Added top-down team target: `blend(Team.Def-TO% * 0.75, Opponent.Off-TO% * 0.25) * PACE * 0.66`

The Excel model computes steals bottom-up: `Stl% * mins_frac * PACE`. Individual KenPom Stl% values only reconstruct ~84% of actual team steals. Our model uses team-level turnover-forcing data to set the correct team total, then distributes to players proportionally by their bottom-up weights.

**Team-level deviation:** 1.13x on EXISTING_MODEL data. On full data: 7.48 STL/team vs NCAA average ~7.5 (-0.3% gap). The old model produced 6.4 STL/team (-14% gap).

**Why this is better:** The 84% reconstruction gap is a known property of KenPom individual Stl% — analogous to how individual DR%/OR% don't fully reconstruct team rebounds. The Excel model simply accepts this undercount. Our top-down approach uses `Def-TO%` (team's ability to force turnovers) multiplied by the steal-to-turnover rate (0.66, matching the NCAA average of ~66% of forced turnovers being steals) to derive a correct team target.

**Confidence: 82/100.** The 0.66 steal-to-turnover rate is well-supported by NCAA data. The team target approach is consistent with how PTS (VEGAS_PTS), 3PM (3P FGM), REB (team OR%/DR%), and BLK (Team.Def.Blk%) are now handled. Confidence is not higher because the 0.66 rate is a league-wide constant — in reality, pressing teams likely have a higher steal/TO ratio than shot-blocking teams.

---

## PROJ BLK — Deviation: 0.60x

**What changed:** Two changes:
1. **Bug fix:** Changed from `OPP_2P_FGA + OPP_3P_FGA` (all opponent FGA) to `OPP_2P_FGA` only. KenPom defines Blk% as `Blocks / (%Min * Opponent_2P_Attempts)` — the denominator is 2P FGA only because virtually all blocks occur on 2-point shots.
2. Added top-down team target: `blend(Team.Def.Blk% * 0.75, Opponent.Def.Blk% * 0.25) * OPP_2P_FGA`

**Team-level deviation:** 0.60x on EXISTING_MODEL data. On full data: 3.33 BLK/team vs NCAA ~3.7 (-10% gap). The old model produced 5.5 BLK/team (+45% over NCAA average).

**Validation against known teams:**
- Alabama: Model=4.53, Actual=4.51 (within 0.01)
- Duke: Model=3.00, Actual=3.92
- UConn: Model=3.68, Actual=6.20
- MAE across 8 known teams: 1.23

**Why this is better:** The Excel model's use of total FGA (2P+3P) in the denominator is a clear bug. KenPom's Blk% definition explicitly uses only 2P FGA — this is documented on the KenPom blog and makes physical sense (you don't block 3-point shots at any meaningful rate). The Excel model overestimated blocks by 45% as a direct result. The top-down anchor using Team.Def.Blk% adds consistency with how all other stats are now handled and compresses the CV from 0.301 to 0.240.

**Confidence: 88/100.** The 2P-FGA-only fix is provably correct from KenPom's definition. The 0.60x deviation looks dramatic but the Excel model was 45% too high — moving from +45% error to -10% error is a clear improvement. Confidence is not higher because the model still underestimates by ~10%, likely due to matchup-specific OPP_2P_FGA being lower than season averages (driven by VEGAS_PTS).

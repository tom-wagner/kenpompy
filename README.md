# KenPom Pipeline

This repo now supports running the pipeline as discrete artifact-producing stages instead of only through `scripts/full_pipeline.py`.

Recommended flow:
1. Scrape KenPom and save the raw workbook/CSV.
2. Run xAI minutes projections and save the minutes-enriched workbook/CSV/XLSX.
3. Run the projection model and save the model workbook.
4. Parse Unabated into a reusable JSON artifact.
5. Match projections to lines, rank EV, and optionally xAI-grade bets.

`scripts/full_pipeline.py` still exists as a convenience wrapper, but the preferred operator path is the staged flow below.

## Stage 1: Scrape KenPom

Scrape all scheduled teams for a date and save the base workbook plus player CSV.

```bash
python3 scripts/stage_scrape_kenpom.py \
  --date 03-10-2026
```

Smoke run:

```bash
python3 scripts/stage_scrape_kenpom.py \
  --date 03-10-2026 \
  --top_n 2
```

Outputs:
- `outputs/kenpom/*.xlsx`
- `outputs/kenpom/*.csv`

## Stage 2: Run Minutes Projections

Take a KenPom workbook, hydrate lineup context, run team-level xAI minutes, optionally run follow-up minutes reviews, and save enriched artifacts.

```bash
python3 scripts/stage_project_minutes.py \
  --kenpom-file "outputs/kenpom/Tue Mar 10 07:29:39 PM.xlsx" \
  --run_x_ai_follow_up_minutes
```

Optional confidence threshold:

```bash
python3 scripts/stage_project_minutes.py \
  --kenpom-file "outputs/kenpom/Tue Mar 10 07:29:39 PM.xlsx" \
  --run_x_ai_follow_up_minutes \
  --x_ai_follow_up_confidence_threshold 0.94
```

Outputs:
- `outputs/pipeline/minutes/*_player_stats.csv`
- `outputs/pipeline/minutes/*_player_stats.xlsx`
- `outputs/pipeline/minutes/*_workbook.xlsx`

Use the saved minutes workbook as the input to the model stage.

## Stage 3: Run The Projection Model

Run the player projection model from a minutes-stage workbook.

```bash
python3 scripts/stage_run_model.py \
  --input-workbook "outputs/pipeline/minutes/full_pipeline_minutes_existing_kp_Tue_Mar_10_07-29-39_PM_saved_2026-03-10_195518_workbook.xlsx"
```

Optional explicit output:

```bash
python3 scripts/stage_run_model.py \
  --input-workbook "outputs/pipeline/minutes/full_pipeline_minutes_existing_kp_Tue_Mar_10_07-29-39_PM_saved_2026-03-10_195518_workbook.xlsx" \
  --output "outputs/model_projections/projections/my_model_run.xlsx"
```

Outputs:
- `outputs/model_projections/projections/*.xlsx`

## Stage 4: Parse Unabated

Parse the raw Unabated payload into a cleaner reusable JSON object.

```bash
python3 scripts/parse_unabated.py \
  --input inputs/unabated/unabatedResponseV7.json \
  --date 03-10-2026 \
  --output outputs/unabated/unabatedResponseV7_parsed_03-10-2026.json
```

If you skip this step, the bet-grading stage can also read the raw payload directly.

## Stage 5: Match Lines And Grade Bets

Take the model output, merge back in minutes/player context from the minutes-stage workbook, match against Unabated, compute EV, and optionally run xAI bet grading.

Without xAI bet grading:

```bash
python3 scripts/stage_grade_bets.py \
  --projections "outputs/model_projections/projections/my_model_run.xlsx" \
  --player-stats "outputs/pipeline/minutes/full_pipeline_minutes_existing_kp_Tue_Mar_10_07-29-39_PM_saved_2026-03-10_195518_workbook.xlsx" \
  --unabated outputs/unabated/unabatedResponseV7_parsed_03-10-2026.json \
  --sim-output unabated_sim_output.json \
  --output outputs/pipeline/final_ranked_bets.csv
```

With xAI bet grading:

```bash
python3 scripts/stage_grade_bets.py \
  --projections "outputs/model_projections/projections/my_model_run.xlsx" \
  --player-stats "outputs/pipeline/minutes/full_pipeline_minutes_existing_kp_Tue_Mar_10_07-29-39_PM_saved_2026-03-10_195518_workbook.xlsx" \
  --unabated inputs/unabated/unabatedResponseV7.json \
  --sim-output unabated_sim_output.json \
  --date 03-10-2026 \
  --run_x_ai_bet_grading_workflow \
  --x_ai_ev_hurdle 1.05 \
  --output outputs/pipeline/final_ranked_bets_with_xai.csv
```

Outputs:
- `outputs/pipeline/*.csv`

## End-To-End Example

```bash
python3 scripts/stage_scrape_kenpom.py --date 03-10-2026
python3 scripts/stage_project_minutes.py --kenpom-file "outputs/kenpom/Tue Mar 10 07:29:39 PM.xlsx" --run_x_ai_follow_up_minutes
python3 scripts/stage_run_model.py --input-workbook "outputs/pipeline/minutes/full_pipeline_minutes_existing_kp_Tue_Mar_10_07-29-39_PM_saved_2026-03-10_195518_workbook.xlsx"
python3 scripts/parse_unabated.py --input inputs/unabated/unabatedResponseV7.json --date 03-10-2026 --output outputs/unabated/unabatedResponseV7_parsed_03-10-2026.json
python3 scripts/stage_grade_bets.py --projections "outputs/model_projections/projections/pipeline_existing_kp_full_pipeline_minutes_existing_kp_Tue_Mar_10_07-29-39_PM_saved_2026-03-10_195518_workbook_saved_2026-03-10_195518.xlsx" --player-stats "outputs/pipeline/minutes/full_pipeline_minutes_existing_kp_Tue_Mar_10_07-29-39_PM_saved_2026-03-10_195518_workbook.xlsx" --unabated outputs/unabated/unabatedResponseV7_parsed_03-10-2026.json --sim-output unabated_sim_output.json --run_x_ai_bet_grading_workflow --x_ai_ev_hurdle 1.05 --output outputs/pipeline/final_ranked_bets_with_xai.csv
```

## Convenience Wrapper

If you still want the one-shot entry point, `scripts/full_pipeline.py` remains available. It is now best thought of as a wrapper around the same stages rather than the only supported workflow.

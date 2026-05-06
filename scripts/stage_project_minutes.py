#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

from scripts import full_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 2: run team-level xAI minutes projections and save artifacts")
    parser.add_argument("--kenpom-file", required=True, help="KenPom workbook with a PlayerStats sheet")
    parser.add_argument(
        "--run_x_ai_follow_up_minutes",
        action="store_true",
        help="Run follow-up xAI minutes reviews for low-confidence players",
    )
    parser.add_argument(
        "--x_ai_follow_up_confidence_threshold",
        type=float,
        default=0.94,
        help="Confidence threshold at or below which follow-up minutes reviews run",
    )
    parser.add_argument(
        "--cache-date",
        default=None,
        help="Optional minutes-cache date in MM-DD-YYYY format; defaults to the workbook date when it can be inferred",
    )
    parser.add_argument("--output-stem", default=None, help="Optional explicit stem for saved artifacts")
    args = parser.parse_args()

    full_pipeline.configure_logging()
    full_pipeline.logger.info(
        "Stage minutes start: kenpom_file=%s run_follow_up=%s threshold=%.2f",
        args.kenpom_file,
        args.run_x_ai_follow_up_minutes,
        args.x_ai_follow_up_confidence_threshold,
    )
    main_module = full_pipeline.load_module(REPO_ROOT / "scripts" / "main.py", "stage_minutes_main_module")
    kenpom_file = Path(args.kenpom_file).expanduser().resolve()
    if not kenpom_file.exists():
        raise SystemExit(f"KenPom workbook not found: {kenpom_file}")
    minutes_cache_date = full_pipeline.infer_minutes_stage_cache_date(
        explicit_date=args.cache_date,
        source_workbook=kenpom_file,
    )
    minutes_cache, minutes_cache_path = full_pipeline.load_minutes_stage_cache(minutes_cache_date)
    full_pipeline.logger.info(
        "Minutes cache ready: date=%s path=%s existing_teams=%s",
        minutes_cache_date,
        minutes_cache_path,
        len(minutes_cache.get("teams", {})),
    )

    player_df = main_module.prepare_player_df(full_pipeline.load_player_stats_from_workbook(kenpom_file))
    full_pipeline.validate_minutes_player_df(
        player_df,
        stage_label="Minutes stage input",
        require_minutes_projection=False,
    )
    team_statuses = full_pipeline.group_team_statuses_from_player_df(player_df)
    full_pipeline.hydrate_recent_lineup_contexts_for_workbook(main_module, team_statuses)
    full_pipeline.run_parallel_team_minutes_workflow(
        main_module=main_module,
        team_statuses=team_statuses,
        run_follow_up_minutes=args.run_x_ai_follow_up_minutes,
        follow_up_threshold=args.x_ai_follow_up_confidence_threshold,
        minutes_cache=minutes_cache,
    )
    full_pipeline.write_minutes_stage_cache(minutes_cache_date, minutes_cache)

    player_df_with_minutes = full_pipeline.combine_team_status_frames(team_statuses)
    full_pipeline.validate_minutes_player_df(player_df_with_minutes, stage_label="Minutes stage output")
    output_stem = args.output_stem or f"minutes_{full_pipeline.build_output_stem('existing', kenpom_file)}"
    csv_path, xlsx_path, workbook_path = full_pipeline.save_minutes_stage_outputs(
        source_workbook=kenpom_file,
        player_df=player_df_with_minutes,
        output_stem=output_stem,
    )
    full_pipeline.validate_path_exists(csv_path, "Minutes player CSV")
    full_pipeline.validate_path_exists(xlsx_path, "Minutes player XLSX")
    full_pipeline.validate_path_exists(workbook_path, "Minutes workbook")
    full_pipeline.logger.info(
        "Stage minutes complete: csv=%s xlsx=%s workbook=%s teams=%s players=%s",
        csv_path,
        xlsx_path,
        workbook_path,
        player_df_with_minutes["Team"].nunique() if "Team" in player_df_with_minutes.columns else 0,
        len(player_df_with_minutes),
    )

    print(f"Minutes player CSV: {csv_path}")
    print(f"Minutes player XLSX: {xlsx_path}")
    print(f"Minutes workbook: {workbook_path}")
    print(f"Teams processed: {player_df_with_minutes['Team'].nunique() if 'Team' in player_df_with_minutes.columns else 0}")
    print(f"Players processed: {len(player_df_with_minutes)}")
    print("Next command:")
    print(
        "python scripts/stage_run_model.py "
        f'--input-workbook "{workbook_path}"'
    )


if __name__ == "__main__":
    main()

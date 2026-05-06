#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

import argparse

from scripts import full_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 1: scrape KenPom data and save the workbook/CSV artifacts")
    parser.add_argument("--date", required=True, help="Date in MM-DD-YYYY format")
    parser.add_argument("--top_n", type=int, default=None, help="Optional team limit for smoke runs")
    args = parser.parse_args()

    full_pipeline.configure_logging()
    full_pipeline.logger.info("Stage scrape start: date=%s top_n=%s", args.date, args.top_n)
    main_module = full_pipeline.load_module(REPO_ROOT / "scripts" / "main.py", "stage_scrape_main_module")

    scrape_bundle = main_module.scrape_kenpom_frames(args.date, top_n=args.top_n)
    player_df = scrape_bundle["player_df"]
    full_pipeline.validate_minutes_player_df(
        player_df,
        stage_label="KenPom scrape stage",
        require_minutes_projection=False,
    )

    workbook_path, csv_path = main_module.save_kenpom_outputs(
        args.date,
        scrape_bundle["four_factors"],
        scrape_bundle["team_stats"],
        scrape_bundle["points_dist"],
        player_df,
    )
    full_pipeline.validate_path_exists(workbook_path, "KenPom workbook")
    full_pipeline.validate_path_exists(csv_path, "KenPom player CSV")
    full_pipeline.logger.info(
        "Stage scrape complete: workbook=%s csv=%s teams=%s players=%s",
        workbook_path,
        csv_path,
        player_df["Team"].nunique() if not player_df.empty and "Team" in player_df.columns else 0,
        len(player_df),
    )

    print(f"KenPom workbook: {workbook_path}")
    print(f"KenPom player CSV: {csv_path}")
    print(f"Scraped teams: {player_df['Team'].nunique() if not player_df.empty and 'Team' in player_df.columns else 0}")
    print(f"Scraped players: {len(player_df)}")
    print("Next command:")
    print(
        "python scripts/stage_project_minutes.py "
        f'--kenpom-file "{workbook_path}" '
        "--run_x_ai_follow_up_minutes "
        "--x_ai_follow_up_confidence_threshold 0.94"
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

from scripts import full_pipeline
from scripts import projection_model


def infer_cli_date_from_path(path: Path) -> str:
    text = path.name

    iso_match = re.search(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)", text)
    if iso_match:
        year, month, day = iso_match.groups()
        return f"{month}-{day}-{year}"

    cli_match = re.search(r"(?<!\d)(\d{2})-(\d{2})-(\d{4})(?!\d)", text)
    if cli_match:
        month, day, year = cli_match.groups()
        return f"{month}-{day}-{year}"

    raise SystemExit(f"Could not infer MM-DD-YYYY date from input workbook name: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 3: run the projection model from a PlayerStats workbook")
    parser.add_argument("--input-workbook", required=True, help="Workbook with a PlayerStats sheet")
    parser.add_argument("--output", default=None, help="Optional output workbook path")
    args = parser.parse_args()

    input_workbook = Path(args.input_workbook).expanduser().resolve()
    if not input_workbook.exists():
        raise SystemExit(f"Input workbook not found: {input_workbook}")

    full_pipeline.configure_logging()
    full_pipeline.logger.info("Stage model start: input_workbook=%s output=%s", input_workbook, args.output)
    player_stats = projection_model.read_player_stats(str(input_workbook), sheet="PlayerStats")
    df_result = projection_model.run_projections(player_stats)
    output_path = Path(args.output).expanduser().resolve() if args.output else (
        full_pipeline.PROJECTIONS_OUTPUT_DIR / f"pipeline_{full_pipeline.build_output_stem('existing', input_workbook)}.xlsx"
    )
    projection_model.write_output(df_result, output_path)
    full_pipeline.validate_projection_output(df_result, output_path)
    full_pipeline.logger.info(
        "Stage model complete: output=%s projected_rows=%s",
        output_path,
        len(df_result),
    )
    cli_date = infer_cli_date_from_path(input_workbook)

    print(f"Projection workbook: {output_path}")
    print(f"Projected rows: {len(df_result)}")
    print("Next commands:")
    print(f'curl "https://content.unabated.com/markets/v2/league/4/propodds.json" -o "inputs/unabated/unabatedResponse_{cli_date}.json"')
    print(
        "python scripts/parse_unabated.py "
        f'--input "inputs/unabated/unabatedResponse_{cli_date}.json" '
        f"--date {cli_date} "
        f'--output "outputs/unabated/unabatedResponse_parsed_{cli_date}.json"'
    )
    print(
        "python scripts/stage_grade_bets.py "
        f'--projections "{output_path}" '
        f'--player-stats "{input_workbook}" '
        '--sim-output "unabated_sim_output.json" '
        f"--date {cli_date} "
        "--run_x_ai_bet_grading_workflow "
        "--x_ai_ev_hurdle 1.05"
    )


if __name__ == "__main__":
    main()

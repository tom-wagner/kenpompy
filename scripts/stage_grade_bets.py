#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

from scripts import full_pipeline
from scripts import unabated_real_lines_eval


UNABATED_PROPODDS_URL = "https://content.unabated.com/markets/v2/league/4/propodds.json"
UNABATED_FETCH_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,la;q=0.8",
    "origin": "https://unabated.com",
    "priority": "u=1, i",
    "referer": "https://unabated.com/",
    "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
    "accept-encoding": "gzip",
}
MAX_RECENT_UNABATED_INPUT_FILES = 3
MAX_RECENT_STAGE_GRADE_CSV_FILES = 10
EXISTING_ANALYSIS_REQUIRED_COLUMNS = {"Player", "team", "stat", "line", "manualRtg", "manualNotes"}
EXISTING_ANALYSIS_MANUAL_COLUMNS = ("manualRtg", "manualNotes")
EXISTING_ANALYSIS_CONTEXT_COLUMNS = (
    "opponent",
    "conf",
    "last_5_games",
    "last_5_games_fouls",
    "kenpom_player_url",
)
EXISTING_ANALYSIS_CARRY_FORWARD_COLUMNS = EXISTING_ANALYSIS_MANUAL_COLUMNS + EXISTING_ANALYSIS_CONTEXT_COLUMNS


def prune_unabated_input_directory(
    directory: Path | None = None,
    max_files: int = MAX_RECENT_UNABATED_INPUT_FILES,
) -> None:
    target_dir = directory or (REPO_ROOT / "inputs" / "unabated")
    target_dir.mkdir(parents=True, exist_ok=True)
    files = [path for path in target_dir.iterdir() if path.is_file()]
    files.sort(key=lambda path: (path.stat().st_mtime, path.name), reverse=True)
    stale_files = files[max_files:]
    for stale_file in stale_files:
        try:
            stale_file.unlink()
        except OSError as exc:
            full_pipeline.logger.warning("Failed to remove old Unabated input %s: %s", stale_file, exc)
    if stale_files:
        full_pipeline.logger.info(
            "Pruned %s old Unabated input files from %s; kept newest %s",
            len(stale_files),
            target_dir,
            min(len(files), max_files),
        )


def is_analysis_csv(path: Path) -> bool:
    if not path.is_file() or path.suffix.lower() != ".csv":
        return False

    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            headers = next(reader, [])
    except (OSError, UnicodeDecodeError, csv.Error):
        return False

    return EXISTING_ANALYSIS_REQUIRED_COLUMNS.issubset(set(headers))


def find_latest_existing_analysis_csv(directory: Path | None = None) -> Path | None:
    target_dir = directory or full_pipeline.PIPELINE_OUTPUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    candidates = [path for path in target_dir.glob("*.csv") if is_analysis_csv(path)]
    if not candidates:
        return None
    candidates.sort(key=lambda path: (path.stat().st_mtime, path.name), reverse=True)
    return candidates[0]


def prune_stage_grade_output_directory(
    directory: Path | None = None,
    max_files: int = MAX_RECENT_STAGE_GRADE_CSV_FILES,
) -> None:
    target_dir = directory or full_pipeline.PIPELINE_OUTPUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    files = [path for path in target_dir.glob("*.csv") if path.is_file()]
    files.sort(key=lambda path: (path.stat().st_mtime, path.name), reverse=True)
    stale_files = files[max_files:]
    for stale_file in stale_files:
        try:
            stale_file.unlink()
        except OSError as exc:
            full_pipeline.logger.warning("Failed to remove old pipeline CSV %s: %s", stale_file, exc)
    if stale_files:
        full_pipeline.logger.info(
            "Pruned %s old pipeline CSV files from %s; kept newest %s",
            len(stale_files),
            target_dir,
            min(len(files), max_files),
        )


def fetch_unabated_payload() -> Path:
    output_dir = REPO_ROOT / "inputs" / "unabated"
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"unabatedResponse_fetch_{timestamp}.json"
    request = Request(f"{UNABATED_PROPODDS_URL}?v={uuid4()}", headers=UNABATED_FETCH_HEADERS)

    try:
        with urlopen(request, timeout=30) as response:
            raw_bytes = response.read()
            encoding = (response.headers.get("Content-Encoding") or "").lower()
    except HTTPError as exc:
        raise SystemExit(f"Failed to fetch Unabated input: HTTP {exc.code}") from exc
    except URLError as exc:
        raise SystemExit(f"Failed to fetch Unabated input: {exc.reason}") from exc

    if encoding == "gzip":
        raw_bytes = gzip.GzipFile(fileobj=BytesIO(raw_bytes)).read()

    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SystemExit("Fetched Unabated payload was not valid JSON") from exc

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    full_pipeline.logger.info("Fetched Unabated payload to %s", output_path)
    return output_path


def analysis_match_key(player_name: object, team: object, stat_category: object) -> str:
    return "-".join(str(part).strip().lower() for part in (player_name, team, stat_category))


def analysis_player_match_key(player_name: object, team: object) -> str:
    return "-".join(str(part).strip().lower() for part in (player_name, team))


def normalize_line_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        try:
            numeric = float(text)
        except ValueError:
            return text
    else:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value).strip()

    if math.isnan(numeric):
        return ""
    if numeric.is_integer():
        return str(int(numeric))
    return str(numeric).rstrip("0").rstrip(".")


def normalize_existing_analysis_value(value: object) -> object:
    if pd.isna(value):
        return ""
    if isinstance(value, str):
        return value.strip()
    return value


def coerce_existing_value_for_output_column(series: pd.Series, value: object) -> object:
    if value == "":
        return ""
    if pd.api.types.is_string_dtype(series.dtype) and not isinstance(value, str):
        return str(value)
    return value


def output_value_is_missing(value: object) -> bool:
    if pd.isna(value):
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def merge_existing_analysis(output_df: pd.DataFrame, existing_analysis_path: Path | None) -> pd.DataFrame:
    if existing_analysis_path is None:
        return output_df

    analysis_df = pd.read_csv(existing_analysis_path)
    missing = sorted(EXISTING_ANALYSIS_REQUIRED_COLUMNS - set(analysis_df.columns))
    if missing:
        raise SystemExit(
            f"Existing analysis missing required columns: {', '.join(missing)}: {existing_analysis_path}"
        )

    latest_by_key: dict[str, dict[str, object]] = {}
    latest_context_by_player: dict[str, dict[str, object]] = {}
    for _, row in analysis_df.iterrows():
        key = analysis_match_key(row.get("Player"), row.get("team"), row.get("stat"))
        player_key = analysis_player_match_key(row.get("Player"), row.get("team"))
        existing_values = {"line": normalize_line_value(row.get("line"))}
        for column in EXISTING_ANALYSIS_CARRY_FORWARD_COLUMNS:
            existing_values[column] = normalize_existing_analysis_value(row.get(column))
        existing_values["manualRtg"] = str(existing_values["manualRtg"]).strip()
        existing_values["manualNotes"] = str(existing_values["manualNotes"]).strip()
        latest_by_key[key] = existing_values
        latest_context_by_player[player_key] = {
            column: existing_values[column] for column in EXISTING_ANALYSIS_CONTEXT_COLUMNS
        }

    merged = output_df.copy()
    for column in EXISTING_ANALYSIS_CARRY_FORWARD_COLUMNS:
        if column not in merged.columns:
            merged[column] = ""
    copied_rows = 0
    line_changed_rows = 0
    for index, row in merged.iterrows():
        key = analysis_match_key(row.get("Player"), row.get("team"), row.get("stat"))
        player_key = analysis_player_match_key(row.get("Player"), row.get("team"))
        existing = latest_by_key.get(key)
        existing_context = latest_context_by_player.get(player_key)

        if existing_context is not None:
            for column in EXISTING_ANALYSIS_CONTEXT_COLUMNS:
                if column in analysis_df.columns and output_value_is_missing(row.get(column)):
                    merged.at[index, column] = coerce_existing_value_for_output_column(
                        merged[column], existing_context[column]
                    )

        if existing is None:
            continue

        for column in EXISTING_ANALYSIS_MANUAL_COLUMNS:
            merged.at[index, column] = coerce_existing_value_for_output_column(merged[column], existing[column])
        notes = existing["manualNotes"]
        old_line = existing["line"]
        new_line = normalize_line_value(row.get("line"))
        if old_line and new_line and old_line != new_line:
            prefix = f"LINE_CHANGED_{old_line}_TO_{new_line}:"
            notes = f"{prefix} {notes}".strip() if notes else prefix
            line_changed_rows += 1
        merged.at[index, "manualNotes"] = notes
        copied_rows += 1

    full_pipeline.logger.info(
        "Merged existing analysis from %s: matched_rows=%s line_changed_rows=%s",
        existing_analysis_path,
        copied_rows,
        line_changed_rows,
    )
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 4/5: parse lines, match model output, and optionally xAI-grade bets")
    parser.add_argument("--projections", required=True, help="Projection workbook or CSV")
    parser.add_argument("--player-stats", default=None, help="Minutes-stage workbook or CSV to enrich projections with context columns")
    parser.add_argument("--unabated", default=None, help="Raw or parsed Unabated payload; if omitted, fetch and save a fresh raw payload")
    parser.add_argument("--sim-output", required=True, help="Simulation lookup JSON")
    parser.add_argument("--existing_analysis", default=None, help="CSV of prior manual analysis to copy into manualRtg/manualNotes")
    parser.add_argument("--date", default=None, help="Optional event date filter in MM-DD-YYYY format")
    parser.add_argument(
        "--run_x_ai_bet_grading_workflow",
        action="store_true",
        help="Grade qualifying bets with xAI after EV ranking",
    )
    parser.add_argument("--x_ai_ev_hurdle", type=float, default=1.2, help="Minimum expected_value required before calling xAI")
    parser.add_argument("--output", default=None, help="Output CSV path")
    args = parser.parse_args()

    full_pipeline.configure_logging()
    full_pipeline.logger.info(
        "Stage grade start: projections=%s player_stats=%s unabated=%s sim_output=%s existing_analysis=%s run_xai=%s ev_hurdle=%.2f output=%s",
        args.projections,
        args.player_stats,
        args.unabated,
        args.sim_output,
        args.existing_analysis,
        args.run_x_ai_bet_grading_workflow,
        args.x_ai_ev_hurdle,
        args.output,
    )
    projections_path = Path(args.projections).expanduser().resolve()
    sim_path = Path(args.sim_output).expanduser().resolve()
    if not projections_path.exists():
        raise SystemExit(f"Projections input not found: {projections_path}")
    if not sim_path.exists():
        raise SystemExit(f"Simulation output not found: {sim_path}")
    existing_analysis_path = None
    if args.existing_analysis:
        existing_analysis_path = Path(args.existing_analysis).expanduser().resolve()
        if not existing_analysis_path.exists():
            raise SystemExit(f"Existing analysis input not found: {existing_analysis_path}")
    else:
        existing_analysis_path = find_latest_existing_analysis_csv()
        if existing_analysis_path is not None:
            full_pipeline.logger.info(
                "Auto-selected existing analysis CSV for manual notes carry-forward: %s",
                existing_analysis_path,
            )

    if args.unabated:
        unabated_path = Path(args.unabated).expanduser().resolve()
        if not unabated_path.exists():
            raise SystemExit(f"Unabated input not found: {unabated_path}")
    else:
        unabated_path = fetch_unabated_payload()

    target_date = None
    if args.date:
        parser_module = full_pipeline.load_module(REPO_ROOT / "scripts" / "parse_unabated.py", "stage_grade_parse_unabated_module")
        target_date = parser_module.normalize_cli_date(args.date)

    try:
        projections = unabated_real_lines_eval.read_projections(projections_path)
        player_stats_df = None
        if args.player_stats:
            player_stats_path = Path(args.player_stats).expanduser().resolve()
            if not player_stats_path.exists():
                raise SystemExit(f"Player stats input not found: {player_stats_path}")
            player_stats_df = unabated_real_lines_eval.read_projections(player_stats_path)
        projections = full_pipeline.merge_player_stats_context(projections, player_stats_df)

        markets = full_pipeline.parse_unabated_input(unabated_path, target_date=target_date)
        sim_data = full_pipeline.load_sim_data(sim_path)
        output_df = full_pipeline.build_rows(
            projections,
            markets,
            sim_data,
            "existing",
            projections_path,
            call_x_ai=args.run_x_ai_bet_grading_workflow,
            x_ai_ev_hurdle=args.x_ai_ev_hurdle,
            execute_xai_requests=not args.run_x_ai_bet_grading_workflow,
        )
        if args.run_x_ai_bet_grading_workflow:
            workload = full_pipeline.estimate_xai_bet_grading_workload(output_df)
            full_pipeline.logger.info(
                "xAI bet grading preview: total_rows=%s eligible_bets=%s cache_hits=%s api_calls_needed=%s unique_eligible_cache_keys=%s",
                workload["total_rows"],
                workload["eligible_bets"],
                workload["cache_hits"],
                workload["api_calls_needed"],
                workload["unique_eligible_cache_keys"],
            )
            proceed_with_api_calls = full_pipeline.prompt_to_proceed_with_xai_calls(
                workload["api_calls_needed"],
                workload["cache_hits"],
            )
            output_df = full_pipeline.run_parallel_bet_grading(output_df, allow_api_calls=proceed_with_api_calls)
        output_df = merge_existing_analysis(output_df, existing_analysis_path)
        full_pipeline.validate_final_output_df(output_df)

        output_path = Path(args.output).expanduser().resolve() if args.output else (
            full_pipeline.PIPELINE_OUTPUT_DIR
            / f"{datetime.now().strftime('%Y-%m-%d_%H%M%S')}_stage_grade_{projections_path.stem}.csv"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_df.to_csv(output_path, index=False)
        full_pipeline.validate_final_output_df(output_df, output_path)
        full_pipeline.logger.info(
            "Stage grade complete: output=%s rows=%s xai_graded=%s",
            output_path,
            len(output_df),
            int(output_df["xAiScore"].notna().sum()) if "xAiScore" in output_df.columns else 0,
        )

        print(f"Projection input: {projections_path}")
        if args.player_stats:
            print(f"Player stats context: {Path(args.player_stats).expanduser().resolve()}")
        print(f"Output rows: {len(output_df)}")
        print(f"CSV written to: {output_path}")
        print("Next command:")
        print("# Final stage complete; no further pipeline stage.")
    finally:
        prune_stage_grade_output_directory()
        prune_unabated_input_directory()


if __name__ == "__main__":
    main()

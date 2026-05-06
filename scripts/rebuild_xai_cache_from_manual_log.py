#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

from scripts import full_pipeline


RAW_RESPONSE_RE = re.compile(
    r"^\S+\s+\S+\s+INFO xAI scoring raw response for "
    r"(?P<player>.+?) \| (?P<team>.+?) \| (?P<stat>.+?) \| (?P<bet_side>over|under) \| EV=(?P<ev>\d+(?:\.\d+)?):$"
)


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def stage_grade_csv_paths() -> list[Path]:
    candidates = sorted(
        (
            path
            for path in (REPO_ROOT / "outputs" / "pipeline").glob("*.csv")
            if "stage_grade" in path.name
        ),
        key=lambda path: (path.stat().st_mtime, path.name),
        reverse=True,
    )
    if not candidates:
        raise SystemExit("No stage-grade CSV files found under outputs/pipeline")
    return candidates


def parse_manual_log(log_path: Path) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    entries: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    lines = log_path.read_text().splitlines()
    idx = 0

    while idx < len(lines):
        match = RAW_RESPONSE_RE.match(lines[idx])
        if not match:
            idx += 1
            continue

        payload_line = lines[idx + 1].strip() if idx + 1 < len(lines) else ""
        try:
            payload = json.loads(payload_line)
        except json.JSONDecodeError:
            idx += 1
            continue

        if not isinstance(payload, dict):
            idx += 1
            continue

        key = (
            normalize_text(match.group("player")),
            normalize_text(match.group("team")),
            normalize_text(match.group("stat")),
            normalize_text(match.group("bet_side")),
        )
        entries[key] = payload
        idx += 2

    return entries


def load_stage_grade_rows(csv_paths: list[Path]) -> pd.DataFrame:
    required = {"Player", "team", "stat", "bet_side", "line"}
    frames: list[pd.DataFrame] = []

    for csv_path in csv_paths:
        df = pd.read_csv(csv_path)
        missing = required - set(df.columns)
        if missing:
            raise SystemExit(f"Stage-grade CSV missing required columns {sorted(missing)}: {csv_path}")
        subset = df[list(required)].copy()
        subset["_source_csv"] = str(csv_path)
        frames.append(subset)

    if not frames:
        raise SystemExit("No usable stage-grade CSV rows found")
    return pd.concat(frames, ignore_index=True)


def rebuild_cache(log_entries: dict[tuple[str, str, str, str], dict[str, Any]], stage_grade_df: pd.DataFrame) -> tuple[dict[str, Any], list[tuple[str, str, str, str]]]:
    cache_entries: dict[str, Any] = {}
    unmatched: list[tuple[str, str, str, str]] = []

    row_lookup = {
        (
            normalize_text(row.Player),
            normalize_text(row.team),
            normalize_text(row.stat),
            normalize_text(row.bet_side),
        ): row
        for row in stage_grade_df.itertuples(index=False)
    }

    for key, payload in log_entries.items():
        row = row_lookup.get(key)
        if row is None:
            unmatched.append(key)
            continue
        cache_key = full_pipeline.build_xai_bet_cache_key(
            normalize_text(row.Player),
            normalize_text(row.team),
            normalize_text(row.stat),
            row.line,
        )
        cache_entries[cache_key] = payload

    return cache_entries, unmatched


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild xAI bet cache snapshots from a manual xAI log file")
    parser.add_argument("--manual-log", required=True, help="Path to the flat manual xAI log file")
    parser.add_argument("--stage-grade-csv", default=None, help="Optional single stage-grade CSV to limit matching scope")
    parser.add_argument("--output", default=None, help="Optional explicit JSON output path")
    args = parser.parse_args()

    manual_log = Path(args.manual_log).expanduser().resolve()
    if not manual_log.exists():
        raise SystemExit(f"Manual log not found: {manual_log}")

    if args.stage_grade_csv:
        selected_stage_grade_csv_paths = [Path(args.stage_grade_csv).expanduser().resolve()]
        if not selected_stage_grade_csv_paths[0].exists():
            raise SystemExit(f"Stage-grade CSV not found: {selected_stage_grade_csv_paths[0]}")
    else:
        selected_stage_grade_csv_paths = stage_grade_csv_paths()

    log_entries = parse_manual_log(manual_log)
    stage_grade_df = load_stage_grade_rows(selected_stage_grade_csv_paths)
    cache_entries, unmatched = rebuild_cache(log_entries, stage_grade_df)

    output_path = Path(args.output).expanduser().resolve() if args.output else (
        full_pipeline.CACHE_OUTPUT_DIR / f"xai_bet_cache_rebuilt_{pd.Timestamp.now().strftime('%Y-%m-%d_%H%M%S')}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(cache_entries, ensure_ascii=False, indent=2, sort_keys=True))

    print(f"Manual log: {manual_log}")
    print(f"Stage-grade CSV count: {len(selected_stage_grade_csv_paths)}")
    print(f"Cache entries written: {len(cache_entries)}")
    print(f"Unmatched log entries: {len(unmatched)}")
    print(f"Output: {output_path}")
    if unmatched:
        print("Sample unmatched:")
        for item in unmatched[:10]:
            print("  " + " | ".join(item))


if __name__ == "__main__":
    main()

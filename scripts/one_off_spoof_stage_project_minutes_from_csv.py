#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

from scripts import full_pipeline


def normalize_text(value: object) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text in {"", "N/A", "nan", "None"} else text


def build_minutes_overlay(minutes_csv: Path) -> pd.DataFrame:
    source_df = pd.read_csv(minutes_csv)
    required_columns = {
        "Player",
        "team",
        "proj_mins",
        "conf",
        "minutes_injury_summary",
        "minutes_confidence_justification",
        "kenpom_player_url",
    }
    missing = sorted(required_columns - set(source_df.columns))
    if missing:
        raise SystemExit(f"Minutes CSV missing required columns: {', '.join(missing)}")

    rows: list[dict[str, object]] = []
    seen: dict[tuple[str, str], dict[str, object]] = {}
    for _, row in source_df.iterrows():
        key = (
            full_pipeline.canonicalize_player_name(row.get("Player")),
            full_pipeline.canonicalize_team_name(row.get("team")),
        )
        candidate = {
            "key": key,
            "MinsProj": pd.to_numeric(row.get("proj_mins"), errors="coerce"),
            "MinsProjConfidence": pd.to_numeric(row.get("conf"), errors="coerce"),
            "MinsProjInjurySummary": normalize_text(row.get("minutes_injury_summary")),
            "MinsProjConfidenceJustification": normalize_text(row.get("minutes_confidence_justification")),
            "KenPomPlayerURL": normalize_text(row.get("kenpom_player_url")),
        }
        existing = seen.get(key)
        if existing is None or (
            pd.notna(candidate["MinsProjConfidence"])
            and (
                pd.isna(existing["MinsProjConfidence"])
                or float(candidate["MinsProjConfidence"]) > float(existing["MinsProjConfidence"])
            )
        ):
            seen[key] = candidate

    rows.extend(seen.values())
    overlay_df = pd.DataFrame(rows)
    if overlay_df.empty:
        raise SystemExit(f"No per-player minutes rows found in {minutes_csv}")
    return overlay_df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-off: spoof stage_project_minutes.py outputs from an existing minutes CSV"
    )
    parser.add_argument("--kenpom-file", required=True, help="KenPom workbook with a PlayerStats sheet")
    parser.add_argument("--minutes-csv", required=True, help="Existing CSV containing per-player minutes fields")
    parser.add_argument("--output-stem", default=None, help="Optional explicit stem for saved artifacts")
    args = parser.parse_args()

    kenpom_file = Path(args.kenpom_file).expanduser().resolve()
    minutes_csv = Path(args.minutes_csv).expanduser().resolve()
    if not kenpom_file.exists():
        raise SystemExit(f"KenPom workbook not found: {kenpom_file}")
    if not minutes_csv.exists():
        raise SystemExit(f"Minutes CSV not found: {minutes_csv}")

    main_module = full_pipeline.load_module(REPO_ROOT / "scripts" / "main.py", "one_off_stage_minutes_main_module")
    player_df = main_module.prepare_player_df(full_pipeline.load_player_stats_from_workbook(kenpom_file))
    if "Team" in player_df.columns:
        player_df = pd.concat(
            [
                main_module.apply_last5_minutes_fallback(team_df.copy())
                for _, team_df in player_df.groupby("Team", sort=False, dropna=False)
            ],
            ignore_index=True,
        )
    full_pipeline.validate_minutes_player_df(
        player_df,
        stage_label="Spoof minutes stage input",
        require_minutes_projection=True,
    )
    for column in ("MinsProjInjurySummary", "MinsProjConfidenceJustification", "KenPomPlayerURL"):
        if column in player_df.columns:
            player_df[column] = player_df[column].astype("object")

    overlay_df = build_minutes_overlay(minutes_csv)
    overlay_map = {
        row["key"]: row
        for row in overlay_df.to_dict(orient="records")
    }
    player_df = player_df.copy()
    player_df["_merge_key"] = player_df.apply(
        lambda row: (
            full_pipeline.canonicalize_player_name(row.get("Name")),
            full_pipeline.canonicalize_team_name(row.get("Team")),
        ),
        axis=1,
    )

    for index, row in player_df.iterrows():
        key = row["_merge_key"]
        overlay_row = overlay_map.get(key)
        if overlay_row is None:
            continue
        player_df.at[index, "MinsProj"] = overlay_row["MinsProj"]
        player_df.at[index, "MinsProjConfidence"] = overlay_row["MinsProjConfidence"]
        player_df.at[index, "MinsProjInjurySummary"] = overlay_row["MinsProjInjurySummary"]
        player_df.at[index, "MinsProjConfidenceJustification"] = overlay_row["MinsProjConfidenceJustification"]
        if overlay_row["KenPomPlayerURL"]:
            player_df.at[index, "KenPomPlayerURL"] = overlay_row["KenPomPlayerURL"]

    player_df = player_df.drop(columns=["_merge_key"])
    full_pipeline.validate_minutes_player_df(player_df, stage_label="Spoof minutes stage output")

    output_stem = args.output_stem or f"minutes_{full_pipeline.build_output_stem('existing', kenpom_file)}"
    csv_path, xlsx_path, workbook_path = full_pipeline.save_minutes_stage_outputs(
        source_workbook=kenpom_file,
        player_df=player_df,
        output_stem=output_stem,
    )

    print(f"Minutes player CSV: {csv_path}")
    print(f"Minutes player XLSX: {xlsx_path}")
    print(f"Minutes workbook: {workbook_path}")
    print(f"Teams processed: {player_df['Team'].nunique() if 'Team' in player_df.columns else 0}")
    print(f"Players processed: {len(player_df)}")
    print("Next command:")
    print(
        "python scripts/stage_run_model.py "
        f'--input-workbook "{workbook_path}"'
    )


if __name__ == "__main__":
    main()

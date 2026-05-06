#!/usr/bin/env python3
"""
Evaluate projection outputs against posted Unabated player-prop lines.

Usage:
    python3 scripts/unabated_real_lines_eval.py --unabated PATH --projections PATH

The script accepts either:
- a raw Unabated payload with ``people``/``odds``/``teams``/``marketSources``, or
- a previously parsed Unabated JSON emitted by ``parse_unabated.py``.

Projection inputs may be:
- a model output workbook with a ``Projections`` sheet,
- a PlayerStats-style workbook with projection columns present, or
- a CSV containing the same columns.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "outputs" / "evals"


def load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FULL_PIPELINE = load_module(REPO_ROOT / "scripts" / "full_pipeline.py", "full_pipeline_eval_module")
PARSE_UNABATED = load_module(REPO_ROOT / "scripts" / "parse_unabated.py", "parse_unabated_eval_module")


def parse_unabated_input(unabated_path: Path, target_date: str | None = None) -> dict[str, Any]:
    payload = json.loads(unabated_path.read_text())
    if "people" in payload and "odds" in payload:
        return PARSE_UNABATED.parse_unabated(payload, target_date=target_date)
    return payload


def read_projections(projections_path: Path) -> pd.DataFrame:
    suffix = projections_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(projections_path)
    if suffix not in {".xlsx", ".xlsm", ".xls"}:
        raise ValueError(f"Unsupported projections file type: {projections_path.suffix}")

    workbook = pd.ExcelFile(projections_path)
    if "Projections" in workbook.sheet_names:
        return pd.read_excel(projections_path, sheet_name="Projections")
    if "PlayerStats" in workbook.sheet_names:
        return pd.read_excel(projections_path, sheet_name="PlayerStats")
    return pd.read_excel(projections_path, sheet_name=workbook.sheet_names[0])


def safe_float(value: Any, default: float = float("nan")) -> float:
    try:
        if value is None:
            return default
        result = float(value)
        if math.isnan(result):
            return default
        return result
    except (TypeError, ValueError):
        return default


def projection_file_label(path: Path) -> str:
    return path.stem


def average_numeric(values: list[Any]) -> float:
    numeric_values = [safe_float(value) for value in values]
    filtered = [value for value in numeric_values if not math.isnan(value)]
    if not filtered:
        return float("nan")
    return sum(filtered) / len(filtered)


def aggregate_market_entry(books: dict[str, Any]) -> tuple[dict[str, Any] | None, str, int]:
    book_rows: list[tuple[str, dict[str, Any]]] = []
    for book_name, entry in books.items():
        if book_name == "avgObj":
            continue
        if not isinstance(entry, dict):
            continue
        line = safe_float(entry.get("line"))
        over = safe_float(entry.get("over"))
        under = safe_float(entry.get("under"))
        if math.isnan(line) and math.isnan(over) and math.isnan(under):
            continue
        book_rows.append((FULL_PIPELINE.market_book_display_name(book_name, entry), entry))

    if not book_rows:
        return None, "", 0

    avg_entry = books.get("avgObj")
    if not isinstance(avg_entry, dict):
        avg_entry = {
            "line": average_numeric([entry.get("line") for _, entry in book_rows]),
            "over": average_numeric([entry.get("over") for _, entry in book_rows]),
            "under": average_numeric([entry.get("under") for _, entry in book_rows]),
        }

    details = " | ".join(
        f"{book_name} {entry.get('line')} O {entry.get('over')}; U {entry.get('under')}"
        for book_name, entry in sorted(book_rows)
    )
    return avg_entry, details, len(book_rows)


def build_eval_rows(projections: pd.DataFrame, markets: dict[str, Any]) -> pd.DataFrame:
    market_lookup = FULL_PIPELINE.build_market_lookup(markets)
    rows: list[dict[str, Any]] = []

    for _, row in projections.iterrows():
        player_name = str(row.get("Name", "")).strip()
        if not player_name:
            continue

        matched = FULL_PIPELINE.resolve_market_player(player_name, row.get("Team"), market_lookup)
        if not matched:
            continue

        market_player_name, player_markets = matched
        for market_stat, books in player_markets.items():
            if str(market_stat).startswith("__"):
                continue
            if not isinstance(books, dict):
                continue

            projection_value = FULL_PIPELINE.projection_value_for_market(row, market_stat)
            if projection_value is None:
                continue

            avg_entry, market_details, book_count = aggregate_market_entry(books)
            if avg_entry is None:
                continue

            line = safe_float(avg_entry.get("line"))
            if math.isnan(line):
                continue

            diff = projection_value - line
            rows.append(
                {
                    "Player": market_player_name,
                    "team": FULL_PIPELINE.display_team(row.get("Team")),
                    "opponent": FULL_PIPELINE.display_team(row.get("NextOpponent")),
                    "stat": market_stat,
                    "market_line": round(line, 4),
                    "projection": round(projection_value, 4),
                    "diff": round(diff, 4),
                    "abs_diff": round(abs(diff), 4),
                    "squared_error": round(diff * diff, 6),
                    "lean": "over" if diff > 0 else "under" if diff < 0 else "push",
                    "market_over_odds": avg_entry.get("over"),
                    "market_under_odds": avg_entry.get("under"),
                    "book_count": book_count,
                    "market_books": market_details,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(["abs_diff", "Player", "stat"], ascending=[False, True, True]).reset_index(drop=True)


def summarize_eval(eval_df: pd.DataFrame) -> pd.DataFrame:
    if eval_df.empty:
        return pd.DataFrame(columns=["stat", "count", "mae", "rmse", "bias"])

    summary_rows = []
    for stat, group in eval_df.groupby("stat"):
        count = int(len(group))
        mae = float(group["abs_diff"].mean())
        rmse = math.sqrt(float(group["squared_error"].mean()))
        bias = float(group["diff"].mean())
        summary_rows.append(
            {
                "stat": stat,
                "count": count,
                "mae": round(mae, 4),
                "rmse": round(rmse, 4),
                "bias": round(bias, 4),
            }
        )

    summary = pd.DataFrame(summary_rows)
    return summary.sort_values(["mae", "count", "stat"], ascending=[False, False, True]).reset_index(drop=True)


def default_output_path(projections_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    return OUTPUT_DIR / f"unabated_real_lines_eval_{projection_file_label(projections_path)}_{timestamp}.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate projections against posted Unabated lines")
    parser.add_argument("--unabated", required=True, help="Raw or parsed Unabated JSON path")
    parser.add_argument("--projections", required=True, help="Projection workbook or CSV path")
    parser.add_argument("--date", default=None, help="Optional event date filter in MM-DD-YYYY format")
    parser.add_argument("--output", default=None, help="Optional detailed eval CSV path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    unabated_path = Path(args.unabated).expanduser().resolve()
    projections_path = Path(args.projections).expanduser().resolve()
    if not unabated_path.exists():
        raise SystemExit(f"Unabated input not found: {unabated_path}")
    if not projections_path.exists():
        raise SystemExit(f"Projections input not found: {projections_path}")

    target_date = None
    if args.date:
        try:
            target_date = PARSE_UNABATED.normalize_cli_date(args.date)
        except ValueError as exc:
            raise SystemExit(str(exc))

    markets = parse_unabated_input(unabated_path, target_date=target_date)
    projections = read_projections(projections_path)
    eval_df = build_eval_rows(projections, markets)
    summary_df = summarize_eval(eval_df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = Path(args.output).expanduser().resolve() if args.output else default_output_path(projections_path)
    eval_df.to_csv(output_path, index=False)

    print(f"Unabated input: {unabated_path}")
    print(f"Projections input: {projections_path}")
    print(f"Matched player/stat rows: {len(eval_df)}")
    print(f"Detailed eval CSV: {output_path}")
    if summary_df.empty:
        print("No matched player/stat rows found.")
        return

    print("\nSummary by stat")
    for _, row in summary_df.iterrows():
        print(
            f"{row['stat']}: count={int(row['count'])}, "
            f"mae={row['mae']:.4f}, rmse={row['rmse']:.4f}, bias={row['bias']:.4f}"
        )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

from __future__ import annotations

import argparse
import html
import math
import sys
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

from scripts import full_pipeline


DEFAULT_ETR_FILE = REPO_ROOT / "inputs" / "other" / "etr.csv"
DEFAULT_RG_FILE = REPO_ROOT / "inputs" / "other" / "rg.csv"
DEFAULT_OUTPUT_SUFFIX = "_with_providers"
FUZZY_COMPOSITE_MATCH_THRESHOLD = 0.93
FUZZY_PLAYER_MATCH_THRESHOLD = 0.90
FUZZY_TEAM_MATCH_THRESHOLD = 0.80
MANUAL_CARRY_COLUMNS = ("manualRtg", "manualNotes")
OUTPUT_CARRY_COLUMNS = MANUAL_CARRY_COLUMNS + ("kenpom_player_url",)

RG_STAT_COLUMN_MAP = {
    "points": "PTS",
    "rebounds": "REB",
    "assists": "AST",
    "steals": "STL",
    "blocks": "BLK",
    "threes": "3PM",
    "points_assists": ("PTS", "AST"),
    "points_rebounds": ("PTS", "REB"),
    "rebounds_assists": ("REB", "AST"),
    "points_rebounds_assists": ("PTS", "REB", "AST"),
}
OUR_STAT_COMPONENT_MAP = {
    "points": ("PROJ PTS",),
    "rebounds": ("PROJ REB",),
    "assists": ("PROJ AST",),
    "steals": ("PROJ STL",),
    "blocks": ("PROJ BLK",),
    "threes": ("PROJ 3PM",),
    "points_assists": ("PROJ PTS", "PROJ AST"),
    "points_rebounds": ("PROJ PTS", "PROJ REB"),
    "rebounds_assists": ("PROJ REB", "PROJ AST"),
    "points_rebounds_assists": ("PROJ PTS", "PROJ REB", "PROJ AST"),
}
FANTASY_POINTS_COMPONENTS = {
    "PROJ PTS": 1.0,
    "PROJ 3PM": 0.5,
    "PROJ REB": 1.25,
    "PROJ AST": 1.5,
    "PROJ STL": 2.0,
    "PROJ BLK": 2.0,
    "PROJ TO": -0.5,
}


@dataclass(frozen=True)
class ProviderRecord:
    player_name: str
    player_key: str
    team_name: str
    primary_team_key: str
    composite_key: str
    team_aliases: tuple[str, ...]
    values: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Augment a stage analysis file with external provider minutes and projections."
    )
    parser.add_argument("--input-file", required=True, help="Input CSV or XLSX containing Player/team/stat columns.")
    parser.add_argument("--output", default=None, help="Optional output path. Defaults to *_with_providers.<ext>.")
    parser.add_argument("--etr", action="store_true", help="Append ETR columns using inputs/other/etr.csv.")
    parser.add_argument("--rg", action="store_true", help="Append RG columns using inputs/other/rg.csv.")
    parser.add_argument("--etr-file", default=str(DEFAULT_ETR_FILE), help="Optional override for the ETR CSV path.")
    parser.add_argument("--rg-file", default=str(DEFAULT_RG_FILE), help="Optional override for the RG CSV path.")
    parser.add_argument(
        "--projections-file",
        default=None,
        help="Model projections workbook/CSV used for ETR-derived stat scaling.",
    )
    return parser.parse_args()


def clean_text(value: object) -> str:
    return html.unescape(str(value or "")).strip()


def canonicalize_player(value: object) -> str:
    return full_pipeline.canonicalize_player_name(clean_text(value))


def canonicalize_team(value: object) -> str:
    return full_pipeline.canonicalize_team_name(clean_text(value))


def canonicalize_stat(value: object) -> str:
    text = clean_text(value).lower()
    return text.replace(" ", "_").replace("-", "_")


def numeric_or_nan(value: object) -> float:
    if value is None:
        return math.nan
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number


def composite_key(player_value: object, team_value: object) -> str:
    return f"{canonicalize_player(player_value)}:{canonicalize_team(team_value)}"


def build_unique_player_team_map(df: pd.DataFrame, *, player_col: str, team_col: str) -> dict[str, str]:
    team_sets: dict[str, set[str]] = defaultdict(set)
    for _, row in df.iterrows():
        player_key = canonicalize_player(row.get(player_col))
        team_key = canonicalize_team(row.get(team_col))
        if player_key and team_key:
            team_sets[player_key].add(team_key)
    return {player_key: next(iter(team_keys)) for player_key, team_keys in team_sets.items() if len(team_keys) == 1}


def build_provider_records(
    provider_df: pd.DataFrame,
    *,
    player_col: str,
    team_col: str,
    values_map: dict[str, str],
    extra_team_cols: tuple[str, ...] = (),
    unique_target_teams_by_player: dict[str, str] | None = None,
) -> list[ProviderRecord]:
    records: list[ProviderRecord] = []
    for _, row in provider_df.iterrows():
        player_name = clean_text(row.get(player_col))
        if not player_name:
            continue
        player_key = canonicalize_player(player_name)
        primary_team_text = clean_text(row.get(team_col))
        primary_team_key = canonicalize_team(primary_team_text)
        team_aliases: set[str] = {team_key for team_key in [primary_team_key] if team_key}
        for extra_col in extra_team_cols:
            alias_key = canonicalize_team(row.get(extra_col))
            if alias_key:
                team_aliases.add(alias_key)
        inferred_team = (unique_target_teams_by_player or {}).get(player_key)
        if inferred_team:
            team_aliases.add(inferred_team)
        if not team_aliases:
            team_aliases.add("")
        values = {output_col: row.get(source_col) for output_col, source_col in values_map.items()}
        records.append(
            ProviderRecord(
                player_name=player_name,
                player_key=player_key,
                team_name=primary_team_text,
                primary_team_key=primary_team_key,
                composite_key=f"{player_key}:{primary_team_key}",
                team_aliases=tuple(sorted(team_aliases)),
                values=values,
            )
        )
    return records


def same_player_signature(left_player_key: str, right_player_key: str) -> bool:
    return full_pipeline.same_name_signature(left_player_key, right_player_key)


class ProviderMatcher:
    def __init__(self, records: list[ProviderRecord]) -> None:
        self.records = records
        self.by_composite: dict[str, list[ProviderRecord]] = defaultdict(list)
        self.by_player: dict[str, list[ProviderRecord]] = defaultdict(list)
        for record in records:
            self.by_player[record.player_key].append(record)
            for team_alias in record.team_aliases:
                self.by_composite[f"{record.player_key}:{team_alias}"].append(record)

    def resolve(self, player_name: object, team_name: object) -> ProviderRecord | None:
        player_key = canonicalize_player(player_name)
        team_key = canonicalize_team(team_name)
        if not player_key:
            return None

        exact_matches = self.by_composite.get(f"{player_key}:{team_key}", [])
        if len(exact_matches) == 1:
            return exact_matches[0]

        player_matches = self.by_player.get(player_key, [])
        unique_player_matches = dedupe_records(player_matches)
        if len(unique_player_matches) == 1:
            return unique_player_matches[0]
        if len(unique_player_matches) > 1:
            exact_team_ranked = rank_records_by_team(unique_player_matches, team_key)
            if exact_team_ranked and exact_team_ranked[0][0] >= 0.92:
                best_score = exact_team_ranked[0][0]
                next_score = exact_team_ranked[1][0] if len(exact_team_ranked) > 1 else 0.0
                if best_score - next_score >= 0.03:
                    return exact_team_ranked[0][1]

        ranked_by_parts: list[tuple[float, ProviderRecord]] = []
        for record in self.records:
            if not same_player_signature(player_key, record.player_key):
                continue
            player_score = SequenceMatcher(a=player_key, b=record.player_key).ratio()
            team_score = max(
                (
                    SequenceMatcher(a=team_key, b=team_alias).ratio()
                    for team_alias in record.team_aliases
                    if team_alias
                ),
                default=0.0,
            )
            if player_score < FUZZY_PLAYER_MATCH_THRESHOLD or team_score < FUZZY_TEAM_MATCH_THRESHOLD:
                continue
            combined_score = (player_score * 0.75) + (team_score * 0.25)
            ranked_by_parts.append((combined_score, record))
        if ranked_by_parts:
            ranked_by_parts.sort(key=lambda item: item[0], reverse=True)
            best_score, best_record = ranked_by_parts[0]
            next_score = ranked_by_parts[1][0] if len(ranked_by_parts) > 1 else 0.0
            if best_score - next_score >= 0.03:
                return best_record

        target_key = f"{player_key}:{team_key}"
        ranked: list[tuple[float, ProviderRecord]] = []
        for record in self.records:
            if not same_player_signature(player_key, record.player_key):
                continue
            best_candidate_score = 0.0
            for alias_team in record.team_aliases:
                candidate_key = f"{record.player_key}:{alias_team}"
                score = SequenceMatcher(a=target_key, b=candidate_key).ratio()
                if score > best_candidate_score:
                    best_candidate_score = score
            if best_candidate_score >= FUZZY_COMPOSITE_MATCH_THRESHOLD:
                ranked.append((best_candidate_score, record))
        if not ranked:
            return None
        ranked.sort(key=lambda item: item[0], reverse=True)
        best_score, best_record = ranked[0]
        next_score = ranked[1][0] if len(ranked) > 1 else 0.0
        if best_score - next_score >= 0.03:
            return best_record
        return None


def dedupe_records(records: list[ProviderRecord]) -> list[ProviderRecord]:
    unique: dict[tuple[str, str], ProviderRecord] = {}
    for record in records:
        unique[(record.player_key, record.primary_team_key)] = record
    return list(unique.values())


def rank_records_by_team(records: list[ProviderRecord], target_team_key: str) -> list[tuple[float, ProviderRecord]]:
    ranked: list[tuple[float, ProviderRecord]] = []
    for record in records:
        score = max(
            (
                SequenceMatcher(a=target_team_key, b=team_alias).ratio()
                for team_alias in record.team_aliases
                if team_alias
            ),
            default=0.0,
        )
        ranked.append((score, record))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return ranked


def read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise SystemExit(f"Unsupported input format: {path}")


def write_table(df: pd.DataFrame, path: Path) -> None:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(path, index=False)
        return
    if suffix in {".xlsx", ".xls"}:
        sheet_name = "Projections" if "stat" not in df.columns else "Sheet1"
        df.to_excel(path, sheet_name=sheet_name, index=False)
        return
    raise SystemExit(f"Unsupported output format: {path}")


def infer_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}{DEFAULT_OUTPUT_SUFFIX}{input_path.suffix}")


def order_provider_columns(df: pd.DataFrame) -> pd.DataFrame:
    preferred_order = [
        "etr_mins",
        "etr_mins_diff",
        "etr_proj",
        "etr_proj_diff",
        "rg_mins",
        "rg_mins_diff",
        "rg_proj",
        "rg_proj_diff",
    ]
    present_provider_columns = [column for column in preferred_order if column in df.columns]
    if not present_provider_columns:
        return df
    non_provider_columns = [column for column in df.columns if column not in present_provider_columns]
    return df[non_provider_columns + present_provider_columns]


def required_columns(df: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(df.columns))
    if missing:
        raise SystemExit(f"{label} missing required columns: {', '.join(missing)}")


def analysis_match_key(player_name: object, team: object, stat_category: object) -> str:
    return "-".join(str(part).strip().lower() for part in (player_name, team, stat_category))


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


def carry_forward_manual_fields(output_df: pd.DataFrame, existing_output_path: Path | None) -> pd.DataFrame:
    if existing_output_path is None or not existing_output_path.exists():
        return output_df

    analysis_df = pd.read_csv(existing_output_path)
    required_columns(analysis_df, {"Player", "team", "stat"} | set(MANUAL_CARRY_COLUMNS), "Existing output")

    latest_by_key: dict[str, dict[str, object]] = {}
    for _, row in analysis_df.iterrows():
        key = analysis_match_key(row.get("Player"), row.get("team"), row.get("stat"))
        existing_values = {"line": normalize_line_value(row.get("line"))}
        for column in OUTPUT_CARRY_COLUMNS:
            existing_values[column] = normalize_existing_analysis_value(row.get(column))
        existing_values["manualRtg"] = str(existing_values["manualRtg"]).strip()
        existing_values["manualNotes"] = str(existing_values["manualNotes"]).strip()
        existing_values["kenpom_player_url"] = str(existing_values["kenpom_player_url"]).strip()
        latest_by_key[key] = existing_values

    merged = output_df.copy()
    for column in OUTPUT_CARRY_COLUMNS:
        if column not in merged.columns:
            merged[column] = ""
        else:
            merged[column] = merged[column].astype("object")

    for index, row in merged.iterrows():
        key = analysis_match_key(row.get("Player"), row.get("team"), row.get("stat"))
        existing = latest_by_key.get(key)
        if existing is None:
            continue
        for column in OUTPUT_CARRY_COLUMNS:
            merged.at[index, column] = coerce_existing_value_for_output_column(merged[column], existing[column])
        old_line = existing["line"]
        new_line = normalize_line_value(row.get("line"))
        notes = existing["manualNotes"]
        if old_line and new_line and old_line != new_line:
            prefix = f"LINE_CHANGED_{old_line}_TO_{new_line}:"
            notes = f"{prefix} {notes}".strip() if notes else prefix
        merged.at[index, "manualNotes"] = notes
    return merged


def compute_our_fantasy_points(row: pd.Series) -> float:
    total = 0.0
    found = False
    for column, weight in FANTASY_POINTS_COMPONENTS.items():
        number = numeric_or_nan(row.get(column))
        if math.isnan(number):
            continue
        found = True
        total += number * weight
    return total if found else math.nan


def rounded_or_nan(value: float, digits: int = 2) -> float:
    if math.isnan(value):
        return math.nan
    return round(value, digits)


def compute_diff(provider_value: float, model_value: object) -> float:
    if math.isnan(provider_value):
        return math.nan
    model_number = numeric_or_nan(model_value)
    if math.isnan(model_number):
        return math.nan
    return provider_value - model_number


def compute_scaled_stat_projection(projection_row: pd.Series, stat_value: object, scale_factor: float) -> float:
    if math.isnan(scale_factor):
        return math.nan
    components = OUR_STAT_COMPONENT_MAP.get(canonicalize_stat(stat_value))
    if not components:
        return math.nan
    total = 0.0
    found = False
    for column in components:
        number = numeric_or_nan(projection_row.get(column))
        if math.isnan(number):
            return math.nan
        found = True
        total += number * scale_factor
    return total if found else math.nan


def build_projection_lookup(projections_df: pd.DataFrame) -> tuple[ProviderMatcher, dict[tuple[str, str], pd.Series]]:
    required_columns(
        projections_df,
        {"Name", "Team", "PROJ PTS", "PROJ REB", "PROJ AST", "PROJ STL", "PROJ BLK", "PROJ TO", "PROJ 3PM"},
        "Projections file",
    )
    projection_rows: dict[tuple[str, str], pd.Series] = {}
    unique_target_teams = build_unique_player_team_map(projections_df, player_col="Name", team_col="Team")
    records = build_provider_records(
        projections_df,
        player_col="Name",
        team_col="Team",
        values_map={},
        unique_target_teams_by_player=unique_target_teams,
    )
    for record, (_, row) in zip(records, projections_df.iterrows(), strict=False):
        projection_rows[(record.player_key, record.primary_team_key)] = row
    return ProviderMatcher(records), projection_rows


def resolve_projection_row(
    matcher: ProviderMatcher,
    projection_rows: dict[tuple[str, str], pd.Series],
    player_name: object,
    team_name: object,
) -> pd.Series | None:
    match = matcher.resolve(player_name, team_name)
    if match is None:
        return None
    return projection_rows.get((match.player_key, match.primary_team_key))


def augment_with_rg(base_df: pd.DataFrame, rg_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    required_columns(base_df, {"Player", "team", "stat"}, "Input file")
    required_columns(rg_df, {"PLAYER", "TEAMNAME", "TEAM", "MINUTES"}, "RG file")
    unique_target_teams = build_unique_player_team_map(base_df, player_col="Player", team_col="team")
    rg_values_map = {"rg_mins": "MINUTES"}
    for source_col in {"PTS", "REB", "AST", "STL", "BLK", "3PM"}:
        if source_col in rg_df.columns:
            rg_values_map[source_col] = source_col
    records = build_provider_records(
        rg_df,
        player_col="PLAYER",
        team_col="TEAMNAME",
        extra_team_cols=("TEAM",),
        values_map=rg_values_map,
        unique_target_teams_by_player=unique_target_teams,
    )
    matcher = ProviderMatcher(records)
    augmented = base_df.copy()
    augmented["rg_mins"] = math.nan
    augmented["rg_mins_diff"] = math.nan
    augmented["rg_proj"] = math.nan
    augmented["rg_proj_diff"] = math.nan
    matched_rows = 0
    for index, row in augmented.iterrows():
        match = matcher.resolve(row.get("Player"), row.get("team"))
        if match is None:
            continue
        matched_rows += 1
        rg_mins = numeric_or_nan(match.values.get("rg_mins"))
        augmented.at[index, "rg_mins"] = rounded_or_nan(rg_mins)
        augmented.at[index, "rg_mins_diff"] = rounded_or_nan(compute_diff(rg_mins, row.get("proj_mins")))
        stat_mapping = RG_STAT_COLUMN_MAP.get(canonicalize_stat(row.get("stat")))
        if stat_mapping is None:
            continue
        if isinstance(stat_mapping, tuple):
            total = 0.0
            for source_col in stat_mapping:
                total += numeric_or_nan(match.values.get(source_col))
            rg_proj = total
            augmented.at[index, "rg_proj"] = rounded_or_nan(rg_proj)
            augmented.at[index, "rg_proj_diff"] = rounded_or_nan(compute_diff(rg_proj, row.get("stat_projection")))
            continue
        rg_proj = numeric_or_nan(match.values.get(stat_mapping))
        augmented.at[index, "rg_proj"] = rounded_or_nan(rg_proj)
        augmented.at[index, "rg_proj_diff"] = rounded_or_nan(compute_diff(rg_proj, row.get("stat_projection")))
    return augmented, matched_rows


def augment_with_etr(
    base_df: pd.DataFrame,
    etr_df: pd.DataFrame,
    projections_df: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    required_columns(base_df, {"Player", "team", "stat"}, "Input file")
    required_columns(etr_df, {"Name", "Team", "Minutes", "DK Points"}, "ETR file")
    projection_matcher, projection_rows = build_projection_lookup(projections_df)
    unique_target_teams = build_unique_player_team_map(base_df, player_col="Player", team_col="team")
    team_hints = build_unique_player_team_map(projections_df, player_col="Name", team_col="Team")
    records = build_provider_records(
        etr_df,
        player_col="Name",
        team_col="Team",
        values_map={"etr_mins": "Minutes", "etr_dk_points": "DK Points"},
        unique_target_teams_by_player={**team_hints, **unique_target_teams},
    )
    matcher = ProviderMatcher(records)
    augmented = base_df.copy()
    augmented["etr_mins"] = math.nan
    augmented["etr_mins_diff"] = math.nan
    augmented["etr_proj"] = math.nan
    augmented["etr_proj_diff"] = math.nan
    matched_rows = 0
    for index, row in augmented.iterrows():
        match = matcher.resolve(row.get("Player"), row.get("team"))
        if match is None:
            continue
        projection_row = resolve_projection_row(projection_matcher, projection_rows, row.get("Player"), row.get("team"))
        if projection_row is None:
            continue
        our_fantasy_points = compute_our_fantasy_points(projection_row)
        etr_fantasy_points = numeric_or_nan(match.values.get("etr_dk_points"))
        scale_factor = math.nan
        if not math.isnan(our_fantasy_points) and abs(our_fantasy_points) > 1e-9 and not math.isnan(etr_fantasy_points):
            scale_factor = etr_fantasy_points / our_fantasy_points
        etr_mins = numeric_or_nan(match.values.get("etr_mins"))
        etr_proj = compute_scaled_stat_projection(
            projection_row,
            row.get("stat"),
            scale_factor,
        )
        augmented.at[index, "etr_mins"] = rounded_or_nan(etr_mins)
        augmented.at[index, "etr_mins_diff"] = rounded_or_nan(compute_diff(etr_mins, row.get("proj_mins")))
        augmented.at[index, "etr_proj"] = rounded_or_nan(etr_proj)
        augmented.at[index, "etr_proj_diff"] = rounded_or_nan(compute_diff(etr_proj, row.get("stat_projection")))
        matched_rows += 1
    return augmented, matched_rows


def main() -> None:
    args = parse_args()
    if not args.etr and not args.rg:
        raise SystemExit("At least one provider flag is required: --etr and/or --rg")
    if args.etr and not args.projections_file:
        raise SystemExit("--projections-file is required when --etr is enabled")

    input_path = Path(args.input_file).expanduser().resolve()
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")
    output_path = Path(args.output).expanduser().resolve() if args.output else infer_output_path(input_path)
    base_df = read_table(input_path)

    matched_summary: list[str] = []
    if args.rg:
        rg_path = Path(args.rg_file).expanduser().resolve()
        if not rg_path.exists():
            raise SystemExit(f"RG file not found: {rg_path}")
        rg_df = pd.read_csv(rg_path)
        base_df, matched_rows = augment_with_rg(base_df, rg_df)
        matched_summary.append(f"rg_matches={matched_rows}")

    if args.etr:
        etr_path = Path(args.etr_file).expanduser().resolve()
        projections_path = Path(args.projections_file).expanduser().resolve()
        if not etr_path.exists():
            raise SystemExit(f"ETR file not found: {etr_path}")
        if not projections_path.exists():
            raise SystemExit(f"Projections file not found: {projections_path}")
        etr_df = pd.read_csv(etr_path)
        projections_df = read_table(projections_path)
        base_df, matched_rows = augment_with_etr(base_df, etr_df, projections_df)
        matched_summary.append(f"etr_matches={matched_rows}")

    base_df = carry_forward_manual_fields(base_df, output_path)
    base_df = order_provider_columns(base_df)
    write_table(base_df, output_path)
    print(f"Wrote augmented file: {output_path}")
    if matched_summary:
        print(" ".join(matched_summary))


if __name__ == "__main__":
    main()

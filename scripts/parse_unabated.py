#!/usr/bin/env python3
"""
Parse an Unabated response into a cleaner player -> stat -> sportsbook object.

Usage:
    ./scripts/parse_unabated.py --input unabatedResponse.json

Supported betTypeIds in this script:
    69 -> threes
    70 -> assists
    71 -> blocks
    73 -> points
    74 -> points_assists
    75 -> points_rebounds
    76 -> points_rebounds_assists
    77 -> rebounds
    78 -> rebounds_assists
    81 -> steals
    82 -> stocks
    84 -> turnovers

Unsupported betTypeIds observed in this payload, with current best guesses:
    158 -> likely a yes/no milestone market with no numeric line
           Example values: Anthony Roy under +850, Christian Coleman under +850,
           Emanuel Sharp under +550
    330 -> likely double-double
           Example values: Christian Coleman 0.5, Kingston Flemings 0.5,
           Chris Cenac 0.5, Joseph Tugler 0.5
    631 -> fantasy points
           Example values:
             Anthony Roy: 21.5
             Braden Smith: 33.5 / 34.05
             Emanuel Sharp: 26.5
             Chris Cenac: 23.5
             Darius Acuff: 37.15
"""

from __future__ import annotations

import argparse
import difflib
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


BET_TYPE_MAP = {
    69: "threes",
    70: "assists",
    71: "blocks",
    73: "points",
    74: "points_assists",
    75: "points_rebounds",
    76: "points_rebounds_assists",
    77: "rebounds",
    78: "rebounds_assists",
    81: "steals",
    82: "stocks",
    84: "turnovers",
}

SUPPORTED_BET_TYPE_IDS = {
    69,
    70,
    71,
    73,
    74,
    75,
    76,
    77,
    78,
    81,
    82,
    84,
}

ALLOWED_BET_SUBTYPES = {None, "Milestone-Alt"}
MARKET_VARIANT_KEY_SEPARATOR = "__variant__"

# IMPORTANT: Instead of deleting from this list when removing a book, comment the book out instead
ALLOWED_BOOK_LABELS = {
    "FanDuel": "fanduel",
    "Caesars": "caesars",
    "Bet365": "bet365",
    "Circa": "circa",
    "DraftKings": "draftkings",
    "TheScore US": "espnbet",
    "Fanatics": "fanatics",
    "BetMGM": "betmgm",
    "Sleeper": "sleeper",
    "Splash Sports": "splash_sports",
    "ProphetX": "prophetx",
}

# IMPORTANT: Instead of deleting from this list when removing a book, comment the book out instead
ALLOWED_BOOK_ALIASES = {
    "fanduel": "fanduel",
    "fanduel_delayed": "fanduel",
    "caesars": "caesars",
    "caesars_internal": "caesars",
    "caesarsdirect": "caesars",
    "bet365": "bet365",
    "bet365_internal": "bet365",
    "circa": "circa",
    "circa_delayed": "circa",
    "espn_bet": "espnbet",
    "espnbet": "espnbet",
    "fanatics": "fanatics",
    "betmgm": "betmgm",
    "betmgm_direct": "betmgm",
    "draftkings": "draftkings",
    "thescore_us": "espnbet",
    "sleeper": "sleeper",
    "splash_sports": "splash_sports",
    "splashsports": "splash_sports",
    "prophet_exchange": "prophetx",
    "prophetx": "prophetx",
}

ALLOWED_BOOKS = frozenset({*ALLOWED_BOOK_LABELS.values(), *ALLOWED_BOOK_ALIASES.values()})

TARGET_TIMEZONE = ZoneInfo("America/Chicago")


def normalize_book_name(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    aliases = {
        "fan_duel": "fanduel",
        "caesars_sportsbook": "caesars",
        "caesars": "caesars",
        "draft_kings": "draftkings",
        "bet_mgm": "betmgm",
        "hard_rock_bet": "hardrock",
        "hard_rock_internal": "hardrock_internal",
        "espn_bet": "espnbet",
    }
    return aliases.get(normalized, normalized)


def canonical_book_name(name: str) -> str | None:
    normalized = normalize_book_name(name)
    direct = ALLOWED_BOOK_ALIASES.get(normalized)
    if direct:
        return direct

    # Conservative fuzzy fallback for minor source-label variations.
    matches = difflib.get_close_matches(normalized, list(ALLOWED_BOOK_ALIASES.keys()), n=1, cutoff=0.9)
    if matches:
        return ALLOWED_BOOK_ALIASES[matches[0]]
    return None


def player_name(person: dict) -> str:
    parts = [person.get("firstName"), person.get("lastName")]
    return " ".join(part for part in parts if part)


def normalize_team_name(name: str | None) -> str:
    if not name:
        return ""
    text = re.sub(r"[^a-z0-9]+", " ", str(name).lower()).strip()
    tokens = []
    for token in text.split():
        if token in {"state", "st"}:
            tokens.append("st")
        elif token in {"saint", "st"}:
            tokens.append("st")
        else:
            tokens.append(token)
    return " ".join(tokens)


def american_to_implied_prob(price: int | float | None) -> float | None:
    if price in (None, 0):
        return None
    price = float(price)
    if price > 0:
        return 100.0 / (price + 100.0)
    return abs(price) / (abs(price) + 100.0)


def implied_prob_to_american(prob: float | None) -> int | None:
    if prob is None or prob <= 0 or prob >= 1:
        return None
    if prob >= 0.5:
        return int(round(-100.0 * prob / (1.0 - prob)))
    return int(round(100.0 * (1.0 - prob) / prob))


def average_american(prices: list[int | float]) -> int | None:
    implied_probs = [american_to_implied_prob(price) for price in prices if price not in (None, 0)]
    implied_probs = [prob for prob in implied_probs if prob is not None]
    if not implied_probs:
        return None
    return implied_prob_to_american(sum(implied_probs) / len(implied_probs))


def average_numeric(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None and not math.isnan(float(value))]
    if not clean:
        return None
    avg = sum(clean) / len(clean)
    return round(avg, 4)


def output_path(input_path: Path) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    output_dir = repo_root / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / f"{input_path.stem}_parsed.json"


def normalize_cli_date(value: str | None) -> str | None:
    if not value:
        return None
    match = re.fullmatch(r"(\d{2})-(\d{2})-(\d{4})", str(value).strip())
    if not match:
        raise ValueError(f"Date must be in MM-DD-YYYY format: {value}")
    month, day, year = match.groups()
    return f"{year}-{month}-{day}"


def event_start_date_in_target_timezone(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(TARGET_TIMEZONE).date().isoformat()


def event_start_in_target_timezone(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(TARGET_TIMEZONE)


def format_event_start_cst(value: str | None) -> str | None:
    parsed = event_start_in_target_timezone(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y-%m-%d %I:%M %p %Z")


def best_line_entry(existing: dict | None, candidate: dict) -> dict:
    if existing is None:
        return candidate

    existing_status = existing.get("statusId", 99)
    candidate_status = candidate.get("statusId", 99)
    if existing_status != 1 and candidate_status == 1:
        return candidate
    if existing_status == 1 and candidate_status != 1:
        return existing

    existing_modified = existing.get("modifiedOn") or ""
    candidate_modified = candidate.get("modifiedOn") or ""
    if candidate_modified > existing_modified:
        return candidate
    return existing


def normalize_variant_token(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")


def normalize_line_token(value: object) -> str:
    if value is None:
        return "na"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return normalize_variant_token(str(value)) or "na"
    if math.isnan(numeric):
        return "na"
    if numeric.is_integer():
        return str(int(numeric))
    return str(numeric).replace(".", "_")


def market_entry_key(book_name: str, bet_sub_type: str | None, line_value: object) -> str:
    if bet_sub_type is None:
        return book_name
    subtype_token = normalize_variant_token(bet_sub_type) or "variant"
    line_token = normalize_line_token(line_value)
    return f"{book_name}{MARKET_VARIANT_KEY_SEPARATOR}{subtype_token}_{line_token}"


def base_book_name(book_name: str) -> str:
    return str(book_name).split(MARKET_VARIANT_KEY_SEPARATOR, 1)[0]


def parse_unabated(payload: dict, target_date: str | None = None) -> dict:
    people = payload.get("people", {})
    teams = payload.get("teams", {})
    market_sources = {
        f"ms{source['id']}": canonical_book_name(source["name"])
        for source in payload.get("marketSources", [])
    }

    result: dict[str, dict[str, dict[str, dict]]] = defaultdict(lambda: defaultdict(dict))
    player_storage_keys: dict[tuple[str, str], str] = {}

    for _, markets in payload.get("odds", {}).items():
        for market in markets:
            if not isinstance(market, dict):
                continue
            event_start_raw = market.get("eventStart")
            event_start_cst = event_start_in_target_timezone(event_start_raw)
            event_start = event_start_cst.date().isoformat() if event_start_cst is not None else None
            if target_date and event_start != target_date:
                continue
            if market.get("betSubType") not in ALLOWED_BET_SUBTYPES:
                continue

            bet_type_id = market.get("betTypeId")
            if bet_type_id not in SUPPORTED_BET_TYPE_IDS:
                continue
            stat_category = BET_TYPE_MAP[bet_type_id]
            person = people.get(str(market.get("personId")))
            if not person:
                continue
            name = player_name(person)
            if not name:
                continue
            team_id = market.get("teamId")
            team_name = None
            if team_id is not None:
                team_info = teams.get(str(team_id), {})
                if isinstance(team_info, dict):
                    team_name = team_info.get("name")
            team_name = team_name or person.get("team")
            normalized_team = normalize_team_name(team_name)
            player_key = player_storage_keys.get((name, normalized_team))
            if player_key is None:
                if name not in result:
                    player_key = name
                else:
                    suffix = f" ({team_name})" if team_name else " (unknown team)"
                    player_key = f"{name}{suffix}"
                player_storage_keys[(name, normalized_team)] = player_key
                result[player_key]["__meta__"] = {
                    "player_name": name,
                    "team": team_name,
                    "normalized_team": normalized_team,
                    "event_start_cst_iso": event_start_cst.isoformat() if event_start_cst is not None else None,
                    "game_time_cst": event_start_cst.strftime("%Y-%m-%d %I:%M %p %Z") if event_start_cst else None,
                }

            player_stat = result[player_key][stat_category]

            for side_key, books in market.get("sides", {}).items():
                side_name = "over" if str(side_key).startswith("si0") else "under"
                for market_source_key, line in books.items():
                    if market_source_key not in market_sources:
                        continue
                    if not isinstance(line, dict):
                        continue
                    if line.get("points") is None:
                        continue

                    book_name = market_sources[market_source_key]
                    if book_name is None:
                        continue
                    entry_key = market_entry_key(book_name, market.get("betSubType"), line.get("points"))
                    entry = player_stat.setdefault(
                        entry_key,
                        {
                            "line": None,
                            "over": None,
                            "under": None,
                            "_base_book": book_name,
                            "_variant": market.get("betSubType"),
                        },
                    )
                    side_meta = entry.setdefault("_meta", {})

                    current_meta = side_meta.get(side_name)
                    chosen = best_line_entry(current_meta, line)
                    side_meta[side_name] = chosen

                    entry["line"] = chosen.get("points")
                    entry[side_name] = chosen.get("americanPrice", chosen.get("price"))

            if player_stat:
                book_entries = []
                for book_name, entry in list(player_stat.items()):
                    if book_name == "avgObj":
                        continue
                    meta = entry.pop("_meta", None)
                    if entry.get("line") is None and entry.get("over") is None and entry.get("under") is None:
                        player_stat.pop(book_name, None)
                        continue
                    book_entries.append(entry)

                if book_entries:
                    player_stat["avgObj"] = {
                        "line": average_numeric([entry.get("line") for entry in book_entries]),
                        "over": average_american([entry.get("over") for entry in book_entries]),
                        "under": average_american([entry.get("under") for entry in book_entries]),
                    }

    return {player: dict(stats) for player, stats in result.items()}


def count_lines_by_book(parsed: dict) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for stats in parsed.values():
        if not isinstance(stats, dict):
            continue
        for stat_name, books in stats.items():
            if stat_name == "__meta__" or not isinstance(books, dict):
                continue
            for book_name, entry in books.items():
                if book_name == "avgObj" or not isinstance(entry, dict):
                    continue
                if entry.get("line") is None:
                    continue
                counts[base_book_name(book_name)] += 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse Unabated player prop response into a cleaner JSON object")
    parser.add_argument("--input", required=True, help="Path to unabatedResponse.json")
    parser.add_argument("--date", default=None, help="Filter raw markets to a specific event date in MM-DD-YYYY format")
    parser.add_argument("--output", default=None, help="Optional parsed JSON output path")
    args = parser.parse_args()

    input_file = Path(args.input).expanduser().resolve()
    if not input_file.exists():
        raise SystemExit(f"Input file not found: {input_file}")

    payload = json.loads(input_file.read_text())
    try:
        target_date = normalize_cli_date(args.date)
    except ValueError as exc:
        raise SystemExit(str(exc))
    parsed = parse_unabated(payload, target_date=target_date)

    out_path = Path(args.output).expanduser().resolve() if args.output else output_path(input_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(parsed, indent=2, sort_keys=True))
    book_line_counts = count_lines_by_book(parsed)

    print(f"Parsed {len(parsed)} players")
    print(f"Output written to {out_path}")
    print("Lines posted by book:")
    if book_line_counts:
        for book_name, count in book_line_counts.items():
            print(f"  {book_name}: {count}")
    else:
        print("  none")


if __name__ == "__main__":
    main()

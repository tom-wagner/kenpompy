#!/usr/bin/env python3
"""
Run the full projection -> market matching -> EV pipeline.

This script can reuse an existing KenPom workbook or scrape a fresh one,
parse the Unabated payload, run either projection model, and emit a CSV with
one best-bet row per player/stat market.

Combined-stat probabilities (PA, PR, PRA, RA, stocks) are approximated by
convolving single-stat simulated distributions under an independence
assumption. That is a practical fallback because ``unabated_sim_output.json``
only contains single-stat simulation grids.
"""

from __future__ import annotations

import argparse
import difflib
import gzip
import importlib.util
import json
import logging
import math
import os
import re
import shutil
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

from kenpompy.xai_queue import ParallelXAIQueue, QueueProgress

try:
    from xai_sdk import Client
    from xai_sdk.chat import user as xai_user
    from xai_sdk.tools import web_search, x_search
except ImportError:  # pragma: no cover - optional runtime dependency
    Client = None
    xai_user = None
    web_search = None
    x_search = None


OUTPUTS_DIR = REPO_ROOT / "outputs"
KENPOM_OUTPUT_DIR = OUTPUTS_DIR / "kenpom"
MODEL_OUTPUT_DIR = OUTPUTS_DIR / "model_projections"
PROJECTIONS_OUTPUT_DIR = MODEL_OUTPUT_DIR / "projections"
PIPELINE_OUTPUT_DIR = OUTPUTS_DIR / "pipeline"
MINUTES_OUTPUT_DIR = PIPELINE_OUTPUT_DIR / "minutes"
MINUTES_CACHE_DIR = PIPELINE_OUTPUT_DIR / "minutes_cache"
PIPELINE_XAI_LOG_DIR = OUTPUTS_DIR / "pipeline_x_ai_logs"
CACHE_OUTPUT_DIR = OUTPUTS_DIR / "cache"
TEAM_AVERAGES_PATH = REPO_ROOT / "team_averages.json"
MAX_PARALLEL_XAI_CALLS = 15
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
load_dotenv(REPO_ROOT / ".env")
logger = logging.getLogger(__name__)
XAI_RPC_TIMEOUT_SECONDS = float(os.getenv("XAI_RPC_TIMEOUT_SECONDS", "300"))
CENTRAL_TIMEZONE = ZoneInfo("America/Chicago")

SIM_STAT_MAP = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "blocks": "blocks",
    "steals": "steals",
    "turnovers": "turnovers",
    "threes": "threePointersMade",
}

PROJECTION_STAT_MAP = {
    "points": "PROJ PTS",
    "rebounds": "PROJ REB",
    "assists": "PROJ AST",
    "blocks": "PROJ BLK",
    "steals": "PROJ STL",
    "turnovers": "PROJ TO",
    "threes": "PROJ 3PM",
}

COMBINED_COMPONENTS = {
    "points_assists": ["points", "assists"],
    "points_rebounds": ["points", "rebounds"],
    "points_rebounds_assists": ["points", "rebounds", "assists"],
    "rebounds_assists": ["rebounds", "assists"],
    "stocks": ["steals", "blocks"],
}

NAME_SUFFIX_TOKENS = {"jr", "sr", "ii", "iii", "iv", "v"}
PLAYER_STATS_CONTEXT_COLUMNS = [
    "Name",
    "Team",
    "NextOpponent",
    "MinsProjConfidence",
    "MinsProjInjurySummary",
    "MinsProjConfidenceJustification",
    "KenPomPlayerURL",
    "Ht",
    "ARate",
    "OR%",
    "DR%",
    "Blk%",
    "Game -1",
    "Game -2",
    "Game -3",
    "Game -4",
    "Game -5",
    "Game Fouls -1",
    "Game Fouls -2",
    "Game Fouls -3",
    "Game Fouls -4",
    "Game Fouls -5",
]

OUTPUT_COLUMNS = [
    "Player",
    "team",
    "opponent",
    "proj_mins",
    "conf",
    "last_5_games",
    "last_5_games_fouls",
    "pos",
    "stat",
    "bet_side",
    "stat_projection",
    "line",
    "odds",
    "book",
    "win_pct",
    "expected_value",
    "xAiScore",
    "manualRtg",
    "manualNotes",
    "full_odds_meta_data",
    "team_averages",
    "minutes_injury_summary",
    "minutes_confidence_justification",
    "xAiContext",
    "matched_sim_means",
    "sim_source",
    "game_time_cst",
    "kenpom_player_url",
]

BET_GRADING_TEMP_COLUMNS = [
    "_xai_prompt",
    "_xai_player_name",
    "_xai_team",
    "_xai_stat",
    "_xai_line",
    "_xai_bet_side",
    "_xai_expected_value",
]


def configure_logging() -> Path:
    root_logger = logging.getLogger()
    if getattr(configure_logging, "_configured", False):
        return getattr(configure_logging, "_log_path")

    main_module = load_module(REPO_ROOT / "scripts" / "main.py", "kenpom_main_module_for_retention")
    main_module.prune_output_directories()

    PIPELINE_XAI_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = PIPELINE_XAI_LOG_DIR / f"pipeline_x_ai_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.txt"
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    configure_logging._configured = True
    configure_logging._log_path = log_path
    logger.info("Logging to %s", log_path)
    return log_path


def load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_allowed_grading_books() -> frozenset[str]:
    parser_module = load_module(REPO_ROOT / "scripts" / "parse_unabated.py", "parse_unabated_books_module")
    return frozenset(parser_module.ALLOWED_BOOKS)


ALLOWED_GRADING_BOOKS = load_allowed_grading_books()
MARKET_VARIANT_KEY_SEPARATOR = "__variant__"


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
            logger.warning("Failed to remove old Unabated input %s: %s", stale_file, exc)
    if stale_files:
        logger.info(
            "Pruned %s old Unabated input files from %s; kept newest %s",
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
    logger.info("Fetched Unabated payload to %s", output_path)
    prune_unabated_input_directory(output_dir)
    return output_path


def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    text = text.lower().replace("'", "").replace(".", "")
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    return text


def canonicalize_player_name(value: str) -> str:
    tokens = normalize_name(value).split()
    while tokens and tokens[-1] in NAME_SUFFIX_TOKENS:
        tokens.pop()
    return " ".join(tokens)


def canonicalize_team_name(value: str | None) -> str:
    text = normalize_name(str(value or "").replace("+", " "))
    tokens = []
    for token in text.split():
        if token in {"state", "st", "saint"}:
            tokens.append("st")
        else:
            tokens.append(token)
    return " ".join(tokens)


def display_team(value: str | None) -> str:
    return str(value or "").replace("+", " ")


def sanitize_filename_component(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", "_", text)
    text = text.replace(":", "-")
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def format_cache_line_value(value: Any) -> str:
    number = safe_float(value)
    if math.isnan(number):
        return str(value)
    return f"{number:g}"


def build_xai_bet_cache_key(player_name: str, team: str, bet_category: str, line: Any) -> str:
    return f"{player_name}-{team}-{bet_category}-{format_cache_line_value(line)}"


def build_output_stem(model_type: str, kenpom_file: Path) -> str:
    save_ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    kenpom_ts = sanitize_filename_component(kenpom_file.stem)
    return f"{model_type}_kp_{kenpom_ts}_saved_{save_ts}"


def normalize_minutes_cache_date(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("Minutes cache date cannot be blank")
    return datetime.strptime(text, "%m-%d-%Y").strftime("%m-%d-%Y")


def infer_minutes_stage_cache_date(*, explicit_date: str | None = None, source_workbook: Path | None = None) -> str:
    if explicit_date:
        return normalize_minutes_cache_date(explicit_date)

    if source_workbook is not None:
        match = re.search(
            r"(?P<token>(Mon|Tue|Wed|Thu|Fri|Sat|Sun)_(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)_\d{1,2}_\d{4})",
            source_workbook.stem,
        )
        if match:
            parsed = datetime.strptime(match.group("token"), "%a_%b_%d_%Y")
            return parsed.strftime("%m-%d-%Y")

    return current_central_time().strftime("%m-%d-%Y")


def _normalize_minutes_cache_payload(payload: Any, cache_date: str) -> dict[str, Any]:
    normalized: dict[str, Any] = payload if isinstance(payload, dict) else {}
    teams = normalized.get("teams")
    if not isinstance(teams, dict):
        teams = {}
    normalized["teams"] = teams
    normalized["cacheDate"] = cache_date
    return normalized


def _coerce_json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _coerce_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_coerce_json_safe(item) for item in value]
    if hasattr(value, "items"):
        try:
            return {str(key): _coerce_json_safe(item) for key, item in value.items()}
        except Exception:
            pass
    if hasattr(value, "__iter__") and not isinstance(value, (str, bytes, bytearray)):
        try:
            return [_coerce_json_safe(item) for item in value]
        except Exception:
            pass
    return str(value)


def load_minutes_stage_cache(cache_date: str) -> tuple[dict[str, Any], Path]:
    normalized_date = normalize_minutes_cache_date(cache_date)
    MINUTES_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = MINUTES_CACHE_DIR / f"{normalized_date}.json"
    if not path.exists():
        return _normalize_minutes_cache_payload({}, normalized_date), path

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Skipping unreadable minutes cache file %s: %s", path, exc)
        return _normalize_minutes_cache_payload({}, normalized_date), path

    return _normalize_minutes_cache_payload(payload, normalized_date), path


def write_minutes_stage_cache(cache_date: str, payload: dict[str, Any]) -> Path:
    normalized_date = normalize_minutes_cache_date(cache_date)
    normalized_payload = _normalize_minutes_cache_payload(payload, normalized_date)
    serializable_payload = _coerce_json_safe(normalized_payload)
    MINUTES_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = MINUTES_CACHE_DIR / f"{normalized_date}.json"
    path.write_text(json.dumps(serializable_payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    logger.info("Wrote minutes cache for %s to %s", normalized_date, path)
    return path


def get_minutes_cache_team_entry(cache_payload: dict[str, Any], team: str) -> dict[str, Any]:
    teams = cache_payload.setdefault("teams", {})
    team_entry = teams.get(team)
    if not isinstance(team_entry, dict):
        team_entry = {}
        teams[team] = team_entry
    players = team_entry.get("players")
    if not isinstance(players, dict):
        team_entry["players"] = {}
    return team_entry


def parse_height_inches(value: Any) -> int | None:
    text = str(value or "").strip()
    match = re.fullmatch(r"(\d+)-(\d+)", text)
    if not match:
        return None
    feet, inches = match.groups()
    return int(feet) * 12 + int(inches)


def format_last_5_games(row: pd.Series) -> str:
    values = []
    for idx in range(5, 0, -1):
        value = safe_float(row.get(f"Game -{idx}"))
        if math.isnan(value):
            continue
        values.append(str(int(value)) if float(value).is_integer() else f"{value:g}")
    return "[" + ", ".join(values) + "]"


def format_last_5_games_fouls(row: pd.Series) -> str:
    values = []
    for idx in range(5, 0, -1):
        value = safe_float(row.get(f"Game Fouls -{idx}"))
        if math.isnan(value):
            continue
        values.append(str(int(value)) if float(value).is_integer() else f"{value:g}")
    return "[" + ", ".join(values) + "]"


def current_central_time() -> datetime:
    return datetime.now(CENTRAL_TIMEZONE)


def parse_central_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CENTRAL_TIMEZONE)
    return parsed.astimezone(CENTRAL_TIMEZONE)


def infer_position_bucket(row: pd.Series) -> str:
    height = parse_height_inches(row.get("Ht"))
    a_rate = safe_float(row.get("ARate"), 0.0)
    reb_rate = safe_float(row.get("OR%"), 0.0) + safe_float(row.get("DR%"), 0.0)
    blk_rate = safe_float(row.get("Blk%"), 0.0)

    if height is None:
        if reb_rate >= 22 or blk_rate >= 6:
            return "C"
        if reb_rate >= 17:
            return "FC"
        if a_rate >= 20:
            return "G"
        if a_rate >= 14:
            return "GF"
        return "F"

    if height <= 75:
        return "G" if a_rate >= 14 or reb_rate < 14 else "GF"
    if height <= 78:
        if a_rate >= 18:
            return "G"
        return "GF" if reb_rate < 16 else "F"
    if height <= 80:
        if reb_rate >= 20 or blk_rate >= 4.5:
            return "FC"
        return "F" if a_rate < 18 else "GF"
    if reb_rate >= 22 or blk_rate >= 6:
        return "C"
    return "FC"


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


def normalize_text_or_na(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    text = str(value).strip()
    return text if text else "N/A"


def american_to_decimal(price: float | int | None) -> float | None:
    if price in (None, 0):
        return None
    price = float(price)
    if price > 0:
        return 1.0 + price / 100.0
    return 1.0 + 100.0 / abs(price)


def format_american(price: float | int | None) -> str:
    if price is None:
        return "NA"
    price = int(round(float(price)))
    return f"+{price}" if price > 0 else str(price)


def base_market_book_name(book_key: str, entry: dict[str, Any] | None = None) -> str:
    if isinstance(entry, dict):
        stored = entry.get("_base_book")
        if isinstance(stored, str) and stored:
            return stored
    return str(book_key).split(MARKET_VARIANT_KEY_SEPARATOR, 1)[0]


def market_book_display_name(book_key: str, entry: dict[str, Any] | None = None) -> str:
    base_book = base_market_book_name(book_key, entry)
    variant = entry.get("_variant") if isinstance(entry, dict) else None
    if variant:
        return f"{base_book} [{variant}]"
    return base_book


def get_xai_api_key() -> str | None:
    return os.getenv("XAI_API_KEY") or os.getenv("X_AI_API_KEY")


def create_xai_chat():
    if Client is None or xai_user is None or web_search is None or x_search is None:
        logger.warning("xAI SDK unavailable; skipping xAI bet scoring")
        return None

    api_key = get_xai_api_key()
    if not api_key:
        logger.warning("xAI API key missing; skipping xAI bet scoring")
        return None

    client = Client(api_key=api_key, timeout=XAI_RPC_TIMEOUT_SECONDS)
    return client.chat.create(
        model="grok-4-1-fast-reasoning",
        tools=[
            web_search(),
            x_search(),
        ],
    )


def parse_xai_score_response(text: str) -> tuple[int | str, str]:
    raw = str(text or "").strip()
    if not raw:
        return "N/A", "N/A"

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return "N/A", raw[:200] or "N/A"

    if not isinstance(parsed, dict):
        return "N/A", raw[:200] or "N/A"

    score = parsed.get("betScore")
    notes = str(parsed.get("evaluationNotes") or "").strip()
    try:
        score_value = int(score)
    except (TypeError, ValueError):
        score_value = "N/A"
    else:
        if not (1 <= score_value <= 100):
            score_value = "N/A"

    return score_value, notes or "N/A"


def parse_xai_response_payload(payload: Any) -> tuple[int | str, str]:
    if isinstance(payload, dict):
        score = payload.get("betScore")
        notes = str(payload.get("evaluationNotes") or "").strip()
        try:
            score_value = int(score)
        except (TypeError, ValueError):
            score_value = "N/A"
        else:
            if not (1 <= score_value <= 100):
                score_value = "N/A"
        return score_value, notes or "N/A"
    return parse_xai_score_response("" if payload is None else json.dumps(payload))


def load_recent_xai_bet_cache() -> dict[str, Any]:
    CACHE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cache: dict[str, Any] = {}
    files = sorted(
        (path for path in CACHE_OUTPUT_DIR.iterdir() if path.is_file() and path.suffix.lower() == ".json"),
        key=lambda path: (path.stat().st_mtime, path.name),
    )
    for path in files:
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Skipping unreadable xAI bet cache file %s: %s", path, exc)
            continue
        if not isinstance(payload, dict):
            logger.warning("Skipping malformed xAI bet cache file %s: expected object", path)
            continue
        for key, value in payload.items():
            cache[str(key)] = value
    logger.info("Loaded xAI bet cache entries=%s files=%s from %s", len(cache), len(files), CACHE_OUTPUT_DIR)
    return cache


def estimate_xai_bet_grading_workload(output_df: pd.DataFrame) -> dict[str, Any]:
    if output_df.empty or "_xai_prompt" not in output_df.columns:
        return {
            "total_rows": int(len(output_df)),
            "eligible_bets": 0,
            "cache_hits": 0,
            "api_calls_needed": 0,
            "unique_eligible_cache_keys": 0,
        }

    cache_entries = load_recent_xai_bet_cache()
    eligible_mask = output_df["_xai_prompt"].notna() & output_df["_xai_prompt"].astype(str).ne("")
    eligible_rows = output_df.loc[eligible_mask]
    cache_hits = 0
    eligible_keys: list[str] = []
    for _, row in eligible_rows.iterrows():
        cache_key = build_xai_bet_cache_key(
            str(row["_xai_player_name"]),
            str(row["_xai_team"]),
            str(row["_xai_stat"]),
            row["_xai_line"],
        )
        eligible_keys.append(cache_key)
        if cache_key in cache_entries:
            cache_hits += 1

    return {
        "total_rows": int(len(output_df)),
        "eligible_bets": int(len(eligible_rows)),
        "cache_hits": int(cache_hits),
        "api_calls_needed": int(len(eligible_rows) - cache_hits),
        "unique_eligible_cache_keys": int(len(set(eligible_keys))),
    }


def write_xai_bet_cache_snapshot(entries: dict[str, Any]) -> Path | None:
    if not entries:
        logger.info("Skipping xAI bet cache snapshot; no analyzed bets")
        return None
    CACHE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_OUTPUT_DIR / f"xai_bet_cache_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.json"
    path.write_text(json.dumps(entries, ensure_ascii=False, indent=2, sort_keys=True))
    logger.info("Wrote xAI bet cache snapshot entries=%s path=%s", len(entries), path)
    return path


def log_xai_scoring_request_and_response(
    *,
    player_name: str,
    team: str,
    stat: str,
    bet_side: str,
    expected_value: float,
    prompt: str,
    response_text: str,
    score: int | str,
    context: str,
) -> None:
    subject = f"{player_name} | {team} | {stat} | {bet_side} | EV={expected_value:.4f}"
    logger.info("xAI scoring request for %s:\n%s", subject, prompt)
    logger.info("xAI scoring raw response for %s:\n%s", subject, response_text or "<no response>")
    logger.info(
        "xAI scoring parsed result for %s: score=%s context=%s",
        subject,
        score,
        context,
    )


def parse_unabated_input(unabated_path: Path, target_date: str | None = None) -> dict[str, Any]:
    payload = json.loads(unabated_path.read_text())
    if "people" in payload and "odds" in payload:
        parser_module = load_module(REPO_ROOT / "scripts" / "parse_unabated.py", "parse_unabated_module")
        return parser_module.parse_unabated(payload, target_date=target_date)
    if any(
        isinstance(stats, dict) and "__meta__" not in stats
        for stats in payload.values()
    ):
        raise SystemExit(
            f"Parsed Unabated input missing __meta__ team metadata: {unabated_path}. "
            "Use the raw Unabated payload or regenerate the parsed file with scripts/parse_unabated.py."
        )
    return payload


def build_market_lookup(markets: dict[str, Any]) -> dict[str, Any]:
    lookup: dict[str, Any] = {
        "exact": defaultdict(list),
        "canonical": defaultdict(list),
        "exact_by_team": defaultdict(list),
        "canonical_by_team": defaultdict(list),
    }
    for storage_key, stats in markets.items():
        meta = stats.get("__meta__", {}) if isinstance(stats, dict) else {}
        name = meta.get("player_name") or storage_key
        team = meta.get("team")
        normalized_team = meta.get("normalized_team") or canonicalize_team_name(team)
        entry = {
            "storage_key": storage_key,
            "player_name": name,
            "team": team,
            "normalized_team": normalized_team,
            "stats": stats,
        }
        lookup["exact"][normalize_name(name)].append(entry)
        lookup["canonical"][canonicalize_player_name(name)].append(entry)
        if normalized_team:
            lookup["exact_by_team"][(normalize_name(name), normalized_team)].append(entry)
            lookup["canonical_by_team"][(canonicalize_player_name(name), normalized_team)].append(entry)
    lookup["canonical_keys"] = sorted(lookup["canonical"].keys())
    return lookup


def same_name_signature(left: str, right: str) -> bool:
    left_tokens = left.split()
    right_tokens = right.split()
    if not left_tokens or not right_tokens:
        return False
    return left_tokens[-1] == right_tokens[-1] and left_tokens[0][0] == right_tokens[0][0]


def resolve_market_player(
    player_name: str,
    team_name: str | dict[str, Any] | None,
    market_lookup: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]] | None:
    if market_lookup is None and isinstance(team_name, dict):
        market_lookup = team_name
        team_name = None
    if market_lookup is None:
        raise TypeError("market_lookup is required")

    exact_key = normalize_name(player_name)
    normalized_team = canonicalize_team_name(team_name)
    exact_matches = market_lookup["exact_by_team"].get((exact_key, normalized_team), []) if normalized_team else []
    if not exact_matches:
        exact_matches = market_lookup["exact"].get(exact_key, [])
    if len(exact_matches) == 1:
        match = exact_matches[0]
        return match["player_name"], match["stats"]

    canonical_key = canonicalize_player_name(player_name)
    canonical_matches = (
        market_lookup["canonical_by_team"].get((canonical_key, normalized_team), [])
        if normalized_team
        else []
    )
    if not canonical_matches:
        canonical_matches = market_lookup["canonical"].get(canonical_key, [])
    if len(canonical_matches) == 1:
        match = canonical_matches[0]
        return match["player_name"], match["stats"]
    if not canonical_key:
        return None

    close_key_source = (
        [key for key, team in market_lookup["canonical_by_team"].keys() if team == normalized_team]
        if normalized_team
        else market_lookup["canonical_keys"]
    )
    close_keys = difflib.get_close_matches(canonical_key, close_key_source, n=3, cutoff=0.92)
    ranked_matches = []
    for candidate_key in close_keys:
        if not same_name_signature(canonical_key, candidate_key):
            continue
        if normalized_team:
            candidate_matches = market_lookup["canonical_by_team"].get((candidate_key, normalized_team), [])
        else:
            candidate_matches = market_lookup["canonical"].get(candidate_key, [])
        if len(candidate_matches) != 1:
            continue
        score = difflib.SequenceMatcher(a=canonical_key, b=candidate_key).ratio()
        ranked_matches.append((score, candidate_matches[0]))

    if not ranked_matches:
        return None

    ranked_matches.sort(key=lambda item: item[0], reverse=True)
    best_score, best_match = ranked_matches[0]
    next_score = ranked_matches[1][0] if len(ranked_matches) > 1 else 0.0
    if best_score >= 0.95 and (len(ranked_matches) == 1 or best_score - next_score >= 0.03):
        return best_match["player_name"], best_match["stats"]
    return None


def load_sim_data(sim_path: Path) -> dict[str, Any]:
    return json.loads(sim_path.read_text())


def load_team_averages(team_averages_path: Path = TEAM_AVERAGES_PATH) -> dict[str, dict[str, Any]]:
    if not team_averages_path.exists():
        return {}
    payload = json.loads(team_averages_path.read_text())
    teams = payload.get("teams", {})
    return teams if isinstance(teams, dict) else {}


def build_team_projection_summaries(
    projections: pd.DataFrame,
    team_averages: dict[str, dict[str, Any]],
) -> dict[str, str]:
    stat_map = [
        ("PROJ REB", "rpg", "Team REB"),
        ("PROJ AST", "apg", "Team AST"),
        ("PROJ 3PM", "three_pm", "Team 3PM"),
        ("PROJ STL", "spg", "Team STL"),
        ("PROJ BLK", "bpg", "Team BLK"),
        ("PROJ TO", "topg", "Team TO"),
    ]
    summaries: dict[str, str] = {}
    if projections.empty or "Team" not in projections.columns:
        return summaries

    for team_name, team_rows in projections.groupby("Team", dropna=True):
        display_name = display_team(team_name)
        canonical_team = canonicalize_team_name(display_name)
        season_stats = team_averages.get(canonical_team, {})
        parts = []
        for projection_col, season_key, label in stat_map:
            total = safe_float(team_rows.get(projection_col).sum()) if projection_col in team_rows.columns else float("nan")
            season_avg = safe_float(season_stats.get(season_key)) if isinstance(season_stats, dict) else float("nan")
            if math.isnan(total) or math.isnan(season_avg):
                continue
            parts.append(f"{label}: {total - season_avg:.2f}")
        if parts:
            summaries[canonical_team] = ", ".join(parts)
    return summaries


def build_team_average_fallbacks(team_averages: dict[str, dict[str, Any]]) -> dict[str, str]:
    stat_map = [
        ("rpg", "Team REB"),
        ("apg", "Team AST"),
        ("three_pm", "Team 3PM"),
        ("spg", "Team STL"),
        ("bpg", "Team BLK"),
        ("topg", "Team TO"),
    ]
    fallbacks = {}
    for team_name, stats in team_averages.items():
        if not isinstance(stats, dict):
            continue
        parts = []
        for stat_key, label in stat_map:
            value = safe_float(stats.get(stat_key))
            if math.isnan(value):
                continue
            parts.append(f"{label}: {value:.2f}")
        if parts:
            fallbacks[canonicalize_team_name(team_name)] = ", ".join(parts)
    return fallbacks


def build_team_projected_stats(projections: pd.DataFrame) -> dict[str, dict[str, float]]:
    stat_map = {
        "BLK": "PROJ BLK",
        "REB": "PROJ REB",
        "AST": "PROJ AST",
        "STL": "PROJ STL",
        "TO": "PROJ TO",
        "3PM": "PROJ 3PM",
        "PTS": "PROJ PTS",
    }
    results: dict[str, dict[str, float]] = {}
    if projections.empty or "Team" not in projections.columns:
        return results

    for team_name, team_rows in projections.groupby("Team", dropna=True):
        team_stats: dict[str, float] = {}
        for out_key, column in stat_map.items():
            if column not in team_rows.columns:
                continue
            value = safe_float(team_rows[column].sum())
            if math.isnan(value):
                continue
            team_stats[out_key] = round(value, 4)
        results[canonicalize_team_name(display_team(team_name))] = team_stats
    return results


def build_team_season_average_stats(team_averages: dict[str, dict[str, Any]]) -> dict[str, dict[str, float]]:
    stat_map = {
        "BLK": "bpg",
        "REB": "rpg",
        "AST": "apg",
        "STL": "spg",
        "TO": "topg",
        "3PM": "three_pm",
        "PTS": "ppg",
    }
    results: dict[str, dict[str, float]] = {}
    for team_name, stats in team_averages.items():
        if not isinstance(stats, dict):
            continue
        team_stats: dict[str, float] = {}
        for out_key, stat_key in stat_map.items():
            value = safe_float(stats.get(stat_key))
            if math.isnan(value):
                continue
            team_stats[out_key] = round(value, 4)
        results[canonicalize_team_name(team_name)] = team_stats
    return results


def bucket_grid_for_stat(sim_data: dict[str, Any], sim_stat: str, bucket: str) -> list[dict[str, Any]]:
    raw = sim_data.get(sim_stat, {}).get(bucket, {})
    grid = []
    for _, obj in raw.items():
        mean = safe_float(obj.get("mean"))
        if math.isnan(mean):
            continue
        pmf = {}
        for entry in obj.get("simulationDetails", []):
            if not entry.get("isWholeNumber"):
                continue
            total = int(round(safe_float(entry.get("total"), -1)))
            if total < 0:
                continue
            pmf[total] = safe_float(entry.get("occurrences"), 0.0) / 10000.0
        if pmf:
            grid.append({"mean": mean, "pmf": pmf})
    grid.sort(key=lambda item: item["mean"])
    return grid


def poisson_pmf(mean: float) -> dict[int, float]:
    mean = max(mean, 0.0)
    if mean == 0:
        return {0: 1.0}
    max_total = max(12, int(math.ceil(mean + 8.0 * math.sqrt(mean + 1.0))))
    pmf = {}
    total_prob = 0.0
    for k in range(max_total + 1):
        prob = math.exp(-mean) * (mean ** k) / math.factorial(k)
        pmf[k] = prob
        total_prob += prob
    if total_prob < 1.0:
        pmf[max_total] += 1.0 - total_prob
    return pmf


def blend_pmfs(left: dict[int, float], right: dict[int, float], weight: float) -> dict[int, float]:
    keys = set(left) | set(right)
    return {key: (1.0 - weight) * left.get(key, 0.0) + weight * right.get(key, 0.0) for key in keys}


def closest_distribution(sim_data: dict[str, Any], sim_stat: str, bucket: str, projection: float) -> tuple[dict[int, float], float, str]:
    grid = bucket_grid_for_stat(sim_data, sim_stat, bucket)
    if not grid:
        raise KeyError(f"No simulation grid for stat={sim_stat} bucket={bucket}")
    if projection <= grid[0]["mean"] or projection >= grid[-1]["mean"]:
        return poisson_pmf(projection), projection, "poisson_fallback"

    for idx in range(1, len(grid)):
        left = grid[idx - 1]
        right = grid[idx]
        if projection <= right["mean"]:
            span = right["mean"] - left["mean"]
            weight = 0.0 if span <= 0 else (projection - left["mean"]) / span
            return blend_pmfs(left["pmf"], right["pmf"], weight), projection, "interpolated_sim"

    return poisson_pmf(projection), projection, "poisson_fallback"


def convolve_pmfs(pmfs: list[dict[int, float]]) -> dict[int, float]:
    result = {0: 1.0}
    for pmf in pmfs:
        next_result = defaultdict(float)
        for total_a, prob_a in result.items():
            for total_b, prob_b in pmf.items():
                next_result[total_a + total_b] += prob_a * prob_b
        result = dict(next_result)
    return result


def pmf_probabilities(pmf: dict[int, float], line: float) -> tuple[float, float, float]:
    over = 0.0
    under = 0.0
    push = 0.0
    for total, prob in pmf.items():
        if total > line:
            over += prob
        elif total < line:
            under += prob
        else:
            push += prob
    return over, under, push


def is_valid_grading_line(value: Any) -> bool:
    try:
        line = float(value)
    except (TypeError, ValueError):
        return False
    return not math.isnan(line) and line != 0.0


def full_market_metadata(books: dict[str, Any]) -> str:
    parts = []
    for book, entry in sorted(books.items()):
        if book == "avgObj":
            continue
        base_book = base_market_book_name(book, entry if isinstance(entry, dict) else None)
        if base_book not in ALLOWED_GRADING_BOOKS:
            continue
        line = entry.get("line")
        if not is_valid_grading_line(line):
            continue
        over = format_american(entry.get("over"))
        under = format_american(entry.get("under"))
        parts.append(f"{market_book_display_name(book, entry)} {line} O {over}; U {under}")
    return " | ".join(parts)


def best_candidate_for_market(
    player_name: str,
    market_stat: str,
    books: dict[str, Any],
    projection_value: float,
    pmf: dict[int, float],
) -> dict[str, Any] | None:
    metadata = full_market_metadata(books)
    best = None
    for book, entry in books.items():
        if book == "avgObj":
            continue
        base_book = base_market_book_name(book, entry)
        if base_book not in ALLOWED_GRADING_BOOKS:
            continue
        line = entry.get("line")
        if not is_valid_grading_line(line):
            continue
        over_prob, under_prob, push_prob = pmf_probabilities(pmf, float(line))
        for side, win_prob in (("over", over_prob), ("under", under_prob)):
            price = entry.get(side)
            decimal_odds = american_to_decimal(price)
            if decimal_odds is None:
                continue
            expected_value = win_prob * decimal_odds
            expected_profit = win_prob * (decimal_odds - 1.0) - (1.0 - win_prob - push_prob)
            candidate = {
                "Player": player_name,
                "stat": market_stat,
                "bet_side": side,
                "stat_projection": round(projection_value, 4),
                "line": line,
                "odds": int(round(float(price))),
                "book": base_book,
                "win_pct": round(win_prob, 2),
                "push_likelihood": round(push_prob, 4),
                "expected_value": round(expected_value, 4),
                "full_odds_meta_data": metadata,
            }
            if best is None or candidate["expected_value"] > best["expected_value"]:
                best = candidate
    return best


def run_model(model_type: str, kenpom_file: Path) -> tuple[pd.DataFrame, Path]:
    if model_type != "existing":
        raise ValueError(f"Unsupported model_type: {model_type}")

    module_path = REPO_ROOT / "scripts" / "projection_model.py"
    output_name = f"pipeline_{build_output_stem(model_type, kenpom_file)}.xlsx"

    module = load_module(module_path, f"{model_type}_projection_module")
    df = module.read_player_stats(str(kenpom_file), sheet="PlayerStats")
    df_result = module.run_projections(df)
    output_path = MODEL_OUTPUT_DIR / output_name
    module.write_output(df_result, output_path)
    return df_result, output_path


def extract_target_teams_from_markets(markets: dict[str, Any]) -> set[str]:
    teams: set[str] = set()
    for stats in markets.values():
        if not isinstance(stats, dict):
            continue
        meta = stats.get("__meta__", {})
        normalized_team = meta.get("normalized_team") or canonicalize_team_name(meta.get("team"))
        if normalized_team:
            teams.add(normalized_team)
    return teams


def load_player_stats_from_workbook(kenpom_file: Path) -> pd.DataFrame:
    return pd.read_excel(kenpom_file, sheet_name="PlayerStats")


def merge_player_stats_context(projections: pd.DataFrame, player_stats: pd.DataFrame | None) -> pd.DataFrame:
    if player_stats is None or player_stats.empty:
        return projections

    def normalize_merge_frame(df: pd.DataFrame) -> pd.DataFrame:
        index_names = [
            name for name in (df.index.names if isinstance(df.index, pd.MultiIndex) else [df.index.name])
            if name is not None
        ]
        merge_key_overlap = {"Name", "Team"} & set(index_names) & set(df.columns)
        if merge_key_overlap:
            return df.reset_index(drop=True)
        return df

    projections = normalize_merge_frame(projections)
    player_stats = normalize_merge_frame(player_stats)

    available_context = [col for col in PLAYER_STATS_CONTEXT_COLUMNS if col in player_stats.columns]
    if "Name" not in available_context or "Team" not in available_context:
        return projections

    context_df = player_stats[available_context].copy()
    merged = projections.copy()
    if "Name" not in merged.columns or "Team" not in merged.columns:
        raise RuntimeError("Projections input must include Name and Team columns")

    merged = merged.merge(context_df, on=["Name", "Team"], how="left", suffixes=("", "__ctx"))
    for col in available_context:
        if col in {"Name", "Team"}:
            continue
        context_col = f"{col}__ctx"
        if context_col not in merged.columns:
            continue
        if col not in projections.columns:
            merged[col] = merged[context_col]
        else:
            merged[col] = merged[col].where(merged[col].notna(), merged[context_col])
        merged = merged.drop(columns=[context_col])
    return merged


def group_team_statuses_from_player_df(player_df: pd.DataFrame) -> list[dict[str, Any]]:
    if player_df.empty or "Team" not in player_df.columns:
        return []
    team_statuses: list[dict[str, Any]] = []
    for team_name, team_df in player_df.groupby("Team", sort=False, dropna=True):
        team_statuses.append(
            {
                "team": display_team(team_name),
                "df": team_df.copy(),
                "recent_lineup_context": None,
                "xai_status": "not_attempted",
                "issues": [],
                "response_text": None,
                "low_confidence_adjustment_status": "not_attempted",
                "low_confidence_adjustment_issues": [],
                "low_confidence_response_text": None,
            }
        )
    return team_statuses


def combine_team_status_frames(team_statuses: list[dict[str, Any]]) -> pd.DataFrame:
    frames = [status["df"] for status in team_statuses if status.get("df") is not None]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def resolve_target_scrape_teams(team_statuses: list[dict[str, Any]], target_teams: set[str]) -> set[str]:
    resolved = set()
    for status in team_statuses:
        if canonicalize_team_name(status.get("team")) in target_teams:
            resolved.add(status["team"])
    return resolved


def write_augmented_kenpom_workbook(source_workbook: Path, player_df: pd.DataFrame, output_path: Path) -> None:
    shutil.copy2(source_workbook, output_path)
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        player_df.to_excel(writer, sheet_name="PlayerStats", index=False)


def save_minutes_stage_outputs(
    *,
    source_workbook: Path,
    player_df: pd.DataFrame,
    output_stem: str,
) -> tuple[Path, Path, Path]:
    MINUTES_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = MINUTES_OUTPUT_DIR / f"{output_stem}_player_stats.csv"
    xlsx_path = MINUTES_OUTPUT_DIR / f"{output_stem}_player_stats.xlsx"
    workbook_path = MINUTES_OUTPUT_DIR / f"{output_stem}_workbook.xlsx"
    player_df.to_csv(csv_path, index=False)
    player_df.to_excel(xlsx_path, sheet_name="PlayerStats", index=False)
    write_augmented_kenpom_workbook(source_workbook, player_df, workbook_path)
    return csv_path, xlsx_path, workbook_path


def hydrate_recent_lineup_contexts_for_workbook(main_module, team_statuses: list[dict[str, Any]]) -> None:
    if not team_statuses:
        return

    try:
        browser = main_module.login('twagner55@gmail.com', 'NtnWk3974P')
    except Exception as exc:
        logger.warning("Unable to initialize KenPom browser for lineup hydration: %s", exc)
        return

    try:
        for status in team_statuses:
            team = status.get("team")
            if not team:
                continue
            try:
                status["recent_lineup_context"] = main_module.get_recent_lineup_context(browser, team)
            except Exception as exc:
                logger.warning("Recent lineup hydration failed for %s: %s", team, exc)
                status["recent_lineup_context"] = {
                    "lineups": [],
                    "unknownPctMinutes": None,
                    "coveragePctMinutes": 0.0,
                }
    finally:
        close_method = getattr(browser, "close", None)
        if callable(close_method):
            try:
                close_method()
            except Exception:
                pass


def validate_path_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise RuntimeError(f"{label} was not written: {path}")


def validate_minutes_player_df(
    player_df: pd.DataFrame,
    *,
    stage_label: str,
    require_minutes_projection: bool = True,
) -> None:
    required_columns = {"Team", "Name"}
    if require_minutes_projection:
        required_columns.update({"MinsProj", "MinsProjConfidence"})
    missing = sorted(required_columns - set(player_df.columns))
    if missing:
        raise RuntimeError(f"{stage_label} missing required columns: {', '.join(missing)}")
    if player_df.empty:
        raise RuntimeError(f"{stage_label} produced no player rows")
    name_series = player_df["Name"]
    team_series = player_df["Team"]
    if name_series.isna().any() or name_series.astype(str).str.strip().eq("").any():
        raise RuntimeError(f"{stage_label} contains blank player names")
    if team_series.isna().any() or team_series.astype(str).str.strip().eq("").any():
        raise RuntimeError(f"{stage_label} contains blank team names")
    if player_df.duplicated(subset=["Team", "Name"]).any():
        dupes = player_df.loc[player_df.duplicated(subset=["Team", "Name"], keep=False), ["Team", "Name"]]
        sample = dupes.head(5).to_dict("records")
        raise RuntimeError(f"{stage_label} contains duplicate team/player rows: {sample}")

    if require_minutes_projection:
        mins = pd.to_numeric(player_df["MinsProj"], errors="coerce")
        conf = pd.to_numeric(player_df["MinsProjConfidence"], errors="coerce")
        if mins.isna().any():
            raise RuntimeError(f"{stage_label} contains null/invalid MinsProj values")
        if conf.isna().any():
            raise RuntimeError(f"{stage_label} contains null/invalid MinsProjConfidence values")
        if ((conf < 0) | (conf > 1)).any():
            raise RuntimeError(f"{stage_label} contains out-of-range MinsProjConfidence values")


def validate_projection_output(projections: pd.DataFrame, projection_file: Path) -> None:
    validate_path_exists(projection_file, "Projection workbook")
    required_columns = {"Name", "Team", "MINS PROJ"}
    missing = sorted(required_columns - set(projections.columns))
    if missing:
        raise RuntimeError(f"Projection output missing required columns: {', '.join(missing)}")
    if projections.empty:
        raise RuntimeError("Projection output is empty")
    if pd.to_numeric(projections["MINS PROJ"], errors="coerce").isna().all():
        raise RuntimeError("Projection output has no valid MINS PROJ values")


def validate_final_output_df(output_df: pd.DataFrame, output_path: Path | None = None) -> None:
    required_columns = {"Player", "team", "stat", "expected_value", "xAiScore", "xAiContext"}
    missing = sorted(required_columns - set(output_df.columns))
    if missing:
        raise RuntimeError(f"Final output missing required columns: {', '.join(missing)}")
    if output_df.empty:
        logger.warning("Final output dataframe is empty")
    if output_df.duplicated(subset=["Player", "team", "stat"]).any():
        dupes = output_df.loc[output_df.duplicated(subset=["Player", "team", "stat"], keep=False), ["Player", "team", "stat"]]
        sample = dupes.head(5).to_dict("records")
        raise RuntimeError(f"Final output contains duplicate Player/team/stat rows: {sample}")
    if output_path is not None:
        validate_path_exists(output_path, "Final output CSV")


def prompt_retry_failed_queue_items(label: str, failures: list[dict[str, Any]]) -> bool:
    if not failures:
        return False
    if not sys.stdin.isatty():
        logger.warning("Skipping interactive retry for %s because stdin is not a TTY", label)
        return False

    print(f"\nThe following {label} xAI tasks failed and are in the DLQ:")
    for failure in failures:
        print(f"- {failure['label']}: {failure['error']}")

    while True:
        try:
            answer = input("Retry DLQ tasks? Y to retry, N to continue: ").strip().upper()
        except EOFError:
            return False
        if answer in {"Y", "N"}:
            return answer == "Y"
        print("Please enter Y or N.")


def prompt_to_proceed_with_xai_calls(api_calls_needed: int, cache_hits: int) -> bool:
    if api_calls_needed <= 0:
        return True
    if not sys.stdin.isatty():
        logger.warning(
            "Skipping interactive xAI confirmation because stdin is not a TTY; defaulting to no outbound xAI calls "
            "(api_calls_needed=%s cache_hits=%s)",
            api_calls_needed,
            cache_hits,
        )
        return False

    print(f"\nxAI grading preview: {cache_hits} cache hits, {api_calls_needed} new xAI call(s) needed.")
    while True:
        try:
            answer = input(f"Proceed with {api_calls_needed} xAI call(s)? Y/N: ").strip().upper()
        except EOFError:
            return False
        if answer in {"Y", "N"}:
            return answer == "Y"
        print("Please enter Y or N.")


def apply_last5_fallback_to_status(main_module, status: dict[str, Any], reason: str) -> None:
    status["df"] = main_module.apply_last5_minutes_fallback(status.get("df"), reason=reason)
    if status.get("xai_status") != "ok":
        status["xai_status"] = "fallback_last5"
    if status.get("low_confidence_adjustment_status") not in {"ok", "not_needed"}:
        status["low_confidence_adjustment_status"] = "fallback_last5"


def log_queue_progress(label: str, progress: QueueProgress) -> None:
    logger.info(
        "%s queue %s: completed=%s/%s active=%s queue_depth=%s succeeded=%s failed=%s max_parallel=%s",
        label,
        progress.event,
        progress.completed,
        progress.total,
        progress.active,
        progress.pending,
        progress.succeeded,
        progress.failed,
        progress.max_parallel,
    )


def run_parallel_team_minutes_workflow(
    *,
    main_module,
    team_statuses: list[dict[str, Any]],
    run_follow_up_minutes: bool,
    follow_up_threshold: float,
    minutes_cache: dict[str, Any] | None = None,
) -> None:
    queue = ParallelXAIQueue(max_parallel=MAX_PARALLEL_XAI_CALLS)
    target_statuses = list(team_statuses)
    minutes_cache = minutes_cache if isinstance(minutes_cache, dict) else {"teams": {}}
    total_teams = len(target_statuses)
    logger.info(
        "Starting team minutes workflow for %s teams; follow_up_minutes=%s threshold=%.2f max_parallel=%s",
        total_teams,
        run_follow_up_minutes,
        follow_up_threshold,
        MAX_PARALLEL_XAI_CALLS,
    )

    def initial_worker(status: dict[str, Any]):
        team_cache_entry = get_minutes_cache_team_entry(minutes_cache, status["team"])
        cached_team_minutes = team_cache_entry.get("teamMinutes")
        if isinstance(cached_team_minutes, dict):
            cached_result = cached_team_minutes.get("result")
            cached_projection_data = cached_team_minutes.get("projectionData")
            if isinstance(cached_result, dict) and cached_projection_data is not None:
                cache_issues = main_module.validate_xai_projection_data(status["df"], cached_projection_data)
                if not cache_issues:
                    logger.info("Minutes cache hit for %s", status["team"])
                    return cached_result, cached_projection_data, True
                logger.warning(
                    "Ignoring invalid cached team minutes for %s: %s",
                    status["team"],
                    "; ".join(cache_issues),
                )

        outcome = main_module.call_xai_for_team_minutes_with_retries(
            status["team"],
            status["df"],
            recent_lineup_context=status.get("recent_lineup_context"),
        )
        if outcome is None:
            raise RuntimeError("no response from xAI")
        return outcome[0], outcome[1], False

    pending_statuses = list(target_statuses)
    while pending_statuses:
        logger.info(
            "Submitting initial team minutes batch: pending_teams=%s processed=%s remaining=%s",
            len(pending_statuses),
            sum(1 for status in target_statuses if status.get("xai_status") in {"ok", "fallback_last5"}),
            len(pending_statuses),
        )
        run_result = queue.run(
            pending_statuses,
            initial_worker,
            progress_callback=lambda progress: log_queue_progress("Team minutes", progress),
        )
        for status, outcome in run_result.successes:
            xai_result, projection_data, from_cache = outcome
            status["response_text"] = xai_result["text"]
            status["minutes_cache_hit"] = bool(from_cache)
            issues = main_module.validate_xai_projection_data(status["df"], projection_data)
            if issues:
                status["xai_status"] = "malformed"
                status["issues"] = issues
                logger.warning(
                    "Initial team minutes validation failed for %s; teams_processed=%s/%s teams_remaining=%s issues=%s",
                    status["team"],
                    sum(1 for item in target_statuses if item.get("xai_status") == "ok"),
                    total_teams,
                    sum(1 for item in target_statuses if item.get("xai_status") not in {"ok", "fallback_last5"}),
                    "; ".join(issues),
                )
                continue
            updated_df = main_module.apply_xai_minutes_projection(status["df"], projection_data)
            status["df"] = main_module.rebalance_team_minutes(updated_df)
            status["xai_status"] = "ok"
            status["issues"] = []
            if not from_cache:
                team_cache_entry = get_minutes_cache_team_entry(minutes_cache, status["team"])
                team_cache_entry["teamMinutes"] = {
                    "projectionData": projection_data,
                    "result": xai_result,
                    "updatedAt": datetime.now().isoformat(),
                }
            processed_count = sum(1 for item in target_statuses if item.get("xai_status") == "ok")
            logger.info(
                "Initial team minutes complete for %s; teams_processed=%s/%s teams_remaining=%s",
                status["team"],
                processed_count,
                total_teams,
                total_teams - processed_count,
            )

        dlq_failures: list[dict[str, Any]] = []
        next_pending: list[dict[str, Any]] = []
        for status in target_statuses:
            if status.get("xai_status") == "malformed":
                dlq_failures.append(
                    {
                        "status": status,
                        "label": status["team"],
                        "error": "; ".join(status.get("issues", [])) or "malformed response",
                    }
                )
        for failure in run_result.dlq:
            status = failure.task
            status["xai_status"] = "failed"
            status["issues"] = [failure.error]
            dlq_failures.append({"status": status, "label": status["team"], "error": failure.error})
        for status in target_statuses:
            if status.get("xai_status") in {"failed", "malformed"}:
                next_pending.append(status)

        if next_pending and prompt_retry_failed_queue_items("team-minutes", dlq_failures):
            pending_statuses = next_pending
            continue
        for status in next_pending:
            apply_last5_fallback_to_status(main_module, status, "xai_minutes_failure")
            completed_count = sum(
                1 for item in target_statuses if item.get("xai_status") in {"ok", "fallback_last5"}
            )
            logger.warning(
                "Falling back to last-5 minutes for %s; teams_completed=%s/%s teams_remaining=%s",
                status["team"],
                completed_count,
                total_teams,
                total_teams - completed_count,
            )
        break

    final_minutes_ok = sum(1 for status in target_statuses if status.get("xai_status") == "ok")
    final_minutes_fallback = sum(1 for status in target_statuses if status.get("xai_status") == "fallback_last5")
    logger.info(
        "Initial team minutes stage complete: total_teams=%s xai_success=%s fallback_last5=%s failed=%s",
        total_teams,
        final_minutes_ok,
        final_minutes_fallback,
        total_teams - final_minutes_ok - final_minutes_fallback,
    )

    if not run_follow_up_minutes:
        for status in target_statuses:
            if status.get("xai_status") == "ok":
                status["low_confidence_adjustment_status"] = "skipped"
        logger.info("Follow-up minutes disabled; skipped follow-up review for %s xAI-completed teams", final_minutes_ok)
        return

    follow_up_candidates = []
    for status in target_statuses:
        if status.get("xai_status") != "ok":
            continue
        low_confidence_players = main_module._low_confidence_players(status["df"], threshold=follow_up_threshold)
        status["low_confidence_players"] = low_confidence_players
        if not low_confidence_players:
            status["low_confidence_adjustment_status"] = "not_needed"
            status["low_confidence_adjustment_issues"] = []
            continue
        follow_up_candidates.append(status)

    logger.info(
        "Follow-up minutes candidate selection complete: candidate_teams=%s teams_without_follow_up_need=%s threshold=%.2f",
        len(follow_up_candidates),
        sum(1 for status in target_statuses if status.get("low_confidence_adjustment_status") == "not_needed"),
        follow_up_threshold,
    )

    def follow_up_worker(status: dict[str, Any]):
        overlay_status = {
            "recent_lineup_context": status.get("recent_lineup_context"),
        }
        updated_df = main_module.process_low_confidence_minutes(
            status["team"],
            status["df"],
            overlay_status,
            run_follow_up=True,
            follow_up_threshold=follow_up_threshold,
            minutes_cache=minutes_cache,
        )
        return updated_df, overlay_status

    pending_follow_ups = list(follow_up_candidates)
    while pending_follow_ups:
        logger.info(
            "Submitting follow-up minutes batch: pending_teams=%s processed=%s remaining=%s",
            len(pending_follow_ups),
            sum(
                1
                for status in follow_up_candidates
                if status.get("low_confidence_adjustment_status") in {"ok", "not_needed"}
            ),
            len(pending_follow_ups),
        )
        run_result = queue.run(
            pending_follow_ups,
            follow_up_worker,
            progress_callback=lambda progress: log_queue_progress("Follow-up minutes", progress),
        )
        for status, outcome in run_result.successes:
            updated_df, overlay_status = outcome
            status["df"] = updated_df
            for key, value in overlay_status.items():
                status[key] = value
            status["low_confidence_adjustment_status"] = "ok"
            status["low_confidence_adjustment_issues"] = []
            completed_count = sum(
                1
                for item in follow_up_candidates
                if item.get("low_confidence_adjustment_status") == "ok"
            )
            logger.info(
                "Follow-up minutes complete for %s; teams_processed=%s/%s teams_remaining=%s",
                status["team"],
                completed_count,
                len(follow_up_candidates),
                len(follow_up_candidates) - completed_count,
            )

        dlq_failures = []
        next_pending = []
        for failure in run_result.dlq:
            status = failure.task
            status["low_confidence_adjustment_status"] = "failed"
            status["low_confidence_adjustment_issues"] = [failure.error]
            dlq_failures.append(
                {"status": status, "label": status["team"], "error": failure.error}
            )
        for status in follow_up_candidates:
            if status.get("low_confidence_adjustment_status") in {"failed", "malformed"}:
                next_pending.append(status)

        if next_pending and prompt_retry_failed_queue_items("follow-up-minutes", dlq_failures):
            pending_follow_ups = next_pending
            continue
        break

    logger.info(
        "Follow-up minutes stage complete: candidate_teams=%s succeeded=%s not_needed=%s failed=%s",
        len(follow_up_candidates),
        sum(1 for status in follow_up_candidates if status.get("low_confidence_adjustment_status") == "ok"),
        sum(1 for status in target_statuses if status.get("low_confidence_adjustment_status") == "not_needed"),
        sum(1 for status in follow_up_candidates if status.get("low_confidence_adjustment_status") == "failed"),
    )


def projection_value_for_market(row: pd.Series, market_stat: str) -> float | None:
    if market_stat in PROJECTION_STAT_MAP:
        value = safe_float(row.get(PROJECTION_STAT_MAP[market_stat]))
        return None if math.isnan(value) else value
    if market_stat in COMBINED_COMPONENTS:
        components = COMBINED_COMPONENTS[market_stat]
        total = 0.0
        for component in components:
            value = projection_value_for_market(row, component)
            if value is None:
                return None
            total += value
        return total
    return None


def component_projections_for_market(row: pd.Series, market_stat: str) -> dict[str, float]:
    if market_stat not in COMBINED_COMPONENTS:
        return {}
    components: dict[str, float] = {}
    for component in COMBINED_COMPONENTS[market_stat]:
        value = projection_value_for_market(row, component)
        if value is not None:
            components[component] = round(value, 4)
    return components


def build_xai_bet_scoring_prompt(
    *,
    player_name: str,
    team: str,
    opponent: str,
    minutes_projection: float,
    last_5_games_minutes_log: list[float | int | None],
    stat: str,
    bet_side: str,
    over_under_line: float,
    stat_projection: float,
    win_likelihood: float,
    component_projections: dict[str, float] | None,
    team_projected_stats: dict[str, float],
    team_season_averages: dict[str, float],
    minutes_projection_confidence: float | None = None,
    minutes_injury_summary: str | None = None,
    minutes_confidence_justification: str | None = None,
) -> str:
    data = {
        "player": player_name,
        "team": team,
        "opponent": opponent,
        "bet": {
            "stat": stat,
            "side": bet_side,
            "line": over_under_line,
            "projection": round(stat_projection, 4),
            "winLikelihood": round(win_likelihood, 4),
            "componentProjections": component_projections or {},
        },
        "minutes": {
            "projection": round(minutes_projection, 4),
            "confidence": None if minutes_projection_confidence is None or math.isnan(minutes_projection_confidence) else round(minutes_projection_confidence, 4),
            "last5": last_5_games_minutes_log,
            "injurySummary": minutes_injury_summary or "N/A",
            "confidenceJustification": minutes_confidence_justification or "N/A",
        },
        "teamProjectedStats": team_projected_stats,
        "teamSeasonAverages": team_season_averages,
    }

    return (
        "Score this specific college basketball player prop from 1-100 after adversarial review.\n"
        "Start by trying to disprove the bet.\n"
        "Use x_search and web_search to check recent news, injuries, lineup or rotation changes, coach quotes, foul trouble, role changes, and minutes trends for the player.\n"
        "Attack the minutes projection first. Then judge whether the stat projection is reasonable versus the recent minutes log, recent game-by-game production in this stat, the player's likely role, and the team projected stat environment versus team season averages.\n"
        "For combined bets, evaluate both the combined projection and each component projection separately, and compare them to recent component production.\n"
        "Score the bet itself, not the player overall. Lower the score for unstable minutes, weak role certainty, fragile recent production, stale assumptions, or news that makes the edge less trustworthy.\n"
        'Return exactly one JSON object with this exact schema: {"betScore":1,"evaluationNotes":"100-300 chars"}\n'
        "Rules: betScore must be an integer 1-100. evaluationNotes must be 100-300 characters. No markdown. No extra keys. No extra text.\n"
        f"Data: {json.dumps(data, separators=(',', ':'))}"
    )


def score_bet_with_xai(
    prompt: str,
    *,
    player_name: str,
    team: str,
    stat: str,
    line: float,
    bet_side: str,
    expected_value: float,
) -> tuple[int | str, str, Any]:
    chat = create_xai_chat()
    if chat is None:
        return "N/A", "N/A", {"betScore": "N/A", "evaluationNotes": "N/A"}

    logger.info(
        "Starting xAI bet scoring call for %s | %s | %s | line=%s | %s | EV=%.4f",
        player_name,
        team,
        stat,
        format_cache_line_value(line),
        bet_side,
        expected_value,
    )
    chat.append(xai_user(prompt))

    response = None
    text_parts: list[str] = []
    for response, chunk in chat.stream():
        if getattr(chunk, "content", None):
            text_parts.append(chunk.content)

    result_text = "".join(text_parts).strip()
    score, context = parse_xai_score_response(result_text)
    try:
        response_payload: Any = json.loads(result_text) if result_text else {"betScore": "N/A", "evaluationNotes": "N/A"}
    except json.JSONDecodeError:
        response_payload = {"rawResponse": result_text or "N/A"}
    logger.info(
        "Completed xAI bet scoring call for %s | %s | %s | line=%s | %s | EV=%.4f; response_chars=%s citations=%s",
        player_name,
        team,
        stat,
        format_cache_line_value(line),
        bet_side,
        expected_value,
        len(result_text),
        len(getattr(response, "citations", []) if response is not None else []),
    )
    log_xai_scoring_request_and_response(
        player_name=player_name,
        team=team,
        stat=stat,
        bet_side=bet_side,
        expected_value=expected_value,
        prompt=prompt,
        response_text=result_text,
        score=score,
        context=context,
    )
    return score, context, response_payload


def market_distribution(
    row: pd.Series,
    market_stat: str,
    bucket: str,
    sim_data: dict[str, Any],
) -> tuple[dict[int, float], list[float], list[str]]:
    if market_stat in PROJECTION_STAT_MAP:
        projection = projection_value_for_market(row, market_stat)
        if projection is None:
            raise KeyError(f"Missing projection for {market_stat}")
        pmf, matched_mean, source = closest_distribution(sim_data, SIM_STAT_MAP[market_stat], bucket, projection)
        return pmf, [matched_mean], [source]

    if market_stat in COMBINED_COMPONENTS:
        pmfs = []
        matched_means = []
        sources = []
        for component in COMBINED_COMPONENTS[market_stat]:
            projection = projection_value_for_market(row, component)
            if projection is None:
                raise KeyError(f"Missing projection for {component}")
            pmf, matched_mean, source = closest_distribution(sim_data, SIM_STAT_MAP[component], bucket, projection)
            pmfs.append(pmf)
            matched_means.append(matched_mean)
            sources.append(source)
        return convolve_pmfs(pmfs), matched_means, sources

    raise KeyError(f"Unsupported market stat: {market_stat}")


def build_rows(
    projections: pd.DataFrame,
    markets: dict[str, Any],
    sim_data: dict[str, Any],
    model_type: str,
    projection_file: Path,
    *,
    call_x_ai: bool = False,
    x_ai_ev_hurdle: float = 1.2,
    execute_xai_requests: bool = True,
) -> pd.DataFrame:
    market_lookup = build_market_lookup(markets)
    team_averages = load_team_averages()
    team_projection_summaries = build_team_projection_summaries(projections, team_averages)
    team_average_fallbacks = build_team_average_fallbacks(team_averages)
    team_projected_stats = build_team_projected_stats(projections)
    team_season_average_stats = build_team_season_average_stats(team_averages)
    now_central = current_central_time()
    rows = []

    for _, row in projections.iterrows():
        player_name = str(row.get("Name", "")).strip()
        if not player_name:
            continue
        projected_minutes = safe_float(row.get("MINS PROJ"), 0.0)
        if projected_minutes <= 0:
            continue

        matched = resolve_market_player(player_name, row.get("Team"), market_lookup)
        if not matched:
            continue

        market_player_name, player_markets = matched
        player_meta = player_markets.get("__meta__", {}) if isinstance(player_markets, dict) else {}
        game_time_cst = player_meta.get("game_time_cst")
        event_start_central = parse_central_datetime(player_meta.get("event_start_cst_iso"))
        if event_start_central is not None and event_start_central <= now_central:
            continue
        bucket = infer_position_bucket(row)

        for market_stat, books in player_markets.items():
            if str(market_stat).startswith("__"):
                continue
            if market_stat in {"double_double", "fantasy_points"}:
                continue
            if market_stat not in PROJECTION_STAT_MAP and market_stat not in COMBINED_COMPONENTS:
                continue
            if not isinstance(books, dict):
                continue

            if not any(
                book != "avgObj"
                and base_market_book_name(book, entry if isinstance(entry, dict) else None) in ALLOWED_GRADING_BOOKS
                and isinstance(entry, dict)
                and is_valid_grading_line(entry.get("line"))
                for book, entry in books.items()
            ):
                continue

            try:
                pmf, matched_means, sources = market_distribution(row, market_stat, bucket, sim_data)
            except KeyError:
                continue

            best = best_candidate_for_market(
                player_name=market_player_name,
                market_stat=market_stat,
                books=books,
                projection_value=projection_value_for_market(row, market_stat),
                pmf=pmf,
            )
            if best is None:
                continue

            minutes_injury_summary = normalize_text_or_na(row.get("MinsProjInjurySummary"))
            minutes_confidence_justification = normalize_text_or_na(row.get("MinsProjConfidenceJustification"))
            xai_score: int | str = "N/A"
            xai_context = "N/A"
            xai_prompt = None
            if call_x_ai and best["expected_value"] >= x_ai_ev_hurdle:
                prompt = build_xai_bet_scoring_prompt(
                    player_name=market_player_name,
                    team=display_team(row.get("Team")),
                    opponent=display_team(row.get("NextOpponent")),
                    minutes_projection=projected_minutes,
                    last_5_games_minutes_log=[
                        None if math.isnan(value) else int(value) if float(value).is_integer() else value
                        for value in (safe_float(row.get(f"Game -{idx}")) for idx in range(5, 0, -1))
                    ],
                    stat=market_stat,
                    bet_side=str(best["bet_side"]),
                    over_under_line=float(best["line"]),
                    stat_projection=float(best["stat_projection"]),
                    win_likelihood=float(best["win_pct"]),
                    component_projections=component_projections_for_market(row, market_stat),
                    team_projected_stats=team_projected_stats.get(canonicalize_team_name(row.get("Team")), {}),
                    team_season_averages=team_season_average_stats.get(canonicalize_team_name(row.get("Team")), {}),
                    minutes_projection_confidence=safe_float(row.get("MinsProjConfidence")),
                    minutes_injury_summary=minutes_injury_summary,
                    minutes_confidence_justification=minutes_confidence_justification,
                )
                xai_prompt = prompt

            best.update(
                {
                    "team": display_team(row.get("Team")),
                    "opponent": display_team(row.get("NextOpponent")),
                    "team_averages": team_projection_summaries.get(canonicalize_team_name(row.get("Team")))
                    or team_average_fallbacks.get(canonicalize_team_name(row.get("Team")), ""),
                    "proj_mins": round(projected_minutes, 4),
                    "conf": safe_float(row.get("MinsProjConfidence")),
                    "last_5_games": format_last_5_games(row),
                    "last_5_games_fouls": format_last_5_games_fouls(row),
                    "pos": bucket,
                    "matched_sim_means": ",".join(f"{value:.2f}" for value in matched_means) or "N/A",
                    "sim_source": ",".join(sources),
                    "game_time_cst": game_time_cst or "N/A",
                    "kenpom_player_url": row.get("KenPomPlayerURL"),
                    "minutes_injury_summary": minutes_injury_summary,
                    "minutes_confidence_justification": minutes_confidence_justification,
                    "xAiScore": xai_score,
                    "manualRtg": "",
                    "manualNotes": "",
                    "xAiContext": xai_context,
                    "_xai_prompt": xai_prompt,
                    "_xai_player_name": market_player_name,
                    "_xai_team": display_team(row.get("Team")),
                    "_xai_stat": market_stat,
                    "_xai_line": float(best["line"]),
                    "_xai_bet_side": str(best["bet_side"]),
                    "_xai_expected_value": float(best["expected_value"]),
                }
            )
            rows.append(best)

    df = pd.DataFrame(rows)
    if df.empty:
        empty_columns = list(OUTPUT_COLUMNS)
        if call_x_ai:
            empty_columns.extend(col for col in BET_GRADING_TEMP_COLUMNS if col not in empty_columns)
        return pd.DataFrame(columns=empty_columns)
    for col in ("push_likelihood", "model_type", "projection_file"):
        if col in df.columns:
            df = df.drop(columns=col)
    df = df.sort_values(["expected_value", "Player", "stat"], ascending=[False, True, True])
    df = df.reset_index(drop=True)
    if call_x_ai and execute_xai_requests:
        df = run_parallel_bet_grading(df)
    elif not call_x_ai:
        df = df.drop(columns=[col for col in BET_GRADING_TEMP_COLUMNS if col in df.columns], errors="ignore")
    ordered = [col for col in OUTPUT_COLUMNS if col in df.columns] + [col for col in df.columns if col not in OUTPUT_COLUMNS]
    return df[ordered].reset_index(drop=True)


def run_parallel_bet_grading(output_df: pd.DataFrame, allow_api_calls: bool = True) -> pd.DataFrame:
    if output_df.empty or "_xai_prompt" not in output_df.columns:
        return output_df

    queue = ParallelXAIQueue(max_parallel=MAX_PARALLEL_XAI_CALLS)
    graded = output_df.copy()
    graded["xAiScore"] = graded["xAiScore"].astype("object")
    graded["xAiContext"] = graded["xAiContext"].astype("object")
    cache_entries = load_recent_xai_bet_cache()
    run_cache_snapshot: dict[str, Any] = {}
    eligible_mask = graded["_xai_prompt"].notna() & graded["_xai_prompt"].astype(str).ne("")
    pending_tasks = []
    cache_hits = 0
    for index, row in graded.loc[eligible_mask].iterrows():
        line = row["_xai_line"]
        cache_key = build_xai_bet_cache_key(
            str(row["_xai_player_name"]),
            str(row["_xai_team"]),
            str(row["_xai_stat"]),
            line,
        )
        cached_payload = cache_entries.get(cache_key)
        if cached_payload is not None:
            score, context = parse_xai_response_payload(cached_payload)
            graded.at[int(index), "xAiScore"] = score
            graded.at[int(index), "xAiContext"] = context
            run_cache_snapshot[cache_key] = cached_payload
            cache_hits += 1
            continue
        pending_tasks.append(
            {
                "row_index": int(index),
                "prompt": row["_xai_prompt"],
                "player_name": row["_xai_player_name"],
                "team": row["_xai_team"],
                "stat": row["_xai_stat"],
                "line": float(line),
                "bet_side": row["_xai_bet_side"],
                "expected_value": float(row["_xai_expected_value"]),
                "cache_key": cache_key,
            }
        )
    total_eligible = int(eligible_mask.sum())
    total_to_call = len(pending_tasks)

    logger.info(
        "Starting bet grading workflow: total_rows=%s eligible_bets=%s skipped_bets=%s cache_hits=%s api_calls_needed=%s max_parallel=%s",
        len(graded),
        total_eligible,
        len(graded) - total_eligible,
        cache_hits,
        total_to_call,
        MAX_PARALLEL_XAI_CALLS,
    )

    if not allow_api_calls:
        logger.info(
            "Skipping outbound xAI bet grading calls after cache pass: eligible_bets=%s cache_hits=%s pending_api_calls=%s",
            total_eligible,
            cache_hits,
            total_to_call,
        )
        return graded.drop(columns=[col for col in BET_GRADING_TEMP_COLUMNS if col in graded.columns], errors="ignore")

    def worker(task: dict[str, Any]) -> tuple[int | str, str, Any]:
        return score_bet_with_xai(
            task["prompt"],
            player_name=task["player_name"],
            team=task["team"],
            stat=task["stat"],
            line=task["line"],
            bet_side=task["bet_side"],
            expected_value=task["expected_value"],
        )

    while pending_tasks:
        logger.info(
            "Submitting bet grading batch: pending_bets=%s processed=%s remaining=%s",
            len(pending_tasks),
            int(graded.loc[eligible_mask, "xAiScore"].notna().sum()),
            total_eligible - int(graded.loc[eligible_mask, "xAiScore"].notna().sum()),
        )
        run_result = queue.run(
            pending_tasks,
            worker,
            progress_callback=lambda progress: log_queue_progress("Bet grading", progress),
        )
        for task, outcome in run_result.successes:
            score, context, response_payload = outcome
            graded.at[task["row_index"], "xAiScore"] = score
            graded.at[task["row_index"], "xAiContext"] = context
            cache_entries[task["cache_key"]] = response_payload
            run_cache_snapshot[task["cache_key"]] = response_payload
            graded_count = int(graded.loc[eligible_mask, "xAiScore"].notna().sum())
            logger.info(
                "Bet grading complete for %s | %s | %s; bets_processed=%s/%s bets_remaining=%s",
                task["player_name"],
                task["team"],
                task["stat"],
                graded_count,
                total_eligible,
                total_eligible - graded_count,
            )

        if not run_result.dlq:
            break

        retryable = [failure.task for failure in run_result.dlq]
        if retryable and prompt_retry_failed_queue_items(
            "bet-grading",
            [
                {
                    "label": f'{failure.task["player_name"]} | {failure.task["team"]} | {failure.task["stat"]}',
                    "error": failure.error,
                }
                for failure in run_result.dlq
            ],
        ):
            pending_tasks = retryable
            continue

        for failure in run_result.dlq:
            logger.warning(
                "Bet grading failed for %s | %s | %s: %s",
                failure.task["player_name"],
                failure.task["team"],
                failure.task["stat"],
                failure.error,
            )
        break

    completed_count = int(graded.loc[eligible_mask, "xAiScore"].notna().sum())
    logger.info(
        "Bet grading workflow complete: eligible_bets=%s graded=%s ungraded=%s",
        total_eligible,
        completed_count,
        total_eligible - completed_count,
    )
    write_xai_bet_cache_snapshot(run_cache_snapshot)

    return graded.drop(columns=[col for col in BET_GRADING_TEMP_COLUMNS if col in graded.columns], errors="ignore")


def count_matched_players(projections: pd.DataFrame, markets: dict[str, Any]) -> int:
    market_lookup = build_market_lookup(markets)
    matched_players = 0
    for _, row in projections.iterrows():
        player_name = str(row.get("Name", "")).strip()
        if not player_name:
            continue
        projected_minutes = safe_float(row.get("MINS PROJ"), 0.0)
        if projected_minutes <= 0:
            continue
        if resolve_market_player(player_name, row.get("Team"), market_lookup):
            matched_players += 1
    return matched_players


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full player-prop pipeline")
    parser.add_argument("--kenpom-file", default=None, help="Existing KenPom workbook to use")
    parser.add_argument(
        "--date",
        default=None,
        help="Event date in MM-DD-YYYY format for Unabated filtering; also used as the KenPom scrape date if no workbook is supplied",
    )
    parser.add_argument(
        "--unabated",
        default=None,
        help="Raw or parsed Unabated payload; if omitted, fetch and save a fresh raw payload",
    )
    parser.add_argument("--sim-output", default=str(REPO_ROOT / "unabated_sim_output.json"), help="Simulation lookup JSON")
    parser.add_argument("--model-type", choices=["existing"], default="existing", help="Projection model to run")
    parser.add_argument(
        "--run_x_ai_follow_up_minutes",
        action="store_true",
        help="Run the follow-up low-confidence team minutes xAI workflow after the initial team minutes pass",
    )
    parser.add_argument(
        "--x_ai_follow_up_confidence_threshold",
        type=float,
        default=0.94,
        help="Confidence threshold at or below which players are eligible for follow-up minutes review",
    )
    parser.add_argument(
        "--run_x_ai_bet_grading_workflow",
        action="store_true",
        help="Score qualifying bets with xAI after the simulation and EV ranking steps",
    )
    parser.add_argument(
        "--call_x_ai",
        dest="run_x_ai_bet_grading_workflow",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--x_ai_ev_hurdle", type=float, default=1.2, help="Minimum expected_value required before calling xAI")
    parser.add_argument("--output", default=None, help="Output CSV path")
    return parser.parse_args()


def main() -> None:
    log_path = configure_logging()
    args = parse_args()
    logger.info(
        "Full pipeline start: kenpom_file=%s date=%s unabated=%s sim_output=%s run_follow_up=%s follow_up_threshold=%.2f run_bet_grading=%s ev_hurdle=%.2f output=%s",
        args.kenpom_file,
        args.date,
        args.unabated,
        args.sim_output,
        args.run_x_ai_follow_up_minutes,
        args.x_ai_follow_up_confidence_threshold,
        args.run_x_ai_bet_grading_workflow,
        args.x_ai_ev_hurdle,
        args.output,
    )
    main_module = load_module(REPO_ROOT / "scripts" / "main.py", "kenpom_main_module")

    target_date = None
    if args.date:
        parser_module = load_module(REPO_ROOT / "scripts" / "parse_unabated.py", "parse_unabated_module_for_date")
        target_date = parser_module.normalize_cli_date(args.date)

    sim_path = Path(args.sim_output).expanduser().resolve()
    if not sim_path.exists():
        raise SystemExit(f"Simulation output not found: {sim_path}")
    if args.unabated:
        unabated_path = Path(args.unabated).expanduser().resolve()
        if not unabated_path.exists():
            raise SystemExit(f"Unabated input not found: {unabated_path}")
    else:
        unabated_path = fetch_unabated_payload()

    markets = parse_unabated_input(unabated_path, target_date=target_date)
    target_teams = extract_target_teams_from_markets(markets)
    logger.info(
        "Parsed Unabated markets: players=%s target_teams=%s target_date=%s",
        len(markets),
        len(target_teams),
        target_date,
    )

    scraped_csv_path: Path | None = None
    if args.kenpom_file:
        kenpom_file = Path(args.kenpom_file).expanduser().resolve()
        scraped_player_df = main_module.prepare_player_df(load_player_stats_from_workbook(kenpom_file))
        team_statuses = group_team_statuses_from_player_df(scraped_player_df)
        hydrate_recent_lineup_contexts_for_workbook(main_module, team_statuses)
        logger.info(
            "Loaded KenPom workbook: path=%s teams=%s players=%s",
            kenpom_file,
            len(team_statuses),
            len(scraped_player_df),
        )
    elif args.date:
        logger.info("Starting fresh KenPom scrape for date=%s", args.date)
        scrape_bundle = main_module.scrape_kenpom_frames(
            args.date,
            target_teams_for_lineups=target_teams,
        )
        scraped_player_df = scrape_bundle["player_df"]
        team_statuses = group_team_statuses_from_player_df(scraped_player_df)
        recent_lineup_contexts = {
            status.get("team"): status.get("recent_lineup_context")
            for status in scrape_bundle["team_statuses"]
        }
        for status in team_statuses:
            status["recent_lineup_context"] = recent_lineup_contexts.get(status["team"])
        kenpom_file, scraped_csv_path = main_module.save_kenpom_outputs(
            args.date,
            scrape_bundle["four_factors"],
            scrape_bundle["team_stats"],
            scrape_bundle["points_dist"],
            scraped_player_df,
        )
        logger.info("Saved bulk KenPom scrape workbook to %s", kenpom_file)
        logger.info("Saved bulk KenPom scrape CSV to %s", scraped_csv_path)
    else:
        raise SystemExit("Pass either --kenpom-file or --date")

    if not kenpom_file.exists():
        raise SystemExit(f"KenPom workbook not found: {kenpom_file}")
    validate_minutes_player_df(
        scraped_player_df,
        stage_label="KenPom player scrape/load",
        require_minutes_projection=False,
    )
    minutes_cache_date = infer_minutes_stage_cache_date(
        explicit_date=args.date if args.date else None,
        source_workbook=kenpom_file,
    )
    minutes_cache, minutes_cache_path = load_minutes_stage_cache(minutes_cache_date)
    logger.info(
        "Minutes cache ready: date=%s path=%s existing_teams=%s",
        minutes_cache_date,
        minutes_cache_path,
        len(minutes_cache.get("teams", {})),
    )
    sim_data = load_sim_data(sim_path)
    logger.info("Loaded simulation data from %s", sim_path)

    target_scrape_teams = resolve_target_scrape_teams(team_statuses, target_teams)
    logger.info(
        "Minutes stage setup: total_scraped_teams=%s target_teams_with_unabated_lines=%s",
        len(team_statuses),
        len(target_scrape_teams),
    )
    run_parallel_team_minutes_workflow(
        main_module=main_module,
        team_statuses=team_statuses,
        run_follow_up_minutes=args.run_x_ai_follow_up_minutes,
        follow_up_threshold=args.x_ai_follow_up_confidence_threshold,
        minutes_cache=minutes_cache,
    )
    write_minutes_stage_cache(minutes_cache_date, minutes_cache)
    player_df_with_minutes = combine_team_status_frames(team_statuses)
    validate_minutes_player_df(player_df_with_minutes, stage_label="Minutes projection stage")

    minutes_output_stem = f"full_pipeline_minutes_{build_output_stem(args.model_type, kenpom_file)}"
    minutes_csv_path, minutes_xlsx_path, minutes_workbook_path = save_minutes_stage_outputs(
        source_workbook=kenpom_file,
        player_df=player_df_with_minutes,
        output_stem=minutes_output_stem,
    )
    validate_path_exists(minutes_csv_path, "Minutes player CSV")
    validate_path_exists(minutes_xlsx_path, "Minutes player XLSX")
    validate_path_exists(minutes_workbook_path, "Minutes workbook")
    logger.info(
        "Minutes artifacts saved: csv=%s xlsx=%s workbook=%s teams=%s players=%s",
        minutes_csv_path,
        minutes_xlsx_path,
        minutes_workbook_path,
        player_df_with_minutes["Team"].nunique() if "Team" in player_df_with_minutes.columns else 0,
        len(player_df_with_minutes),
    )

    projections, projection_file = run_model(args.model_type, minutes_workbook_path)
    projections = merge_player_stats_context(projections, player_df_with_minutes)
    validate_projection_output(projections, projection_file)
    logger.info(
        "Projection model complete: model_type=%s projection_file=%s rows=%s",
        args.model_type,
        projection_file,
        len(projections),
    )
    output_df = build_rows(
        projections,
        markets,
        sim_data,
        args.model_type,
        projection_file,
        call_x_ai=args.run_x_ai_bet_grading_workflow,
        x_ai_ev_hurdle=args.x_ai_ev_hurdle,
    )

    if output_df.empty and target_date:
        all_markets = parse_unabated_input(unabated_path, target_date=None)
        unfiltered_matches = count_matched_players(projections, all_markets)
        filtered_matches = count_matched_players(projections, markets)
        if filtered_matches == 0 and unfiltered_matches > 0:
            print(
                "Warning: no player markets matched after filtering Unabated to "
                f"{target_date}. The same inputs match {unfiltered_matches} players without the "
                "date filter. `--date` is treated as the event date, not the KenPom workbook timestamp."
            )

    PIPELINE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    validate_final_output_df(output_df)
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        output_path = PIPELINE_OUTPUT_DIR / f"full_pipeline_{build_output_stem(args.model_type, kenpom_file)}.csv"

    output_df.to_csv(output_path, index=False)
    validate_final_output_df(output_df, output_path)
    logger.info(
        "Full pipeline complete: output=%s rows=%s xai_graded=%s log_path=%s",
        output_path,
        len(output_df),
        int(output_df["xAiScore"].notna().sum()) if "xAiScore" in output_df.columns else 0,
        log_path,
    )
    print(f"Unabated input: {unabated_path}")
    print(f"Simulation input: {sim_path}")
    print(f"KenPom workbook: {kenpom_file}")
    if scraped_csv_path is not None:
        print(f"KenPom player CSV: {scraped_csv_path}")
    print(f"Target teams with Unabated-lined players: {len(target_scrape_teams)}")
    print(f"Minutes player CSV: {minutes_csv_path}")
    print(f"Minutes player XLSX: {minutes_xlsx_path}")
    print(f"Minutes workbook: {minutes_workbook_path}")
    print(f"Projection workbook: {projection_file}")
    print(f"Final output CSV: {output_path}")
    print(f"Output rows: {len(output_df)}")
    if args.run_x_ai_bet_grading_workflow:
        print(f"xAI log written to: {log_path}")


if __name__ == "__main__":
    main()

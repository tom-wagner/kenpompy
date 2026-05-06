import sys
from pathlib import Path

# Ensure the repo package wins over any installed kenpompy distribution when
# the script is executed as `python3 scripts/main.py`.
REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

from kenpompy.utils import AuthenticationError, RateLimitError, login
from kenpompy.FanMatch import FanMatch
import kenpompy.summary as kp_summary
import kenpompy.team as kp_team
import pandas as pd
from functools import reduce
from datetime import datetime
import time
from random import randint
import numpy as np
import os
import shutil
import json
import re
import difflib
import logging
import warnings
from urllib.parse import unquote
from dotenv import load_dotenv
from xai_sdk import Client
from xai_sdk.chat import user
from xai_sdk.tools import web_search, x_search
from pandas.errors import PerformanceWarning

OUTPUTS_DIR = REPO_ROOT / 'outputs'
KENPOM_OUTPUT_DIR = OUTPUTS_DIR / 'kenpom'
KENPOM_XAI_LOG_DIR = OUTPUTS_DIR / 'kenpom_x_ai_logs'
EVALS_OUTPUT_DIR = OUTPUTS_DIR / 'evals'
MODEL_OUTPUT_DIR = OUTPUTS_DIR / 'model_projections'
PROJECTIONS_OUTPUT_DIR = MODEL_OUTPUT_DIR / 'projections'
PIPELINE_OUTPUT_DIR = OUTPUTS_DIR / 'pipeline'
MINUTES_OUTPUT_DIR = PIPELINE_OUTPUT_DIR / 'minutes'
PIPELINE_XAI_LOG_DIR = OUTPUTS_DIR / 'pipeline_x_ai_logs'
CACHE_OUTPUT_DIR = OUTPUTS_DIR / 'cache'
OUTPUT_RETENTION_DIRS = (
    KENPOM_XAI_LOG_DIR,
    KENPOM_OUTPUT_DIR,
    EVALS_OUTPUT_DIR,
    MODEL_OUTPUT_DIR,
    PROJECTIONS_OUTPUT_DIR,
    PIPELINE_OUTPUT_DIR,
    MINUTES_OUTPUT_DIR,
    PIPELINE_XAI_LOG_DIR,
    CACHE_OUTPUT_DIR,
)
MAX_RECENT_OUTPUT_FILES = 3
MAX_RECENT_XAI_CACHE_FILES = 10
MAX_RECENT_PIPELINE_XAI_LOG_FILES = 10
MINUTES_ROLLING_COLUMNS = [
    'Last3AvgMins',
    'Last3MinutesDetail',
    'Last5AvgMins',
    'Last3MinsStdDev',
    'Last5MinsStdDev',
    'Last5FoulsDetail',
    'MinsProj',
    'MinsProjConfidence',
    'MinsProjInjurySummary',
    'MinsProjConfidenceJustification',
]
MINUTES_STDDEV_INVESTIGATION_THRESHOLD = 4.0
XAI_MAX_RETRIES = 3
LOW_CONFIDENCE_MINUTES_THRESHOLD = 0.94
XAI_RPC_TIMEOUT_SECONDS = float(os.getenv('XAI_RPC_TIMEOUT_SECONDS', '300'))
XAI_ACCEPTABLE_MINUTE_TOTAL_DELTA = 0.01
XAI_MAIN_MINUTES_LOOKBACK = 7
XAI_FOLLOWUP_MINUTES_LOOKBACK = 10
XAI_RECENT_LINEUPS_LIMIT = 10
LAST5_FALLBACK_CONFIDENCE = 0.5
load_dotenv(REPO_ROOT / '.env')
warnings.filterwarnings("ignore", category=PerformanceWarning)
logger = logging.getLogger(__name__)
NAME_SUFFIX_TOKENS = {'jr', 'sr', 'ii', 'iii', 'iv', 'v'}


def configure_logging():
    root_logger = logging.getLogger()
    if getattr(configure_logging, "_configured", False):
        return getattr(configure_logging, "_log_path", None)

    KENPOM_XAI_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = KENPOM_XAI_LOG_DIR / f"kenpom_x_ai_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.txt"
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    configure_logging._configured = True
    configure_logging._log_path = log_path
    logger.info('Logging to %s', log_path)
    return log_path


def prune_output_directories(max_files=MAX_RECENT_OUTPUT_FILES):
    for directory in OUTPUT_RETENTION_DIRS:
        directory.mkdir(parents=True, exist_ok=True)
        files = [path for path in directory.iterdir() if path.is_file()]
        files.sort(key=lambda path: (path.stat().st_mtime, path.name), reverse=True)
        if directory == CACHE_OUTPUT_DIR:
            keep_count = MAX_RECENT_XAI_CACHE_FILES
        elif directory == PIPELINE_XAI_LOG_DIR:
            keep_count = MAX_RECENT_PIPELINE_XAI_LOG_FILES
        else:
            keep_count = max_files
        stale_files = files[keep_count:]
        for stale_file in stale_files:
            try:
                stale_file.unlink()
            except OSError as exc:
                logger.warning('Failed to remove old output file %s: %s', stale_file, exc)
        if stale_files:
            logger.info(
                'Pruned %s old files from %s; kept newest %s',
                len(stale_files),
                directory,
                min(len(files), keep_count),
            )


def log_xai_request_and_response(call_kind, team, request_text, result, target_player=None):
    subject = f'{team} ({target_player})' if target_player else team
    logger.info('xAI %s request for %s:\n%s', call_kind, subject, request_text)

    if result is None:
        logger.info('xAI %s response for %s: <no response>', call_kind, subject)
        return

    logger.info('xAI %s response for %s:\n%s', call_kind, subject, result.get('text', ''))
    citations = result.get('citations') or []
    if citations:
        logger.info(
            'xAI %s citations for %s:\n%s',
            call_kind,
            subject,
            json.dumps(citations, ensure_ascii=False, indent=2, default=str),
        )


def get_teams_playing(browser, date_object):
    """Return the KenPom-normalized team list for the specified game date."""
    fanmatch_date = date_object.strftime('%Y-%m-%d')
    season = str(date_object.year)
    fanmatch = FanMatch(browser, fanmatch_date)
    if fanmatch.fm_df is None:
        return []

    valid_teams = kp_team.get_valid_teams(browser, season=season)
    valid_team_set = set(valid_teams)

    def normalize_fanmatch_team_name(team_name):
        normalized_name = normalize_team_name(team_name)
        if normalized_name in valid_team_set:
            return normalized_name

        matches = [
            team for team in valid_teams
            if normalized_name == team or normalized_name.startswith(team + ' ')
        ]
        if matches:
            return max(matches, key=len)

        return normalized_name

    teams = []
    for _, row in fanmatch.fm_df.iterrows():
        teams.append(normalize_fanmatch_team_name(row['PredictedWinner']))
        teams.append(normalize_fanmatch_team_name(row['PredictedLoser']))

    return teams


def _coerce_minutes_series(df, columns):
    available = [col for col in columns if col in df.columns]
    if not available:
        return pd.DataFrame(index=df.index)
    return df[available].apply(pd.to_numeric, errors='coerce')


def append_minutes_rolling_columns(player_df):
    if player_df.empty:
        for col in MINUTES_ROLLING_COLUMNS:
            player_df[col] = pd.Series(dtype='object')
        return player_df

    last3_cols = [f'Game -{i}' for i in range(1, 4)]
    last5_cols = [f'Game -{i}' for i in range(1, 6)]
    foul_cols = [f'Game Fouls -{i}' for i in range(1, 7)]

    last3 = _coerce_minutes_series(player_df, last3_cols)
    last5 = _coerce_minutes_series(player_df, last5_cols)

    fc_per_40 = pd.to_numeric(player_df.get('FC/40'), errors='coerce').fillna(0.0)
    for idx in range(1, 7):
        minutes = pd.to_numeric(player_df.get(f'Game -{idx}'), errors='coerce')
        player_df[f'Game Fouls -{idx}'] = (fc_per_40 * minutes / 40.0).round(1)

    player_df['Last3AvgMins'] = last3.mean(axis=1)
    player_df['Last3MinutesDetail'] = player_df.apply(
        lambda row: _format_minutes_detail(_recent_minutes_list(row, lookback=3)),
        axis=1,
    )
    player_df['Last5AvgMins'] = last5.mean(axis=1)
    player_df['Last3MinsStdDev'] = last3.std(axis=1, ddof=0)
    player_df['Last5MinsStdDev'] = last5.std(axis=1, ddof=0)
    player_df['Last5FoulsDetail'] = player_df.apply(
        lambda row: _format_minutes_detail([row.get(f'Game Fouls -{idx}') for idx in range(5, 0, -1)]),
        axis=1,
    )
    if 'MinsProj' not in player_df.columns:
        player_df['MinsProj'] = pd.NA
    if 'MinsProjConfidence' not in player_df.columns:
        player_df['MinsProjConfidence'] = pd.NA
    if 'MinsProjInjurySummary' not in player_df.columns:
        player_df['MinsProjInjurySummary'] = pd.NA
    if 'MinsProjConfidenceJustification' not in player_df.columns:
        player_df['MinsProjConfidenceJustification'] = pd.NA

    ordered = [col for col in player_df.columns if col not in MINUTES_ROLLING_COLUMNS] + MINUTES_ROLLING_COLUMNS
    return player_df[ordered]


def prepare_player_df(player_df):
    if player_df is None:
        return pd.DataFrame()
    prepared = player_df.copy()
    if not prepared.empty:
        if "Name" not in prepared.columns:
            prepared["Name"] = pd.Series(prepared.index, index=prepared.index, dtype="object")
        elif prepared.index.name == "Name" or "Name" in (prepared.index.names if isinstance(prepared.index, pd.MultiIndex) else [prepared.index.name]):
            missing_name_mask = prepared["Name"].isna()
            if not missing_name_mask.any():
                missing_name_mask = prepared["Name"].astype(str).str.strip().eq("")
            if missing_name_mask.any():
                prepared.loc[missing_name_mask, "Name"] = prepared.index[missing_name_mask]

        prepared["Name"] = prepared["Name"].where(prepared["Name"].notna(), "")
        prepared["Name"] = prepared["Name"].astype(str).str.strip()
        prepared = prepared[prepared["Name"].ne("")]
        prepared = prepared.rename(columns={"Pct.1": "Player.2Pt%", "Pct.2": "Player.3Pt%"})
    return append_minutes_rolling_columns(prepared)


def apply_last5_minutes_fallback(team_df, reason='last_5_average_fallback'):
    if team_df is None or team_df.empty:
        return team_df

    updated = team_df.copy()
    fallback_source = updated['Last5AvgMins'] if 'Last5AvgMins' in updated.columns else pd.Series(index=updated.index, dtype='float64')
    fallback_minutes = pd.to_numeric(fallback_source, errors='coerce')
    if fallback_minutes.empty or fallback_minutes.isna().all():
        fallback_minutes = pd.Series(
            [
                _clean_float(row.get('Game -1')) or 0.0
                for _, row in updated.iterrows()
            ],
            index=updated.index,
            dtype='float64',
        )

    updated['MinsProj'] = fallback_minutes
    updated['MINS PROJ'] = fallback_minutes
    updated['MinsProjConfidence'] = LAST5_FALLBACK_CONFIDENCE
    updated['MinsProjInjurySummary'] = ''
    updated['MinsProjConfidenceJustification'] = (
        'Used average of the last 5 games for minutes projection'
        if reason == 'last_5_average_fallback'
        else f'Used average of the last 5 games after {reason}'
    )
    return rebalance_team_minutes(updated)


def _clean_float(value):
    try:
        out = float(value)
        return None if pd.isna(out) else out
    except (TypeError, ValueError):
        return None


def _clean_text(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _normalize_person_name(value):
    text = _clean_text(value)
    if not text:
        return ''
    text = text.lower().replace("'", '').replace('.', '')
    text = re.sub(r'[^a-z0-9]+', ' ', text).strip()
    return text


def _canonical_person_name(value):
    tokens = _normalize_person_name(value).split()
    while tokens and tokens[-1] in NAME_SUFFIX_TOKENS:
        tokens.pop()
    return ' '.join(tokens)


def _same_name_signature(left, right):
    left_tokens = left.split()
    right_tokens = right.split()
    if not left_tokens or not right_tokens:
        return False
    return left_tokens[-1] == right_tokens[-1] and left_tokens[0][0] == right_tokens[0][0]


def _recent_minutes_list(row, lookback=3):
    values = []
    for idx in range(lookback, 0, -1):
        val = _clean_float(row.get(f'Game -{idx}'))
        values.append(None if val is None else round(val, 2))
    return values


def _format_minutes_detail(values):
    formatted = []
    for value in values:
        if value is None:
            formatted.append('None')
        elif float(value).is_integer():
            formatted.append(str(int(value)))
        else:
            formatted.append(str(value))
    return '[' + ', '.join(formatted) + ']'


def _projected_margin_from_score(projected_score):
    if not isinstance(projected_score, str):
        return None

    match = re.search(r'(\d+)\s*-\s*(\d+)', projected_score)
    if not match:
        return None

    return abs(int(match.group(1)) - int(match.group(2)))


def _team_context_from_df(team, team_df):
    opponent = ''
    projected_score = ''

    if team_df is not None and not team_df.empty:
        if 'NextOpponent' in team_df.columns:
            for value in team_df['NextOpponent']:
                if isinstance(value, str) and value.strip():
                    opponent = value.strip().replace('+', ' ')
                    break

        if 'KenPomResult' in team_df.columns:
            for value in team_df['KenPomResult']:
                if isinstance(value, str) and value.strip():
                    projected_score = value.strip()
                    break

    return {
        'team': team,
        'opponent': opponent or 'Unknown Opponent',
        'projected_score': projected_score or 'Unknown',
        'projected_margin': _projected_margin_from_score(projected_score),
    }


def _team_minutes_dictionary(team_df, lookback=XAI_MAIN_MINUTES_LOOKBACK):
    minutes_dict = {}
    if team_df is None or team_df.empty:
        return minutes_dict

    for index_value, row in team_df.iterrows():
        name = row.get('Name')
        if not isinstance(name, str) or not name.strip():
            name = index_value
        if not isinstance(name, str) or not name.strip():
            continue
        stddev = _clean_float(row.get('Last5MinsStdDev'))
        minutes_dict[name.strip()] = {
            'minutes': _recent_minutes_list(row, lookback=lookback),
            'stdDev': None if stddev is None else round(stddev, 2),
        }
    return minutes_dict


def _team_projection_dictionary(team_df):
    projection_dict = {}
    if team_df is None or team_df.empty:
        return projection_dict

    for index_value, row in team_df.iterrows():
        name = row.get('Name')
        if not isinstance(name, str) or not name.strip():
            name = index_value
        if not isinstance(name, str) or not name.strip():
            continue
        projection_dict[name.strip()] = _clean_float(row.get('MinsProj'))
    return projection_dict


def _recent_lineups_payload(lineups_df):
    payload = []
    if lineups_df is None or lineups_df.empty:
        return payload

    for _, row in lineups_df.iterrows():
        lineup = []
        for position in ['PG', 'SG', 'SF', 'PF', 'C']:
            name = _clean_text(row.get(f'{position}_Name'))
            if name:
                lineup.append(name)

        pct = _clean_float(row.get('Pct'))
        payload.append({
            'pctMinutes': None if pct is None else round(pct / 100.0, 4),
            'lineup': lineup,
        })
    return payload


def _trim_recent_lineups_payload(lineups_payload, max_lineups=XAI_RECENT_LINEUPS_LIMIT):
    return list(lineups_payload[:max_lineups])


def get_recent_lineup_context(browser, team):
    try:
        lineups_df = kp_team.get_recent_lineups(browser, team=team)
    except Exception as exc:
        logger.warning('Recent lineup scrape failed for %s: %s', team, exc)
        return {
            'lineups': [],
            'unknownPctMinutes': None,
            'coveragePctMinutes': 0.0,
        }

    lineups_payload = _recent_lineups_payload(lineups_df)
    trimmed_lineups = _trim_recent_lineups_payload(lineups_payload)
    return {
        'lineups': trimmed_lineups,
        'unknownPctMinutes': _clean_float(lineups_df.attrs.get('unknown_pct')),
        'coveragePctMinutes': round(sum((_clean_float(lineup.get('pctMinutes')) or 0.0) for lineup in trimmed_lineups), 4),
    }


def _output_dictionary_example(team_df):
    output = {}
    if team_df is None or team_df.empty:
        return output
    for name in _player_name_list(team_df):
        output[name] = {
            'minutes': 0,
            'confidence': 0.0,
            'injurySummary': 'Per-player injury/availability note, or "" if none and confidence >= 0.9',
            'confidenceJustification': 'Per-player confidence note, or "" if no extra context is needed and confidence >= 0.9',
        }
    return output


def _adjustment_dictionary_example(player_names):
    return {name: 0 for name in player_names}


def _normalize_target_player(target_player):
    if isinstance(target_player, (list, tuple, set)):
        for value in target_player:
            clean_value = _clean_text(value)
            if clean_value:
                return clean_value
        return ''
    return _clean_text(target_player) or ''


def _low_confidence_adjustment_example(team_df, target_player):
    target_name = _normalize_target_player(target_player)
    if not target_name:
        return {}

    roster_names = _player_name_list(team_df)
    if target_name not in roster_names:
        roster_names = [target_name] + [name for name in roster_names if name != target_name]

    counter_players = [name for name in roster_names if name != target_name][:2]
    if len(counter_players) >= 2:
        return {
            target_name: {
                'minutesAdjustment': 0,
                'confidence': 0.82,
                'injurySummary': 'Short player-specific note if relevant, or ""',
                'confidenceJustification': 'Short player-specific confidence note if needed, or ""',
            },
            counter_players[0]: {
                'minutesAdjustment': 0,
                'confidence': 0.9,
                'injurySummary': '',
                'confidenceJustification': '',
            },
            counter_players[1]: {
                'minutesAdjustment': 0,
                'confidence': 0.9,
                'injurySummary': '',
                'confidenceJustification': '',
            },
        }
    if len(counter_players) == 1:
        return {
            target_name: {
                'minutesAdjustment': 0,
                'confidence': 0.82,
                'injurySummary': 'Short player-specific note if relevant, or ""',
                'confidenceJustification': 'Short player-specific confidence note if needed, or ""',
            },
            counter_players[0]: {
                'minutesAdjustment': 0,
                'confidence': 0.9,
                'injurySummary': '',
                'confidenceJustification': '',
            },
        }
    return {
        target_name: {
            'minutesAdjustment': 0,
            'confidence': 0.82,
            'injurySummary': 'Short player-specific note if relevant, or ""',
            'confidenceJustification': 'Short player-specific confidence note if needed, or ""',
        }
    }


def _player_name_list(team_df):
    if team_df is None or team_df.empty:
        return []
    names = []
    seen = set()
    for index_value, row in team_df.iterrows():
        name = row.get('Name')
        if not isinstance(name, str) or not name.strip():
            name = index_value
        if not isinstance(name, str) or not name.strip():
            continue
        clean_name = name.strip()
        if clean_name in seen:
            continue
        seen.add(clean_name)
        names.append(clean_name)
    return names


def _low_confidence_players(team_df, threshold=LOW_CONFIDENCE_MINUTES_THRESHOLD):
    players = []
    if team_df is None or team_df.empty:
        return players

    for index_value, row in team_df.iterrows():
        raw_name = row.get('Name')
        if not isinstance(raw_name, str) or not raw_name.strip():
            raw_name = index_value
        name = str(raw_name).strip() if raw_name is not None else ''
        if not name:
            continue
        pct_min = _clean_float(row.get('%Min'))
        if pct_min is None or pct_min < 25:
            continue
        confidence = _clean_float(row.get('MinsProjConfidence'))
        if confidence is None or confidence > threshold:
            continue
        players.append(name)
    return players


def _should_run_low_confidence_adjustments(team_df, target_players):
    if not target_players or team_df is None or team_df.empty:
        return False, 'no_target_players'

    target_set = set(target_players)
    for index_value, row in team_df.iterrows():
        raw_name = row.get('Name')
        if not isinstance(raw_name, str) or not raw_name.strip():
            raw_name = index_value
        name = str(raw_name).strip() if raw_name is not None else ''
        if name not in target_set:
            continue

        confidence = _clean_float(row.get('MinsProjConfidence'))
        projected_minutes = _clean_float(row.get('MinsProj'))
        injury_summary = _clean_text(row.get('MinsProjInjurySummary'))
        last5_stddev = _clean_float(row.get('Last5MinsStdDev'))

        if injury_summary:
            return True, f'injury_context:{name}'
        if confidence is not None and confidence < 0.80:
            return True, f'very_low_confidence:{name}'
        if projected_minutes is not None and projected_minutes >= 15 and last5_stddev is not None and last5_stddev >= 5:
            return True, f'high_minutes_high_variance:{name}'

    return False, 'low_value_targets'


def _should_run_low_confidence_adjustment_for_player(team_df, target_player):
    should_run, reason = _should_run_low_confidence_adjustments(team_df, [target_player])
    return should_run, reason


def _low_confidence_minutes_context(team_df, target_players):
    context = {}
    if team_df is None or team_df.empty:
        return context

    target_set = set(target_players)
    for index_value, row in team_df.iterrows():
        raw_name = row.get('Name')
        if not isinstance(raw_name, str) or not raw_name.strip():
            raw_name = index_value
        name = str(raw_name).strip() if raw_name is not None else ''
        if name not in target_set:
            continue
        context[name] = {
            'recentMinutes': _recent_minutes_list(row, lookback=XAI_FOLLOWUP_MINUTES_LOOKBACK),
            'currentProjection': _clean_float(row.get('MinsProj')),
            'confidence': _clean_float(row.get('MinsProjConfidence')),
            'injurySummary': _clean_text(row.get('MinsProjInjurySummary')) or '',
            'confidenceJustification': _clean_text(row.get('MinsProjConfidenceJustification')) or '',
            'last5StdDev': _clean_float(row.get('Last5MinsStdDev')),
        }
    return context


def _extract_json_object(text):
    stripped = text.strip()
    if not stripped:
        return None

    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    for pattern in (r'\{.*\}', r'\[.*\]'):
        match = re.search(pattern, stripped, re.DOTALL)
        if not match:
            continue
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            continue

    return None


def _normalize_xai_minutes_projection(projection_data):
    if isinstance(projection_data, list):
        normalized = {}
        for player_projection in projection_data:
            if not isinstance(player_projection, dict):
                continue
            name = _clean_text(player_projection.get('name'))
            if name:
                normalized[name] = player_projection
        return normalized

    if not isinstance(projection_data, dict):
        return {}

    players = projection_data.get('players')
    if isinstance(players, list):
        normalized = {}
        for player_projection in players:
            if not isinstance(player_projection, dict):
                continue
            name = _clean_text(player_projection.get('name'))
            if name:
                normalized[name] = player_projection
        return normalized

    normalized = {}
    for name, player_projection in projection_data.items():
        clean_name = _clean_text(name)
        if clean_name and isinstance(player_projection, dict):
            normalized[clean_name] = player_projection
    return normalized


def _normalize_xai_adjustment_projection(adjustment_data):
    if not isinstance(adjustment_data, dict):
        return {}

    normalized = {}
    for name, player_adjustment in adjustment_data.items():
        clean_name = _clean_text(name)
        if not clean_name:
            continue
        if isinstance(player_adjustment, dict):
            normalized[clean_name] = player_adjustment
            continue
        numeric_adjustment = _clean_float(player_adjustment)
        if numeric_adjustment is None:
            continue
        normalized[clean_name] = {
            'minutesAdjustment': numeric_adjustment,
            'confidence': None,
            'injurySummary': '',
            'confidenceJustification': '',
        }
    return normalized


def _build_xai_projection_indexes(projection_map):
    exact = {}
    canonical = {}
    for raw_name, projection in projection_map.items():
        normalized_name = _normalize_person_name(raw_name)
        canonical_name = _canonical_person_name(raw_name)
        if normalized_name:
            exact[normalized_name] = projection
        if canonical_name:
            canonical.setdefault(canonical_name, []).append(projection)
    return exact, canonical


def _resolve_xai_player_projection(player_name, exact_index, canonical_index):
    normalized_name = _normalize_person_name(player_name)
    canonical_name = _canonical_person_name(player_name)

    exact_match = exact_index.get(normalized_name)
    if exact_match is not None:
        return exact_match

    canonical_matches = canonical_index.get(canonical_name, [])
    if len(canonical_matches) == 1:
        return canonical_matches[0]

    close_keys = difflib.get_close_matches(canonical_name, list(canonical_index.keys()), n=3, cutoff=0.92)
    ranked_matches = []
    for candidate_key in close_keys:
        if not _same_name_signature(canonical_name, candidate_key):
            continue
        candidate_matches = canonical_index.get(candidate_key, [])
        if len(candidate_matches) != 1:
            continue
        score = difflib.SequenceMatcher(a=canonical_name, b=candidate_key).ratio()
        ranked_matches.append((score, candidate_matches[0]))

    if not ranked_matches:
        return None

    ranked_matches.sort(key=lambda item: item[0], reverse=True)
    best_score, best_match = ranked_matches[0]
    next_score = ranked_matches[1][0] if len(ranked_matches) > 1 else 0.0
    if best_score >= 0.95 and (len(ranked_matches) == 1 or best_score - next_score >= 0.03):
        return best_match
    return None


def validate_xai_projection_data(team_df, projection_data):
    roster_names = _player_name_list(team_df)
    if not roster_names:
        return ["empty roster"]

    projection_map = _normalize_xai_minutes_projection(projection_data)
    if not projection_map:
        return ["response was not a player projection object/array"]

    projection_names = set(projection_map.keys())
    roster_name_set = set(roster_names)
    missing = sorted(roster_name_set - projection_names)
    unexpected = sorted(projection_names - roster_name_set)
    issues = []
    if missing:
        issues.append(f"missing players: {', '.join(missing[:10])}")
    if unexpected:
        issues.append(f"unexpected players: {', '.join(unexpected[:10])}")

    minute_total = 0.0
    for name in roster_names:
        player_projection = projection_map.get(name)
        if not isinstance(player_projection, dict):
            continue
        minutes = _clean_float(player_projection.get('minutes'))
        confidence = _clean_float(player_projection.get('confidence'))
        if minutes is None:
            issues.append(f"missing minutes for {name}")
            continue
        minute_total += minutes
        if confidence is None or confidence < 0 or confidence > 1:
            issues.append(f"invalid confidence for {name}")
    if abs(minute_total - 200.0) > XAI_ACCEPTABLE_MINUTE_TOTAL_DELTA:
        issues.append(f"minutes sum to {minute_total:g}, expected 200")
    return issues


def validate_xai_adjustment_data(team_df, target_player, adjustment_data):
    roster_names = _player_name_list(team_df)
    if not roster_names:
        return ['empty roster']
    if not target_player:
        return ['missing target player']
    adjustment_map = _normalize_xai_adjustment_projection(adjustment_data)
    if not adjustment_map:
        return ['response was not a player adjustment object']

    clean_keys = [str(key).strip() for key in adjustment_map.keys()]
    target_name = str(target_player).strip()
    roster_set = set(roster_names)
    key_set = set(clean_keys)
    issues = []
    if target_name not in key_set:
        issues.append(f"missing target player: {target_name}")

    unexpected = sorted(key_set - roster_set)
    if unexpected:
        issues.append(f"unexpected players: {', '.join(unexpected[:10])}")

    adjustment_total = 0.0
    for name in clean_keys:
        player_adjustment = adjustment_map.get(name)
        if not isinstance(player_adjustment, dict):
            issues.append(f"invalid adjustment payload for {name}")
            continue
        value = _clean_float(player_adjustment.get('minutesAdjustment'))
        confidence = _clean_float(player_adjustment.get('confidence'))
        if value is None:
            issues.append(f"invalid adjustment for {name}")
            continue
        adjustment_total += value
        if confidence is None or confidence < 0 or confidence > 1:
            issues.append(f"invalid confidence for {name}")

    if abs(adjustment_total) > 0.01:
        issues.append(f"adjustments sum to {adjustment_total:g}, expected 0")
    return issues


def rebalance_team_minutes(team_df):
    if team_df is None or team_df.empty:
        return team_df

    current_minutes = []
    for _, row in team_df.iterrows():
        current_minutes.append(_clean_float(row.get('MinsProj')))

    if not current_minutes or any(value is None for value in current_minutes):
        return team_df

    minute_total = sum(current_minutes)
    delta = round(200.0 - minute_total, 4)
    if abs(delta) <= 0.01:
        return team_df

    pct_min_source = team_df['%Min'] if '%Min' in team_df.columns else pd.Series(index=team_df.index, dtype='float64')
    mins_source = team_df['MinsProj'] if 'MinsProj' in team_df.columns else pd.Series(index=team_df.index, dtype='float64')
    numeric_pct_min = pd.to_numeric(pct_min_source, errors='coerce').fillna(0.0)
    numeric_minutes = pd.to_numeric(mins_source, errors='coerce').fillna(0.0)
    adjustment_order = (
        pd.DataFrame({
            'Name': team_df.index,
            'PctMin': numeric_pct_min.values,
            'MinsProj': numeric_minutes.values,
        })
        .sort_values(['PctMin', 'MinsProj'], ascending=[False, False])
    )

    for _, candidate in adjustment_order.iterrows():
        name = candidate['Name']
        current_value = _clean_float(team_df.at[name, 'MinsProj'])
        if current_value is None:
            continue
        adjusted_value = round(current_value + delta, 4)
        if adjusted_value < 0:
            continue
        team_df.at[name, 'MinsProj'] = adjusted_value
        team_df.at[name, 'MINS PROJ'] = adjusted_value
        return team_df

    return team_df


def apply_xai_minutes_projection(team_df, projection_data):
    if team_df is None or team_df.empty:
        return team_df

    projection_map = _normalize_xai_minutes_projection(projection_data)
    if not projection_map:
        return team_df

    exact_index, canonical_index = _build_xai_projection_indexes(projection_map)

    mins_proj = []
    mins_confidence = []
    mins_injury_summary = []
    mins_confidence_justification = []
    unmatched_names = []
    for index_value, row in team_df.iterrows():
        raw_name = row.get('Name')
        if not isinstance(raw_name, str) or not raw_name.strip():
            raw_name = index_value
        name = str(raw_name).strip() if raw_name is not None else ''
        player_projection = _resolve_xai_player_projection(name, exact_index, canonical_index) if name else None

        if isinstance(player_projection, dict):
            minutes = _clean_float(player_projection.get('minutes'))
            confidence = _clean_float(player_projection.get('confidence'))
            injury_summary = player_projection.get('injurySummary')
            confidence_justification = player_projection.get('confidenceJustification')
        else:
            minutes = _clean_float(row.get('MinsProj'))
            confidence = _clean_float(row.get('MinsProjConfidence'))
            injury_summary = row.get('MinsProjInjurySummary')
            confidence_justification = row.get('MinsProjConfidenceJustification')
            if name:
                unmatched_names.append(name)

        mins_proj.append(minutes)
        mins_confidence.append(confidence)
        mins_injury_summary.append(_clean_text(injury_summary))
        mins_confidence_justification.append(_clean_text(confidence_justification))

    team_df = team_df.copy()
    team_df['MinsProj'] = mins_proj
    team_df['MINS PROJ'] = mins_proj
    team_df['MinsProjConfidence'] = mins_confidence
    team_df['MinsProjInjurySummary'] = mins_injury_summary
    team_df['MinsProjConfidenceJustification'] = mins_confidence_justification
    if unmatched_names:
        logger.warning(
            'xAI minutes projection missed %s/%s players: %s',
            len(unmatched_names),
            len(team_df),
            ', '.join(unmatched_names[:10]),
        )
    return team_df


def apply_xai_minutes_adjustments(team_df, adjustment_data):
    if team_df is None or team_df.empty:
        return team_df

    adjustment_map = _normalize_xai_adjustment_projection(adjustment_data)
    if not adjustment_map:
        return team_df

    team_df = team_df.copy()
    adjusted_minutes = []
    updated_confidence = []
    updated_injury_summary = []
    updated_confidence_justification = []
    for index_value, row in team_df.iterrows():
        raw_name = row.get('Name')
        if not isinstance(raw_name, str) or not raw_name.strip():
            raw_name = index_value
        name = str(raw_name).strip() if raw_name is not None else ''

        current_minutes = _clean_float(row.get('MinsProj'))
        if current_minutes is None:
            current_minutes = _clean_float(row.get('MINS PROJ'))
        player_adjustment = adjustment_map.get(name)
        adjustment = _clean_float(player_adjustment.get('minutesAdjustment')) if isinstance(player_adjustment, dict) else None
        confidence = _clean_float(player_adjustment.get('confidence')) if isinstance(player_adjustment, dict) else None
        injury_summary = player_adjustment.get('injurySummary') if isinstance(player_adjustment, dict) else None
        confidence_justification = player_adjustment.get('confidenceJustification') if isinstance(player_adjustment, dict) else None

        if current_minutes is None:
            adjusted_minutes.append(None)
            updated_confidence.append(_clean_float(row.get('MinsProjConfidence')))
            updated_injury_summary.append(_clean_text(row.get('MinsProjInjurySummary')))
            updated_confidence_justification.append(_clean_text(row.get('MinsProjConfidenceJustification')))
            continue
        if adjustment is None:
            adjusted_minutes.append(current_minutes)
            updated_confidence.append(_clean_float(row.get('MinsProjConfidence')))
            updated_injury_summary.append(_clean_text(row.get('MinsProjInjurySummary')))
            updated_confidence_justification.append(_clean_text(row.get('MinsProjConfidenceJustification')))
            continue

        adjusted_minutes.append(max(0.0, round(current_minutes + adjustment, 4)))
        updated_confidence.append(confidence)
        updated_injury_summary.append(_clean_text(injury_summary))
        updated_confidence_justification.append(_clean_text(confidence_justification))

    team_df['MinsProj'] = adjusted_minutes
    team_df['MINS PROJ'] = adjusted_minutes
    team_df['MinsProjConfidence'] = updated_confidence
    team_df['MinsProjInjurySummary'] = updated_injury_summary
    team_df['MinsProjConfidenceJustification'] = updated_confidence_justification
    return team_df


def get_xai_api_key():
    return os.getenv('XAI_API_KEY') or os.getenv('X_AI_API_KEY')


def create_xai_chat():
    api_key = get_xai_api_key()
    if not api_key:
        logger.warning('xAI API key missing; skipping xAI minutes call')
        return None

    client = Client(api_key=api_key, timeout=XAI_RPC_TIMEOUT_SECONDS)
    return client.chat.create(
        model='grok-4-1-fast-reasoning',
        tools=[
            web_search(),
            x_search(),
        ],
    )


def build_team_minutes_prompt(team, team_df, recent_lineup_context=None):
    context = _team_context_from_df(team, team_df)
    minutes_dict = _team_minutes_dictionary(team_df)
    output_example = _output_dictionary_example(team_df)
    player_names = _player_name_list(team_df)
    recent_lineup_context = recent_lineup_context or {}
    recent_lineups = recent_lineup_context.get('lineups', [])
    unknown_pct = _clean_float(recent_lineup_context.get('unknownPctMinutes'))
    coverage_pct = _clean_float(recent_lineup_context.get('coveragePctMinutes'))
    close_game_instruction = (
        f"The projected margin is {context['projected_margin']} points, so treat this as a likely close game. "
        "For key players in a likely close single-elimination playoff game, project minutes near recent averages when warranted, but do not assume a top-end workload by default."
        if context['projected_margin'] is not None and context['projected_margin'] <= 5
        else "If the game projects to be close, treat this as a single-elimination playoff environment and lean toward key players matching recent minute averages, not automatically exceeding them."
    )
    return f"""We need to project minutes played per player for the {context['team']} men's basketball team's upcoming game vs {context['opponent']}. NOTE: The minutes projected MUST sum to 200 minutes total. The projected score of this game is {context['projected_score']}.

Start with a generic search for injuries related to {context['team']}. Investigate any players that may not be able to play and store that information in context for use in your upcoming projection.

Here are the minute logs by player for the last {XAI_MAIN_MINUTES_LOOKBACK} games: {json.dumps(minutes_dict, ensure_ascii=False)}

In this minutes array the oldest of the {XAI_MAIN_MINUTES_LOOKBACK} games is first, the most recent game is last, and the standard deviation is also included.

Here are the top {XAI_RECENT_LINEUPS_LIMIT} most recent lineups from KenPom, already simplified into JSON:
{json.dumps(recent_lineups, ensure_ascii=False)}

Use lineup combinations to evaluate patterns of players playing together vs playing at different times, and factor that into your player-level minutes projections output.

This lineup sample covers approximately this share of recent minutes:
{json.dumps({'coveragePctMinutes': coverage_pct, 'unknownPctMinutes': unknown_pct}, ensure_ascii=False)}

You must return the exact same player names that appear in this roster list, with no renaming, no suffix changes, no punctuation changes, and no added or removed generational suffixes:
{json.dumps(player_names, ensure_ascii=False)}

Project expected regulation minutes as a realistic central estimate.
Do not default to a ceiling outcome for stars or primary ballhandlers.
When the evidence is mixed, prefer the more sustainable workload over the more optimistic one.

Use the log of recent minutes, any recent injury news and reasoning logic in order to project minutes in the upcoming game. NOTE AGAIN: The minutes projection must sum to 200.

This will be difficult, intricate work that requires precision and injury context. Be careful and diligent in your projection.

IMPORTANT: every player in the incoming roster must be included in the response, even if the player is injured, unavailable, or should receive a 0-minute projection. Do not omit any player for any reason.

Additional instructions:
- Project the full rotation jointly, not player-by-player in isolation. If one player gains minutes, another player or group of teammates must lose them.
- Think critically about players whose minutes have decreased recently. Determine whether the decline is driven by injury, conditioning, foul trouble, poor play, rotation changes, or coaching decisions. When possible, index on direct coach quotes and recent reporting.
- If a player returned or is returning from injury, assume the player can handle a full minutes load unless a coach has specifically said otherwise. For example a game log of [22, 0, 0, 34, 35] should likely be projected at 34-35 even given the 0s.
- Execute player-level `x_search` and `web_search` tool calls for important contributors whose playing-time standard deviation is >= {MINUTES_STDDEV_INVESTIGATION_THRESHOLD}. Use those tool results directly in your minutes projection.
- Do not spend time researching or projecting players likely to receive 10 or fewer minutes unless the player is a star or key contributor who may be returning from injury. In that case, focus multiple tool calls on whether the player is expected to play and what workload is realistic.
- {close_game_instruction}
- Regress unusual one-game minute spikes unless there is clear evidence they reflect a real rotation change.
- Be skeptical of projections above 36 minutes for most players and above 38 minutes unless the player's role is extremely stable, recent usage strongly supports it, and there is no meaningful foul, injury, or rotation risk.
- Bench players with low recent usage and no sign of an expanded role should generally stay at low-minute projections.
- Use the recent lineup JSON in addition to your evaluation of injuries and minute logs. Preserve the strongest recent lineup structures unless injury/news context gives a strong reason to change them.
- Infer player-to-player relationships from the lineup JSON yourself. Look for players who rarely share the floor, appear to alternate for one slot, or rise/fall together, and reflect that in your minute allocation when warranted.
- Note this is a must-win, NCAA tournament game and the top 5-7 contributors will likely play significant minutes towards the top-end of their range.

Return your projection as JSON only. Your response MUST HAVE THE FOLLOWING JSON FORMAT: {json.dumps(output_example, ensure_ascii=False)}

For each player:
- The player name key MUST exactly match one of the provided roster names character-for-character.
- `minutes` must be that player's projected minutes as an integer.
- `confidence` must be that player's confidence score as a float from 0-1.
- `injurySummary` must be a per-player injury or availability note, not a team-level summary.
- `minutes` should represent the most likely regulation workload, not a best-case workload.
- `confidenceJustification` must be a per-player explanation for that specific player's confidence score, not a team-level summary.
- If a player has no relevant injury/availability context and `confidence >= 0.9`, `injurySummary` and `confidenceJustification` should both be `""`.
- Otherwise, include concise player-specific text in those fields.
- Include every player from the provided roster exactly once, including injured players and players projected for 0 minutes.
"""


def build_low_confidence_minutes_adjustment_prompt(team, team_df, target_player, recent_lineup_context=None):
    context = _team_context_from_df(team, team_df)
    normalized_target_player = _normalize_target_player(target_player)
    player_context = _low_confidence_minutes_context(team_df, [normalized_target_player])
    adjustment_example = _low_confidence_adjustment_example(team_df, normalized_target_player)
    minutes_dict = _team_minutes_dictionary(team_df)
    current_projection_dict = _team_projection_dictionary(team_df)
    roster_names = _player_name_list(team_df)
    recent_lineup_context = recent_lineup_context or {}
    recent_lineups = recent_lineup_context.get('lineups', [])
    unknown_pct = _clean_float(recent_lineup_context.get('unknownPctMinutes'))
    coverage_pct = _clean_float(recent_lineup_context.get('coveragePctMinutes'))
    return f"""We already have an initial minutes projection for the {context['team']} men's basketball team's upcoming game vs {context['opponent']}. The projected score is {context['projected_score']}.

Your task is to DIG DEEP on this specific player with a low-confidence minutes projection and recommend a minutes redistribution centered on that player:
{json.dumps(normalized_target_player, ensure_ascii=False)}

Here is the current player-specific context for that player:
{json.dumps(player_context, ensure_ascii=False)}

Here are the current projected minutes for the full team roster. These are the live projections after any prior low-confidence adjustments already applied in this run:
{json.dumps(current_projection_dict, ensure_ascii=False)}

Here are the minute logs by player for the last {XAI_MAIN_MINUTES_LOOKBACK} games:
{json.dumps(minutes_dict, ensure_ascii=False)}

Here are the top {XAI_RECENT_LINEUPS_LIMIT} most recent lineups from KenPom, already simplified into JSON:
{json.dumps(recent_lineups, ensure_ascii=False)}

This lineup sample covers approximately this share of recent minutes:
{json.dumps({'coveragePctMinutes': coverage_pct, 'unknownPctMinutes': unknown_pct}, ensure_ascii=False)}

Research instructions:
- Assess recent form and recent minutes trends for each targeted player.
- Evaluate recent playing-time trends for the targeted player and corroborate those trends with recent news or comments from the coach regarding playing time whenever possible.
- Look for coach quotes about the player's role or playing time.
- Look for updates on injuries, availability, conditioning, foul trouble, lineup changes, and rotation changes.
- Use both `x_search` and `web_search` tool calls while researching these players.
- If a player returned or is returning from injury, assume the player can handle a full minutes load unless a coach has specifically said otherwise. For example a game log of [22, 0, 0, 34, 35] should likely be projected at 34-35 even given the 0s.

Adjustment instructions:
- Return minute ADJUSTMENTS relative to the current projection, not new total projections.
- The response must include the targeted player and any other players whose minutes should change to keep the team balanced.
- The total of all minute adjustments must equal 0. If the targeted player is +3, one or more teammates must sum to -3.
- Choose adjustments that keep each affected player's resulting minutes projection as an integer.
- Be cautious with adjustments. Only change minutes if there is solid evidence from reporting, rotation context, or recent usage that the current projection is wrong.
- Prefer small adjustments toward the player's sustainable role rather than toward an optimistic upside workload.
- If the evidence is mixed, avoid upward adjustments to already-high minute projections.
- Use small, realistic adjustments unless the reporting strongly supports a bigger move.
- Prefer adjusting the players most directly connected to the targeted player's role, based on recent usage and lineup overlap.
- Use the recent lineup JSON in addition to your evaluation of injuries and minute logs.
- Infer player-to-player relationships from the lineup JSON yourself. Look for players who rarely share the floor, appear to alternate for one slot, or rise/fall together, and reflect that in the adjustments when warranted.

Roster keys must match one of these player names exactly with no character differences whatsoever:
{json.dumps(roster_names, ensure_ascii=False)}

Return JSON only. The required response format MUST EXACTLY MATCH THIS EXAMPLE: {json.dumps(adjustment_example, ensure_ascii=False)} (just an example; should not affect your work)

Rules:
- Every key must exactly match one of the provided roster names character-for-character.
- Every value must be an object with `minutesAdjustment`, `confidence`, `injurySummary`, and `confidenceJustification`.
- `minutesAdjustment` must be a numeric minute adjustment that preserves integer resulting minutes projections, such as 2, -1, or 0.
- `confidence` must be that player's updated confidence score as a float from 0-1.
- `injurySummary` must be a per-player injury or availability note, not a team-level summary.
- `confidenceJustification` must be a per-player explanation for that specific player's updated confidence score, not a team-level summary.
- If a player has no relevant injury/availability context and `confidence >= 0.9`, `injurySummary` and `confidenceJustification` should both be `""`.
- Include the targeted player exactly once.
- Include only players whose minutes should change, except the targeted player which must always be included.
- The minute adjustments in the response must sum to 0.
- Do not include explanations, markdown, code fences, or any text outside the JSON object.
"""


def build_team_minutes_reformat_prompt(team_df, malformed_response, issues):
    output_example = _output_dictionary_example(team_df)
    player_names = _player_name_list(team_df)
    issue_lines = '\n'.join(f'- {issue}' for issue in issues) if issues else '- response was malformed'
    return f"""Your previous response was malformed. Do not redo the research. Reformat your previous answer into valid JSON only.

Why it was malformed:
{issue_lines}

Roster keys must match these player names exactly with no character differences whatsoever:
{json.dumps(player_names, ensure_ascii=False)}

The team total minutes in the reformatted response must sum to exactly 200.

Valid example object:
{json.dumps(output_example, ensure_ascii=False)}

Your malformed response to reformat:
{malformed_response}

Return only a valid JSON object matching the example structure exactly. Include every rostered player exactly once, use the roster keys exactly as provided, and make sure total projected minutes sum to 200.
"""


def build_low_confidence_minutes_adjustment_reformat_prompt(team_df, target_player, malformed_response, issues):
    adjustment_example = _low_confidence_adjustment_example(team_df, target_player)
    roster_names = _player_name_list(team_df)
    issue_lines = '\n'.join(f'- {issue}' for issue in issues) if issues else '- response was malformed'
    return f"""Your previous response was malformed. Do not redo the research. Reformat your previous answer into valid JSON only.

Why it was malformed:
{issue_lines}

Roster keys must match one of these player names exactly with no character differences whatsoever:
{json.dumps(roster_names, ensure_ascii=False)}

Here is an example of the proper xAI response format:
{json.dumps(adjustment_example, ensure_ascii=False)}

The minute adjustments in the reformatted response must sum to 0.

Your malformed response to reformat:
{malformed_response}

Return only a valid JSON object using roster player names exactly. Each included player object must contain `minutesAdjustment`, `confidence`, `injurySummary`, and `confidenceJustification`. Include the targeted player exactly once, include any teammate counter-balancing adjustments needed, and make sure the adjustments sum to 0.
"""


def _submit_xai_chat_request(chat, request_text, call_kind, team, start_log_message, complete_log_message, target_player=None):
    if chat is None:
        return None

    logger.info(start_log_message, team) if target_player is None else logger.info(start_log_message, team, target_player)
    chat.append(user(request_text))

    response = None
    text_parts = []
    for response, chunk in chat.stream():
        if chunk.content:
            text_parts.append(chunk.content)

    result = {
        'text': ''.join(text_parts).strip(),
        'citations': getattr(response, 'citations', []) if response is not None else [],
    }
    logger.info(
        complete_log_message,
        team,
        len(result['text']),
        len(result['citations']),
    ) if target_player is None else logger.info(
        complete_log_message,
        team,
        target_player,
        len(result['text']),
        len(result['citations']),
    )
    log_xai_request_and_response(call_kind, team, request_text, result, target_player=target_player)
    return result


def call_xai_for_team_minutes(team, team_df, recent_lineup_context=None, chat=None, request_text=None, call_kind='team call'):
    if chat is None:
        chat = create_xai_chat()
    if chat is None:
        return None

    if request_text is None:
        request_text = build_team_minutes_prompt(team, team_df, recent_lineup_context=recent_lineup_context)
    return _submit_xai_chat_request(
        chat,
        request_text,
        call_kind,
        team,
        'Starting xAI minutes call for %s',
        'Completed xAI minutes call for %s; response_chars=%s citations=%s',
    )


def call_xai_for_low_confidence_minutes_adjustments(team, team_df, target_player, recent_lineup_context=None, chat=None, request_text=None, call_kind='low-confidence follow-up'):
    if chat is None:
        chat = create_xai_chat()
    if chat is None:
        return None

    if request_text is None:
        request_text = build_low_confidence_minutes_adjustment_prompt(
            team,
            team_df,
            target_player,
            recent_lineup_context=recent_lineup_context,
        )

    return _submit_xai_chat_request(
        chat,
        request_text,
        call_kind,
        team,
        'Starting low-confidence xAI adjustment call for %s (%s)',
        'Completed low-confidence xAI adjustment call for %s (%s); response_chars=%s citations=%s',
        target_player=target_player,
    )


def call_xai_for_team_minutes_with_retries(team, team_df, recent_lineup_context=None):
    chat = create_xai_chat()
    if chat is None:
        return None

    attempts = XAI_MAX_RETRIES + 1
    last_error = None
    request_text = build_team_minutes_prompt(team, team_df, recent_lineup_context=recent_lineup_context)
    call_kind = 'team call'

    for attempt in range(1, attempts + 1):
        try:
            logger.info('xAI attempt %s/%s for %s', attempt, attempts, team)
            result = call_xai_for_team_minutes(
                team,
                team_df,
                recent_lineup_context=recent_lineup_context,
                chat=chat,
                request_text=request_text,
                call_kind=call_kind,
            )
            if result is None:
                logger.warning('xAI returned no result for %s on attempt %s', team, attempt)
                return None

            projection_data = _extract_json_object(result['text'])
            if projection_data is None:
                logger.warning('Malformed xAI response for %s on attempt %s: %s', team, attempt, result['text'][:2000])
                issues = ['response was not valid JSON']
                raise ValueError('; '.join(issues))

            issues = validate_xai_projection_data(team_df, projection_data)
            if issues:
                logger.warning('Malformed xAI response for %s on attempt %s: %s', team, attempt, '; '.join(issues))
                raise ValueError('; '.join(issues))

            logger.info('xAI JSON parsed successfully for %s on attempt %s', team, attempt)
            return result, projection_data
        except Exception as exc:
            last_error = exc
            logger.warning('xAI attempt %s/%s failed for %s: %s', attempt, attempts, team, exc)
            if attempt < attempts:
                issues = [part.strip() for part in str(exc).split(';') if part.strip()]
                request_text = build_team_minutes_reformat_prompt(team_df, result['text'] if 'result' in locals() and result else '', issues)
                call_kind = 'team call reformat follow-up'
                logger.info('Retrying xAI for %s with malformed-response reformat request after brief pause', team)
                time.sleep(2)

    logger.error('xAI failed for %s after %s attempts: %s', team, attempts, last_error)
    return None


def call_xai_for_low_confidence_adjustments_with_retries(team, team_df, target_player, recent_lineup_context=None):
    chat = create_xai_chat()
    if chat is None:
        return None

    attempts = XAI_MAX_RETRIES + 1
    last_error = None
    request_text = build_low_confidence_minutes_adjustment_prompt(
        team,
        team_df,
        target_player,
        recent_lineup_context=recent_lineup_context,
    )
    call_kind = 'low-confidence follow-up'

    for attempt in range(1, attempts + 1):
        try:
            logger.info('Low-confidence adjustment attempt %s/%s for %s (%s)', attempt, attempts, team, target_player)
            result = call_xai_for_low_confidence_minutes_adjustments(
                team,
                team_df,
                target_player,
                recent_lineup_context=recent_lineup_context,
                chat=chat,
                request_text=request_text,
                call_kind=call_kind,
            )
            if result is None:
                logger.warning('xAI returned no low-confidence adjustment result for %s (%s) on attempt %s', team, target_player, attempt)
                return None

            adjustment_data = _extract_json_object(result['text'])
            if adjustment_data is None:
                logger.warning('Malformed low-confidence response for %s (%s) on attempt %s: %s', team, target_player, attempt, result['text'][:2000])
                issues = ['response was not valid JSON']
                raise ValueError('; '.join(issues))

            issues = validate_xai_adjustment_data(team_df, target_player, adjustment_data)
            if issues:
                raise ValueError('; '.join(issues))

            logger.info('xAI adjustment JSON parsed successfully for %s (%s) on attempt %s', team, target_player, attempt)
            return result, adjustment_data
        except Exception as exc:
            last_error = exc
            logger.warning('Low-confidence adjustment attempt %s/%s failed for %s (%s): %s', attempt, attempts, team, target_player, exc)
            if attempt < attempts:
                issues = [part.strip() for part in str(exc).split(';') if part.strip()]
                request_text = build_low_confidence_minutes_adjustment_reformat_prompt(
                    team_df,
                    target_player,
                    result['text'] if 'result' in locals() and result else '',
                    issues,
                )
                call_kind = 'low-confidence reformat follow-up'
                logger.info('Retrying low-confidence adjustment for %s (%s) with malformed-response reformat request after brief pause', team, target_player)
                time.sleep(2)

    logger.error('Low-confidence adjustment failed for %s (%s) after %s attempts: %s', team, target_player, attempts, last_error)
    return None


def process_low_confidence_minutes(
    team,
    df,
    team_status,
    run_follow_up=True,
    follow_up_threshold=LOW_CONFIDENCE_MINUTES_THRESHOLD,
    minutes_cache=None,
):
    low_confidence_players = _low_confidence_players(df, threshold=follow_up_threshold)
    team_status['low_confidence_players'] = low_confidence_players
    if not run_follow_up:
        team_status['low_confidence_adjustment_status'] = 'skipped'
        team_status['low_confidence_adjustment_issues'] = []
        return df
    if not low_confidence_players:
        team_status['low_confidence_adjustment_status'] = 'not_needed'
        team_status['low_confidence_adjustment_issues'] = []
        return df

    updated_df = df
    player_results = {}
    issues = []
    applied_adjustments = {}
    total_players = len(low_confidence_players)

    logger.info(
        'Starting low-confidence follow-up for %s: players=%s threshold=%.2f',
        team,
        total_players,
        follow_up_threshold,
    )

    for index, player_name in enumerate(low_confidence_players, start=1):
        logger.info(
            'Low-confidence follow-up queued for %s (%s): player=%s players_processed=%s/%s players_remaining=%s',
            team,
            index,
            player_name,
            index - 1,
            total_players,
            total_players - index + 1,
        )
        should_run, reason = _should_run_low_confidence_adjustment_for_player(updated_df, player_name)
        player_results[player_name] = {
            'reason': reason,
            'status': 'pending',
        }
        if not should_run:
            player_results[player_name]['status'] = 'skipped'
            logger.info(
                'Low-confidence follow-up skipped for %s (%s): player=%s reason=%s players_processed=%s/%s players_remaining=%s',
                team,
                index,
                player_name,
                reason,
                index,
                total_players,
                total_players - index,
            )
            continue

        cache_hit = False
        cache_entry = None
        if isinstance(minutes_cache, dict):
            team_cache = minutes_cache.setdefault('teams', {}).setdefault(team, {})
            players_cache = team_cache.get('players')
            if not isinstance(players_cache, dict):
                players_cache = {}
                team_cache['players'] = players_cache
            cache_entry = players_cache.get(player_name)
            if isinstance(cache_entry, dict):
                cached_result = cache_entry.get('result')
                cached_adjustment_data = cache_entry.get('adjustmentData')
                cache_issues = validate_xai_adjustment_data(updated_df, player_name, cached_adjustment_data)
                if isinstance(cached_result, dict) and not cache_issues:
                    outcome = cached_result, cached_adjustment_data
                    cache_hit = True
                    logger.info('Minutes follow-up cache hit for %s (%s)', team, player_name)
                else:
                    logger.warning(
                        'Ignoring invalid cached follow-up minutes for %s (%s): %s',
                        team,
                        player_name,
                        '; '.join(cache_issues) if cache_issues else 'missing result payload',
                    )
                    outcome = None
            else:
                outcome = None
        else:
            outcome = None

        if outcome is None:
            outcome = call_xai_for_low_confidence_adjustments_with_retries(
                team,
                updated_df,
                player_name,
                recent_lineup_context=team_status.get('recent_lineup_context'),
            )
        if outcome is None:
            player_results[player_name]['status'] = 'failed'
            issues.append(f'{player_name}: no response from xAI')
            logger.warning(
                'Low-confidence follow-up failed for %s (%s): player=%s players_processed=%s/%s players_remaining=%s',
                team,
                index,
                player_name,
                index,
                total_players,
                total_players - index,
            )
            continue

        xai_result, adjustment_data = outcome
        print(f"xAI low-confidence adjustment for {team} / {player_name}: {xai_result['text']}")
        player_results[player_name]['response_text'] = xai_result['text']
        player_results[player_name]['cache_hit'] = cache_hit

        validation_issues = validate_xai_adjustment_data(updated_df, player_name, adjustment_data)
        if validation_issues:
            logger.warning(
                'Low-confidence adjustment response for %s (%s) failed validation: %s',
                team,
                player_name,
                '; '.join(validation_issues),
            )
            player_results[player_name]['status'] = 'malformed'
            player_results[player_name]['issues'] = validation_issues
            issues.extend([f'{player_name}: {issue}' for issue in validation_issues])
            logger.warning(
                'Low-confidence follow-up malformed for %s (%s): player=%s players_processed=%s/%s players_remaining=%s issues=%s',
                team,
                index,
                player_name,
                index,
                total_players,
                total_players - index,
                '; '.join(validation_issues),
            )
            continue

        updated_df = apply_xai_minutes_adjustments(updated_df, adjustment_data)
        applied_adjustments.update(adjustment_data)
        if isinstance(minutes_cache, dict) and not cache_hit:
            team_cache = minutes_cache.setdefault('teams', {}).setdefault(team, {})
            players_cache = team_cache.get('players')
            if not isinstance(players_cache, dict):
                players_cache = {}
                team_cache['players'] = players_cache
            players_cache[player_name] = {
                'adjustmentData': adjustment_data,
                'result': xai_result,
                'updatedAt': datetime.now().isoformat(),
            }
        player_results[player_name]['status'] = 'ok'
        player_results[player_name]['adjustment'] = adjustment_data
        logger.info(
            'Low-confidence follow-up complete for %s (%s): player=%s players_processed=%s/%s players_remaining=%s',
            team,
            index,
            player_name,
            index,
            total_players,
            total_players - index,
        )

    team_status['low_confidence_response_text'] = json.dumps(player_results, ensure_ascii=False)
    team_status['low_confidence_adjustment_player_results'] = player_results
    team_status['low_confidence_adjustment_issues'] = issues
    team_status['low_confidence_adjustments'] = applied_adjustments
    updated_df = rebalance_team_minutes(updated_df)
    team_status['low_confidence_players_after_adjustment'] = _low_confidence_players(updated_df)

    if issues:
        team_status['low_confidence_adjustment_status'] = 'partial' if any(
            result.get('status') == 'ok' for result in player_results.values()
        ) else 'failed'
    elif any(result.get('status') == 'ok' for result in player_results.values()):
        team_status['low_confidence_adjustment_status'] = 'ok'
    else:
        team_status['low_confidence_adjustment_status'] = 'skipped'

    logger.info(
        'Low-confidence follow-up finished for %s: total_players=%s succeeded=%s skipped=%s failed=%s malformed=%s status=%s',
        team,
        total_players,
        sum(1 for result in player_results.values() if result.get('status') == 'ok'),
        sum(1 for result in player_results.values() if result.get('status') == 'skipped'),
        sum(1 for result in player_results.values() if result.get('status') == 'failed'),
        sum(1 for result in player_results.values() if result.get('status') == 'malformed'),
        team_status['low_confidence_adjustment_status'],
    )

    return updated_df


def prompt_retry_failed_teams(team_statuses):
    pending = [
        status for status in team_statuses
        if status.get('xai_status') in {'failed', 'malformed'}
        or status.get('low_confidence_adjustment_status') in {'failed', 'malformed'}
    ]
    if not pending:
        return False

    print("\nThe following teams did not receive a valid xAI response:")
    for status in pending:
        if status.get('xai_status') in {'failed', 'malformed'}:
            issues = "; ".join(status.get('issues', [])) or status.get('xai_status', 'unknown')
        else:
            issues = "; ".join(status.get('low_confidence_adjustment_issues', [])) or status.get('low_confidence_adjustment_status', 'unknown')
        print(f"- {status['team']}: {issues}")

    while True:
        answer = input("Retry failed or malformed teams? Y to retry, N to continue: ").strip().upper()
        if answer in {'Y', 'N'}:
            return answer == 'Y'
        print("Please enter Y or N.")


def scrape_kenpom_frames(date_string, top_n=None, target_teams_for_lineups=None):
    configure_logging()
    prune_output_directories()
    date_object = datetime.strptime(date_string, '%m-%d-%Y')

    browser = login('twagner55@gmail.com', 'NtnWk3974P')

    four_factors = kp_summary.get_fourfactors(browser).set_index('Team')
    team_stats = kp_summary.get_teamstats(browser).set_index('Team')
    team_stats_def = kp_summary.get_teamstats(browser, True).set_index('Team')
    for column in team_stats_def.columns:
        team_stats_def.rename(columns={column: 'Def.' + column}, inplace=True)
    points_dist = kp_summary.get_pointdist(browser).set_index('Team')

    teams = get_teams_playing(browser, date_object)
    if not teams:
        return {
            'browser': browser,
            'teams': [],
            'team_statuses': [],
            'four_factors': four_factors,
            'team_stats': team_stats,
            'team_stats_def': team_stats_def,
            'points_dist': points_dist,
            'player_df': pd.DataFrame(),
        }

    team_statuses = []
    for team in (teams[:top_n] if top_n is not None else teams):
        team_status = {
            'team': team,
            'df': None,
            'recent_lineup_context': None,
            'xai_status': 'not_attempted',
            'issues': [],
            'response_text': None,
            'low_confidence_adjustment_status': 'not_attempted',
            'low_confidence_adjustment_issues': [],
            'low_confidence_response_text': None,
        }
        try:
            df = kp_team.get_player_expanded(
                browser,
                date_string,
                team_with_spaces=team,
                team_stats=team_stats,
                team_stats_def=team_stats_def,
                four_factors=four_factors,
                points_dist=points_dist,
            )
            df = prepare_player_df(df)
            team_status['recent_lineup_context'] = get_recent_lineup_context(browser, team)
        except AuthenticationError as exc:
            print(f"Stopping scrape after authentication failure for {team}: {exc}")
            break
        except RateLimitError as exc:
            print(f"Stopping scrape after rate limit for {team}: {exc}")
            break
        team_status['df'] = df
        team_statuses.append(team_status)
        time.sleep(randint(10, 20))

    player_frames = [status['df'] for status in team_statuses if status.get('df') is not None]
    player_df = pd.concat(player_frames) if player_frames else pd.DataFrame()
    if not player_df.empty and 'NextOpponent' in player_df.columns:
        player_df = player_df[player_df['NextOpponent'].notnull()]
    elif not player_df.empty:
        print("Warning: 'NextOpponent' column missing from player_df; skipping opponent filter")

    return {
        'browser': browser,
        'teams': teams,
        'team_statuses': team_statuses,
        'four_factors': four_factors,
        'team_stats': team_stats,
        'team_stats_def': team_stats_def,
        'points_dist': points_dist,
        'player_df': player_df,
    }


def save_kenpom_outputs(date_string, four_factors, team_stats, points_dist, player_df):
    date_object = datetime.strptime(date_string, '%m-%d-%Y')
    formatted_date = date_object.strftime('%a_%b_%d_%Y')
    timestamp = datetime.now().strftime('%H%M%S')

    four_factors.to_excel(REPO_ROOT / f"{formatted_date}.xlsx", sheet_name='TeamFourFactors')
    with pd.ExcelWriter(REPO_ROOT / f'{formatted_date}.xlsx', mode='a') as writer:
        team_stats.to_excel(writer, sheet_name='TeamStats')
    with pd.ExcelWriter(REPO_ROOT / f'{formatted_date}.xlsx', mode='a') as writer:
        points_dist.to_excel(writer, sheet_name='PointsDist')
    with pd.ExcelWriter(REPO_ROOT / f'{formatted_date}.xlsx', mode='a') as writer:
        player_df.to_excel(writer, sheet_name='PlayerStats')

    KENPOM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    source = REPO_ROOT / f'{formatted_date}.xlsx'
    destination = KENPOM_OUTPUT_DIR / f'{formatted_date}_{timestamp}.xlsx'
    shutil.move(source, destination)
    csv_path = destination.with_suffix('.csv')
    player_df.to_csv(csv_path, index=False)
    return destination, csv_path

# usage `python3 03-19-2024`
def main_fn(date_string, top_n=None):
    configure_logging()
    prune_output_directories()
    print(date_string)
    date_object = datetime.strptime(date_string, '%m-%d-%Y')
    formatted_date = date_object.strftime('%a %b %d')

    # Returns an authenticated browser that can then be used to scrape pages that require authorization.
    browser = login('twagner55@gmail.com', 'NtnWk3974P')
    # Then you can request specific pages that will be parsed into convenient dataframes:

    # Returns a pandas dataframe containing the efficiency and tempo stats for the current season (https://kenpom.com/summary.php).

    # df = kp_team.get_player_expanded(browser, 'Maryland')

    four_factors = kp_summary.get_fourfactors(browser)
    four_factors = four_factors.set_index('Team')
    print('four_factors')
    print(four_factors)

    team_stats = kp_summary.get_teamstats(browser)
    team_stats = team_stats.set_index('Team')
    print('team_stats')
    print(team_stats)

    team_stats_def = kp_summary.get_teamstats(browser, True)
    team_stats_def = team_stats_def.set_index('Team')

    # prevent clashing column names
    for column in team_stats_def.columns:
        team_stats_def.rename(columns={column: 'Def.' + column}, inplace=True)

    points_dist = kp_summary.get_pointdist(browser)
    points_dist = points_dist.set_index('Team')
    print('points_dist')
    print(points_dist)

    teams = get_teams_playing(browser, date_object)
    print('teams')
    print(teams)
    if not teams:
        print(f'No teams scheduled on {date_object.strftime("%Y-%m-%d")}')
        return [four_factors, team_stats, team_stats_def, points_dist, pd.DataFrame()]

    team_statuses = []
    for team in (teams[:top_n] if top_n is not None else teams):
        team_status = {
            'team': team,
            'df': None,
            'recent_lineup_context': None,
            'xai_status': 'not_attempted',
            'issues': [],
            'response_text': None,
            'low_confidence_adjustment_status': 'not_attempted',
            'low_confidence_adjustment_issues': [],
            'low_confidence_response_text': None,
        }
        try:
            df = kp_team.get_player_expanded(browser, date_string, team_with_spaces=team, team_stats=team_stats, team_stats_def=team_stats_def, four_factors=four_factors, points_dist=points_dist)
            recent_lineup_context = get_recent_lineup_context(browser, team)
            team_status['recent_lineup_context'] = recent_lineup_context
            # Future hook: replace the placeholder prompt with the real per-team
            # minutes projection prompt and use the response to populate `MinsProj`.
            try:
                xai_outcome = call_xai_for_team_minutes_with_retries(team, df, recent_lineup_context=recent_lineup_context)
                if xai_outcome is not None:
                    xai_result, projection_data = xai_outcome
                    team_status['response_text'] = xai_result['text']
                    print(f"xAI team call for {team}: {xai_result['text']}")
                    issues = validate_xai_projection_data(df, projection_data)
                    if issues:
                        logger.warning('xAI response for %s failed validation: %s', team, '; '.join(issues))
                        team_status['xai_status'] = 'malformed'
                        team_status['issues'] = issues
                    else:
                        df = apply_xai_minutes_projection(df, projection_data)
                        df = rebalance_team_minutes(df)
                        team_status['xai_status'] = 'ok'
                        team_status['issues'] = []
                        df = process_low_confidence_minutes(team, df, team_status)
                else:
                    team_status['xai_status'] = 'failed'
                    team_status['issues'] = ['no response from xAI']
            except Exception as exc:
                team_status['xai_status'] = 'failed'
                team_status['issues'] = [str(exc)]
                print(f"Warning: xAI team call failed for {team}: {exc}")
        except AuthenticationError as exc:
            print(f"Stopping scrape after authentication failure for {team}: {exc}")
            break
        except RateLimitError as exc:
            print(f"Stopping scrape after rate limit for {team}: {exc}")
            break
        team_status['df'] = df
        team_statuses.append(team_status)
        time.sleep(randint(10, 20))

    while prompt_retry_failed_teams(team_statuses):
        pending_statuses = [
            status for status in team_statuses
            if status.get('xai_status') in {'failed', 'malformed'}
            or status.get('low_confidence_adjustment_status') in {'failed', 'malformed'}
        ]
        for status in pending_statuses:
            team = status['team']
            df = status.get('df')
            if df is None:
                continue
            try:
                if status.get('xai_status') in {'failed', 'malformed'}:
                    xai_outcome = call_xai_for_team_minutes_with_retries(
                        team,
                        df,
                        recent_lineup_context=status.get('recent_lineup_context'),
                    )
                    if xai_outcome is None:
                        status['xai_status'] = 'failed'
                        status['issues'] = ['no response from xAI']
                        continue
                    xai_result, projection_data = xai_outcome
                    status['response_text'] = xai_result['text']
                    print(f"xAI retry for {team}: {xai_result['text']}")
                    issues = validate_xai_projection_data(df, projection_data)
                    if issues:
                        logger.warning('Retried xAI response for %s failed validation: %s', team, '; '.join(issues))
                        status['xai_status'] = 'malformed'
                        status['issues'] = issues
                        continue
                    updated_df = apply_xai_minutes_projection(df, projection_data)
                    updated_df = rebalance_team_minutes(updated_df)
                    status['df'] = updated_df
                    status['xai_status'] = 'ok'
                    status['issues'] = []
                    status['low_confidence_adjustment_status'] = 'not_attempted'
                    status['low_confidence_adjustment_issues'] = []
                    updated_df = process_low_confidence_minutes(team, updated_df, status)
                    status['df'] = updated_df
                else:
                    updated_df = process_low_confidence_minutes(team, df, status)
                    status['df'] = updated_df
            except Exception as exc:
                if status.get('xai_status') in {'failed', 'malformed'}:
                    status['xai_status'] = 'failed'
                    status['issues'] = [str(exc)]
                    logger.warning('Retry xAI call failed for %s: %s', team, exc)
                else:
                    status['low_confidence_adjustment_status'] = 'failed'
                    status['low_confidence_adjustment_issues'] = [str(exc)]
                    logger.warning('Retry low-confidence adjustment failed for %s: %s', team, exc)

    player_frames = [status['df'] for status in team_statuses if status.get('df') is not None]
    player_df = pd.concat(player_frames) if player_frames else pd.DataFrame()

    # Some scrape runs can return frames without NextOpponent populated.
    # Avoid attribute-style access here so the script can still complete.
    if not player_df.empty and 'NextOpponent' in player_df.columns:
        player_df = player_df[player_df['NextOpponent'].notnull()]
    elif not player_df.empty:
        print("Warning: 'NextOpponent' column missing from player_df; skipping opponent filter")

    # rename PCT
    if not player_df.empty:
        player_df = player_df.rename(columns={"Pct.1": "Player.2Pt%", "Pct.2": "Player.3Pt%"})
    player_df = append_minutes_rolling_columns(player_df)

    print('player_df')
    print(player_df)
    print(player_df.columns.tolist())

    four_factors.to_excel(REPO_ROOT / f"{formatted_date}.xlsx",
                sheet_name='TeamFourFactors')

    with pd.ExcelWriter(REPO_ROOT / f'{formatted_date}.xlsx', mode='a') as writer:
        team_stats.to_excel(writer, sheet_name='TeamStats')

    with pd.ExcelWriter(REPO_ROOT / f'{formatted_date}.xlsx', mode='a') as writer:
        points_dist.to_excel(writer, sheet_name='PointsDist')

    with pd.ExcelWriter(REPO_ROOT / f'{formatted_date}.xlsx', mode='a') as writer:
        player_df.to_excel(writer, sheet_name='PlayerStats')

    # Move the file
    KENPOM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    source = REPO_ROOT / f'{formatted_date}.xlsx'
    destination = KENPOM_OUTPUT_DIR / f'{formatted_date}_{datetime.now().strftime("%H%M%S")}.xlsx'
    shutil.move(source, destination)


    return [four_factors, team_stats, team_stats_def, points_dist, player_df]


def calculate_player_props(four_factors, team_stats, team_stats_def, points_dist, player_df):
    """
    Calculate player props using the logic from PlayerStats.csv with team total calibration
    """
    def calculate_minutes_projection(player_row):
        # URGENT TODO: Add ETR PROJECTIONS / MANUAL OVERRIDE

        """Calculate projected minutes based on recent games"""
        for column in ('MinsProj', 'MINS PROJ', 'MINS_PROJ'):
            explicit_projection = _clean_float(player_row.get(column))
            if explicit_projection is not None:
                return explicit_projection

        recent_games = []
        for i in range(1, 7):  # Last 6 games
            game_col = f'Game -{i}'
            if game_col in player_row and not pd.isna(player_row[game_col]):
                recent_games.append(float(player_row[game_col]))
        
        if not recent_games:
            return 0
            
        weights = np.array([0.35, 0.25, 0.15, 0.1, 0.1, 0.05])[:len(recent_games)]
        weights = weights / weights.sum()
        return np.average(recent_games, weights=weights)

    # Track team-level projections for scaling
    team_projections = {}
    
    # First pass - calculate initial projections
    initial_projections = {}
    
    for player_name, player in player_df.iterrows():
        if pd.isna(player.get('NextOpponent')):
            continue
            
        team_name = normalize_team_name(player['Team'])
        opp_name = normalize_team_name(player['NextOpponent'])
        
        # Get team and opponent data
        team_factors = four_factors.loc[team_name]
        opp_factors = four_factors.loc[opp_name]
        opp_def_stats = team_stats_def.loc[opp_name]
        team_shooting = team_stats.loc[team_name]
        team_points = points_dist.loc[team_name]

        [team_pace, opp_pace] = [float(team_factors['AdjTempo']), float(opp_factors['AdjTempo'])]
        [high_pace, low_pace] = [max(team_pace, opp_pace), min(team_pace, opp_pace)]
        proj_poss = (high_pace * .7) + (low_pace * .3)
        
        # Initialize team tracking if needed
        if team_name not in team_projections:
            initial_projections[team_name] = {}
            kp_total = float(player['KenPomResult'].split('-')[0].replace('W, ', '')) if player['KenPomResult'].startswith('W') else float(player['KenPomResult'].split('-')[1]) if not pd.isna(player.get('KenPomResult')) else None
            team_projections[team_name] = {
                'KP_TOTAL': kp_total,
                'pace': proj_poss,
                'initial_points': 0,
                'three_pt_makes': 0,
                'three_pt_misses': 0,
                'two_pt_makes': 0,
                'two_pt_misses': 0,
                'total_fga': 0,
                'total_misses': 0,
                'total_ft_attempts': 0,
                'total_ft_misses': 0,
                'live_ball_ft_misses': 0,
                'ft_makes': 0,
                'turnovers': 0
            }
        
        # Base calculations
        proj_minutes = calculate_minutes_projection(player)
        
        player_to_rate = float(player.get('TORate', 15)) / 100
        opp_def_to_rate = float(opp_factors['Def-TO%']) / 100

        weighted_to_rate = (player_to_rate * 0.7) + (opp_def_to_rate * 0.3)

        # Get player's possession usage rate
        poss_rate = float(player.get('%Poss', 20)) / 100  # Percentage of possessions used while on floor

        # Get player's foul drawn rate per 40 minutes and adjust to per-possession
        fd_per_40 = float(player.get('FD/40', 4.0))  # Fouls drawn per 40 minutes
        fd_per_poss = fd_per_40 / proj_poss  # Convert directly to per possession

        # Calculate possessions that end in a field goal attempt
        player_poss = proj_poss * poss_rate

        # Calculate possessions that don't result in shots:
        # - weighted_to_rate is the portion that end in turnovers
        # - fd_per_poss * 0.35 is the portion that end in non-shooting fouls
        non_shooting_poss_rate = weighted_to_rate + (fd_per_poss * 0.35)

        # Calculate possessions that can result in shots
        shooting_poss = player_poss * (1 - non_shooting_poss_rate)

        fga = shooting_poss

        # Calculate player's actual 3PA rate
        player_3pa_rate = fga / proj_poss if proj_poss > 0 else team_shooting['3PA%'] / 100

        # Get opponent's 3PA% allowed
        opp_3pa_allowed = float(opp_def_stats['Def.3PA%'])

        # Weight 70% player tendency, 30% opponent allowance
        three_pt_rate = (player_3pa_rate * 0.70) + (opp_3pa_allowed / 100 * 0.30)
        three_pa = fga * three_pt_rate
        two_pa = fga * (1 - three_pt_rate)
        
        # 3PT%: 70% player, 30% opponent defense
        player_3pt = float(player.get('Player.3Pt%', team_shooting['3P%']))  # Use player 3P% or fall back to team
        opp_3pt_defense = float(opp_def_stats['Def.3P%']) / 100
        three_pt_pct = (player_3pt * 0.70) + (opp_3pt_defense * 0.30)

        # 2PT%: 65% player, 35% opponent defense
        player_2pt = float(player.get('Player.2Pt%', team_shooting['2P%']))  # Use player 2P% or fall back to team
        opp_2pt_defense = float(opp_def_stats['Def.2P%']) / 100
        two_pt_pct = (player_2pt * 0.65) + (opp_2pt_defense * 0.35)
        
        # Calculate makes and misses
        three_pm = three_pa * three_pt_pct
        two_pm = two_pa * two_pt_pct
        three_misses = three_pa - three_pm
        two_misses = two_pa - two_pm
        
        points_from_2 = two_pm * 2
        points_from_3 = three_pm * 3

        player_ft_rate = float(player.get('FTRate', team_points['Off-FT']))  # Player's FTRate, fall back to team rate
        opp_ft_allowed_rate = float(opp_factors['Def-FTRate'])  # Opponent's defensive free throw rate

        # Weight 70% player tendency, 30% defensive tendency
        weighted_ft_rate = (player_ft_rate * 0.75) + (opp_ft_allowed_rate * 0.25)

        # Calculate FT attempts using weighted rate
        ft_attempts = fga * (weighted_ft_rate / 100)  # Divide by 100 since FT Rate is per 100 FGA

        # Free throw shooting percentage - use player's FT% if available, fall back to team rate
        ft_percentage = float(player.get('FT%', team_shooting['FT%'])) / 100  # FT shooting percentage
        ft_makes = ft_attempts * ft_percentage
        ft_misses = ft_attempts - ft_makes

        # Free throw sequence distribution (typical college basketball)
        one_and_one_pct = 0.35  # 35% of FT sequences are one-and-one
        double_bonus_pct = 0.45  # 45% are double bonus
        and_one_pct = 0.15  # 15% are and-one situations
        three_shot_pct = 0.05  # 5% are three-shot fouls

        # Calculate reboundable misses for each type of sequence
        one_and_one_sequences = (ft_attempts * one_and_one_pct) / 2  # divide by 2 since these come in pairs
        one_and_one_misses = one_and_one_sequences * (1 - ft_percentage)  # all first shot misses are reboundable

        double_bonus_sequences = (ft_attempts * double_bonus_pct) / 2  # divide by 2 since these come in pairs
        double_bonus_reboundable_misses = double_bonus_sequences * (1 - ft_percentage)  # only second shot misses

        and_one_attempts = ft_attempts * and_one_pct
        and_one_reboundable_misses = and_one_attempts * (1 - ft_percentage)  # all misses are reboundable

        three_shot_sequences = (ft_attempts * three_shot_pct) / 3  # divide by 3 since these come in triples
        three_shot_reboundable_misses = three_shot_sequences * (1 - ft_percentage)  # only last shot misses

        # Total reboundable misses
        live_ball_ft_misses = (one_and_one_misses + 
                              double_bonus_reboundable_misses + 
                              and_one_reboundable_misses + 
                              three_shot_reboundable_misses)
        
        turnovers = player_poss * weighted_to_rate
        total_points = points_from_2 + points_from_3 + ft_makes

        team_projections[team_name]['initial_points'] += total_points
        team_projections[team_name]['three_pt_makes'] += three_pm
        team_projections[team_name]['three_pt_misses'] += three_misses
        team_projections[team_name]['two_pt_makes'] += two_pm
        team_projections[team_name]['two_pt_misses'] += two_misses
        team_projections[team_name]['total_fga'] += fga
        team_projections[team_name]['total_misses'] += (three_misses + two_misses + live_ball_ft_misses)
        team_projections[team_name]['total_ft_attempts'] += ft_attempts
        team_projections[team_name]['total_ft_misses'] += ft_misses
        team_projections[team_name]['live_ball_ft_misses'] += live_ball_ft_misses
        team_projections[team_name]['ft_makes'] += ft_makes
        team_projections[team_name]['turnovers'] += turnovers

        initial_projections[team_name][player_name] = {
            # Core stats
            'minutes': proj_minutes,
            'points': total_points,
            
            # Detailed shooting stats
            'three_pm': three_pm,
            'three_pa': three_pa,
            'three_misses': three_misses,
            'two_pm': two_pm,
            'two_pa': two_pa,
            'two_misses': two_misses,
            'fga': fga,
            'fgm': two_pm + three_pm,
            
            # Free throw details
            'ft_attempts': ft_attempts,
            'ft_made': ft_makes,
            'ft_misses': ft_misses,
            'live_ball_ft_misses': live_ball_ft_misses,
            
            'turnovers': turnovers,
        }

    print(team_projections)
    # Calculate adjustment factors for each team
    team_adjustment_factors = {}
    for team_name, stats in team_projections.items():
        # adjustment_factor = (
        #     stats['three_pt_makes'] + 
        #     stats['three_pt_misses'] +
        #     stats['two_pt_makes'] + 
        #     stats['two_pt_misses'] +
        #     stats['turnovers'] + 
        #     (stats['ft_makes'] / 1.8)
        # ) / stats['pace']
        adjustment_factor = (
            (stats['three_pt_makes'] + stats['three_pt_misses'] +  # This is total FGA
            stats['two_pt_makes'] + stats['two_pt_misses']) -
            (stats['total_misses'] * 0.3) +  # Approximate offensive rebounds (30% of misses)
            stats['turnovers'] +
            (stats['total_ft_attempts'] * 0.475)  # Standard possession adjustment for FTs
        ) / stats['pace']
        team_adjustment_factors[team_name] = adjustment_factor

    # Adjust individual player projections based on team adjustment factors
    adjusted_projections = {}
    for team_name, players in initial_projections.items():
        adjusted_projections[team_name] = {}
        adjustment_factor = team_adjustment_factors[team_name]
        
        for player_name, stats in players.items():
            adjusted_projections[team_name][player_name] = {}
            
            # Copy minutes directly without adjustment
            adjusted_projections[team_name][player_name]['minutes'] = stats['minutes']
            
            # Adjust all other stats by the team adjustment factor
            for stat, value in stats.items():
                if stat != 'minutes':
                    adjusted_projections[team_name][player_name][stat] = value / adjustment_factor

    # Adjust team projections by the adjustment factors
    adjusted_team_projections = {}
    for team_name, stats in team_projections.items():
        adjusted_team_projections[team_name] = {
            'KP_TOTAL': stats['KP_TOTAL'],  # Retain KP total
            'pace': stats['pace'],  # Retain pace
        }
        
        adjustment_factor = team_adjustment_factors[team_name]
        
        # Adjust all other stats by the team adjustment factor
        for stat, value in stats.items():
            if stat not in ['KP_TOTAL', 'pace']:
                adjusted_team_projections[team_name][stat] = value / adjustment_factor

    print(adjusted_team_projections)

    # Adjust teams to match KenPom totals while maintaining possession consistency
    for team_name, stats in adjusted_team_projections.items():
        if 'KP_TOTAL' not in stats or pd.isna(stats['KP_TOTAL']):
            continue
        
        # Calculate points per possession
        initial_ppp = stats['initial_points'] / stats['pace']
        target_ppp = stats['KP_TOTAL'] / stats['pace']
        
        # Scale efficiency (not volume)
        efficiency_scalar = target_ppp / initial_ppp
        
        # Stats that represent efficiency (makes relative to attempts)
        efficiency_stats = [
            ('three_pm', 'three_pa'),
            ('two_pm', 'two_pa'),
            ('ft_made', 'ft_attempts'),
            ('fgm', 'fga')
        ]
        
        # Adjust player stats
        for player_name in adjusted_projections[team_name]:
            player_stats = adjusted_projections[team_name][player_name]
            
            # Scale makes while preserving attempts
            for make_stat, attempt_stat in efficiency_stats:
                if make_stat in player_stats and attempt_stat in player_stats:
                    player_stats[make_stat] *= efficiency_scalar
                    # Recalculate misses to maintain attempts
                    miss_stat = make_stat.replace('_pm', '_misses').replace('fgm', 'fga')
                    if miss_stat in player_stats:
                        player_stats[miss_stat] = player_stats[attempt_stat] - player_stats[make_stat]
            
            # Update points based on new makes
            if 'points' in player_stats:
                player_stats['points'] = (
                    player_stats.get('three_pm', 0) * 3 +
                    player_stats.get('two_pm', 0) * 2 +
                    player_stats.get('ft_made', 0)
                )
        
        # Adjust team totals similarly
        team_efficiency_pairs = [
            ('three_pt_makes', 'three_pt_misses'),
            ('two_pt_makes', 'two_pt_misses'),
            ('ft_makes', 'total_ft_attempts')
        ]
        
        for makes_stat, attempts_stat in team_efficiency_pairs:
            stats[makes_stat] *= efficiency_scalar
            # Recalculate misses to maintain total attempts
            total_attempts = stats[makes_stat] + stats[attempts_stat]
            stats[attempts_stat] = total_attempts - stats[makes_stat]
        
        # Update team points
        stats['initial_points'] = (
            stats['three_pt_makes'] * 3 +
            stats['two_pt_makes'] * 2 +
            stats['ft_makes']
        )

    print(adjusted_team_projections)

    # Now continue with assists/rebounds/steals/blocks calculations using adjusted shooting numbers
    for player_name, player in player_df.iterrows():
        team_name = normalize_team_name(player['Team'])
        opp_name = normalize_team_name(player['NextOpponent'])

        opp_def_stats = team_stats_def.loc[opp_name]
        opp_misc_stats = team_stats.loc[team_name]
        
        adjusted_team_stats = adjusted_team_projections[team_name]
        _nan_opp = {'three_pt_misses': float('nan'), 'two_pt_misses': float('nan'), 'live_ball_ft_misses': float('nan'), 'total_fga': float('nan')}
        opp_stats = adjusted_team_projections.get(opp_name, _nan_opp)
        adjusted_player_stats = adjusted_projections[team_name][player_name]

        player_assist_rate = float(player.get('ARate')) / 100
        player_assists = player_assist_rate * (adjusted_player_stats['minutes'] / 40) * (adjusted_team_stats['two_pt_makes'] + adjusted_team_stats['three_pt_makes'])
        
        # Rebounds calculation
        player_oreb_rate = float(player.get('OR%', 5)) / 100
        player_dreb_rate = float(player.get('DR%', 10)) / 100
        opp_oreb_rate = float(four_factors.loc[opp_name]['Off-OR%']) / 100
        opp_oreb_rate_allowed = float(four_factors.loc[opp_name]['Def-OR%']) / 100
        
        weighted_oreb_rate = (player_oreb_rate * 0.75) + (opp_oreb_rate_allowed * 0.25)
        weighted_dreb_rate = (player_dreb_rate * 0.75) + (opp_oreb_rate * 0.25)
        
        team_misses = adjusted_team_stats['three_pt_misses'] + adjusted_team_stats['two_pt_misses'] + adjusted_team_stats['live_ball_ft_misses']
        opp_misses = opp_stats['three_pt_misses'] + opp_stats['two_pt_misses'] + opp_stats['live_ball_ft_misses']
        
        minutes_ratio = adjusted_player_stats['minutes'] / 40
        oreb = weighted_oreb_rate * opp_misses * minutes_ratio
        dreb = weighted_dreb_rate * team_misses * minutes_ratio

        player_block_rate = float(player.get('Blk%', 1.0)) / 100  # Default to 1% if not available
        opp_block_rate = float(opp_misc_stats['Blk%']) / 100
        weighted_block_rate = (player_block_rate * 0.85) + (opp_block_rate * 0.15)
        blocks = weighted_block_rate * (adjusted_player_stats['minutes'] / 40) * opp_stats['total_fga']

        player_steal_rate = float(player.get('Stl%', 1.0)) / 100  # Default to 1% if not available
        opp_steal_rate = float(opp_misc_stats['Stl%']) / 100
        weighted_steal_rate = (player_steal_rate * 0.80) + (opp_steal_rate * 0.20)
        steals = weighted_steal_rate * (adjusted_player_stats['minutes'] / 40) * adjusted_team_stats['pace']

        adjusted_projections[team_name][player_name].update({
            'blocks': blocks,
            'steals': steals,
            'rebounds': oreb + dreb,
            'assists': player_assists,
        })

    print(adjusted_projections)

    return adjusted_projections

    """
    TODO: Scaling logic needs to be rewritten. Current considerations:
    
    1. Points Scaling
    - Need to scale individual player points to match KenPom team total
    - Should preserve relative scoring distributions between players
    - Consider using weighted scaling based on usage rates
    
    2. Assists Scaling  
    - Total team assists should align with made field goals
    - Need to account for % of made shots that are assisted
    - Consider opponent defensive assist rates
    
    3. Other Stats
    - Rebounds should sum close to total available rebounds
    - Steals/blocks may need team-level constraints
    - Minutes should sum to 200 (40 min * 5 players)
    
    4. Implementation
    - May want separate scaling passes for different stat categories
    - Could use iterative scaling to converge on targets
    - Need to handle edge cases (missing data, extreme values)
    """


def normalize_team_name(team_name):
    """Normalize team names as they sometimes change unexpectedly"""
    # Decode URL-encoded characters (e.g., %26 -> &, %27 -> ')
    normalized_name = unquote(team_name)
    # Replace + signs with spaces
    normalized_name = normalized_name.replace('+', ' ')
    return normalized_name

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date in MM-DD-YYYY format")
    parser.add_argument("--top_n", type=int, default=None, help="Limit to top N teams")
    args = parser.parse_args()
    [four_factors, team_stats, team_stats_def, points_dist, player_df] = main_fn(args.date, top_n=args.top_n)
    projections = calculate_player_props(four_factors, team_stats, team_stats_def, points_dist, player_df)

    print(len(projections))

#!/usr/bin/env python3

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

from scripts import full_pipeline


TEAM_RESPONSE_RE = re.compile(
    r"^\S+\s+\S+\s+INFO xAI team call(?: reformat follow-up)? response for (?P<team>.+):$"
)
TEAM_CITATIONS_RE = re.compile(r"^\S+\s+\S+\s+INFO xAI team call citations for (?P<team>.+):$")
FOLLOWUP_RESPONSE_RE = re.compile(
    r"^\S+\s+\S+\s+INFO xAI low-confidence(?: reformat)? follow-up response for (?P<team>.+) \((?P<player>.+)\):$"
)
FOLLOWUP_CITATIONS_RE = re.compile(
    r"^\S+\s+\S+\s+INFO xAI low-confidence follow-up citations for (?P<team>.+) \((?P<player>.+)\):$"
)


def _parse_json_line(line: str) -> Any | None:
    text = line.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _parse_citations_line(line: str) -> list[Any]:
    payload = _parse_json_line(line)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, str):
        try:
            parsed = ast.literal_eval(payload)
        except (SyntaxError, ValueError):
            return [payload]
        return parsed if isinstance(parsed, list) else [parsed]
    return []


def _extract_updated_at(line: str) -> str:
    prefix = line.split(" INFO ", 1)[0]
    dt = datetime.strptime(prefix, "%Y-%m-%d %H:%M:%S,%f")
    return dt.isoformat()


def parse_manual_log(log_path: Path) -> dict[str, Any]:
    cache_payload: dict[str, Any] = {"teams": {}}
    lines = log_path.read_text(encoding="utf-8").splitlines()
    idx = 0

    while idx < len(lines):
        line = lines[idx]

        team_match = TEAM_RESPONSE_RE.match(line)
        if team_match and idx + 1 < len(lines):
            team = team_match.group("team").strip()
            response_text = lines[idx + 1].strip()
            projection_data = _parse_json_line(response_text)
            if isinstance(projection_data, dict):
                citations = []
                if idx + 3 < len(lines):
                    citations_match = TEAM_CITATIONS_RE.match(lines[idx + 2])
                    if citations_match and citations_match.group("team").strip() == team:
                        citations = _parse_citations_line(lines[idx + 3])
                        idx += 2
                team_entry = full_pipeline.get_minutes_cache_team_entry(cache_payload, team)
                team_entry["teamMinutes"] = {
                    "projectionData": projection_data,
                    "result": {
                        "text": response_text,
                        "citations": citations,
                    },
                    "updatedAt": _extract_updated_at(line),
                }
            idx += 2
            continue

        followup_match = FOLLOWUP_RESPONSE_RE.match(line)
        if followup_match and idx + 1 < len(lines):
            team = followup_match.group("team").strip()
            player = followup_match.group("player").strip()
            response_text = lines[idx + 1].strip()
            adjustment_data = _parse_json_line(response_text)
            if isinstance(adjustment_data, dict):
                citations = []
                if idx + 3 < len(lines):
                    citations_match = FOLLOWUP_CITATIONS_RE.match(lines[idx + 2])
                    if (
                        citations_match
                        and citations_match.group("team").strip() == team
                        and citations_match.group("player").strip() == player
                    ):
                        citations = _parse_citations_line(lines[idx + 3])
                        idx += 2
                team_entry = full_pipeline.get_minutes_cache_team_entry(cache_payload, team)
                players_cache = team_entry.setdefault("players", {})
                if not isinstance(players_cache, dict):
                    players_cache = {}
                    team_entry["players"] = players_cache
                players_cache[player] = {
                    "adjustmentData": adjustment_data,
                    "result": {
                        "text": response_text,
                        "citations": citations,
                    },
                    "updatedAt": _extract_updated_at(line),
                }
            idx += 2
            continue

        idx += 1

    return cache_payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild minutes cache from a completed kenpom_x_ai log")
    parser.add_argument("--manual-log", required=True, help="Path to the kenpom_x_ai log file")
    parser.add_argument("--cache-date", required=True, help="Minutes cache date in MM-DD-YYYY format")
    parser.add_argument("--output", default=None, help="Optional explicit JSON output path")
    args = parser.parse_args()

    log_path = Path(args.manual_log).expanduser().resolve()
    if not log_path.exists():
        raise SystemExit(f"Manual log not found: {log_path}")

    cache_payload = parse_manual_log(log_path)
    normalized_date = full_pipeline.normalize_minutes_cache_date(args.cache_date)
    output_path = Path(args.output).expanduser().resolve() if args.output else (
        full_pipeline.MINUTES_CACHE_DIR / f"{normalized_date}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            full_pipeline._coerce_json_safe(
                full_pipeline._normalize_minutes_cache_payload(cache_payload, normalized_date)
            ),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    teams = cache_payload.get("teams", {})
    follow_up_count = sum(
        len(team_entry.get("players", {}))
        for team_entry in teams.values()
        if isinstance(team_entry, dict)
    )
    print(f"Manual log: {log_path}")
    print(f"Cache date: {normalized_date}")
    print(f"Teams with team minutes: {sum(1 for entry in teams.values() if isinstance(entry, dict) and 'teamMinutes' in entry)}")
    print(f"Follow-up player entries: {follow_up_count}")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()

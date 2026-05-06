import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

from kenpompy.misc import get_current_season
from kenpompy.team import get_recent_lineups, get_valid_teams
from kenpompy.utils import login


DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "recent_lineups"


def slugify_team_name(team_name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", team_name).strip("_")
    return slug.lower() or "team"


def lineup_df_to_json_payload(lineups_df):
    payload = []
    for _, row in lineups_df.iterrows():
        lineup = []
        for position in ["PG", "SG", "SF", "PF", "C"]:
            name = row.get(f"{position}_Name")
            if name:
                lineup.append(str(name))

        pct = row.get("Pct")
        payload.append(
            {
                "pctMinutes": None if pct is None else float(pct) / 100.0,
                "lineup": lineup,
            }
        )
    return payload


def scrape_team_lineups(browser, team: str, season: int):
    lineups_df = get_recent_lineups(browser, team=team, season=season)
    return lineup_df_to_json_payload(lineups_df)


def write_team_payload(output_dir: Path, team: str, payload):
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{slugify_team_name(team)}.json"
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


def parse_args():
    parser = argparse.ArgumentParser(description="Scrape KenPom recent lineups into simple JSON.")
    parser.add_argument("--team", action="append", dest="teams", help="Specific team to scrape. Can be provided multiple times.")
    parser.add_argument("--season", type=int, default=None, help="Season year. Defaults to current KenPom season.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for output JSON files.")
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Delay between team requests when scraping multiple teams.")
    parser.add_argument("--email", default=None, help="KenPom login email. Overrides EMAIL from the environment.")
    parser.add_argument("--password", default=None, help="KenPom login password. Overrides PASSWORD from the environment.")
    return parser.parse_args()


def main():
    load_dotenv(REPO_ROOT / ".env")
    args = parse_args()

    email = args.email or os.getenv("EMAIL")
    password = args.password or os.getenv("PASSWORD")
    if not email or not password:
        raise RuntimeError("Missing EMAIL/PASSWORD credentials. Set them in the environment or .env.")
    browser = login(email, password)

    season = args.season
    teams = args.teams or get_valid_teams(browser, season=season)
    if season is None:
        season = int(get_current_season(browser))

    output_dir = Path(args.output_dir)
    results = []

    for index, team in enumerate(teams):
        payload = scrape_team_lineups(browser, team=team, season=season)
        output_path = write_team_payload(output_dir, team, payload)
        results.append(
            {
                "team": team,
                "season": season,
                "outputPath": str(output_path),
                "lineups": payload,
            }
        )
        if index < len(teams) - 1:
            time.sleep(args.sleep_seconds)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()

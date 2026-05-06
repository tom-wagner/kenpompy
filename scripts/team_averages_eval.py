"""
team_averages_eval.py

Evaluate projection model output against actual NCAA team per-game averages.

NOTE: This evaluation is only useful for team-level calibration. It sums player
projections to team totals and compares those totals against season per-game
team averages, so it cannot assess player-level projection quality. Two models
can produce nearly identical team MAE/RMSE/R^2 while differing materially in
player allocations. Earlier experiments in this repo produced almost the same
team-level scores even though player AST/REB allocations changed substantially,
because both normalized players back to similar team targets.
Example failure mode: a model can over-project a low-assist center and under-
project a lead guard, yet still look fine here if the team AST total matches.
Use player-level box score backtests to evaluate player props.

Usage:
    python3 scripts/team_averages_eval.py --projections projections_FILE.xlsx
    python3 scripts/team_averages_eval.py --projections projections_FILE.xlsx --verbose
    python3 scripts/team_averages_eval.py --projections projections_FILE.xlsx --team Alabama

Reads team_averages.json (actual per-game stats from sports-reference.com)
and compares against projected team totals from the model.
"""

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVAL_OUTPUT_DIR = REPO_ROOT / 'outputs' / 'evals'
DEFAULT_ACTUALS_PATH = REPO_ROOT / 'team_averages.json'


# ---------------------------------------------------------------------------
# Name mapping: sports-reference team names -> KenPom URL-encoded names
# ---------------------------------------------------------------------------

# KenPom uses URL-encoded names with + for spaces and some abbreviations.
# This mapping covers common mismatches. Teams not in this map are matched
# by fuzzy normalization (lowercase, strip punctuation, collapse spaces).

SR_TO_KENPOM = {
    "Arizona State": "Arizona+St.",
    "Arkansas State": "Arkansas+St.",
    "Arkansas-Pine Bluff": "Arkansas+Pine+Bluff",
    "Ball State": "Ball+St.",
    "Boise State": "Boise+St.",
    "Boston University": "Boston+U.",
    "Bowling Green": "Bowling+Green",
    "Brigham Young": "BYU",
    "Cal Poly": "Cal+Poly",
    "Cal State Bakersfield": "Cal+St.+Bakersfield",
    "Cal State Fullerton": "Cal+St.+Fullerton",
    "Cal State Northridge": "CSUN",
    "Central Arkansas": "Central+Arkansas",
    "Central Connecticut State": "Central+Connecticut",
    "Central Michigan": "Central+Michigan",
    "Charleston Southern": "Charleston+Southern",
    "Chicago State": "Chicago+St.",
    "Cleveland State": "Cleveland+St.",
    "Coastal Carolina": "Coastal+Carolina",
    "College of Charleston": "Charleston",
    "Colorado State": "Colorado+St.",
    "Connecticut": "Connecticut",
    "Coppin State": "Coppin+St.",
    "Cornell": "Cornell",
    "Creighton": "Creighton",
    "Dartmouth": "Dartmouth",
    "Delaware State": "Delaware+St.",
    "Denver": "Denver",
    "DePaul": "DePaul",
    "Detroit Mercy": "Detroit",
    "Drake": "Drake",
    "Drexel": "Drexel",
    "Duke": "Duke",
    "Duquesne": "Duquesne",
    "East Carolina": "East+Carolina",
    "East Tennessee State": "East+Tennessee+St.",
    "Eastern Illinois": "Eastern+Illinois",
    "Eastern Kentucky": "Eastern+Kentucky",
    "Eastern Michigan": "Eastern+Michigan",
    "Eastern Washington": "Eastern+Washington",
    "Elon": "Elon",
    "Evansville": "Evansville",
    "Fairfield": "Fairfield",
    "Fairleigh Dickinson": "Fairleigh+Dickinson",
    "Florida A&M": "Florida+A%26M",
    "Florida Atlantic": "Florida+Atlantic",
    "Florida Gulf Coast": "Florida+Gulf+Coast",
    "Florida International": "FIU",
    "Florida State": "Florida+St.",
    "Fordham": "Fordham",
    "Fort Wayne": "Purdue+Fort+Wayne",
    "Fresno State": "Fresno+St.",
    "Furman": "Furman",
    "Gardner-Webb": "Gardner+Webb",
    "George Mason": "George+Mason",
    "George Washington": "George+Washington",
    "Georgetown": "Georgetown",
    "Georgia Southern": "Georgia+Southern",
    "Georgia State": "Georgia+St.",
    "Georgia Tech": "Georgia+Tech",
    "Gonzaga": "Gonzaga",
    "Grambling": "Grambling",
    "Grand Canyon": "Grand+Canyon",
    "Green Bay": "Green+Bay",
    "Hampton": "Hampton",
    "Hartford": "Hartford",
    "Harvard": "Harvard",
    "Hawaii": "Hawai'i",
    "High Point": "High+Point",
    "Hofstra": "Hofstra",
    "Holy Cross": "Holy+Cross",
    "Houston": "Houston",
    "Houston Christian": "Houston+Christian",
    "Howard": "Howard",
    "Idaho": "Idaho",
    "Idaho State": "Idaho+St.",
    "Illinois": "Illinois",
    "Illinois State": "Illinois+St.",
    "Incarnate Word": "Incarnate+Word",
    "Indiana": "Indiana",
    "Indiana State": "Indiana+St.",
    "Iona": "Iona",
    "Iowa": "Iowa",
    "Iowa State": "Iowa+St.",
    "IUPUI": "IUPUI",
    "Jackson State": "Jackson+St.",
    "Jacksonville": "Jacksonville",
    "Jacksonville State": "Jacksonville+St.",
    "James Madison": "James+Madison",
    "Kansas": "Kansas",
    "Kansas State": "Kansas+St.",
    "Kennesaw State": "Kennesaw+St.",
    "Kent State": "Kent+St.",
    "Kentucky": "Kentucky",
    "La Salle": "La+Salle",
    "Lafayette": "Lafayette",
    "Lamar": "Lamar",
    "Le Moyne": "Le+Moyne",
    "Lehigh": "Lehigh",
    "Liberty": "Liberty",
    "Lindenwood": "Lindenwood",
    "Lipscomb": "Lipscomb",
    "Little Rock": "Little+Rock",
    "Long Beach State": "Long+Beach+St.",
    "Long Island University": "LIU",
    "Longwood": "Longwood",
    "Louisiana": "Louisiana",
    "Louisiana Tech": "Louisiana+Tech",
    "Louisiana-Monroe": "UL+Monroe",
    "Louisville": "Louisville",
    "Loyola (IL)": "Loyola+Chicago",
    "Loyola (MD)": "Loyola+MD",
    "Loyola Marymount": "Loyola+Marymount",
    "LSU": "LSU",
    "Maine": "Maine",
    "Manhattan": "Manhattan",
    "Marist": "Marist",
    "Marquette": "Marquette",
    "Marshall": "Marshall",
    "Maryland": "Maryland",
    "Maryland-Baltimore County": "UMBC",
    "Maryland-Eastern Shore": "Maryland+Eastern+Shore",
    "Massachusetts": "Massachusetts",
    "Massachusetts-Lowell": "UMass+Lowell",
    "McNeese State": "McNeese+St.",
    "Memphis": "Memphis",
    "Mercer": "Mercer",
    "Merrimack": "Merrimack",
    "Miami (FL)": "Miami+FL",
    "Miami (OH)": "Miami+OH",
    "Michigan": "Michigan",
    "Michigan State": "Michigan+St.",
    "Middle Tennessee": "Middle+Tennessee",
    "Milwaukee": "Milwaukee",
    "Minnesota": "Minnesota",
    "Mississippi": "Ole+Miss",
    "Mississippi State": "Mississippi+St.",
    "Mississippi Valley State": "Mississippi+Valley+St.",
    "Missouri": "Missouri",
    "Missouri State": "Missouri+St.",
    "Monmouth": "Monmouth",
    "Montana": "Montana",
    "Montana State": "Montana+St.",
    "Morehead State": "Morehead+St.",
    "Morgan State": "Morgan+St.",
    "Mount St. Mary's": "Mount+St.+Mary's",
    "Murray State": "Murray+St.",
    "Navy": "Navy",
    "Nebraska": "Nebraska",
    "Nevada": "Nevada",
    "Nevada-Las Vegas": "UNLV",
    "New Hampshire": "New+Hampshire",
    "New Mexico": "New+Mexico",
    "New Mexico State": "New+Mexico+St.",
    "New Orleans": "New+Orleans",
    "Niagara": "Niagara",
    "Nicholls State": "Nicholls+St.",
    "Norfolk State": "Norfolk+St.",
    "North Alabama": "North+Alabama",
    "North Carolina": "North+Carolina",
    "North Carolina A&T": "North+Carolina+A%26T",
    "North Carolina Central": "North+Carolina+Central",
    "North Carolina State": "N.C.+State",
    "North Dakota": "North+Dakota",
    "North Dakota State": "North+Dakota+St.",
    "North Florida": "North+Florida",
    "North Texas": "North+Texas",
    "Northeastern": "Northeastern",
    "Northern Arizona": "Northern+Arizona",
    "Northern Colorado": "Northern+Colorado",
    "Northern Illinois": "Northern+Illinois",
    "Northern Iowa": "Northern+Iowa",
    "Northern Kentucky": "Northern+Kentucky",
    "Northwestern": "Northwestern",
    "Northwestern State": "Northwestern+St.",
    "Notre Dame": "Notre+Dame",
    "Oakland": "Oakland",
    "Ohio": "Ohio",
    "Ohio State": "Ohio+St.",
    "Oklahoma": "Oklahoma",
    "Oklahoma State": "Oklahoma+St.",
    "Old Dominion": "Old+Dominion",
    "Omaha": "Omaha",
    "Oral Roberts": "Oral+Roberts",
    "Oregon": "Oregon",
    "Oregon State": "Oregon+St.",
    "Pacific": "Pacific",
    "Penn State": "Penn+St.",
    "Pennsylvania": "Penn",
    "Pepperdine": "Pepperdine",
    "Pittsburgh": "Pittsburgh",
    "Portland": "Portland",
    "Portland State": "Portland+St.",
    "Prairie View A&M": "Prairie+View+A%26M",
    "Presbyterian": "Presbyterian",
    "Princeton": "Princeton",
    "Providence": "Providence",
    "Purdue": "Purdue",
    "Purdue Fort Wayne": "Purdue+Fort+Wayne",
    "Quinnipiac": "Quinnipiac",
    "Radford": "Radford",
    "Rhode Island": "Rhode+Island",
    "Rice": "Rice",
    "Richmond": "Richmond",
    "Rider": "Rider",
    "Robert Morris": "Robert+Morris",
    "Rutgers": "Rutgers",
    "Sacramento State": "Sacramento+St.",
    "Sacred Heart": "Sacred+Heart",
    "Saint Francis (PA)": "St.+Francis+PA",
    "Saint Joseph's": "Saint+Joseph's",
    "Saint Louis": "Saint+Louis",
    "Saint Mary's (CA)": "Saint+Mary's",
    "Saint Peter's": "Saint+Peter's",
    "Sam Houston State": "Sam+Houston+St.",
    "Samford": "Samford",
    "San Diego": "San+Diego",
    "San Diego State": "San+Diego+St.",
    "San Francisco": "San+Francisco",
    "San Jose State": "San+Jose+St.",
    "Santa Clara": "Santa+Clara",
    "Seattle": "Seattle",
    "Seton Hall": "Seton+Hall",
    "Siena": "Siena",
    "South Alabama": "South+Alabama",
    "South Carolina": "South+Carolina",
    "South Carolina State": "South+Carolina+St.",
    "South Carolina Upstate": "USC+Upstate",
    "South Dakota": "South+Dakota",
    "South Dakota State": "South+Dakota+St.",
    "South Florida": "South+Florida",
    "Southeast Missouri State": "Southeast+Missouri+St.",
    "Southeastern Louisiana": "Southeastern+Louisiana",
    "Southern": "Southern",
    "Southern California": "USC",
    "Southern Illinois": "Southern+Illinois",
    "Southern Illinois-Edwardsville": "SIU+Edwardsville",
    "Southern Methodist": "SMU",
    "Southern Mississippi": "Southern+Miss",
    "Southern Utah": "Southern+Utah",
    "St. Bonaventure": "St.+Bonaventure",
    "St. Francis (NY)": "St.+Francis+NY",
    "St. John's (NY)": "St.+John's",
    "St. Thomas (MN)": "St.+Thomas",
    "Stanford": "Stanford",
    "Stephen F. Austin": "Stephen+F.+Austin",
    "Stetson": "Stetson",
    "Stonehill": "Stonehill",
    "Stony Brook": "Stony+Brook",
    "Syracuse": "Syracuse",
    "Tarleton State": "Tarleton+St.",
    "Temple": "Temple",
    "Tennessee": "Tennessee",
    "Tennessee State": "Tennessee+St.",
    "Tennessee Tech": "Tennessee+Tech",
    "Texas": "Texas",
    "Texas A&M": "Texas+A%26M",
    "Texas A&M-Commerce": "Texas+A%26M+Commerce",
    "Texas A&M-Corpus Christi": "Texas+A%26M+Corpus+Christi",
    "Texas Christian": "TCU",
    "Texas Southern": "Texas+Southern",
    "Texas State": "Texas+St.",
    "Texas Tech": "Texas+Tech",
    "Texas-Arlington": "UT+Arlington",
    "Texas-Rio Grande Valley": "UT+Rio+Grande+Valley",
    "Texas-San Antonio": "UTSA",
    "Toledo": "Toledo",
    "Towson": "Towson",
    "Troy": "Troy",
    "Tulane": "Tulane",
    "Tulsa": "Tulsa",
    "UAB": "UAB",
    "UC Davis": "UC+Davis",
    "UC Irvine": "UC+Irvine",
    "UC Riverside": "UC+Riverside",
    "UC San Diego": "UC+San+Diego",
    "UC Santa Barbara": "UC+Santa+Barbara",
    "UCF": "UCF",
    "UCLA": "UCLA",
    "UNC Asheville": "UNC+Asheville",
    "UNC Greensboro": "UNC+Greensboro",
    "UNC Wilmington": "UNC+Wilmington",
    "Utah": "Utah",
    "Utah State": "Utah+St.",
    "Utah Tech": "Utah+Tech",
    "Utah Valley": "Utah+Valley",
    "Valparaiso": "Valparaiso",
    "Vanderbilt": "Vanderbilt",
    "Vermont": "Vermont",
    "Villanova": "Villanova",
    "Virginia": "Virginia",
    "Virginia Commonwealth": "VCU",
    "Virginia Military Institute": "VMI",
    "Virginia Tech": "Virginia+Tech",
    "Wagner": "Wagner",
    "Wake Forest": "Wake+Forest",
    "Washington": "Washington",
    "Washington State": "Washington+St.",
    "Weber State": "Weber+St.",
    "West Virginia": "West+Virginia",
    "Western Carolina": "Western+Carolina",
    "Western Illinois": "Western+Illinois",
    "Western Kentucky": "Western+Kentucky",
    "Western Michigan": "Western+Michigan",
    "Wichita State": "Wichita+St.",
    "William & Mary": "William+%26+Mary",
    "Winthrop": "Winthrop",
    "Wisconsin": "Wisconsin",
    "Wofford": "Wofford",
    "Wright State": "Wright+St.",
    "Wyoming": "Wyoming",
    "Xavier": "Xavier",
    "Yale": "Yale",
    "Youngstown State": "Youngstown+St.",
    # Simple cases where SR name == KenPom name with + for spaces
    "Alabama": "Alabama",
    "Alabama A&M": "Alabama+A%26M",
    "Alabama State": "Alabama+St.",
    "Albany (NY)": "Albany",
    "Alcorn State": "Alcorn+St.",
    "American": "American",
    "Appalachian State": "Appalachian+St.",
    "Arizona": "Arizona",
    "Arkansas": "Arkansas",
    "Auburn": "Auburn",
    "Austin Peay": "Austin+Peay",
    "Baylor": "Baylor",
    "Bellarmine": "Bellarmine",
    "Belmont": "Belmont",
    "Bethune-Cookman": "Bethune+Cookman",
    "Binghamton": "Binghamton",
    "Boston College": "Boston+College",
    "Bradley": "Bradley",
    "Brown": "Brown",
    "Bryant": "Bryant",
    "Bucknell": "Bucknell",
    "Buffalo": "Buffalo",
    "Butler": "Butler",
    "California": "California",
    "California Baptist": "California+Baptist",
    "Campbell": "Campbell",
    "Canisius": "Canisius",
    "Charlotte": "Charlotte",
    "Chattanooga": "Chattanooga",
    "Cincinnati": "Cincinnati",
    "Clemson": "Clemson",
    "Colorado": "Colorado",
    "Columbia": "Columbia",
    "Georgia": "Georgia",
    "Illinois-Chicago": "Illinois+Chicago",
    "Illinois Chicago": "Illinois+Chicago",
    "Louisiana State": "LSU",
    "Nebraska-Omaha": "Nebraska+Omaha",
    "Omaha": "Nebraska+Omaha",
    "Grambling": "Grambling+St.",
    "Nicholls State": "Nicholls",
    "Southeast Missouri State": "Southeast+Missouri",
    "Tennessee-Martin": "Tennessee+Martin",
    "Queens (NC)": "Queens",
    "Sam Houston": "Sam+Houston+St.",
    "California Baptist": "Cal+Baptist",
    "Nebraska-Omaha": "Nebraska+Omaha",
    "Prairie View": "Prairie+View+A%26M",
}


def build_kenpom_lookup(sr_name):
    """Convert a sports-reference team name to KenPom format."""
    if sr_name in SR_TO_KENPOM:
        return SR_TO_KENPOM[sr_name]
    # Fallback: replace spaces with +
    return sr_name.replace(' ', '+')


def normalize_name(name):
    """Normalize a team name for fuzzy matching."""
    import re
    s = name.lower()
    s = s.replace('+', ' ').replace('%26', '&').replace('%27', "'")
    s = re.sub(r'[^a-z0-9 ]', '', s)
    return ' '.join(s.split())


# ---------------------------------------------------------------------------
# Stat mapping: model columns -> team_averages.json keys
# ---------------------------------------------------------------------------

STAT_MAP = {
    'PROJ PTS':  'ppg',
    'PROJ AST':  'apg',
    'PROJ REB':  'rpg',
    'PROJ 3PM':  'three_pm',
    'PROJ TO':   'topg',
    'PROJ STL':  'spg',
    'PROJ BLK':  'bpg',
}


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def load_actuals(path='team_averages.json'):
    with open(path) as f:
        data = json.load(f)
    return data['teams']


def load_projections(path):
    df = pd.read_excel(path, sheet_name='Projections')
    # Sum player projections to team totals
    team_totals = {}
    for team, grp in df.groupby('Team'):
        team_totals[str(team)] = {col: grp[col].sum() for col in STAT_MAP}
    return team_totals


def match_teams(proj_teams, actual_teams):
    """Match projection team names to actual team names. Returns list of (proj_name, sr_name, actual_dict)."""

    # Build reverse lookup: normalized KenPom name -> proj team name
    proj_norm = {}
    for pt in proj_teams:
        proj_norm[normalize_name(pt)] = pt

    # Build forward lookup: SR name -> KenPom name -> normalized
    matches = []
    unmatched_sr = []
    matched_proj = set()

    for sr_name, stats in actual_teams.items():
        kp = build_kenpom_lookup(sr_name)
        kp_norm = normalize_name(kp)

        if kp_norm in proj_norm:
            pname = proj_norm[kp_norm]
            matches.append((pname, sr_name, stats))
            matched_proj.add(pname)
        else:
            # Try normalizing the SR name directly
            sr_norm = normalize_name(sr_name)
            if sr_norm in proj_norm:
                pname = proj_norm[sr_norm]
                matches.append((pname, sr_name, stats))
                matched_proj.add(pname)
            else:
                unmatched_sr.append(sr_name)

    unmatched_proj = [p for p in proj_teams if p not in matched_proj]
    return matches, unmatched_sr, unmatched_proj


def evaluate(proj_path, actuals_path='team_averages.json', verbose=False, team_filter=None):
    actual_teams = load_actuals(actuals_path)
    proj_teams = load_projections(proj_path)

    matches, unmatched_sr, unmatched_proj = match_teams(proj_teams, actual_teams)

    if team_filter:
        tf = team_filter.lower()
        matches = [(p, s, a) for p, s, a in matches
                    if tf in p.lower() or tf in s.lower()]

    print(f"Matched {len(matches)} teams | "
          f"Unmatched in projections: {len(unmatched_proj)} | "
          f"Unmatched in actuals: {len(unmatched_sr)}")

    if verbose and unmatched_proj:
        print(f"\n  Unmatched projection teams (no actual data):")
        for t in sorted(unmatched_proj)[:20]:
            print(f"    {t}")

    # Compute per-stat errors
    results = {}
    for stat_col, stat_key in STAT_MAP.items():
        errors = []
        for proj_name, sr_name, actual_stats in matches:
            projected = proj_teams[proj_name][stat_col]
            actual = actual_stats[stat_key]
            err = projected - actual
            errors.append({
                'proj_name': proj_name,
                'sr_name': sr_name,
                'projected': projected,
                'actual': actual,
                'error': err,
                'abs_error': abs(err),
                'pct_error': err / actual * 100 if actual != 0 else 0,
            })

        errors.sort(key=lambda x: x['abs_error'], reverse=True)
        abs_errors = [e['abs_error'] for e in errors]
        raw_errors = [e['error'] for e in errors]
        pct_errors = [abs(e['pct_error']) for e in errors]

        results[stat_col] = {
            'mae': np.mean(abs_errors),
            'rmse': np.sqrt(np.mean(np.square(raw_errors))),
            'mean_bias': np.mean(raw_errors),
            'median_ae': np.median(abs_errors),
            'p90_ae': np.percentile(abs_errors, 90),
            'mape': np.mean(pct_errors),
            'r_squared': np.corrcoef(
                [e['projected'] for e in errors],
                [e['actual'] for e in errors]
            )[0, 1] ** 2 if len(errors) > 1 else 0,
            'n': len(errors),
            'worst': errors[:5],
        }

    # Print results
    print(f"\n{'='*80}")
    print(f"MODEL EVALUATION: Projected Team Totals vs Actual Per-Game Averages")
    print(f"{'='*80}")

    # Summary table
    stat_short = {
        'PROJ PTS': 'PTS', 'PROJ AST': 'AST', 'PROJ REB': 'REB',
        'PROJ 3PM': '3PM', 'PROJ TO': 'TO', 'PROJ STL': 'STL', 'PROJ BLK': 'BLK',
    }

    print(f"\n{'Stat':>5s}  {'MAE':>6s}  {'RMSE':>6s}  {'Bias':>7s}  "
          f"{'MedAE':>6s}  {'P90':>6s}  {'MAPE%':>6s}  {'R^2':>5s}  {'N':>4s}")
    print(f"{'-'*5}  {'-'*6}  {'-'*6}  {'-'*7}  {'-'*6}  {'-'*6}  {'-'*6}  {'-'*5}  {'-'*4}")

    for stat_col in STAT_MAP:
        r = results[stat_col]
        label = stat_short[stat_col]
        print(f"{label:>5s}  {r['mae']:6.2f}  {r['rmse']:6.2f}  {r['mean_bias']:+7.2f}  "
              f"{r['median_ae']:6.2f}  {r['p90_ae']:6.2f}  {r['mape']:5.1f}%  "
              f"{r['r_squared']:5.3f}  {r['n']:4d}")

    # Detailed per-stat breakdown
    for stat_col in STAT_MAP:
        r = results[stat_col]
        label = stat_short[stat_col]
        print(f"\n--- {label} (MAE={r['mae']:.2f}, Bias={r['mean_bias']:+.2f}, R²={r['r_squared']:.3f}) ---")
        print(f"  Worst 5 misses:")
        for w in r['worst'][:5]:
            print(f"    {w['sr_name']:30s}  proj={w['projected']:6.1f}  actual={w['actual']:6.1f}  "
                  f"err={w['error']:+6.1f}  ({w['pct_error']:+.1f}%)")

    # Overall grade
    print(f"\n{'='*80}")
    print(f"OVERALL SUMMARY")
    print(f"{'='*80}")
    total_mae = np.mean([results[s]['mae'] for s in STAT_MAP])
    print(f"  Average MAE across all stats: {total_mae:.2f}")
    print(f"  Average R² across all stats:  "
          f"{np.mean([results[s]['r_squared'] for s in STAT_MAP]):.3f}")
    print(f"  Average MAPE across all stats: "
          f"{np.mean([results[s]['mape'] for s in STAT_MAP]):.1f}%")

    # Bias direction summary
    print(f"\n  Bias direction (positive = model over-predicts):")
    for stat_col in STAT_MAP:
        r = results[stat_col]
        label = stat_short[stat_col]
        direction = "OVER " if r['mean_bias'] > 0 else "UNDER"
        print(f"    {label:>4s}: {direction} by {abs(r['mean_bias']):.2f} ({r['mean_bias']/np.mean([e['actual'] for e in r['worst'][:r['n']]])*100 if r['n'] > 0 else 0:+.1f}%)")

    return results


def save_eval_json(results, proj_path, output_dir=DEFAULT_EVAL_OUTPUT_DIR):
    """Save evaluation results to a timestamped JSON file."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stat_short = {
        'PROJ PTS': 'PTS', 'PROJ AST': 'AST', 'PROJ REB': 'REB',
        'PROJ 3PM': '3PM', 'PROJ TO': 'TO', 'PROJ STL': 'STL', 'PROJ BLK': 'BLK',
    }

    import datetime
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
    input_stem = os.path.splitext(os.path.basename(proj_path))[0]

    serializable = {}
    for stat_col, r in results.items():
        label = stat_short.get(stat_col, stat_col)
        worst = []
        for w in r['worst']:
            worst.append({
                'team': w['sr_name'],
                'projected': round(w['projected'], 2),
                'actual': round(w['actual'], 2),
                'error': round(w['error'], 2),
                'pct_error': round(w['pct_error'], 1),
            })
        serializable[label] = {
            'mae': round(r['mae'], 4),
            'rmse': round(r['rmse'], 4),
            'mean_bias': round(r['mean_bias'], 4),
            'median_ae': round(r['median_ae'], 4),
            'p90_ae': round(r['p90_ae'], 4),
            'mape': round(r['mape'], 2),
            'r_squared': round(r['r_squared'], 4),
            'n': r['n'],
            'worst_5': worst,
        }

    output = {
        'timestamp': timestamp,
        'projections_file': proj_path,
        'overall': {
            'avg_mae': round(np.mean([r['mae'] for r in results.values()]), 4),
            'avg_r_squared': round(np.mean([r['r_squared'] for r in results.values()]), 4),
            'avg_mape': round(np.mean([r['mape'] for r in results.values()]), 2),
        },
        'stats': serializable,
    }

    filename = f"eval_{timestamp}_{input_stem}.json"
    filepath = output_dir / filename
    with open(filepath, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"\nEval results saved to: {filepath}")
    return str(filepath)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Evaluate projection model against actual team averages')
    parser.add_argument('--projections', required=True,
                        help='Path to projections Excel file (output of projection_model.py)')
    parser.add_argument('--actuals', default=str(DEFAULT_ACTUALS_PATH),
                        help='Path to team_averages.json')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show unmatched teams and additional detail')
    parser.add_argument('--team', default=None,
                        help='Filter to a specific team (partial match)')
    parser.add_argument('--output-dir', default=str(DEFAULT_EVAL_OUTPUT_DIR),
                        help='Directory to save eval JSON output')
    args = parser.parse_args()

    if not os.path.exists(args.projections):
        print(f"Error: projections file not found: {args.projections}")
        sys.exit(1)
    if not os.path.exists(args.actuals):
        print(f"Error: actuals file not found: {args.actuals}")
        sys.exit(1)

    results = evaluate(args.projections, args.actuals, verbose=args.verbose, team_filter=args.team)
    save_eval_json(results, args.projections, output_dir=args.output_dir)


if __name__ == '__main__':
    main()

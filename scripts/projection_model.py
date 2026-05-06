"""
projection_model.py

Replicates EXISTING_MODEL.xlsx PlayerStats projection formulas in Python.

IMPORTANT: This is the only supported projection model in the repo.

Known improvement areas from prior exploration:
1. Minutes projection is still the biggest model risk. The current L3-based
   approach can materially under-project active players after one-off zeros,
   suppressed games, foul-trouble outliers, or noisy recent rotations, which
   then cascades into false under edges across multiple stats. A better design
   would blend recent-game minutes, season role, opponent/game context, and
   injury/availability information while explicitly forcing realistic 200-minute
   team allocations.
2. Points remain structurally conservative because team scoring is anchored to
   the KenPom game projection rather than a stronger market-implied scoring
   target. Prior eval work suggested part of the residual AST and 3PM
   underprojection is downstream of this low PTS anchor rather than broken
   stat-specific inputs.
3. Assists are much stronger than the Excel baseline after the ARate fix and
   Team.A% / Opponent.Def.A% anchoring, but team AST targets could still be
   improved with a stronger external calibration. Prior investigation suggested
   the remaining bias is mostly tied to low projected FGM, not an A% mapping
   bug.
4. Rebounds are materially better after removing the 1.1x multiplier and using
   a top-down team rebound target, but player allocation can likely improve with
   more explicit matchup-specific rebound-opportunity modeling, especially for
   offensive vs defensive rebound splits.
5. Threes are in good shape, but a light opponent perimeter-defense adjustment
   could help at the margin as long as it does not reintroduce inconsistencies
   like projecting more 3-point scoring than total points.
6. Turnovers are one of the strongest stats in the model. Remaining work here
   is mostly calibration and monitoring rather than redesign.
7. Steals improved substantially after the top-down forced-turnover anchor, but
   the steal-to-turnover conversion is still a league-wide constant. Team-style-
   specific calibration could improve aggressive pressure teams and low-steal
   half-court defenses.
8. Blocks improved substantially after switching to opponent 2PA-only logic,
   but they still run a bit low on some teams. A better opponent shot-profile
   estimate or matchup-specific 2PA expectation is the main next lever.

Input: MAIN_SCRIPT_OUTPUT.xlsx (or any file with the same PlayerStats schema).
Output: DataFrame with PROJ PTS, PROJ AST, PROJ REB, PROJ 3PM, PROJ TO, PROJ STL, PROJ BLK.

Formula reference (from EXISTING_MODEL.xlsx PlayerStats tab, header row 3, data row 4+):

Shooting projections (team-level, based on KenPom score):
  2P FGM  (IZ): ((Team.Off-2P/100 * VEGAS_PTS)*0.75 + (Opponent.Def-2P/100 * VEGAS_PTS)*0.25) / 2
  3P FGM  (JA): ((Team.Off-3P/100 * VEGAS_PTS)*0.75 + (Opponent.Def-3P/100 * VEGAS_PTS)*0.25) / 3
  2P FGA  (JB): 2P_FGM / ((Team.2P%*0.6 + Opponent.Def.2P%*0.4) / 100)
  3P FGA  (JC): 3P_FGM / ((Team.3P%*0.83 + Opponent.Def.3P%*0.17) / 100)
  FTM     (JD): ((Team.Off-FT*0.6 + Opponent.Def-FT*0.4) / 100) * VEGAS_PTS
  FTA     (JE): FTM / (Team.FT% / 100)
  (Same pattern for OPP using VEGAS_OPP and flipped team/opp columns.)

Player projections:
  PACE        (GV): AVERAGE(Opponent.AdjTempo, Team.AdjTempo)
  PTS/40      (GX): (%Poss/100) * (ORtg/100) * PACE
  SIMPLE PTS  (GY): PTS/40 * MINS_PROJ / 40
  PROJ PTS    (GZ): SIMPLE_PTS / (sum_team_SIMPLE_PTS / VEGAS_PTS)
  TEAM AST    (HB): (2P_FGM + 3P_FGM) * Team.A% / 100
  AST UNADJ   (HC): (2P_FGM + 3P_FGM) * (ARate/100) * (MINS_PROJ/40)
  PROJ AST    (HD): AST_UNADJ * TEAM_AST / sum_team_AST_UNADJ
  DefRebOpp   (HF): (OPP_2P_FGA - OPP_2P_FGM) + (OPP_3P_FGA - OPP_3P_FGM) + (OPP_FTA - OPP_FTM)*0.7
  OffRebOpp   (HG): (2P_FGA - 2P_FGM) + (3P_FGA - 3P_FGM) + (FTA - FTM)*0.7
  DefReb      (HH): DefRebOpp * DR%/100 * MINS_PROJ/40
  OffReb      (HI): OffRebOpp * OR%/100 * MINS_PROJ/40
  PROJ REB    (HJ): (DefReb + OffReb) * 1.1
  3PM/MIN     (HO): 3PM_season / (%Min/100 * 40 * G)  [iferror 0]
  UNADJ 3PM   (HP): 3PM/MIN * MINS_PROJ
  PROJ 3PM    (HQ): (3P_FGM / sum_team_UNADJ_3PM) * UNADJ_3PM
  PROJ TO     (HS): PACE * (%Poss/100) * ((TORate*0.7 + Opponent.Def-TO%*0.3)/100) * MINS_PROJ/40
  PROJ STL    (HV): (Stl%/100) * (MINS_PROJ/40) * PACE
  PROJ BLK    (HY): (MINS_PROJ/40) * (OPP_2P_FGA + OPP_3P_FGA) * (Blk%/100)
"""

import argparse
import datetime as dt
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_OUTPUT_DIR = REPO_ROOT / 'outputs' / 'model_projections'
PROJECTIONS_OUTPUT_DIR = MODEL_OUTPUT_DIR / 'projections'
EXISTING_MODEL_PATH = MODEL_OUTPUT_DIR / 'EXISTING_MODEL.xlsx'


# ---------------------------------------------------------------------------
# Model weights (from EXISTING_MODEL.xlsx formulas)
# ---------------------------------------------------------------------------

# Points distribution blending: team tendency vs. opponent tendency
OFF_DEF_DIST_TEAM_WT   = 0.75
OFF_DEF_DIST_OPP_WT    = 0.25

# Shooting % blending for FGA calculation (2P)
SHOOTING_2P_TEAM_WT    = 0.60
SHOOTING_2P_OPP_WT     = 0.40

# Shooting % blending for FGA calculation (3P)
SHOOTING_3P_TEAM_WT    = 0.83
SHOOTING_3P_OPP_WT     = 0.17

# FT distribution blending
FT_DIST_TEAM_WT        = 0.60
FT_DIST_OPP_WT         = 0.40

# Turnover rate blending: player vs. opponent defense (bottom-up per-player)
TO_RATE_PLAYER_WT      = 0.85
TO_RATE_OPP_DEF_WT     = 0.15

# Top-down TO anchor: blend team's offensive TO rate with opponent's defensive TO rate
TO_TOPDOWN_TEAM_WT     = 0.85
TO_TOPDOWN_OPP_WT      = 0.15

# FT miss reboundability factor
FT_MISS_REB_FACTOR     = 0.70

# Rebound projection multiplier.
# Previously 1.10 to approximate team/dead-ball rebounds not captured by
# individual DR%/OR%.  Set to 1.0 per user request — model will slightly
# underestimate total rebounds vs NCAA box scores (~3-5 RPG gap from
# uncaptured dead-ball/team rebounds).
REBOUND_MULTIPLIER     = 1.00

# Minutes per regulation game
MINUTES_PER_GAME       = 40.0

# Steal rate: multiplicative opponent adjustment.
# Stl% is already per-possession, so we do NOT blend additively with Opponent.Off-TO%
# (they're on different scales: Stl% ~2%, Off-TO% ~17%).
# Instead: adjust multiplicatively based on how turnover-prone the opponent is
# relative to the league average.
STL_OPP_ADJUSTMENT_WT  = 0.20  # 80% base Stl%, 20% opponent context

# Top-down STL calibration: KenPom individual Stl% values only reconstruct ~84%
# of actual team steals. Use Def-TO% * PACE * steal_rate as team target, then
# distribute proportionally by each player's stl_weight.
# NCAA average: ~59-60% of turnovers forced are steals (rest are violations,
# offensive fouls, etc.).  Previous value of 0.66 overestimated by ~0.70 SPG.
STEAL_TO_TURNOVER_RATE = 0.598

# PTS matchup adjustment: weight player's base ORtg vs opponent matchup factor
# 70% player efficiency, 30% opponent defensive matchup
PTS_MATCHUP_PLAYER_WT  = 0.70
PTS_MATCHUP_OPP_WT     = 0.30

# AST team-level anchor: use KenPom Team.A% (assists/FGM) blended with
# Opponent.Def.A% (opponent's A% allowed).  Pomeroy research shows A% is
# 71% offense-controlled, 29% defense-controlled.
AST_OFFENSE_CONTROL_WT = 0.71
AST_DEFENSE_CONTROL_WT = 0.29


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def safe_float(val, default=0.0):
    """Convert to float, returning *default* for NaN or unparseable values."""
    try:
        v = float(val)
        return default if math.isnan(v) else v
    except (TypeError, ValueError):
        return default


def safe_div(numerator, denominator, default=0.0):
    """Divide, returning *default* when *denominator* is zero."""
    return numerator / denominator if denominator != 0 else default


def parse_kenpom_result(result_str):
    """
    Parse KenPomResult like ``'W, 77-66'`` or ``'L, 77-66'``.

    The score is always ``WINNER_SCORE-LOSER_SCORE``.

    Returns ``(team_score, opp_score, kp_total)`` or ``(None, None, None)``.
    """
    if pd.isna(result_str) or str(result_str).strip() == '':
        return None, None, None
    s = str(result_str).strip()
    if len(s) < 4:
        return None, None, None
    wl = s[0]  # 'W' or 'L'
    try:
        w_score, l_score = (float(x) for x in s[3:].split('-'))
    except (ValueError, IndexError):
        return None, None, None
    team_score = w_score if wl == 'W' else l_score
    opp_score  = l_score if wl == 'W' else w_score
    return team_score, opp_score, w_score + l_score


def parse_three_pm_made(raw):
    """
    Extract season three-pointers made from a ``'made-attempted'`` string.

    Excel sometimes date-coerces small fractions (e.g. ``"3-8"`` becomes
    ``March 8``).  When that happens openpyxl/pandas returns a ``datetime``;
    we recover the month as the *made* count.
    """
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return 0.0
    if isinstance(raw, (dt.datetime, dt.date)):
        return float(raw.month)
    try:
        return float(str(raw).split('-')[0])
    except (ValueError, IndexError):
        return 0.0


def parse_made_attempted(raw):
    """
    Parse a ``'made-attempted'`` string (e.g. ``'62-112'``) into ``(made, attempted)``.

    Handles Excel date-coercion the same way as :func:`parse_three_pm_made`.
    """
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return 0.0, 0.0
    if isinstance(raw, (dt.datetime, dt.date)):
        return float(raw.month), float(raw.day)
    parts = str(raw).split('-')
    try:
        made = float(parts[0])
        att = float(parts[1]) if len(parts) > 1 else 0.0
        return made, att
    except (ValueError, IndexError):
        return 0.0, 0.0


def compute_scoring_profile(row):
    """
    Compute a player's scoring profile: fraction of season points from 2P, 3P, FT.

    Returns ``(share_2p, share_3p, share_ft)``.  Sums to ~1.0 if data is available,
    or ``(0.33, 0.33, 0.34)`` as a neutral fallback.
    """
    made_2, _ = parse_made_attempted(row.get('2PM-A'))
    made_3 = parse_three_pm_made(row.get('3PM-A'))
    made_ft, _ = parse_made_attempted(row.get('FTM-A'))

    total_pts = made_2 * 2 + made_3 * 3 + made_ft
    if total_pts <= 0:
        return 0.33, 0.33, 0.34  # neutral fallback
    return (made_2 * 2 / total_pts,
            made_3 * 3 / total_pts,
            made_ft / total_pts)


# ---------------------------------------------------------------------------
# Input reading
# ---------------------------------------------------------------------------

def read_player_stats(filepath, sheet='PlayerStats'):
    """
    Read the PlayerStats sheet from an Excel file.

    Handles two formats:
      - **MAIN_SCRIPT_OUTPUT** — header row 1, data from row 2.
      - **EXISTING_MODEL** — header row 3, data from row 4.

    Auto-detects which format is in use.
    """
    raw = pd.read_excel(filepath, sheet_name=sheet, header=None, nrows=4)
    candidate_rows = [0, 2]
    header_row = None

    for row_idx in candidate_rows:
        header_values = {
            str(value).strip()
            for value in raw.iloc[row_idx].tolist()
            if isinstance(value, str) and value.strip()
        }
        if 'Name' in header_values:
            header_row = row_idx
            break

    if header_row is None:
        for row_idx in candidate_rows:
            df = pd.read_excel(filepath, sheet_name=sheet, header=row_idx)
            if 'Name' in df.columns:
                header_row = row_idx
                break

    if header_row is None:
        raise ValueError(
            f"Could not locate a PlayerStats header row in {filepath!r}; "
            f"tried rows {[idx + 1 for idx in candidate_rows]}."
        )

    df = pd.read_excel(filepath, sheet_name=sheet, header=header_row)

    # Keep only rows where Name is a non-empty string
    df = df[df['Name'].apply(lambda x: isinstance(x, str) and x.strip() != '')]
    return df.reset_index(drop=True)


def get_mins_proj(row, overrides=None):
    """
    Determine projected minutes for a player.

    Priority:
      1. *overrides* dict keyed by ``(Name, Team)`` or ``Name``.
      2. ``MinsProj`` column from the scraper/xAI pipeline.
      3. ``MINS PROJ`` column from the file (EXISTING_MODEL carries this).
         A value of **0** means the player is explicitly projected not to play.
      4. **L3 MIN**: average of ``Game -1``, ``Game -2``, ``Game -3``.
      5. Fallback: ``%Min / 100 * 40``.
    """
    name = str(row.get('Name', ''))
    team = str(row.get('Team', ''))

    if overrides:
        for key in [(name, team), name]:
            if key in overrides:
                return overrides[key]

    for col in ('MinsProj', 'MINS PROJ', 'MINS_PROJ'):
        if col in row.index:
            v = safe_float(row.get(col), default=float('nan'))
            if not math.isnan(v):
                return v   # includes 0 — explicit "no minutes" projection

    # L3 MIN: average of last 3 games
    games = [safe_float(row.get(f'Game -{i}'), default=float('nan')) for i in range(1, 4)]
    games = [g for g in games if not math.isnan(g)]
    if games:
        return float(np.mean(games))

    # Fallback: season %Min * 40
    return safe_float(row.get('%Min', 0)) / 100.0 * MINUTES_PER_GAME


# ---------------------------------------------------------------------------
# Team-level shooting projections
# ---------------------------------------------------------------------------

def _project_side_shooting(row, score, prefix_off, prefix_def_dist, prefix_pct, prefix_def_pct):
    """
    Project one side's (team or opponent) shooting volumes.

    Parameters
    ----------
    row : pd.Series
        A single player row (used to look up team/opp column values).
    score : float
        Projected total points for this side.
    prefix_off : str
        Column-name prefix for the offensive point distribution
        (e.g. ``'Team'`` → ``'Team.Off-2P'``, ``'Team.Off-3P'``, ``'Team.Off-FT'``).
    prefix_def_dist : str
        Column-name prefix for the *opposing* defensive point distribution
        (e.g. ``'Opponent'`` → ``'Opponent.Def-2P'`` etc.).
    prefix_pct : str
        Column-name prefix for offensive shooting percentages
        (e.g. ``'Team'`` → ``'Team.2P%'``, ``'Team.3P%'``, ``'Team.FT%'``).
    prefix_def_pct : str
        Column-name prefix for the *opposing* defensive shooting percentages
        (e.g. ``'Opponent'`` → ``'Opponent.Def.2P%'`` etc.).

    Returns
    -------
    dict  with keys ``'2P FGM'``, ``'3P FGM'``, ``'2P FGA'``, ``'3P FGA'``,
          ``'FTM'``, ``'FTA'``.
    """
    # --- Field-goal makes (from point-distribution shares) ---
    off_2p  = safe_float(row.get(f'{prefix_off}.Off-2P', 0))
    def_2p  = safe_float(row.get(f'{prefix_def_dist}.Def-2P', 0))
    fgm_2p  = (off_2p / 100 * score * OFF_DEF_DIST_TEAM_WT +
               def_2p / 100 * score * OFF_DEF_DIST_OPP_WT) / 2

    off_3p  = safe_float(row.get(f'{prefix_off}.Off-3P', 0))
    def_3p  = safe_float(row.get(f'{prefix_def_dist}.Def-3P', 0))
    fgm_3p  = (off_3p / 100 * score * OFF_DEF_DIST_TEAM_WT +
               def_3p / 100 * score * OFF_DEF_DIST_OPP_WT) / 3

    # --- Field-goal attempts (from shooting percentages) ---
    pct_2p     = safe_float(row.get(f'{prefix_pct}.2P%', 50))
    def_pct_2p = safe_float(row.get(f'{prefix_def_pct}.2P%', 50))
    blended_2p = (pct_2p * SHOOTING_2P_TEAM_WT + def_pct_2p * SHOOTING_2P_OPP_WT) / 100
    fga_2p     = safe_div(fgm_2p, blended_2p)

    pct_3p     = safe_float(row.get(f'{prefix_pct}.3P%', 33))
    def_pct_3p = safe_float(row.get(f'{prefix_def_pct}.3P%', 33))
    blended_3p = (pct_3p * SHOOTING_3P_TEAM_WT + def_pct_3p * SHOOTING_3P_OPP_WT) / 100
    fga_3p     = safe_div(fgm_3p, blended_3p)

    # --- Free throws ---
    off_ft  = safe_float(row.get(f'{prefix_off}.Off-FT', 0))
    def_ft  = safe_float(row.get(f'{prefix_def_dist}.Def-FT', 0))
    ftm     = (off_ft * FT_DIST_TEAM_WT + def_ft * FT_DIST_OPP_WT) / 100 * score

    ft_pct  = safe_float(row.get(f'{prefix_pct}.FT%', 70))
    fta     = safe_div(ftm, ft_pct / 100)

    return {
        '2P FGM': fgm_2p,  '3P FGM': fgm_3p,
        '2P FGA': fga_2p,  '3P FGA': fga_3p,
        'FTM':    ftm,      'FTA':    fta,
    }


def compute_team_shooting(row, team_score, opp_score):
    """
    Compute team **and** opponent shooting volume projections.

    These values are constant for every player on the same team.

    Returns a dict with keys prefixed ``''`` (team) and ``'OPP '`` (opponent).
    """
    team = _project_side_shooting(
        row, team_score,
        prefix_off='Team', prefix_def_dist='Opponent',
        prefix_pct='Team', prefix_def_pct='Opponent.Def',
    )
    opp = _project_side_shooting(
        row, opp_score,
        prefix_off='Opponent', prefix_def_dist='Team',
        prefix_pct='Opponent', prefix_def_pct='Team.Def',
    )
    result = dict(team)
    for k, v in opp.items():
        result[f'OPP {k}'] = v
    return result


# ---------------------------------------------------------------------------
# Rebound-opportunity helper
# ---------------------------------------------------------------------------

def _rebound_opportunities(shooting, prefix=''):
    """
    Total missed shots available to rebound.

    ``prefix`` is ``''`` for the team's own misses (offensive rebound opportunity)
    or ``'OPP '`` for the opponent's misses (defensive rebound opportunity).
    """
    p = prefix
    return ((shooting[f'{p}2P FGA'] - shooting[f'{p}2P FGM']) +
            (shooting[f'{p}3P FGA'] - shooting[f'{p}3P FGM']) +
            (shooting[f'{p}FTA']    - shooting[f'{p}FTM']) * FT_MISS_REB_FACTOR)


# ---------------------------------------------------------------------------
# Player-level projection (first pass, pre-normalization)
# ---------------------------------------------------------------------------

def compute_player_initial(row, shooting, mins_proj, league_avg_off_to=16.9):
    """
    Compute per-player projections that do **not** require team-level sums.

    Returns a dict of intermediate values needed for the normalization pass.
    """
    poss_rate = safe_float(row.get('%Poss', 0)) / 100.0
    ortg      = safe_float(row.get('ORtg', 0))  / 100.0
    mins_frac = mins_proj / MINUTES_PER_GAME

    opp_tempo  = safe_float(row.get('Opponent.AdjTempo', 65))
    team_tempo = safe_float(row.get('Team.AdjTempo', 65))
    pace = (opp_tempo + team_tempo) / 2.0

    simple_pts = poss_rate * ortg * pace * mins_frac

    team_fgm = shooting['2P FGM'] + shooting['3P FGM']

    # Assists unadjusted (HC)
    # ARate = 100 * AST / ((MP/(TmMP/5)) * TmFG - FG)  (Pomeroy assist rate)
    # The denominator is *teammate* FGM, not total team FGM.
    # We must subtract the player's per-game FGM to avoid ~16% inflation.
    a_rate    = safe_float(row.get('ARate', 0)) / 100.0
    made_2_s, _ = parse_made_attempted(row.get('2PM-A'))
    made_3_s    = parse_three_pm_made(row.get('3PM-A'))
    games_played_ast = safe_float(row.get('G', 1))
    player_fgm_pg = (made_2_s + made_3_s) / games_played_ast if games_played_ast > 0 else 0
    teammate_fgm  = max(team_fgm - player_fgm_pg, 0)
    ast_unadj = teammate_fgm * a_rate * mins_frac

    # Rebounds — player-level weight for distribution
    def_reb_opp = _rebound_opportunities(shooting, prefix='OPP ')
    off_reb_opp = _rebound_opportunities(shooting, prefix='')

    dr_pct  = safe_float(row.get('DR%', 0)) / 100.0
    or_pct  = safe_float(row.get('OR%', 0)) / 100.0
    reb_weight = (def_reb_opp * dr_pct + off_reb_opp * or_pct) * mins_frac

    # Turnovers (HS)
    to_rate     = safe_float(row.get('TORate', 0))
    opp_def_to  = safe_float(row.get('Opponent.Def-TO%', 0))
    weighted_to = (to_rate * TO_RATE_PLAYER_WT + opp_def_to * TO_RATE_OPP_DEF_WT) / 100.0
    proj_to = pace * poss_rate * weighted_to * mins_frac

    # Steals (HV) — Stl% is already per-possession, so base formula is:
    #   stl_pct * mins_frac * pace
    # Multiplicative opponent adjustment: if opponent turns it over more than
    # league average, boost steal projection proportionally.
    stl_pct    = safe_float(row.get('Stl%', 0)) / 100.0
    opp_off_to = safe_float(row.get('Opponent.Off-TO%', league_avg_off_to))
    opp_stl_factor = safe_div(opp_off_to, league_avg_off_to, 1.0)
    proj_stl = stl_pct * mins_frac * pace * (
        (1.0 - STL_OPP_ADJUSTMENT_WT) + STL_OPP_ADJUSTMENT_WT * opp_stl_factor)

    # Blocks (HY)
    # KenPom Blk% = Blocks / (%Min * Opponents' 2P attempts)
    # The denominator is 2P FGA only — nearly all blocks are on 2-point shots.
    # Player weight for distribution; team target computed in step 4.
    blk_pct     = safe_float(row.get('Blk%', 0)) / 100.0
    blk_weight  = mins_frac * blk_pct

    # 3PM rate (HO, HP)
    pct_min       = safe_float(row.get('%Min', 0)) / 100.0
    games_played  = safe_float(row.get('G', 1))
    three_pm_made = parse_three_pm_made(row.get('3PM-A'))
    total_minutes = pct_min * MINUTES_PER_GAME * games_played
    unadj_3pm     = safe_div(three_pm_made, total_minutes) * mins_proj

    return {
        'pace':       pace,
        'simple_pts': simple_pts,
        'ast_unadj':  ast_unadj,
        'reb_weight': reb_weight,
        'proj_to':    proj_to,
        'proj_stl':   proj_stl,
        'blk_weight': blk_weight,
        'unadj_3pm':  unadj_3pm,
        'mins_proj':  mins_proj,
    }


# ---------------------------------------------------------------------------
# Normalization: scale player stats to match team totals
# ---------------------------------------------------------------------------

def _normalize(player_value, team_sum, team_target):
    """Scale *player_value* so the team sums to *team_target*."""
    return safe_div(player_value * team_target, team_sum)


# ---------------------------------------------------------------------------
# Main projection engine
# ---------------------------------------------------------------------------

def run_projections(df, mins_overrides=None):
    """
    Compute all projections and return a **new** DataFrame with added columns.

    The input *df* is not mutated.
    """
    df = df.copy()

    # ---- Step 1: Team-level KenPom scores and shooting volumes ----
    team_score_map    = {}   # team -> (team_score, opp_score, kp_total)
    team_shooting_map = {}   # team -> shooting dict

    seen_teams = set()
    for idx, row in df.iterrows():
        team = str(row.get('Team', ''))
        if not team or team in seen_teams:
            continue
        seen_teams.add(team)

        team_score, opp_score, kp_total = parse_kenpom_result(row.get('KenPomResult', ''))
        if team_score is None:
            continue
        team_score_map[team] = (team_score, opp_score, kp_total)
        try:
            team_shooting_map[team] = compute_team_shooting(row, team_score, opp_score)
        except Exception as e:
            print(f"Warning: could not compute shooting for team '{team}': {e}")

    # ---- Step 1b: League-average defensive shooting % for matchup adjustment ----
    teams_df = df.drop_duplicates('Team')
    league_avg_def_2p = teams_df['Opponent.Def.2P%'].apply(
        lambda x: safe_float(x, float('nan'))).dropna().mean()
    league_avg_def_3p = teams_df['Opponent.Def.3P%'].apply(
        lambda x: safe_float(x, float('nan'))).dropna().mean()
    league_avg_def_ftrate = teams_df['Opponent.Def-FTRate'].apply(
        lambda x: safe_float(x, float('nan'))).dropna().mean()
    league_avg_off_to = teams_df['Opponent.Off-TO%'].apply(
        lambda x: safe_float(x, float('nan'))).dropna().mean()
    # Fallbacks if columns missing
    if math.isnan(league_avg_def_2p):   league_avg_def_2p = 50.0
    if math.isnan(league_avg_def_3p):   league_avg_def_3p = 34.0
    if math.isnan(league_avg_def_ftrate): league_avg_def_ftrate = 35.0
    if math.isnan(league_avg_off_to):   league_avg_off_to = 16.9

    # ---- Step 2: Per-player initial (pre-normalization) values ----
    initial = {}  # idx -> dict
    for idx, row in df.iterrows():
        team = str(row.get('Team', ''))
        if team not in team_score_map or team not in team_shooting_map:
            continue
        mins = get_mins_proj(row, mins_overrides)
        try:
            init = compute_player_initial(row, team_shooting_map[team], mins,
                                            league_avg_off_to=league_avg_off_to)

            # Matchup adjustment: adjust simple_pts based on player scoring
            # profile vs opponent defensive profile
            share_2p, share_3p, share_ft = compute_scoring_profile(row)
            opp_def_2p = safe_float(row.get('Opponent.Def.2P%', league_avg_def_2p))
            opp_def_3p = safe_float(row.get('Opponent.Def.3P%', league_avg_def_3p))
            opp_def_ftrate = safe_float(row.get('Opponent.Def-FTRate', league_avg_def_ftrate))

            matchup_factor = (
                share_2p * safe_div(opp_def_2p, league_avg_def_2p, 1.0) +
                share_3p * safe_div(opp_def_3p, league_avg_def_3p, 1.0) +
                share_ft * safe_div(opp_def_ftrate, league_avg_def_ftrate, 1.0)
            )
            init['simple_pts'] *= (PTS_MATCHUP_PLAYER_WT +
                                   PTS_MATCHUP_OPP_WT * matchup_factor)
            # 3PM weight: use scoring profile × points instead of raw 3PM rate.
            # This ties 3PM distribution to the player's scoring volume and
            # 3-point profile, preventing 3PM*3 > PTS violations.
            init['threept_weight'] = share_3p * init['simple_pts']
            initial[idx] = init
        except Exception as e:
            print(f"Warning: error on player '{row.get('Name', idx)}': {e}")

    # ---- Step 3: Accumulate team-level sums for normalization ----
    team_sums = {}
    for idx, vals in initial.items():
        team = str(df.at[idx, 'Team'])
        sums = team_sums.setdefault(team, {
            'simple_pts': 0.0, 'ast_unadj': 0.0, 'threept_weight': 0.0,
            'stl_weight': 0.0, 'reb_weight': 0.0, 'blk_weight': 0.0,
            'to_weight': 0.0, 'team_fgm': 0.0,
        })
        if sums['team_fgm'] == 0.0 and team in team_shooting_map:
            s = team_shooting_map[team]
            sums['team_fgm'] = s['2P FGM'] + s['3P FGM']
        sums['simple_pts']    += vals['simple_pts']
        sums['ast_unadj']     += vals['ast_unadj']
        sums['threept_weight'] += vals['threept_weight']
        sums['stl_weight']    += vals['proj_stl']
        sums['reb_weight']    += vals['reb_weight']
        sums['blk_weight']    += vals['blk_weight']
        sums['to_weight']     += vals['proj_to']

    # ---- Step 4: Final projections (with normalization) ----
    rows_out = []
    for idx, row in df.iterrows():
        team = str(row.get('Team', ''))
        if team not in team_score_map or idx not in initial:
            rows_out.append({'_idx': idx})
            continue

        team_score, opp_score, kp_total = team_score_map[team]
        shooting = team_shooting_map[team]
        vals = initial[idx]
        sums = team_sums[team]
        team_fgm = shooting['2P FGM'] + shooting['3P FGM']

        # Normalized projections
        proj_pts = _normalize(vals['simple_pts'], sums['simple_pts'], team_score)

        # AST team-level anchor: Team.A% (assists/FGM) blended with
        # Opponent.Def.A% using Pomeroy 71/29 offense/defense control weights.
        team_a_pct = safe_float(row.get('Team.A%', 55.0))
        opp_def_a_pct = safe_float(row.get('Opponent.Def.A%', 55.0))
        blended_assist_rate = (AST_OFFENSE_CONTROL_WT * team_a_pct +
                               AST_DEFENSE_CONTROL_WT * opp_def_a_pct) / 100.0
        team_ast = team_fgm * blended_assist_rate
        proj_ast = _normalize(vals['ast_unadj'], sums['ast_unadj'], team_ast)

        proj_3pm = _normalize(vals['threept_weight'], sums['threept_weight'], shooting['3P FGM'])
        # Safety cap: 3PM*3 cannot exceed total projected points
        proj_3pm = min(proj_3pm, safe_div(proj_pts, 3.0))

        # Top-down STL: team target from Def-TO% * PACE * steal_rate,
        # distributed proportionally by each player's bottom-up stl weight.
        team_def_to = safe_float(row.get('Team.Def-TO%', 0))
        opp_off_to_team = safe_float(row.get('Opponent.Off-TO%', 0))
        # Blend team's defensive TO forcing with opponent's offensive TO tendency
        blended_to_rate = (team_def_to * 0.75 + opp_off_to_team * 0.25) / 100.0
        team_stl_target = blended_to_rate * vals['pace'] * STEAL_TO_TURNOVER_RATE
        proj_stl = _normalize(vals['proj_stl'], sums['stl_weight'], team_stl_target)

        # Top-down BLK: team target from Team.Def.Blk% * OPP 2P FGA,
        # blended with how blockable the opponent is.
        # Opponent.Def.Blk% = how often this opponent gets blocked (blockability).
        team_def_blk_pct   = safe_float(row.get('Team.Def.Blk%', 9.5))
        opp_blockability   = safe_float(row.get('Opponent.Def.Blk%', 9.5))
        blended_blk_rate = (team_def_blk_pct * 0.85 + opp_blockability * 0.15) / 100.0
        team_blk_target  = blended_blk_rate * shooting['OPP 2P FGA']
        proj_blk = _normalize(vals['blk_weight'], sums['blk_weight'], team_blk_target)

        # Top-down TO: team target from blended offensive TO rate × PACE,
        # then distribute proportionally by each player's bottom-up TO weight.
        team_off_to_rate = safe_float(row.get('Team.Off-TO%', 17.0))
        opp_def_to_rate  = safe_float(row.get('Opponent.Def-TO%', 17.0))
        blended_team_to  = (team_off_to_rate * TO_TOPDOWN_TEAM_WT +
                            opp_def_to_rate * TO_TOPDOWN_OPP_WT) / 100.0
        team_to_target = blended_team_to * vals['pace']
        proj_to = _normalize(vals['proj_to'], sums['to_weight'], team_to_target)

        # Top-down REB: team target from team-level rebounding rates.
        # Defensive rebounds: DefRebOpp * (1 - blended opponent OR rate)
        # Offensive rebounds: OffRebOpp * blended team OR rate
        def_reb_opp = _rebound_opportunities(shooting, prefix='OPP ')
        off_reb_opp = _rebound_opportunities(shooting, prefix='')

        team_def_or_pct = safe_float(row.get('Team.Def-OR%', 30.5))   # opp OR% against us
        opp_off_or_pct  = safe_float(row.get('Opponent.Off-OR%', 30.5))  # this opp's OR%
        team_off_or_pct = safe_float(row.get('Team.Off-OR%', 30.5))   # our OR%
        opp_def_or_pct  = safe_float(row.get('Opponent.Def-OR%', 30.5))  # opp DR failure

        blended_opp_or = (team_def_or_pct * 0.75 + opp_off_or_pct * 0.25) / 100.0
        blended_own_or = (team_off_or_pct * 0.75 + opp_def_or_pct * 0.25) / 100.0

        team_reb_target = def_reb_opp * (1.0 - blended_opp_or) + off_reb_opp * blended_own_or
        proj_reb = _normalize(vals['reb_weight'], sums['reb_weight'], team_reb_target)

        rows_out.append({
            '_idx': idx,
            # Team-level shooting (repeated per player for downstream use)
            'VEGAS PTS':  team_score,   'VEGAS OPP': opp_score,   'KP TOTAL': kp_total,
            **{k: v for k, v in shooting.items()},
            # Player intermediates
            'MINS PROJ':  vals['mins_proj'],
            'PACE':       vals['pace'],
            'SIMPLE PTS': vals['simple_pts'],
            'AST UNADJ':  vals['ast_unadj'],
            'UNADJ 3PM':  vals['threept_weight'],
            'DefRebOpp':  def_reb_opp,
            'OffRebOpp':  off_reb_opp,
            # Final projections
            'PROJ PTS':   proj_pts,
            'TEAM AST':   team_ast,
            'PROJ AST':   proj_ast,
            'PROJ REB':   proj_reb,
            'PROJ 3PM':   proj_3pm,
            'PROJ TO':    proj_to,
            'PROJ STL':   proj_stl,
            'PROJ BLK':   proj_blk,
        })

    # Merge projection columns into df
    proj_df = pd.DataFrame(rows_out).set_index('_idx')
    df = df.drop(columns=[c for c in proj_df.columns if c in df.columns], errors='ignore')
    df = pd.concat([df, proj_df], axis=1).copy()

    # ---- Step 5: Team totals (SUMIF equivalents) ----
    team_col = df['Team'].astype(str)
    for stat, total_col in [('PROJ REB', 'TEAM REB'), ('PROJ TO', 'TEAM TO'),
                             ('PROJ STL', 'TEAM STL'), ('PROJ BLK', 'TEAM BLK'),
                             ('PROJ 3PM', 'TEAM 3PM')]:
        if stat in df.columns:
            df[total_col] = df.groupby(team_col)[stat].transform('sum')

    return df


# ---------------------------------------------------------------------------
# Evaluation against EXISTING_MODEL
# ---------------------------------------------------------------------------

PROJ_STAT_COLS = ['PROJ PTS', 'PROJ AST', 'PROJ REB', 'PROJ 3PM',
                  'PROJ TO', 'PROJ STL', 'PROJ BLK']


def evaluate_against_existing(df_result, existing_path=EXISTING_MODEL_PATH):
    """Compare projection results to the cached values in EXISTING_MODEL.xlsx."""
    df_existing = read_player_stats(existing_path)

    existing_map = {}
    for _, row in df_existing.iterrows():
        key = (str(row.get('Name', '')), str(row.get('Team', '')))
        existing_map[key] = {col: safe_float(row.get(col), float('nan')) for col in PROJ_STAT_COLS}

    print("\n=== Evaluation vs EXISTING_MODEL.xlsx ===")
    stat_diffs = {col: [] for col in PROJ_STAT_COLS}
    matched = 0

    for _, row in df_result.iterrows():
        key = (str(row.get('Name', '')), str(row.get('Team', '')))
        if key not in existing_map:
            continue
        matched += 1
        ref = existing_map[key]
        for col in PROJ_STAT_COLS:
            pred  = safe_float(row.get(col), float('nan'))
            truth = ref.get(col, float('nan'))
            if math.isnan(pred) or math.isnan(truth):
                continue
            stat_diffs[col].append((abs(pred - truth), key, pred, truth))

    print(f"Matched {matched} players\n")
    for col, diffs in stat_diffs.items():
        if not diffs:
            print(f"  {col}: no data")
            continue
        abs_diffs = [d[0] for d in diffs]
        worst = max(diffs, key=lambda x: x[0])
        print(f"  {col:12s}: mean_abs_diff={np.mean(abs_diffs):.4f}  "
              f"max_diff={np.max(abs_diffs):.4f}"
              f"  [worst: {worst[1][0][:20]} pred={worst[2]:.3f} ref={worst[3]:.3f}]")

    return stat_diffs


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

OUTPUT_COLS = [
    'Name', 'Team',
    'MINS PROJ', 'PACE', 'VEGAS PTS', 'VEGAS OPP', 'KP TOTAL',
    '2P FGM', '3P FGM', '2P FGA', '3P FGA', 'FTM', 'FTA',
    'OPP 2P FGM', 'OPP 3P FGM', 'OPP 2P FGA', 'OPP 3P FGA', 'OPP FTM', 'OPP FTA',
    'SIMPLE PTS', 'PROJ PTS', 'TEAM AST', 'AST UNADJ', 'PROJ AST',
    'DefRebOpp', 'OffRebOpp', 'PROJ REB', 'TEAM REB',
    'UNADJ 3PM', 'PROJ 3PM', 'TEAM 3PM',
    'PROJ TO', 'TEAM TO', 'PROJ STL', 'TEAM STL', 'PROJ BLK', 'TEAM BLK',
]


def write_output(df, output_path):
    """Write projections to Excel."""
    out_cols = [c for c in OUTPUT_COLS if c in df.columns]
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df[out_cols].to_excel(output_path, index=False, sheet_name='Projections')
    print(f"Output written to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Player projection model')
    parser.add_argument('--input', required=True,
                        help='Input Excel file (MAIN_SCRIPT_OUTPUT.xlsx or EXISTING_MODEL.xlsx)')
    parser.add_argument('--output', default=None,
                        help='Output Excel file path (default: projections_<input_stem>.xlsx)')
    parser.add_argument('--eval', action='store_true',
                        help='Evaluate projections against EXISTING_MODEL.xlsx')
    parser.add_argument('--existing-model', default=str(EXISTING_MODEL_PATH),
                        help='Path to EXISTING_MODEL.xlsx for evaluation')
    parser.add_argument('--sheet', default='PlayerStats',
                        help='Sheet name to read from input file')
    args = parser.parse_args()

    print(f"Reading player stats from: {args.input}")
    df = read_player_stats(args.input, sheet=args.sheet)
    print(f"Loaded {len(df)} players from {df['Team'].nunique()} teams")

    df_result = run_projections(df)

    if args.output is None:
        stem = os.path.splitext(os.path.basename(args.input))[0]
        args.output = PROJECTIONS_OUTPUT_DIR / f'projections_{stem}.xlsx'
    write_output(df_result, args.output)

    if args.eval:
        evaluate_against_existing(df_result, existing_path=args.existing_model)


if __name__ == '__main__':
    main()

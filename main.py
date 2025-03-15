import sys
from kenpompy.utils import login
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

# usage `python3 03-19-2024`
def main_fn(date_string):
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

    teams = kp_team.get_valid_teams(browser)
    print('teams')
    print(teams)
    dfs = []
    for team in teams[:50]:
        df = kp_team.get_player_expanded(browser, date_string, team_with_spaces=team, team_stats=team_stats, team_stats_def=team_stats_def, four_factors=four_factors, points_dist=points_dist)
        dfs.append(df)
        time.sleep(randint(2, 5))
    
    player_df = pd.concat(dfs)
    player_df = player_df[player_df.NextOpponent.notnull()]

    # rename PCT
    player_df = player_df.rename(columns={"Pct.1": "Player.2Pt%", "Pct.2": "Player.3Pt%"})

    print('player_df')
    print(player_df)
    print(player_df.columns.tolist())

    four_factors.to_excel(f"{formatted_date}.xlsx",
                sheet_name='TeamFourFactors')

    with pd.ExcelWriter(f'{formatted_date}.xlsx', mode='a') as writer:  
        team_stats.to_excel(writer, sheet_name='TeamStats')

    with pd.ExcelWriter(f'{formatted_date}.xlsx', mode='a') as writer:  
        points_dist.to_excel(writer, sheet_name='PointsDist')

    with pd.ExcelWriter(f'{formatted_date}.xlsx', mode='a') as writer:  
        player_df.to_excel(writer, sheet_name='PlayerStats')

    # Move the file
    source = f'{formatted_date}.xlsx'
    destination = os.path.join('excel_outputs', f'{formatted_date} {datetime.now().strftime("%I:%M:%S %p")}.xlsx')
    shutil.move(source, destination)


    return [four_factors, team_stats, team_stats_def, points_dist, player_df]


def calculate_player_props(four_factors, team_stats, team_stats_def, points_dist, player_df):
    """
    Calculate player props using the logic from PlayerStats.csv with team total calibration
    """
    def calculate_minutes_projection(player_row):
        # URGENT TODO: Add ETR PROJECTIONS / MANUAL OVERRIDE

        """Calculate projected minutes based on recent games"""
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
        opp_stats = adjusted_team_projections[opp_name]
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
    # Replace + signs with spaces
    normalized_name = team_name.replace('+', ' ')
    return normalized_name

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 main.py 02-27-2024")
    else:
        date_string = sys.argv[1]
        [four_factors, team_stats, team_stats_def, points_dist, player_df] = main_fn(date_string)
        projections = calculate_player_props(four_factors, team_stats, team_stats_def, points_dist, player_df)

        print(len(projections))

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
    print('team_stats_def')
    print(team_stats_def)

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
    for team in teams[90:105]:
        df = kp_team.get_player_expanded(browser, formatted_date, team_with_spaces=team, team_stats=team_stats, team_stats_def=team_stats_def, four_factors=four_factors, points_dist=points_dist)
        dfs.append(df)
        time.sleep(randint(2, 5))
    
    player_df = pd.concat(dfs)
    player_df = player_df[player_df.NextOpponent.notnull()]

    # rename PCT
    player_df = player_df.rename(columns={"Pct.1": "Player.2Pt%", "Pct.2": "Player.3Pt%"})

    print('player_df')
    print(player_df)
    print(player_df.columns.tolist())

    # four_factors.to_excel(f"{formatted_date}.xlsx",
    #             sheet_name='TeamFourFactors')

    # with pd.ExcelWriter(f'{formatted_date}.xlsx', mode='a') as writer:  
    #     team_stats.to_excel(writer, sheet_name='TeamStats')

    # with pd.ExcelWriter(f'{formatted_date}.xlsx', mode='a') as writer:  
    #     points_dist.to_excel(writer, sheet_name='PointsDist')

    # with pd.ExcelWriter(f'{formatted_date}.xlsx', mode='a') as writer:  
    #     player_df.to_excel(writer, sheet_name='PlayerStats')


    return [four_factors, team_stats, points_dist, player_df]


def calculate_player_props(player_df, four_factors, team_stats, points_dist):
    """
    Calculate player props using the logic from PlayerStats.csv with team total calibration
    """
    def calculate_minutes_projection(player_row):
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

    def calculate_pace_factor(team_row, opp_row):
        """Calculate pace factor based on team and opponent tempo"""
        team_tempo = float(team_row['AdjTempo'])
        opp_tempo = float(opp_row['AdjTempo'])
        return (team_tempo * 0.6 + opp_tempo * 0.4) / 100.0

    # Track team-level projections for scaling
    team_projections = {}
    
    # First pass - calculate initial projections
    initial_projections = {}
    
    for player_name, player in player_df.iterrows():
        if pd.isna(player.get('NextOpponent')):
            continue
            
        team_name = player['Team']
        opp_name = player['NextOpponent']
        
        # Get team and opponent data
        team_factors = four_factors.loc[team_name]
        opp_factors = four_factors.loc[opp_name]
        team_shooting = team_stats.loc[team_name]
        team_points = points_dist.loc[team_name]
        
        # Initialize team tracking if needed
        if team_name not in team_projections:
            team_projections[team_name] = {
                'KP_TOTAL': float(player['Team.KenPomResult'].split('-')[0]) if not pd.isna(player.get('Team.KenPomResult')) else None,
                'initial_points': 0,
                'initial_assists': 0,
                'expected_team_assists': float(team_shooting['A%']) * team_projections[team_name]['KP_TOTAL'] / 100,  # Team assist rate * projected points
                'players': []
            }
        
        # Base calculations
        proj_minutes = calculate_minutes_projection(player)
        if proj_minutes < 5:
            continue
            
        pace_factor = calculate_pace_factor(team_factors, opp_factors)
        team_poss_per_min = float(team_factors['AdjTempo']) / 200
        proj_poss = proj_minutes * team_poss_per_min * pace_factor
        
        # Points calculation
        usage_rate = min(float(player.get('Usage', 20)), 35) / 100
        shot_poss = proj_poss * usage_rate
        fga = shot_poss * 0.85
        
        three_pt_rate = float(team_shooting['3PA%']) / 100
        three_pa = fga * three_pt_rate
        two_pa = fga * (1 - three_pt_rate)
        
        three_pt_pct = float(player.get('Player.3Pt%', team_shooting['3P%'])) / 100
        two_pt_pct = float(player.get('Player.2Pt%', team_shooting['2P%'])) / 100
        
        three_pm = three_pa * three_pt_pct
        two_pm = two_pa * two_pt_pct
        
        points_from_2 = two_pm * 2
        points_from_3 = three_pm * 3
        
        ft_rate = float(team_shooting['FT%']) / 100
        ft_attempts = fga * float(team_points['Off-FT']) / 100 * 0.5
        ft_points = ft_attempts * ft_rate
        
        total_points = points_from_2 + points_from_3 + ft_points
        
        # Assists calculation - use player's assist rate directly
        team_assist_rate = float(team_shooting['A%']) / 100
        player_assist_rate = float(player.get('ARate', team_assist_rate * 100)) / 100
        initial_assists = proj_poss * player_assist_rate  # Use player rate directly
        
        # Rebounds calculation
        player_oreb_rate = float(player.get('ORPct', 5)) / 100
        player_dreb_rate = float(player.get('DRPct', 10)) / 100
        opp_oreb_rate = float(opp_factors['Off-OR%']) / 100
        opp_dreb_rate = float(opp_factors['Def-OR%']) / 100
        
        # Offensive rebounds - player rate vs opponent defensive rebounding
        oreb = proj_poss * player_oreb_rate * (1 - opp_dreb_rate)
        # Defensive rebounds - player rate vs opponent offensive rebounding
        dreb = proj_poss * player_dreb_rate * (1 - opp_oreb_rate)
        total_rebounds = oreb + dreb
        
        # Calculate defensive possessions for the player
        def_poss = proj_minutes * team_poss_per_min * pace_factor  # Same base calculation as offensive possessions

        # Blocks calculation
        player_block_rate = float(player.get('Blk%', 1.0)) / 100  # Default to 1% if not available
        projected_blocks = def_poss * player_block_rate

        # Steals calculation
        player_steal_rate = float(player.get('Stl%', 1.0)) / 100  # Default to 1% if not available
        projected_steals = def_poss * player_steal_rate

        # Turnovers calculation - weighted between player rate and opponent defense
        player_to_rate = float(player.get('TORate', 15.0)) / 100  # Default to 15% if not available
        opp_def_to_rate = float(opp_factors.get('Def-TO%', 18.0)) / 100  # Default to 18% if not available
        
        # Weight 75% player rate, 25% opponent rate
        weighted_to_rate = (player_to_rate * 0.75) + (opp_def_to_rate * 0.25)
        projected_turnovers = proj_poss * weighted_to_rate

        # Store initial projections
        initial_projections[player_name] = {
            'minutes': proj_minutes,
            'points': total_points,
            'assists': initial_assists,
            'rebounds': total_rebounds,
            'three_pm': three_pm,
            'fga': fga,
            'fgm': two_pm + three_pm,
            'three_pa': three_pa,
            'ft_attempts': ft_attempts,
            'ft_made': ft_attempts * ft_rate,
            'team': team_name,
            'blocks': projected_blocks,
            'steals': projected_steals,
            'turnovers': projected_turnovers
        }
        
        # Update team totals
        team_projections[team_name]['initial_points'] += total_points
        team_projections[team_name]['initial_assists'] += initial_assists
        team_projections[team_name]['players'].append(player_name)

    # Second pass - scale points and assists separately
    final_projections = {}
    
    for team_name, team_data in team_projections.items():
        if team_data['KP_TOTAL'] is None or team_data['initial_points'] == 0:
            continue
        
        # Points scaling
        points_scaling = team_data['KP_TOTAL'] / team_data['initial_points']
        
        # Assists scaling - scale to match expected team assists
        assists_scaling = team_data['expected_team_assists'] / team_data['initial_assists'] if team_data['initial_assists'] > 0 else 1.0
        
        for player_name in team_data['players']:
            initial = initial_projections[player_name]
            
            final_projections[player_name] = {
                # Main projections
                'projected_points': round(initial['points'] * points_scaling, 2),
                'projected_assists': round(initial['assists'] * assists_scaling, 2),
                'projected_rebounds': round(initial['rebounds'], 2),
                'projected_3pm': round(initial['three_pm'], 2),
                'projected_fga': round(initial['fga'], 2),
                'projected_fgm': round(initial['fgm'], 2),
                'projected_3pa': round(initial['three_pa'], 2),
                'projected_fta': round(initial['ft_attempts'], 2),
                'projected_ftm': round(initial['ft_made'], 2),
                'projected_blocks': round(initial['blocks'], 2),
                'projected_steals': round(initial['steals'], 2),
                'projected_turnovers': round(initial['turnovers'], 2),
                'minutes': round(initial['minutes'], 2),
                
                # Scaling factors
                'points_scaling_factor': round(points_scaling, 3),
                'assists_scaling_factor': round(assists_scaling, 3),
                'raw_points': round(initial['points'], 2),
                'team_total': team_data['KP_TOTAL'],
                
                # Player rates and inputs
                'usage_rate': round(usage_rate * 100, 2),  # Convert back to percentage
                'player_assist_rate': round(player_assist_rate * 100, 2),
                'player_block_rate': round(player_block_rate * 100, 2),
                'player_steal_rate': round(player_steal_rate * 100, 2),
                'player_turnover_rate': round(player_to_rate * 100, 2),
                'weighted_turnover_rate': round(weighted_to_rate * 100, 2),
                'player_oreb_rate': round(player_oreb_rate * 100, 2),
                'player_dreb_rate': round(player_dreb_rate * 100, 2),
                'weighted_oreb_rate': round(player_oreb_rate * (1 - opp_dreb_rate) * 100, 2),
                'weighted_dreb_rate': round(player_dreb_rate * (1 - opp_oreb_rate) * 100, 2),
                
                # Shooting rates
                'three_pt_rate': round(three_pt_rate * 100, 2),
                'three_pt_percentage': round(three_pt_pct * 100, 2),
                'two_pt_percentage': round(two_pt_pct * 100, 2),
                'ft_percentage': round(ft_rate * 100, 2),
                
                # Possession and pace metrics
                'pace_factor': round(pace_factor, 3),
                'projected_possessions': round(proj_poss, 2),
                'projected_defensive_possessions': round(def_poss, 2),
                'team_poss_per_minute': round(team_poss_per_min, 3),
                
                # Team and opponent metrics used
                'team_assist_rate': round(float(team_shooting['A%']), 2),
                'opponent_defensive_turnover_rate': round(opp_def_to_rate * 100, 2),
                'opponent_offensive_rebound_rate': round(opp_oreb_rate * 100, 2),
                'opponent_defensive_rebound_rate': round(opp_dreb_rate * 100, 2),
                
                # Raw shot attempts before scaling
                'raw_fga': round(fga, 2),
                'raw_three_pa': round(three_pa, 2),
                'raw_two_pa': round(two_pa, 2),
                'raw_ft_attempts': round(ft_attempts, 2),
                
                # Team context
                'team': team_name,
                'opponent': opp_name
            }
    
    return final_projections

def format_output_as_json(projections):
    """Convert projections to the desired JSON format"""
    return {
        player: {
            'projected_points': stats['projected_points'],
            'projected_assists': stats['projected_assists'],
            'projected_rebounds': stats['projected_rebounds'],
            'projected_3pm': stats['projected_3pm'],
            'projected_fga': stats['projected_fga'],
            'projected_fgm': stats['projected_fgm'],
            'projected_3pa': stats['projected_3pa'],
            'projected_fta': stats['projected_fta'],
            'projected_ftm': stats['projected_ftm'],
            'minutes': stats['minutes'],
            'raw_points': stats['raw_points'],
            'team_total': stats['team_total'],
            'points_scaling_factor': stats['points_scaling_factor'],
            'assists_scaling_factor': stats['assists_scaling_factor'],
            'projected_blocks': stats['projected_blocks'],
            'projected_steals': stats['projected_steals'],
            'projected_turnovers': stats['projected_turnovers']
        }
        for player, stats in projections.items()
    }

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 main.py 02-27-2024")
    else:
        date_string = sys.argv[1]
        [four_factors, team_stats, points_dist, player_df] = main_fn(date_string)
        projections = calculate_player_props(four_factors, team_stats, points_dist, player_df)

        print(len(projections))

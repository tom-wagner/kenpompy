import sys
from kenpompy.utils import login
import kenpompy.summary as kp_summary
import kenpompy.team as kp_team
import pandas as pd
from functools import reduce
from datetime import datetime
import time
from random import randint

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
    for team in teams[:5]:
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

    four_factors.to_excel(f"{formatted_date}.xlsx",
                sheet_name='TeamFourFactors')

    with pd.ExcelWriter(f'{formatted_date}.xlsx', mode='a') as writer:  
        team_stats.to_excel(writer, sheet_name='TeamStats')

    with pd.ExcelWriter(f'{formatted_date}.xlsx', mode='a') as writer:  
        points_dist.to_excel(writer, sheet_name='PointsDist')

    with pd.ExcelWriter(f'{formatted_date}.xlsx', mode='a') as writer:  
        player_df.to_excel(writer, sheet_name='PlayerStats')


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 main.py 02-27-2024")
    else:
        date_string = sys.argv[1]
        main_fn(date_string)
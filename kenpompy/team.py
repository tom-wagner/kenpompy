"""
This module contains functions for scraping the team page kenpom.com tables into
pandas dataframes
"""

import pandas as pd
from io import StringIO
from .misc import get_current_season
import re
from cloudscraper import CloudScraper
from bs4 import BeautifulSoup
from codecs import encode, decode
from typing import Optional
from .utils import get_html
import datetime
import traceback

def get_valid_teams(browser: CloudScraper, season: Optional[str]=None):
	"""
	Scrapes the teams (https://kenpom.com) into a list.

	Args:
		browser (CloudScraper): Authenticated browser with full access to kenpom.com generated
			by the `login` function
		season (str, optional): Used to define different seasons. 1999 is the earliest available season.

	Returns:
		team_list (list): List containing all valid teams for the given season on kenpom.com.
	"""

	url = "https://kenpom.com"
	url = url + '?y=' + str(season)

	teams = BeautifulSoup(get_html(browser, url), "html.parser")
	table = teams.find_all('table')[0]
	team_df = pd.read_html(StringIO(str(table)))
	# Get only the team column.
	team_df = team_df[0].iloc[:, 1]
 	# Remove NCAA tourny seeds for previous seasons.
	team_df = team_df.str.replace(r'\d+\**', '', regex=True)
	team_df = team_df.str.rstrip()
	team_df = team_df.dropna()
	# Remove leftover team headers
	team_list = team_df.values.tolist()
	team_list = [team for team in team_df if team != "Team"]

	return team_list

def get_schedule(browser: CloudScraper, team: Optional[str]=None, season: Optional[str]=None):
	"""
	Scrapes a team's schedule from (https://kenpom.com/team.php) into a dataframe.

	Args:
		browser (CloudScraper): Authenticated browser with full access to kenpom.com generated
			by the `login` function
		team (str, optional): Used to determine which team to scrape for schedule.
		season (str, optional): Used to define different seasons. 1999 is the earliest available season.

	Returns:
		team_df (pandas dataframe): Dataframe containing a team's schedule for the given season.

	Raises:
		ValueError if `season` is less than 1999.
		ValueError if `season` is greater than the current year.
		ValueError if `team` is not in the valid team list.
	"""

	url = 'https://kenpom.com/team.php'
	current_season = get_current_season(browser)

	if season:
		if int(season) < 1999:
			raise ValueError(
				'season cannot be less than 1999, as data only goes back that far.')
		if int(season) > int(current_season):
			raise ValueError(
				'season cannot be greater than the current year.')
	else:
		season = current_season

	if team==None or team not in get_valid_teams(browser, season):
			raise ValueError(
				'the team does not exist in kenpom in the given year.  Check that the spelling matches (https://kenpom.com) exactly.')
	
	# Sanitize team name
	team = team.replace(" ", "+")
	team = team.replace("&", "%26")
	url = url + "?team=" + str(team)
	url = url + "&y=" + str(season)

	schedule = BeautifulSoup(get_html(browser, url), "html.parser")
	table = schedule.find_all('table')[1]
	schedule_df = pd.read_html(StringIO(str(table)))

	# Dataframe Tidying
	schedule_df = schedule_df[0]
	# Teams 2010 and earlier do not show their team rank, add column for consistency
	if(len(schedule_df.columns) == 10):
		schedule_df.insert(1, 'Team Rank', '')
	schedule_df.columns = ['Date', 'Team Rank', 'Opponent Rank', 'Opponent Name', 'Result', 'Possession Number',
					  'A', 'Location', 'Record', 'Conference', 'B']
	schedule_df = schedule_df.drop(columns = ['A', 'B'])
	schedule_df = schedule_df.fillna('')

	# Add postseason tournament info to a distinct column
	schedule_df['Postseason'] = None
	# Enumerate tournament names and their row indices
	postseason_labels = schedule_df[(schedule_df['Team Rank'].str.contains('Tournament')) | (schedule_df['Team Rank'].str.contains('Postseason'))].reset_index()[['index', 'Date']].values.tolist()
	# Tournament name preprocessing
	postseason_labels = list(map(lambda x: [x[0], re.sub(r'(?:\sConference)?\sTournament.*?$', '', x[1])], postseason_labels))
	# Loop tournaments in schedule and apply to associated games
	i = 0
	while i < len(postseason_labels):
		if i != len(postseason_labels) - 1:
			schedule_df.loc[postseason_labels[i][0]:postseason_labels[i+1][0]-1, 'Postseason'] = postseason_labels[i][1]
		else:
			schedule_df.loc[postseason_labels[i][0]:, 'Postseason'] = postseason_labels[i][1]
		i += 1
	# Remove table data not corresponding to a scheduled competition
	schedule_df = schedule_df[schedule_df['Date'] != schedule_df['Result']]
	schedule_df = schedule_df[schedule_df['Date'] != 'Date']

	return schedule_df.reset_index(drop=True)

def get_scouting_report(browser: CloudScraper, team: str, season: Optional[int]=None, conference_only: bool=False):
	"""
    Retrieves and parses team scouting report data from (https://kenpom.com/team.php) into a dictionary.

    Args:
    	browser (CloudScraper): The mechanize browser object for web scraping.
    	team (str): team: Used to determine which team to scrape for schedule.
    	season (int, optional): Used to define different seasons. 1999 is the earliest available season.
    	conference_only (bool, optional): When True, only conference-related stats are retrieved; otherwise, all stats are fetched.

    Returns:
    	dict: A dictionary containing various team statistics.

    Raises:
    	ValueError if the provided season is earlier than 1999 or greater than the current year
		ValueError if the team name is invalid or not found in the specified year
	"""

	url = 'https://kenpom.com/team.php'

	current_season = get_current_season(browser)

	if season:
		if int(season) < 1999:
			raise ValueError(
				'season cannot be less than 1999, as data only goes back that far.')
		if int(season) > current_season:
			raise ValueError(
				'season cannot be greater than the current year.')
	else:
		season = int(current_season)

	if team==None or team not in get_valid_teams(browser, season):
			raise ValueError(
				'the team does not exist in kenpom in the given year.  Check that the spelling matches (https://kenpom.com) exactly.')
	
	# Sanitize team name
	team = team.replace(" ", "+")
	team = team.replace("&", "%26")
	url = url + "?team=" + str(team)
	url = url + "&y=" + str(season)

	report = BeautifulSoup(get_html(browser, url), "html.parser")
	scouting_report_scripts = report.find("script", { "type": "text/javascript", "src": ""} )

	extraction_pattern = re.compile(r"\$\(\"td#(?P<token>[A-Za-z0-9]+)\"\)\.html\(\"(.+)\"\);")
	if conference_only:
		pattern = re.compile(r"\$\(':checkbox'\).click\(function\(\) \{([^\}]+)}")
	else:
		pattern = re.compile(r"function tableStart\(\) \{([^\}]+)}")

	stats = extraction_pattern.findall(decode(encode(pattern.search(str(scouting_report_scripts.contents[0])).groups()[0], 'latin-1', 'backslashreplace'), 'unicode-escape'))
	stats = list(map(lambda x: (x[0], float(BeautifulSoup(x[1], "lxml").find('a').contents[0]), int(str(BeautifulSoup(x[1], "lxml").find('span', { "class": "seed" }).contents[0]))), stats[2:]))
	# Defaulting each stat to '' for earlier years which might not have all the stats
	stats_df = {'OE': '', 'OE.Rank': '', 'DE': '', 'DE.Rank': '', 'Tempo': '', 'Tempo.Rank': '', 'APLO': '', 'APLO.Rank': '', 'APLD': '', 'APLD.Rank': '', 'eFG': '', 'eFG.Rank': '', 'DeFG': '', 'DeFG.Rank': '', 'TOPct': '', 'TOPct.Rank': '', 'DTOPct': '', 'DTOPct.Rank': '', 'ORPct': '', 'ORPct.Rank': '', 'DORPct': '', 'DORPct.Rank': '', 'FTR': '', 'FTR.Rank': '', 'DFTR': '', 'DFTR.Rank': '', '3Pct': '', '3Pct.Rank': '', 'D3Pct': '', 'D3Pct.Rank': '', '2Pct': '', '2Pct.Rank': '', 'D2Pct': '', 'D2Pct.Rank': '', 'FTPct': '', 'FTPct.Rank': '', 'DFTPct': '', 'DFTPct.Rank': '', 'BlockPct': '', 'BlockPct.Rank': '', 'DBlockPct': '', 'DBlockPct.Rank': '', 'StlRate': '', 'StlRate.Rank': '', 'DStlRate': '', 'DStlRate.Rank': '', 'NSTRate': '', 'NSTRate.Rank': '', 'DNSTRate': '', 'DNSTRate.Rank': '', '3PARate': '', '3PARate.Rank': '', 'D3PARate': '', 'D3PARate.Rank': '', 'ARate': '', 'ARate.Rank': '', 'DARate': '', 'DARate.Rank': '', 'PD3': '', 'PD3.Rank': '', 'DPD3': '', 'DPD3.Rank': '', 'PD2': '', 'PD2.Rank': '', 'DPD2': '', 'DPD2.Rank': '', 'PD1': '', 'PD1.Rank': '', 'DPD1': '', 'DPD1.Rank': ''}	
	for stat in stats:
		stats_df[stat[0]] = stat[1]
		stats_df[stat[0]+'.Rank'] = stat[2]
	return stats_df

def get_float(v):
	try:
		return float(v)
	except:
		return v


def generate_team_stats(team_name, four_factors, team_stats, team_stats_def, points_dist):
	ff = {k: get_float(four_factors.loc[team_name][k]) for k in four_factors.columns}
	ts = {k: get_float(team_stats.loc[team_name][k]) for k in team_stats.columns}
	pd = {k: get_float(points_dist.loc[team_name][k]) for k in points_dist.columns}
	tsd = {k: get_float(team_stats_def.loc[team_name][k]) for k in team_stats_def.columns}

	return {
		**ff,
		**ts,
		**pd,
		**tsd,
	}


def get_next_opponent(browser, team_name, date_time_formatted):
	print('team_name ' + team_name)
	try:
		today = datetime.datetime.today()
		DATES_TO_CHECK = [
			(today + datetime.timedelta(days=i)).strftime('%a %b %d')
			for i in range(4)
		]

		schedule = get_schedule(browser, team_name)
		schedule = schedule.set_index('Date')
		
		for date in DATES_TO_CHECK:
			try:
				game = schedule.loc[date]
				opponent = game['Opponent Name']
				result = game['Result']
				return (opponent, result)
			except:
				pass

		return (None, None)
	except:
		print('retrying get opponent for: ' + team_name)
		return get_next_opponent(browser, team_name, date_time_formatted)

def get_player_expanded(browser, date_time_formatted, team_with_spaces=None, four_factors=None, team_stats=None, team_stats_def=None, points_dist=None):
	"""
	Scrapes a team's schedule from (https://kenpom.com/team.php) into a dataframe.

	Args:
		browser (mechanicalsoul StatefulBrowser): Authenticated browser with full access to kenpom.com generated
			by the `login` function
		team: Used to determine which team to scrape for schedule.
		season (str, optional): Used to define different seasons. 2002 is the earliest available season.

	Returns:
		team_df (pandas dataframe): Dataframe containing a team's schedule for the given season.

	Raises:
		ValueError if `season` is less than 2002.
		ValueError if `season` is greater than the current year.
		ValueError if `team` is not in the valid team list.
	"""
	print('Starting for: ' + team_with_spaces)
	try:

		url = 'https://kenpom.com/player-expanded.php'

		date = datetime.date.today()
		currentYear = date.strftime("%Y")

		season = int(currentYear)

		if team_with_spaces==None or team_with_spaces not in get_valid_teams(browser, season):
				raise ValueError(
					'the team does not exist in kenpom in the given year.  Check that the spelling matches (https://kenpom.com) exactly.')
		
		# Sanitize team name
		team = team_with_spaces.replace(" ", "+")
		team = team.replace("&", "%26")
		
		url = url + "?team=" + str(team)
		url = url + "&y=" + str(season)

		# MANUAL ADJUSTMENT 2-8-25
		schedule = BeautifulSoup(get_html(browser, url), "html.parser")

		table = schedule.find_all('table')[0]
		stats_df = pd.read_html(str(table))[0]
		stats_df = stats_df.rename(columns={ 'Unnamed: 0': 'Number', 'Unnamed: 1': 'Name'})
		stats_df = stats_df[pd.to_numeric(stats_df.Number, errors='coerce').notnull()]
		
		stats_df['Name'] = stats_df['Name'].str.replace('\d+', '')
		stats_df['Name'] = stats_df['Name'].str.replace(' National Rank', '')
		stats_df['Name'] = stats_df['Name'].str.replace('National Rank', '')
		
		# super hack for Player of the Year rankings
		# TODO: This is broken for KJ Simpson
		for x in range(0, 20):
			stats_df['Name'] = stats_df['Name'].str.replace(f' {x}', '')

		# super hack for removing player ranks by stat from player stats DF
		for index, row in stats_df.iterrows():
			for column, value in row.items():
				if column == 'Name':
					name = value
					continue
				
				stats_df.at[index, column] = get_float(value.split(' ', 1)[0]) if isinstance(value, str) and ' ' in value else get_float(value)

		stats_df = stats_df.set_index('Name')

		# TODO: 1 vs 2 --> Bug for Chicago St. --> No conference table --> super hack for now
		table = schedule.find_all('table')[2] if len(schedule.find_all('table')) == 3 else schedule.find_all('table')[1]
		minutes_df = pd.read_html(str(table))[0]
		minutes_df = minutes_df.fillna(0)

		memoized_team_stats = {}
		memoized_next_opponent_stats = {}

		players = set()
		for x in range(1, 7):
			game = minutes_df.iloc[[-x]]
			game = game.drop(columns=['MinutesMatrixTM', 'Starting Lineup #']) # 'Unnamed: 1_level_1', 'Unnamed: 2_level_1', 'Unnamed: 3_level_1']) # 'Starting Lineup #'
			game = game.sum(axis=0)

			for name, val in game.items(): # iteritems()
				name = name[0].replace('\xa0', ' ')
				stats_df.loc[name, f'Game -{x}'] = val
				players.add(name)

		for player in players:
			stats_df.loc[player, 'Team'] = team

			if team not in memoized_team_stats:
				memoized_team_stats[team] = generate_team_stats(team_with_spaces, four_factors, team_stats, team_stats_def, points_dist)

			for k, v in memoized_team_stats.get(team).items():
				stats_df.loc[player, f'Team.{k}'] = v

			if team not in memoized_next_opponent_stats:
				opponent, result = get_next_opponent(browser, team_with_spaces, date_time_formatted)
				memoized_next_opponent_stats[team] = generate_team_stats(opponent, four_factors, team_stats, team_stats_def, points_dist) if opponent is not None else None

			# team may not have any more games so need to check
			if memoized_next_opponent_stats[team] is not None:
				stats_df.loc[player, 'NextOpponent'] = opponent
				stats_df.loc[player, 'KenPomResult'] = result
				for k, v in memoized_next_opponent_stats.get(team).items():
					stats_df.loc[player, f'Opponent.{k}'] = v

		return stats_df
	except:
		tb = traceback.format_exc()
		print(tb)
		return
		
		# print('trying againg for: ' + team)
		# return get_player_expanded(browser, team=team)

	
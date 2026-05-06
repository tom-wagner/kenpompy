"""
This module contains functions for scraping the team page kenpom.com tables into
pandas dataframes
"""

import pandas as pd
from io import StringIO
from .misc import get_current_season
import re
import unicodedata
from cloudscraper import CloudScraper
from bs4 import BeautifulSoup
from codecs import encode, decode
from typing import Optional
from .utils import AuthenticationError, RateLimitError, get_html
import datetime
import traceback

NAME_SUFFIX_TOKENS = {'jr', 'sr', 'ii', 'iii', 'iv', 'v'}


def _normalize_player_name(value: str) -> str:
	text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
	text = text.lower().replace("'", "").replace(".", "")
	text = re.sub(r"[^a-z0-9]+", " ", text).strip()
	return text


def _canonicalize_player_name(value: str) -> str:
	tokens = _normalize_player_name(value).split()
	while tokens and tokens[-1] in NAME_SUFFIX_TOKENS:
		tokens.pop()
	return " ".join(tokens)


def _extract_player_urls(schedule: BeautifulSoup) -> dict[str, str]:
	player_urls = {}
	for anchor in schedule.find_all('a', href=True):
		href = anchor.get('href', '')
		if 'player.php?p=' not in href:
			continue
		name = anchor.get_text(' ', strip=True).replace('\xa0', ' ')
		if not name:
			continue
		canonical_name = _canonicalize_player_name(name)
		if canonical_name and canonical_name not in player_urls:
			player_urls[canonical_name] = href if href.startswith('http') else f'https://kenpom.com/{href.lstrip("/")}'
	return player_urls


def _get_team_page_soup(browser: CloudScraper, team: str, season: Optional[int | str] = None) -> tuple[BeautifulSoup, int]:
	url = 'https://kenpom.com/team.php'
	current_season = int(get_current_season(browser))

	if season is not None:
		season = int(season)
		if season < 1999:
			raise ValueError(
				'season cannot be less than 1999, as data only goes back that far.')
		if season > current_season:
			raise ValueError(
				'season cannot be greater than the current year.')
	else:
		season = current_season

	if team is None or team not in get_valid_teams(browser, season):
		raise ValueError(
			'the team does not exist in kenpom in the given year.  Check that the spelling matches (https://kenpom.com) exactly.')

	cache = getattr(browser, '_kenpom_team_page_cache', {})
	cache_key = (season, team)
	if cache_key in cache:
		return cache[cache_key], season

	safe_team = team.replace(" ", "+").replace("&", "%26")
	url = f'{url}?team={safe_team}&y={season}'
	team_page = BeautifulSoup(get_html(browser, url), "html.parser")
	cache[cache_key] = team_page
	setattr(browser, '_kenpom_team_page_cache', cache)
	return team_page, season


def _parse_recent_lineup_player(cell: BeautifulSoup) -> dict[str, object]:
	player = {
		'Number': None,
		'Name': None,
		'Height': None,
		'Weight': None,
		'Year': None,
		'KenPomPlayerURL': None,
	}

	player_div = cell.find('div')
	if player_div is None:
		return player

	number_span = player_div.find('span', class_='seed-gray')
	if number_span is not None:
		number_text = number_span.get_text(' ', strip=True)
		number_match = re.search(r'\d+', number_text)
		if number_match:
			player['Number'] = int(number_match.group())

	anchor = player_div.find('a', href=True)
	if anchor is not None:
		player['Name'] = anchor.get_text(' ', strip=True).replace('\xa0', ' ')
		href = anchor.get('href', '')
		player['KenPomPlayerURL'] = href if href.startswith('http') else f'https://kenpom.com/{href.lstrip("/")}'

	meta_span = player_div.find('span', class_=lambda value: value and 'display-block' in value.split())
	if meta_span is not None:
		meta_text = meta_span.get_text(' ', strip=True).replace('\xa0', ' ')
		meta_match = re.match(r'(?P<height>\d+-\d+)\s+(?P<weight>\d+)\s+(?P<year>\S+)', meta_text)
		if meta_match:
			player['Height'] = meta_match.group('height')
			player['Weight'] = int(meta_match.group('weight'))
			player['Year'] = meta_match.group('year')

	return player


def _parse_recent_lineups(team_page: BeautifulSoup) -> pd.DataFrame:
	table = team_page.find('table', id='dc-table2')
	if table is None:
		raise ValueError('Could not find the recent lineups table on the team page.')

	rows = []
	unknown_pct = None
	position_columns = ['PG', 'SG', 'SF', 'PF', 'C']

	for row in table.find_all('tr'):
		cells = row.find_all('td')
		if not cells:
			continue

		cell_texts = [cell.get_text(' ', strip=True) for cell in cells]
		first_cell = cells[0].get_text(' ', strip=True)
		if not first_cell:
			if 'UNKNOWN' not in cell_texts:
				continue

		if 'UNKNOWN' in cell_texts:
			try:
				unknown_pct = float(cells[-1].get_text(' ', strip=True))
			except (TypeError, ValueError):
				unknown_pct = None
			continue

		if not first_cell.isdigit():
			continue

		lineup = {
			'LineupRank': int(first_cell),
		}

		for index, position in enumerate(position_columns, start=1):
			player = _parse_recent_lineup_player(cells[index])
			for key, value in player.items():
				lineup[f'{position}_{key}'] = value

		try:
			lineup['Pct'] = float(cells[-1].get_text(' ', strip=True))
		except (TypeError, ValueError):
			lineup['Pct'] = None

		rows.append(lineup)

	lineups_df = pd.DataFrame(rows)
	lineups_df.attrs['unknown_pct'] = unknown_pct
	return lineups_df

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

	cache = getattr(browser, '_kenpom_valid_teams_cache', {})
	cache_key = str(season)
	if cache_key in cache:
		return cache[cache_key]

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

	cache[cache_key] = team_list
	setattr(browser, '_kenpom_valid_teams_cache', cache)
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

	cache = getattr(browser, '_kenpom_schedule_cache', {})
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

	cache_key = (str(season), team)
	if cache_key in cache:
		return cache[cache_key].copy()
	
	schedule, _ = _get_team_page_soup(browser, team, season)
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

	schedule_df = schedule_df.reset_index(drop=True)
	cache[cache_key] = schedule_df.copy()
	setattr(browser, '_kenpom_schedule_cache', cache)
	return schedule_df


def get_recent_lineups(browser: CloudScraper, team: str, season: Optional[int | str] = None):
	"""
	Scrapes the "Most frequent lineups over the past 5 games" team-page table into
	a pandas dataframe.

	Args:
		browser (CloudScraper): Authenticated browser with full access to kenpom.com generated
			by the `login` function
		team (str): Team name as shown on kenpom.com.
		season (int | str, optional): Season year. Defaults to the current season.

	Returns:
		pandas.DataFrame: One row per lineup, with player metadata expanded into
		position-specific columns. The table's ``UNKNOWN`` percentage is stored in
		``df.attrs["unknown_pct"]``.

	Raises:
		ValueError if `season` is less than 1999.
		ValueError if `season` is greater than the current year.
		ValueError if `team` is not in the valid team list.
		ValueError if the team page does not include the recent lineups table.
	"""
	team_page, _ = _get_team_page_soup(browser, team, season)
	return _parse_recent_lineups(team_page)

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


def _merge_player_expanded_rows(stats_df: pd.DataFrame) -> pd.DataFrame:
	"""
	KenPom's player-expanded table can emit one logical player across multiple rows.

	This shows up for some high-profile players where the player name row is followed
	by a ``National Rank`` row and then a continuation row containing the actual
	stat values. The old parser filtered to rows with a numeric first column before
	merging, which drops those continuation rows and leaves the player with only a
	name plus all-NaN stats.
	"""
	if stats_df.empty:
		return stats_df

	stats_df = stats_df.astype(object).copy()
	number_col = stats_df.columns[0]
	name_col = stats_df.columns[1] if len(stats_df.columns) > 1 else None

	merged_rows = []
	i = 0
	while i < len(stats_df):
		row = stats_df.iloc[i].copy()
		row_num = pd.to_numeric(row.get(number_col), errors='coerce')

		if pd.isna(row_num):
			i += 1
			continue

		j = i + 1
		while j < len(stats_df):
			next_row = stats_df.iloc[j]
			next_num = pd.to_numeric(next_row.get(number_col), errors='coerce')
			if not pd.isna(next_num):
				break

			if name_col is not None:
				next_name = str(next_row.get(name_col)).strip()
				if next_name in {'National Rank', 'Go-to guys (>28% of possessions used)'}:
					j += 1
					continue

			for col in stats_df.columns:
				next_val = next_row.get(col)
				if pd.isna(next_val) or str(next_val).strip() == '':
					continue

				if col == name_col:
					next_text = str(next_val).strip()
					if pd.isna(row.get(col)) or str(row.get(col)).strip() == '':
						row[col] = next_text
					continue

				current_val = row.get(col)
				if pd.isna(current_val) or str(current_val).strip() == '':
					row[col] = next_val

			j += 1

		merged_rows.append(row)
		i = j

	return pd.DataFrame(merged_rows, columns=stats_df.columns)


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


def get_next_opponent(browser, team_with_spaces, date_time_formatted, retries: int = 2):
	"""Get the next opponent for a team starting from the provided date"""
	# First convert from original format (mm-dd-yyyy) to datetime
	date_object = datetime.datetime.strptime(date_time_formatted, '%m-%d-%Y')
	
	# Generate dates to check starting from the provided date, including year
	DATES_TO_CHECK = [
		(date_object + datetime.timedelta(days=i)).strftime('%a %b %-d') # %Y
		for i in range(4)
	]
	
	for attempt in range(retries + 1):
		try:
			schedule = get_schedule(browser, team_with_spaces)
			schedule = schedule.set_index('Date')
			
			for date in DATES_TO_CHECK:
				try:
					game = schedule.loc[date]
					opponent = game['Opponent Name']
					result = game['Result']
					return (opponent, result)
				except Exception:
					pass

			return (None, None)
		except (AuthenticationError, RateLimitError):
			raise
		except Exception:
			if attempt == retries:
				raise
			print('retrying get opponent for: ' + team_with_spaces)

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

		requested_date = datetime.datetime.strptime(date_time_formatted, '%m-%d-%Y').date()
		season = requested_date.year

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

		tables = schedule.find_all('table')
		if not tables:
			title = schedule.find('title')
			title_text = title.string if title else 'no title'
			body_text = schedule.get_text()[:200].strip()
			print(f'No tables found for {team_with_spaces} - page title: "{title_text}", body preview: {body_text}')
			return
		table = tables[0]
		stats_df = pd.read_html(StringIO(str(table)))[0]
		stats_df = _merge_player_expanded_rows(stats_df)
		stats_df = stats_df.rename(columns={ 'Unnamed: 0': 'Number', 'Unnamed: 1': 'Name'})
		stats_df = stats_df[pd.to_numeric(stats_df.Number, errors='coerce').notnull()]
		stats_df = stats_df.astype(object)
		
		stats_df['Name'] = stats_df['Name'].astype(str)
		stats_df['Name'] = stats_df['Name'].str.replace(r'\s+\d+\s+National Rank$', '', regex=True)
		stats_df['Name'] = stats_df['Name'].str.replace(r'\s+National Rank$', '', regex=True)
		stats_df['Name'] = stats_df['Name'].str.replace(r'\s+\d+$', '', regex=True)
		stats_df['Name'] = stats_df['Name'].str.strip()
		player_urls = _extract_player_urls(schedule)
		stats_df['KenPomPlayerURL'] = stats_df['Name'].map(lambda name: player_urls.get(_canonicalize_player_name(name)))

		# super hack for removing player ranks by stat from player stats DF
		for index, row in stats_df.iterrows():
			for column, value in row.items():
				if column == 'Name':
					name = value
					continue
				
				stats_df.at[index, column] = get_float(value.split(' ', 1)[0]) if isinstance(value, str) and ' ' in value else get_float(value)

		stats_df = stats_df.set_index('Name', drop=False)

		# TODO: 1 vs 2 --> Bug for Chicago St. --> No conference table --> super hack for now
		table = schedule.find_all('table')[2] if len(schedule.find_all('table')) == 3 else schedule.find_all('table')[1]
		minutes_df = pd.read_html(StringIO(str(table)))[0]
		minutes_df = minutes_df.fillna(0)

		memoized_team_stats = {}
		memoized_next_opponent_stats = {}
		player_updates = {}

		players = set()
		for x in range(1, 7):
			game = minutes_df.iloc[[-x]]
			game = game.drop(columns=['MinutesMatrixTM', 'Starting Lineup #']) # 'Unnamed: 1_level_1', 'Unnamed: 2_level_1', 'Unnamed: 3_level_1']) # 'Starting Lineup #'
			game = game.sum(axis=0)

			for name, val in game.items(): # iteritems()
				name = name[0].replace('\xa0', ' ')
				player_updates.setdefault(name, {})[f'Game -{x}'] = val
				players.add(name)

		for player in players:
			player_updates.setdefault(player, {})['Team'] = team

			if team not in memoized_team_stats:
				memoized_team_stats[team] = generate_team_stats(team_with_spaces, four_factors, team_stats, team_stats_def, points_dist)

			for k, v in memoized_team_stats.get(team).items():
				player_updates[player][f'Team.{k}'] = v

			if team not in memoized_next_opponent_stats:
				opponent, result = get_next_opponent(browser, team_with_spaces, date_time_formatted)
				memoized_next_opponent_stats[team] = {
					'opponent': opponent,
					'result': result,
					'stats': generate_team_stats(opponent, four_factors, team_stats, team_stats_def, points_dist) if opponent is not None else None,
				}

			# team may not have any more games so need to check
			next_opponent_data = memoized_next_opponent_stats.get(team)
			if next_opponent_data is not None and next_opponent_data.get('stats') is not None:
				player_updates[player]['NextOpponent'] = next_opponent_data.get('opponent')
				player_updates[player]['KenPomResult'] = next_opponent_data.get('result')
				for k, v in next_opponent_data.get('stats', {}).items():
					player_updates[player][f'Opponent.{k}'] = v

		if player_updates:
			updates_df = pd.DataFrame.from_dict(player_updates, orient='index')
			stats_df = stats_df.combine_first(updates_df)

		return stats_df
	except (AuthenticationError, RateLimitError):
		raise
	except Exception:
		tb = traceback.format_exc()
		print(tb)
		return
		
		# print('trying againg for: ' + team)
		# return get_player_expanded(browser, team=team)

	

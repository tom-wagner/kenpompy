"""
This module contains functions for scraping the conference page kenpom.com tables into
pandas dataframes
"""

import pandas as pd
from io import StringIO
from cloudscraper import CloudScraper
from bs4 import BeautifulSoup
from typing import Optional
from .utils import get_html

def get_valid_conferences(browser: CloudScraper, season: Optional[str]=None):
	"""
	Scrapes the conferences (https://kenpom.com/conf.php) into a list.

	Args:
		browser (CloudScraper): Authenticated browser with full access to kenpom.com generated
			by the `login` function
		season (str, optional): Used to define different seasons. 1999 is the earliest available season.

	Returns:
		conference_list (list): List containing all valid conferences for the given season on kenpom.com.
	"""

	url = "https://kenpom.com/conf.php"
	url = url + '?c=B10'
	if(season):
		url = url + '&y=' + str(season)
	confs = BeautifulSoup(get_html(browser, url), "html.parser")
	table = confs.find_all('table')[-1]
	links = table.find_all('a')
	conf_list = []
	for link in links:
		conf_list.append(link['href'].split('=')[-1])
	conf_list.sort()
	return conf_list


def get_aggregate_stats(browser: CloudScraper, conf: Optional[str]=None, season: Optional[str]=None):
	"""
	Scrapes a given conference's stats (https://kenpom.com/conf.php or https://kenpom.com/confstats.php) into a dataframe.

	Args:
		browser (CloudScraper): Authenticated browser with full access to kenpom.com generated
			by the `login` function
		conf (str: optional): conference abbreviation (ie B10, P12). If None, it will grab the table from https://kenpom.com/confstats.php instead of https://kenpom.com/conf.php
		season (str, optional): Used to define different seasons. 1999 is the earliest available season.

	Returns:
		conference_df (dataframe): Dataframe containing aggregate stats of the conference for the given season on kenpom.com.
	"""
	if(conf):
		url = "https://kenpom.com/conf.php"
		url = url + f'?c={conf}'
		if(season):
			url = url + '&y=' + str(season)
		confs = BeautifulSoup(get_html(browser, url), "html.parser")
		#get first table
		table = confs.find_all('table')[-3]
		conf_df = pd.read_html(StringIO(str(table)))[0]
		#get second table
		table = confs.find_all('table')[-2]
		conf2_df = pd.read_html(StringIO(str(table)))[0]
		conf2_df['Value'] = conf2_df['Value'].str.replace('%', '').astype(float)
		conf_df = pd.concat([conf_df, conf2_df])
		#clean table
		conf_df = conf_df.set_index('Stat')
		conf_df = conf_df.drop('Unnamed: 1', axis=1)
		conf_df.columns = ['Value', 'Rank']
		conf_df.index = conf_df.index.str.split(' (', regex=False).str[0]
		return conf_df
	else:
		url = "https://kenpom.com/confstats.php"
		if(season):
			url = url + '?y=' + str(season)
		confs = BeautifulSoup(get_html(browser, url), "html.parser")
		#get table
		table = confs.find_all('table')[0]
		conf_df = pd.read_html(StringIO(str(table)))[0]
		# Clean table
		conf_df = conf_df.set_index('Conf')
		conf_df.columns = [stat[:-1] + 'Rank' if '.1' in stat else stat for stat in conf_df.columns]
		return conf_df

def get_standings(browser: CloudScraper, conf: str, season: Optional[str]=None):
	"""
	Scrapes a given conference's standing stats (https://kenpom.com/conf.php) into a dataframe.

	Args:
		browser (CloudScraper): Authenticated browser with full access to kenpom.com generated
			by the `login` function
		conf (str): conference abbreviation (ie B10, P12)
		season (str, optional): Used to define different seasons. 1999 is the earliest available season.

	Returns:
		conference_df (dataframe): Dataframe containing standing stats of the conference for the given season on kenpom.com.
	"""

	url = "https://kenpom.com/conf.php"
	url = url + f'?c={conf}'
	if(season):
		url = url + '&y=' + str(season)
	confs = BeautifulSoup(get_html(browser, url), "html.parser")
	table = confs.find_all('table')[0]
	conf_df = pd.read_html(StringIO(str(table)))[0]
	# Parse out seed
	conf_df['Seed'] = conf_df['Team'].str.extract('([0-9]+)')
	conf_df['Team'] = conf_df['Team'].str.replace('([0-9]+)', '', regex=True).str.rstrip()

	# Rename Rank headers
	conf_df.columns = [stat[:-1] + 'Rank' if '.1' in stat else stat for stat in conf_df.columns]

	return conf_df


def get_offense(browser: CloudScraper, conf: str, season: Optional[str]=None):
	"""
	Scrapes a given conference's offense only stats (https://kenpom.com/conf.php) into a dataframe.

	Args:
		browser (CloudScraper): Authenticated browser with full access to kenpom.com generated
			by the `login` function
		conf (str): conference abbreviation (ie B10, P12)
		season (str, optional): Used to define different seasons. 1999 is the earliest available season.

	Returns:
		conference_df (dataframe): Dataframe containing offensive stats of the conference for the given season on kenpom.com.
	"""

	url = "https://kenpom.com/conf.php"
	url = url + f'?c={conf}'
	if(season):
		url = url + '&y=' + str(season)
	confs = BeautifulSoup(get_html(browser, url), "html.parser")
	table = confs.find_all('table')[1]
	conf_df = pd.read_html(StringIO(str(table)))[0]

	# Rename Rank headers
	conf_df.columns = [stat[:-1] + 'Rank' if '.1' in stat else stat for stat in conf_df.columns]

	return conf_df


def get_defense(browser: CloudScraper, conf: str, season: Optional[str]=None):
	"""
	Scrapes a given conference's defense only stats (https://kenpom.com) into a dataframe.

	Args:
		browser (CloudScraper): Authenticated browser with full access to kenpom.com generated
			by the `login` function
		conf (str): conference abbreviation (ie B10, P12)
		season (str, optional): Used to define different seasons. 1999 is the earliest available season.

	Returns:
		conference_df (dataframe): Dataframe containing defensive stats of the conference for the given season on kenpom.com.
	"""

	url = "https://kenpom.com/conf.php"
	url = url + f'?c={conf}'
	if(season):
		url = url + '&y=' + str(season)
	confs = BeautifulSoup(get_html(browser, url), "html.parser")
	table = confs.find_all('table')[2]
	conf_df = pd.read_html(StringIO(str(table)))[0]

	# Rename Rank headers
	conf_df.columns = [stat[:-1] + 'Rank' if '.1' in stat else stat for stat in conf_df.columns]

	return conf_df
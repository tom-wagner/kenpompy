"""
The utils module provides utility functions, such as logging in.
"""

import time

import cloudscraper
from cloudscraper import CloudScraper


class KenPomError(Exception):
	"""Base exception for scraper failures."""


class AuthenticationError(KenPomError):
	"""Raised when a response indicates the session is no longer authenticated."""


class RateLimitError(KenPomError):
	"""Raised when kenpom.com responds with HTTP 429."""


def _is_subscription_page(response_content: bytes) -> bool:
	"""Detect the logged-out subscription page returned with HTTP 200."""
	content = response_content.lower()
	return b'kenpom.com subscription' in content and b'forgot password?' in content

def login(email: str, password: str):
	"""
	Logs in to kenpom.com using user credentials.

	Args:
		email (str): User e-mail for login to kenpom.com.
		password (str): User password for login to kenpom.com.

	Returns:
		browser (mechanicalsoup StatefulBrowser): Authenticated browser with full access to kenpom.com.
	"""

	browser = cloudscraper.create_scraper()
	browser.get('https://kenpom.com/index.php')

	form_data = {
		'email': email,
		'password': password,
		'submit': 'Login!',
	}

	# Response page actually throws an error but further navigation works and will show you as logged in.
	browser.post(
		'https://kenpom.com/handlers/login_handler.php',
		data=form_data, 
		allow_redirects=True
	)

	home_page = browser.get('https://kenpom.com/')
	if 'Logged in as' not in home_page.text:
		raise Exception('Logging in failed - check your credentials')

	return browser

def get_html(browser: CloudScraper, url: str, retries: int = 5, backoff_seconds: int = 20):
	"""
	Performs a get request on the specified url.

	Args:
		browser (CloudScraper): Authenticated browser with full access to kenpom.com generated
            by the `login` function.
		url (str): The url to perform the get request on.
	
	Returns:
		html (Bytes | Any): The return content.
	
	Raises:
		Exception if get request gets a non-200 response code.
	"""
	last_error = None
	rate_limit_count = 0
	for attempt in range(retries):
		response = browser.get(url)
		print(f'GET {url} -> {response.status_code} ({len(response.content)} bytes)')

		if response.status_code == 429:
			rate_limit_count += 1
			last_error = RateLimitError(
				f'Rate limited on {url} (429 Too Many Requests) '
				f'[{rate_limit_count}/{retries}]'
			)
		elif response.status_code != 200:
			raise KenPomError(f'Failed to retrieve {url} (status code: {response.status_code})')
		elif _is_subscription_page(response.content):
			raise AuthenticationError(f'Authentication lost while retrieving {url}')
		else:
			return response.content

		if attempt < retries - 1:
			if isinstance(last_error, RateLimitError) and attempt == 0:
				sleep_for = 600
			else:
				sleep_for = backoff_seconds * (attempt + 1)
			print(f'Retrying {url} in {sleep_for} seconds')
			time.sleep(sleep_for)

	if last_error is not None:
		if rate_limit_count >= retries:
			raise RateLimitError(
				f'Exceeded retry limit after {rate_limit_count} HTTP 429 responses for {url}. '
				'Exiting scraper to avoid further rate limiting.'
			)
		raise last_error

	raise KenPomError(f'Failed to retrieve {url} for an unknown reason')

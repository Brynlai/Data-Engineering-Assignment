
"""
Author: Lim Zhao Qing
"""

import requests

def fetch_search_results() -> list:
    """
    Fetches search results from the Wikipedia API.

    Searches Wikipedia for the term 'bahasa' and retrieves a list of article titles.

    Returns:
        list: A list of article titles from the search results.
    """
    print("Fetching search results from Wikipedia API...")
    response = requests.get('https://ms.wikipedia.org/w/api.php', params={
        'action': 'query',
        'format': 'json',
        'list': 'search',
        'utf8': 1,
        'srsearch': 'bahasa',  # Search term
        'srlimit': 40          # Limit the number of search results
    })
    response.raise_for_status()  # Raise an exception for bad status codes
    data = response.json()

    # Extract titles from search results
    titles = [result['title'] for result in data['query']['search']]
    print(f"Extracted titles: {titles}")

    return titles

def fetch_page_content(title: str) -> dict:
    """
    Fetches page content from the Wikipedia API.

    Retrieves the content of a specific Wikipedia page given its title.

    Args:
        title (str): The title of the Wikipedia page.

    Returns:
        dict: The JSON response containing the page content.
    """
    print(f"Fetching page content for title: {title}")
    response = requests.get('https://ms.wikipedia.org/w/api.php', params={
        'action': 'query',
        'format': 'json',
        'prop': 'extracts',
        'exintro': True, # Retrieve only the introduction
        'exlimit': 1,    # Limit to one extract
        'explaintext': True, # Get plain text content
        'titles': title
    })
    response.raise_for_status()  # Raise an exception for bad status codes
    return response.json()

def extract_page_info(page_content: dict) -> list:
    """
    Extracts page information (title and content) from the API response.

    Processes the JSON response from the Wikipedia API and extracts the title and content
    of each page.

    Args:
        page_content (dict): The JSON response from the Wikipedia API.

    Returns:
        list: A list of dictionaries, each containing the title and content of a page.
    """
    print("Extracting page information...")
    data_to_write = []

    # Extract relevant information from the page content
    if 'pages' in page_content['query']:
        for page_info in page_content['query']['pages'].values():
            if 'extract' in page_info:
                info = {
                    'Title': page_info['title'],
                    'Content': page_info['extract']
                }
                print(f"Extracted page info: {info}")
                data_to_write.append(info)

    print("Completed extracting page information.")
    return data_to_write

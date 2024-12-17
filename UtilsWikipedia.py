import requests

def fetch_search_results() -> list:
    """Fetch search results from the Wikipedia API."""
    print("Fetching search results from Wikipedia API...")
    response = requests.get('https://ms.wikipedia.org/w/api.php', params={
        'action': 'query',
        'format': 'json',
        'list': 'search',
        'utf8': 1,
        'srsearch': 'bahasa',  # Search term
        'srlimit': 10          # Limit the number of search results
    })
    response.raise_for_status()  # Raise an exception for bad status codes
    data = response.json()

    # Extract titles from search results
    titles = [result['title'] for result in data['query']['search']]
    print(f"Extracted titles: {titles}")

    return titles

def fetch_page_content(title: str) -> dict:
    """Fetch page content from the Wikipedia API."""
    print(f"Fetching page content for title: {title}")
    response = requests.get('https://ms.wikipedia.org/w/api.php', params={
        'action': 'query',
        'format': 'json',
        'prop': 'extracts',
        'exintro': True,
        'exlimit': 1,
        'explaintext': True,
        'titles': title
    })
    response.raise_for_status()  # Raise an exception for bad status codes
    return response.json()

def extract_page_info(page_content: dict) -> list:
    """Extract page information (title and content) from the response."""
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
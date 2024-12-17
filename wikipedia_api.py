import requests
from typing import List

WIKIPEDIA_API_URL = "https://ms.wikipedia.org/w/api.php"

def fetch_search_results(query: str = "bahasa", limit: int = 10) -> List[str]:
    """
    Fetch search results (titles) from Wikipedia API.
    Limited to a single query response, no pagination.
    """
    print(f"Fetching search results for query: '{query}' with limit: {limit}")
    params = {
        "action": "query",
        "format": "json",
        "list": "search",
        "srsearch": query,
        "srlimit": limit
    }

    response = requests.get(WIKIPEDIA_API_URL, params=params)
    response.raise_for_status()
    data = response.json()

    titles = [result["title"] for result in data["query"]["search"]]
    print(f"Fetched {len(titles)} titles.")
    return titles


def fetch_page_content(title: str) -> str:
    """
    Fetches page content for a specific Wikipedia title.
    """
    print(f"Fetching content for page: '{title}'")
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "exintro": True,
        "explaintext": True,
        "titles": title
    }
    response = requests.get(WIKIPEDIA_API_URL, params=params)
    response.raise_for_status()
    data = response.json()

    pages = data.get("query", {}).get("pages", {})
    for page in pages.values():
        return page.get("extract", "")

    return ""

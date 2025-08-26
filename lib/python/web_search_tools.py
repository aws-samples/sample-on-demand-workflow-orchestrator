import requests
from bs4 import BeautifulSoup
from ddgs import DDGS
from concurrent.futures import ThreadPoolExecutor, wait
from itertools import repeat
from strands import tool

ddgs = DDGS()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def extract_text_from_url(url):
    print("WebBrowser - extract_text_from_url")
    """
    Extracts and returns the text content from the given URL.

    Parameters:
        url (str): The URL of the webpage to extract text from.

    Returns:
        str: The extracted text content of the webpage.
    """
    try:
        # Send a GET request to the URL
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract and return all text content from the webpage
        return soup.get_text(separator='\n', strip=True).encode("ascii", "ignore").decode("utf-8")
    except requests.exceptions.RequestException as e:
        return f"An error occurred while fetching the URL: {e}"

@tool
def text_search(search_string : str, max_results : int = 5) -> dict:
    """
    Performs web search based on search string provided.

    Parameters:
        search_string (str): string to search the web
        max_results (int): max number of web pages to return

    Returns:
        dict: Dictionary of search results
    """
    print("tool - search_string: ", search_string)
    results = []
    try:
        print("DuckDuckGo - self.ddgs.text")
        results = ddgs.text(search_string, max_results=max_results)
    except Exception as ex:
        print("DuckDuckGo - Exception - switching to requests.get")
        url = "https://lite.duckduckgo.com/lite/?q="+search_string
        page = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(page.text, 'html.parser')
        span_tags = soup.find_all('span', class_='link-text')
        results = [{"href": 'https://'+span_tag.text} for span_tag in span_tags]
        results = results[:max_results]
    
    print("DuckDuckGo - starting ThreadPoolExecutor")
    with ThreadPoolExecutor(max_workers=max_results) as executor:
        futures = list(executor.map(
                            extract_text_from_url, 
                            [result["href"] for result in results]
                        ))
        print("DuckDuckGo - ThreadPoolExecutor completed")
        for i, webpage in enumerate(futures):
            results[i]["full_body"] = webpage
    
    return results
    
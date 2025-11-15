import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
pd.set_option('display.max_columns', None)   # show all columns
pd.set_option('display.max_rows', None)      # show all rows
pd.set_option('display.max_colwidth', None)  # show full text in each cell

def extract():
    print("Extracting data...")
    return []

def extract_gutenberg_titles_and_plots(num_books=20):
    """
    Extract top books from Project Gutenberg with only title and plot.

    Args:
        num_books (int): Number of books to extract from top listing.

    Returns:
        pd.DataFrame: columns ['title', 'plot']
    """
    base_url = "https://www.gutenberg.org"
    listing_url = "https://www.gutenberg.org/browse/scores/top"

    response = requests.get(listing_url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Get ebook links
    book_links = [a['href'] for a in soup.select("ol li a") if "/ebooks/" in a['href']]
    book_links = book_links[:num_books]

    books = []

    for link in book_links:
        book_url = base_url + link
        try:
            r = requests.get(book_url)
            book_soup = BeautifulSoup(r.text, "html.parser")

            # Title
            title_tag = book_soup.find("h1")
            title = title_tag.text.strip() if title_tag else "Unknown Title"

            # Plot / Summary
            plot_tag = book_soup.find("div", {"class": "summary-text-container"})
            plot = plot_tag.text.strip() if plot_tag else "No Description"

            books.append({
                "title": title,
                "plot": plot
            })

            # Polite scraping
            time.sleep(1)

        except Exception as e:
            print(f"Error scraping {book_url}: {e}")
            continue

    df = pd.DataFrame(books)
    return df


try:
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

def get_nyt_api_key():
    """
    Get NYT API key securely.
    Priority:
    1. Airflow Variable 'nyt_api_key' (if Airflow is running)
    2. Environment variable NYT_API_KEY
    """
    api_key = None

    if AIRFLOW_AVAILABLE:
        try:
            api_key = Variable.get("nyt_api_key")
        except KeyError:
            pass

    if not api_key:
        api_key = os.getenv("NYT_API_KEY")

    if not api_key:
        raise Exception("NYT API key not found. Set environment variable NYT_API_KEY or Airflow Variable 'nyt_api_key'.")

    return api_key


def extract_nyt_books(list_name='hardcover-fiction', num_books=None):
    """
    Extract NYT bestseller books with title and plot/description.

    Args:
        list_name (str): Bestseller list name.
        num_books (int, optional): Limit number of books to extract.

    Returns:
        pd.DataFrame: columns ['title', 'plot']
    """
    api_key = get_nyt_api_key()
    url = f"https://api.nytimes.com/svc/books/v3/lists/current/{list_name}.json"
    params = {"api-key": api_key}
    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"NYT API request failed: {response.status_code}")

    data = response.json()
    books = []

    for entry in data['results']['books']:
        title = entry['title']
        plot = entry['description'] if entry['description'] else "No Description"
        books.append({"title": title, "plot": plot})

    if num_books:
        books = books[:num_books]

    df = pd.DataFrame(books)
    return df


# Optional test run
if __name__ == "__main__":
    df = extract_nyt_books(num_books=10)
    pd.set_option('display.max_colwidth', None)
    print(df)
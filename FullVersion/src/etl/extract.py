import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
pd.set_option('display.max_columns', None)   # show all columns
pd.set_option('display.max_rows', None)      # show all rows
pd.set_option('display.max_colwidth', None)  # show full text in each cell
import os
api_key = os.environ.get("NYT_API_KEY")
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

OUTPUT_DIR = "/mnt/c/Users/abbas/Documents/GitHub/YetToBeNamedBookProject/FullVersion/data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_nyt_bestsellers(list_names, start_date, end_date, delay=0.5):
    """
    Fetch historical NYT bestsellers and save weekly batches as CSVs.

    :param list_names: list of NYT bestseller list names (e.g., ["hardcover-fiction"])
    :param start_date: datetime object, start of historical range
    :param end_date: datetime object, end of historical range
    :param delay: seconds to wait between requests (for rate limiting)
    """
    current_date = start_date
    NYT_API_KEY = get_nyt_api_key()
    while current_date <= end_date:
        for list_name in list_names:
            url = f"https://api.nytimes.com/svc/books/v3/lists/{current_date.strftime('%Y-%m-%d')}/{list_name}.json"
            params = {"api-key": NYT_API_KEY}

            try:
                response = requests.get(url, params=params, timeout=10)
                data = response.json()
                books = data.get("results", {}).get("books", [])

                if books:
                    df = pd.DataFrame([{
                        "title": book.get("title"),
                        "description": book.get("description")
                    } for book in books])

                    # Save weekly batch
                    filename = f"{OUTPUT_DIR}/{list_name}_{current_date.strftime('%Y-%m-%d')}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved {len(df)} books for {list_name} on {current_date.strftime('%Y-%m-%d')}")

            except Exception as e:
                print(f"Error fetching {list_name} on {current_date}: {e}")

            time.sleep(delay)  # avoid hitting rate limits

        current_date += timedelta(days=7)  # next week

    print("Finished fetching NYT bestsellers!")
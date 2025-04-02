from utils import (
    insert_into_database,
    fetch_page_with_zenrows,
)
from crawl_article.crawl_article_gastroenterology_sql import (
    crawl_article_gut,
    crawl_article_gas,
    crawl_article_hepatology,
    crawl_article_ajg,
    crawl_article_clincal,
)
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError

specialization = "gastroenterology"

# Define the Pydantic model for the data
class GastroenterologyData(BaseModel):
    Journal: str
    Article: list

async def fetch_new_gut_articles(conn):
    """
    Fetches new articles from Gut via RSS feed.
    Compares with existing database entries and inserts only new links.
    """
    name = "Gut"
    url = "https://gut.bmj.com/rss/recent.xml"

    try:
        links = set()  # Use a set to store unique links

        # Fetch the RSS feed
        response = await fetch_page_with_zenrows(url)
        soup = BeautifulSoup(response.html, "html.parser")  # Parse XML response

        # Extract article links from the RSS feed
        links = {element["rdf:resource"] for element in soup.find_all("rdf:li")}

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing article links

            # Filter out already stored links
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {len(Links)}")  # Debugging

            # Validate data using Pydantic
            data = GastroenterologyData(Journal=name, Article=Links)

            # Insert validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_gut(specialization)  # Crawl and process article content

        except ValidationError as e:
            print(f"Validation error: {e}")  
            print(f"Failed to insert {links} into the database.")  

    except Exception as e:
        print(f"Error fetching or processing links for {name}: {e}")  

async def fetch_new_Gastroenterology_articles(conn):
    """
    Fetches new articles from Gastroenterology via RSS feed.
    Compares with existing database entries and inserts only new links.
    """
    name = "Gastroenterology"
    base_url = "https://www.gastrojournal.org/current.rss"

    try:
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")  

        # Extract article links from the RSS feed
        urls = [element["rdf:resource"] for element in soup.find_all("rdf:li")]

        # Extract the latest year from article URLs
        latest_year = max(
            (int(re.search(r"\((\d+)\)", url).group(1)) if re.search(r"\((\d+)\)", url) else 0) for url in urls
        )

        # Filter only the latest year's articles
        links = [
            url for url in urls if re.search(r"\((\d+)\)", url) and int(re.search(r"\((\d+)\)", url).group(1)) == latest_year
        ]

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  

            # Filter out already stored links
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {len(Links)}")  

            # Validate data using Pydantic
            data = GastroenterologyData(Journal=name, Article=Links)

            # Insert into database and crawl article content
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_gas(specialization)  

        except ValidationError as e:
            print(f"Validation error: {e}")  
            print(f"Failed to insert {links} into the database.")  

    except Exception as e:
        print(f"Error fetching or processing links for {name}: {e}")  


async def fetch_heptology_articles(conn):
    """
    Fetches new articles from Journal of Hepatology via RSS feed.
    Compares with existing database entries and inserts only new links.
    """
    name = "Journal of Hepatology"
    base_url = "https://www.journal-of-hepatology.eu/current.rss"

    try:
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")  

        # Extract article links from the RSS feed
        urls = [element["rdf:resource"] for element in soup.find_all("rdf:li")]

        # Extract the latest year from article URLs
        latest_year = max(
            (int(re.search(r"\((\d+)\)", url).group(1)) if re.search(r"\((\d+)\)", url) else 0) for url in urls
        )

        # Filter only the latest year's articles
        links = [
            url for url in urls if re.search(r"\((\d+)\)", url) and int(re.search(r"\((\d+)\)", url).group(1)) == latest_year
        ]

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  

            # Filter out already stored links
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {len(Links)}")  

            # Validate data using Pydantic
            data = GastroenterologyData(Journal=name, Article=Links)

            # Insert into database and crawl article content
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_hepatology(specialization)  

        except ValidationError as e:
            print(f"Validation error: {e}")  
            print(f"Failed to insert {links} into the database.")  

    except Exception as e:
        print(f"Error fetching or processing links for {name}: {e}")  

async def fetch_new_ajg_articles(conn):
    """
    Fetches new articles from the American Journal of Gastroenterology via RSS feed.
    Compares with existing database entries and inserts only new links.
    """
    name = "American Journal of Gastroenterology"
    base_url = "https://journals.lww.com/ajg/_layouts/15/OAKS.Journals/feed.aspx?FeedType=CurrentIssue"

    try:
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")  

        # Extract article links from the RSS feed
        links = [item.find("link").text for item in soup.find_all("item") if item.find("link")]

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  

            # Filter out already stored links
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {len(Links)}")  

            # Validate data using Pydantic
            data = GastroenterologyData(Journal=name, Article=Links)

            # Insert into database and crawl article content
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_ajg(specialization)  

        except ValidationError as e:
            print(f"Validation error: {e}")  
            print(f"Failed to insert {links} into the database.")  

    except Exception as e:
        print(f"Error fetching or processing links for {name}: {e}")  


async def fetch_new_clinical_gas_hept_articles(conn):
    """
    Fetches new articles from Clinical Gastroenterology and Hepatology via RSS feed.
    Compares with existing database entries and inserts only new links.
    """
    name = "Clinical Gastroenterology and Hepatology"  # Journal name
    base_url = "https://www.gastrojournal.org/current.rss"  # RSS feed URL for the journal

    try:
        # Fetch the RSS feed content using ZenRows API
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")  # Parse the XML content

        # Extract article links from the RSS feed
        urls = [element["rdf:resource"] for element in soup.find_all("rdf:li")]

        # Extract the latest year from article URLs using regex
        latest_year = max(
            (
                int(re.search(r"\((\d+)\)", url).group(1))  # Extract year from URL
                if re.search(r"\((\d+)\)", url)
                else 0  # Default to 0 if year is not found
            )
            for url in urls
        )

        # Filter the links for the latest year's articles
        links = [
            url
            for url in urls
            if re.search(r"\((\d+)\)", url) and int(re.search(r"\((\d+)\)", url).group(1)) == latest_year
        ]

        try:
            cursor = conn.cursor()  # Create a database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing article links from the database

            # Filter out links that are already in the database
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {len(Links)}")  # Print the number of updated links

            # Validate data using Pydantic model
            data = GastroenterologyData(
                Journal=name,  # Journal name
                Article=Links,  # List of new articles
            )

            # Insert validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))

            # Crawl and process the article content
            await crawl_article_clincal(specialization)

        except ValidationError as e:
            # Handle validation errors
            print(f"Validation error: {e}")
            print(f"Failed to insert {links} into the database.")  # Debugging failure

    except Exception as e:
        # Handle general errors during fetching or processing
        print(f"Error fetching or processing links for {name}: {e}")
        return []  # Return an empty list in case of error

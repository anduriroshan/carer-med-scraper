from bs4 import BeautifulSoup  # Importing BeautifulSoup for parsing XML and HTML documents
from utils import (  # Importing utility functions for database operations and web scraping
    insert_into_database,
    fetch_page_with_zenrows,
    create_milvus_collection
)
from crawl_article.crawl_article_diabetes_sql import (  # Importing functions for processing crawled articles
    crawl_article_dia,
    crawl_article_diabetes,
    crawl_article_diabetes_care,
    crawl_article_endocrine,
    crawl_article_jcem
)
import re  # Importing regex for pattern matching
from pydantic import BaseModel, ValidationError  # Importing Pydantic for data validation

class DiabetesData(BaseModel):  
    """Pydantic model for validating diabetes & endocrinology article data"""
    Journal: str  # Journal name
    Article: list  # List of article URLs

specialization = "diabetes_endocrinology"  # Specialization for database categorization
create_milvus_collection(specialization)  # Creating a Milvus collection for embeddings

async def fetch_new_diabetes_articles(conn):
    """
    Fetches new articles from the Diabetes journal via RSS feed.
    Compares with existing database entries and inserts only new links.
    """
    name = "Diabetes"
    base_url = "https://diabetesjournals.org/rss/site_1000001/1000003.xml"

    try:
        response = await fetch_page_with_zenrows(base_url)  # Fetch the RSS feed
        soup = BeautifulSoup(response.html, "html.parser")  # Parse XML response

        # Extract article DOIs and construct full URLs
        links = [
            f"https://doi.org/{item.find('prism:doi').text}"
            for item in soup.find_all("item")
            if item.find("prism:doi")
        ]

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing article links

            # Filter out already stored links
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")  

            # Validate data using Pydantic
            data = DiabetesData(Journal=name, Article=Links)

            # Insert into database and crawl article content
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_diabetes(specialization)
            print(f"Successfully inserted {len(Links)} articles into the database.")

        except ValidationError as e:
            print(f"Validation error: {e}")  
            print(f"Failed to insert {links} into the database.")  

    except Exception as e:
        print(f"Error occurred: {e}")  

async def fetch_new_endocrine_review_articles(conn):
    """
    Fetches new articles from Endocrine Review via RSS feed.
    Compares with existing database entries and inserts only new links.
    """
    name = "Endocrine Review"
    base_url = "https://academic.oup.com/rss/site_5593/3466.xml"

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
            print(f"Updated links: {Links}")  

            # Validate data using Pydantic
            data = DiabetesData(Journal=name, Article=Links)

            # Insert into database and crawl article content
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_endocrine(specialization)
            print(f"Successfully inserted {len(Links)} articles into the database.")

        except ValidationError as e:
            print(f"Validation error: {e}")  
            print(f"Failed to insert {links} into the database.")  

    except Exception as e:
        print(f"Error occurred: {e}")  


async def fetch_new_diabetes_care_articles(conn):
    """
    Fetches new articles from Diabetes Care via RSS feed.
    Compares with existing database entries and inserts only new links.
    """
    name = "Diabetes Care"
    base_url = "https://diabetesjournals.org/rss/site_1000003/1000004.xml"

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
            print(f"Updated links: {Links}")  

            # Validate data using Pydantic
            data = DiabetesData(Journal=name, Article=Links)

            # Insert into database and crawl article content
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_diabetes_care(specialization)
            print(f"Successfully inserted {len(Links)} articles into the database.")

        except ValidationError as e:
            print(f"Validation error: {e}")  
            print(f"Failed to insert {links} into the database.")  

    except Exception as e:
        print(f"Error occurred: {e}")  


async def fetch_new_endocrinology_articles(conn):
    """
    Fetches new articles from Journal of Clinical Endocrinology & Metabolism via RSS feed.
    Compares with existing database entries and inserts only new links.
    """
    name = "Journal of Clinical Endocrinology & Metabolism"
    base_url = "https://academic.oup.com/rss/site_5591/3464.xml"

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
            print(f"Updated links: {Links}")  

            # Validate data using Pydantic
            data = DiabetesData(Journal=name, Article=Links)

            # Insert into database and crawl article content
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_jcem(specialization)
            print(f"Successfully inserted {len(Links)} articles into the database.")

        except ValidationError as e:
            print(f"Validation error: {e}")  
            print(f"Failed to insert {links} into the database.")  

    except Exception as e:
        print(f"Error occurred: {e}")  

async def fetch_new_diabetologia_articles(conn):
    """
    Fetches new articles from Diabetologia by scraping its website.
    Compares with existing database entries and inserts only new links.
    """
    name = "Diabetologia"
    base_url = "https://link.springer.com/journal/125/volumes-and-issues"
    req_str = "https://link.springer.com/article"

    try:
        from datetime import datetime  # Import datetime for volume and issue calculation

        links = []  # List to store extracted article links
        current_month = datetime.now().month  # Get the current month
        volume = int(datetime.now().year) - 1957  # Compute the volume number

        # Construct issue URL based on current volume and month
        url = f"{base_url}/{volume}-{current_month}"
        print(f"Fetching: {url}")  # Debugging

        response = await fetch_page_with_zenrows(url)  # Fetch issue page
        soup = BeautifulSoup(response.html, "html.parser")  # Parse HTML content

        # Select article elements from the webpage
        elements = soup.select(
            "body > div > div > main > div > div > div > section > ol > li > article > div > h3"
        )

        if elements:
            for element in elements:
                link = element.find("a")  # Extract anchor tag
                updated_next_url = link.get("href")  # Get article URL
                if req_str in updated_next_url:  # Validate URL pattern
                    links.append(updated_next_url)  # Store article link
                    print(updated_next_url)  # Debugging

        print(f"Total links found for {name}: {len(links)}")  # Print total number of links found

        # Insert into database
        insert_into_database(conn, name, links, specialization, len(links))
        await crawl_article_dia(specialization)  # Crawl and process article content

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle fetch errors

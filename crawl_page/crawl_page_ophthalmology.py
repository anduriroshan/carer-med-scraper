from utils import (
    insert_into_database,  # Function to insert data into the database
    fetch_page_with_scraper_api,  # Function to fetch pages using Scraper API (not used in this code)
    fetch_page_with_zenrows,  # Function to fetch pages using Zenrows
    create_milvus_collection  # Function to create a Milvus collection (not used in this code)
)
from crawl_article.crawl_article_ophthalmology_sql import (
    crawl_article_bmj,  # Function to crawl articles from BMJ (British Medical Journal) (not used in this code)
    crawl_article_jama  # Function to crawl articles from JAMA (not used in this code)
)
from bs4 import BeautifulSoup  # Library for parsing HTML and XML
import re  # Regular expressions (not used in this code)
from pydantic import BaseModel, ValidationError  # Pydantic for data validation
from datetime import datetime  # For working with dates and times
from urllib.parse import urljoin, urlparse  # For URL manipulation

# Define Pydantic model for Ophthalmology data
class OphthalmologyData(BaseModel):
    Journal: str  # Journal name
    Article: list  # List of article links

specialization = "ophthalmology"  # Define the specialization as 'ophthalmology'

# Function to crawl articles from JAMA Ophthalmology using RSS feed
async def crawl_page_jama_ophtha(conn):
    base_url ="https://jamanetwork.com/rss/site_17/73.xml"  # RSS feed URL for JAMA Ophthalmology
    name = "JAMA Ophthalmology"  # Journal name
    links = []  # List to store article links

    try:
        # Fetch the RSS feed using Zenrows
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")  # Parse the RSS XML content

        # Extract and process each item in the RSS feed to get the article links
        links = [
            item.find("link").text
            for item in soup.find_all("item")  # Iterate through each RSS feed item
            if item.find("link")  # Ensure the 'link' tag is present
        ]

        # Database operations to insert the new article links
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing links from the database

            # Filter out the links that already exist in the database
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate the data using Pydantic model
            data = OphthalmologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_jama(specialization)  # Crawl related articles for further processing
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle errors related to the fetching and parsing process

# Function to crawl articles from British Journal of Ophthalmology (BJ Ophthalmology) using a URL pattern
async def crawl_page_bjo_bmj(conn):
    base_url = 'https://bjo.bmj.com/content'  # Base URL for BJ Ophthalmology
    name = "British Journal of Ophthalmology"  # Journal name
    links = []  # List to store article links
    current_year = datetime.now().year  # Get current year
    current_month = datetime.now().month  # Get current month

    try:
        volume = int(current_year) - 1916  # Calculate volume based on the year
        url = f"{base_url}/{volume}/{current_month}"  # Generate the URL based on volume and month
        print(f"Fetching: {url}")
        response = await fetch_page_with_zenrows(url)  # Fetch the page using Zenrows  # Ensure the request was successful
        soup = BeautifulSoup(response.html, "html.parser")  # Parse the HTML content

        # Extract articles from the page using CSS selectors
        elements = soup.select(
            "body > div > section > div > div > div > div > div > div > div > div > div > div > div > div > div > div > div > div > div > ul > li > div > div > div"
        )
        for element in elements:
            link = element.find("a")  # Find the <a> tag with the article link
            if link and link.get("href"):  # Ensure the <a> tag has an href attribute
                links.append(base_url[:20] + link.get("href").strip())  # Construct the full article link

        # Database operations to insert the new article links
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing links from the database

            # Filter out the links that already exist in the database
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate the data using Pydantic model
            data = OphthalmologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_bmj(specialization)  # Crawl related articles for further processing
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle errors related to the fetching and parsing process

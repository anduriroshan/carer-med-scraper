from utils import (
    insert_into_database,
    fetch_page_with_scraper_api,
    fetch_page_with_zenrows,
    create_milvus_collection
)
from crawl_article.crawl_article_radiology_sql import (
    crawl_article_radiology,
    crawl_article_european_radiology,
    crawl_article_arj,
    crawl_article_magnetic_resonance,
    crawl_article_investigative_radiology

)
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from datetime import datetime
from urllib.parse import urljoin,urlparse
import requests
class RadiologyData(BaseModel):
    Journal: str
    Article: list

specialization = "radiology"

async def crawl_page_radiology(conn):
    name = "Radiology"
    base_url = "https://pubs.rsna.org/"
    main_url = "https://pubs.rsna.org/toc/radiology"

    article_links = []

    url = 'https://pubs.rsna.org/toc/radiology/current'
    print(f"Fetching articles from: {url}")
    response = await fetch_page_with_zenrows(url)

    
    soup = BeautifulSoup(response.html, "html.parser")
    issue_items = soup.find_all("div", class_="issue-item")
    
    # Extract links from all issue items
    for item in issue_items:
        h5_tag = item.find("h5", class_="issue-item__title")
        if h5_tag:
            link_tag = h5_tag.find("a", href=True)
            if link_tag:
                full_link = urljoin(base_url, link_tag["href"])
                article_links.append(full_link)
                print(full_link)

    # Insert links into the database
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")

        data = RadiologyData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_radiology(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_european_radiology(conn):
    name='European Radiology'
    base_url='https://link.springer.com/journal/330/volumes-and-issues'
    req_str="https://link.springer.com/article"

    links = []
    volume = int(datetime.now().year) - 1990
    for issue in range(1,13):
        url = base_url + f"/{volume}" + f"-{issue}"
        print(f"Fetching {url}")
        response = await fetch_page_with_zenrows(url)
        soup = BeautifulSoup(response.html, "html.parser")
        elements = soup.select(
            "body > div > div > main > div > div > div > section > ol > li > article > div > h3"
        )  # Get all matching elements
        if elements:
            for element in elements:
                link = element.find("a")
                updated_next_url = link.get("href")
                if req_str in updated_next_url:
                    links.append(updated_next_url)
                    print(updated_next_url)

        # Print the total number of links found
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}

            # Update the database with new links by comparing with existing data
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate the data using Pydantic model
            data = RadiologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_european_radiology(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

async def crawl_page_arj(conn):
    name='American Journal of Roentgenology'
    base_url = "https://www.ajronline.org"
    article_links = []

    toc_url = 'https://www.ajronline.org/toc/ajr/current'
    print(f"Fetching: {toc_url}")

    try:
        response = await fetch_page_with_zenrows(toc_url)
        soup = BeautifulSoup(response.html, "html.parser")

        # Find all article titles
        article_titles = soup.find_all("h3", class_="issue-item__title")

        for title in article_titles:
            a_tag = title.find("a", href=True)
            if a_tag:
                article_url = base_url + a_tag["href"]
                article_links.append(article_url)
                print(f"Found:{article_url}")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching {toc_url}: {e}")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")

        data = RadiologyData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_arj(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}") 


async def crawl_page_magnetic_resonance(conn):
    name = 'Journal of Magnetic Resonance Imaging'
    base_url ="https://onlinelibrary.wiley.com/feed/15222586/most-recent"
    links = []

    try:
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")
        # Extract and process each item in the RSS feed
        links = [
            item.find("link").text
            for item in soup.find_all("item")
            if item.find("link")
        ]
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}

            # Update the database with new links by comparing with existing data
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate the data using Pydantic model
            data = RadiologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_magnetic_resonance(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")


async def crawl_page_investigative_radiology(conn):
    name = 'Investigative Radiology'
    base_url ='https://journals.lww.com/investigativeradiology/_layouts/15/OAKS.Journals/feed.aspx?FeedType=CurrentIssue'
    try:
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")
        # Extract and process each item in the RSS feed
        links = [
            item.find("link").text
            for item in soup.find_all("item")
            if item.find("link")
        ]
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}

            # Update the database with new links by comparing with existing data
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate the data using Pydantic model
            data = RadiologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_investigative_radiology(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")
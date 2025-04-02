from bs4 import BeautifulSoup
from utils import (
    insert_into_database,
    fetch_page_with_zenrows,
    create_milvus_collection
)
from crawl_article.crawl_article_obstetrics_gyno_sql import (
    crawl_article_ajog,
    crawl_article_bjog,
    crawl_article_obs_gyn,
    crawl_article_fertstert

)
from datetime import datetime
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from urllib.parse import urljoin
import requests
from urllib.parse import urljoin, urlparse

class GynecologyData(BaseModel):
    Journal: str
    Article: list

specialization = "obstetrics_gynecology"

async def crawl_page_ajog(conn):
    name = "American Journal of Obstetrics & Gynecology"
    base_url = "https://www.ajog.org"
    issue_link = 'https://www.ajog.org/current'
    article_links = set()
    processed_article_ids = set()

    try:
        print(f"Fetching: {issue_link}")
        response = await fetch_page_with_zenrows(issue_link)
        page_soup = BeautifulSoup(response.html, "html.parser")

        # Iterate over sections with the required class
        sections = page_soup.find_all("section", {"class": "toc__section"})
        for section in sections:
            # Check if the section contains "Original Articles"
            h2_tag = section.find("h2", class_="toc__heading__header top")
            if h2_tag and "original research" in h2_tag.get_text(strip=True).lower():
                for li in section.find_all("li"):
                    article_link = li.find("a", href=True)
                    if article_link:
                        href = article_link["href"]

                        # Parse the URL and extract the unique identifier
                        parsed_url = urlparse(href)
                        path_parts = parsed_url.path.split("/")
                        if "article" in path_parts:
                            identifier_index = path_parts.index("article") + 1
                            if identifier_index < len(path_parts):
                                current_article_id = path_parts[identifier_index]

                                # Check if the identifier is already processed
                                if current_article_id in processed_article_ids:
                                    continue
                                processed_article_ids.add(current_article_id)

                                # Construct full URL and add to final article links
                                full_article_link = urljoin(base_url, href)
                                article_links.add(full_article_link)
                                print(f"Added: {full_article_link}")
    except Exception as e:
        print(f"Error fetching article links from {issue_link}: {e}")

    # Convert article links to a list
    article_links_list = list(article_links)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        Links = [link for link in article_links_list if link not in existing_data]
        print(f"Updated links: {Links}")

        data = GynecologyData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_ajog(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_bjog(conn):
    name = 'BJOG: An International Journal of Obstetrics & Gynaecology'
    base_url = "https://obgyn.onlinelibrary.wiley.com/action/showFeed?jc=18793479&type=etoc&feed=rss"
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
            data = GynecologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_bjog(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_obs_gyn(conn):
    name ='Obstetrics & Gynecology'
    base_url = "https://journals.lww.com/greenjournal/_layouts/15/OAKS.Journals/feed.aspx?FeedType=CurrentIssue"
    try:
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")
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
            print(f"Updated links: {len(Links)}")
            # Validate the data using Pydantic model
            data = GynecologyData(
                Journal=name,
                Article=Links,
            )
            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            # Crawling the article content and inserting into the milvus database
            await crawl_article_obs_gyn(specialization)

        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {links} into the database and CSV file.")
            return
    except Exception as e:
        print(f"Error fetching or processing links for {name}: {e}")
        return []  # Return an empty list in case of error

async def crawl_page_fertstert(conn):
    name = "Fertility and Sterility"
    base_url = "https://www.fertstert.org"
    issue_link = 'https://www.fertstert.org/current'
    article_links = set()
    processed_article_ids = set()

    try:
        print(f"Fetching: {issue_link}")
        response = await fetch_page_with_zenrows(issue_link)
        page_soup = BeautifulSoup(response.html, "html.parser")

        # Iterate over sections with the required class
        sections = page_soup.find_all("section", {"class": "toc__section"})
        for section in sections:
            # Check if the section contains "Original Articles"
            h2_tag = section.find("h2", class_="toc__heading__header top")
            if h2_tag and "original articles" in h2_tag.get_text(strip=True).lower():
                for li in section.find_all("li"):
                    article_link = li.find("a", href=True)
                    if article_link:
                        href = article_link["href"]

                        # Parse the URL and extract the unique identifier
                        parsed_url = urlparse(href)
                        path_parts = parsed_url.path.split("/")
                        if "article" in path_parts:
                            identifier_index = path_parts.index("article") + 1
                            if identifier_index < len(path_parts):
                                current_article_id = path_parts[identifier_index]

                                # Check if the identifier is already processed
                                if current_article_id in processed_article_ids:
                                    continue
                                processed_article_ids.add(current_article_id)

                                # Construct full URL and add to final article links
                                full_article_link = urljoin(base_url, href)
                                article_links.add(full_article_link)
                                print(f"Added: {full_article_link}")
    except Exception as e:
        print(f"Error fetching article links from {issue_link}: {e}")

    # Convert article links to a list
    article_links_list = list(article_links)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        Links = [link for link in article_links_list if link not in existing_data]
        print(f"Updated links: {Links}")

        data = GynecologyData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_fertstert(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")
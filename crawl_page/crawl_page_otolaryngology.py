from utils import (
    insert_into_database,
    fetch_page_with_scraper_api,
    fetch_page_with_zenrows,
    create_milvus_collection
)
from crawl_article.crawl_article_otolaryngology_sql import (
    crawl_article_jama_ent,
    crawl_article_laryngoscope,
    crawl_article_sage,crawl_article_ear_hearings

)
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from datetime import datetime
from urllib.parse import urljoin,urlparse

class OtolaryngologyData(BaseModel):
    Journal: str
    Article: list

specialization = "otolaryngology"

async def crawl_page_jama_ent(conn):
    base_url ="https://jamanetwork.com/rss/site_18/74.xml"
    name = "JAMA Otolaryngology–Head & Neck Surgery"
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
            data = OtolaryngologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_jama_ent(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_laryngoscope(conn):
    name = 'The Laryngoscope'
    base_url ="https://onlinelibrary.wiley.com/action/showFeed?jc=15314995&type=etoc&feed=rss"
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
            data = OtolaryngologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_laryngoscope(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")


async def crawl_page_sage(conn):
    name =  "Annals of Otology, Rhinology & Laryngology"
    base_url ="https://journals.sagepub.com/action/showFeed?ui=0&mi=ehikzz&ai=2b4&jc=aora&type=etoc&feed=rss"
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
            data = OtolaryngologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_sage(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")


async def crawl_page_ear_hearing(conn):
    name = 'Ear and Hearing'
    base_url ="https://journals.lww.com/ear-hearing/_layouts/15/OAKS.Journals/feed.aspx?FeedType=CurrentIssue"
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
            data = OtolaryngologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_ear_hearings(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")
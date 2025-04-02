from utils import (
    insert_into_database,
    fetch_page_with_scraper_api,
    fetch_page_with_zenrows,
    create_milvus_collection,
    connection_config
)
from crawl_article.crawl_article_psychiatry_sql import (
    crawl_article_ajp,
    crawl_article_jama_psy,
    crawl_article_molecular_psychiatry,
    crawl_article_schizophrenia,
)
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from datetime import datetime
from urllib.parse import urljoin,urlparse

class PsychiatryData(BaseModel):
    Journal: str
    Article: list

specialization = "psychiatry"

async def crawl_page_ajp(conn):
    name = 'American Journal of Psychiatry'
    base_url = 'https://psychiatryonline.org'
    article_links = []

   
    issue_url = 'https://psychiatryonline.org/toc/ajp/current'
    print(f"Fetching: {issue_url}")

    try:
        response = await fetch_page_with_zenrows(issue_url)
        soup = BeautifulSoup(response.html, "html.parser")
        
        # Find all h3 elements with the given class
        h3_tags = soup.find_all("h3", class_="card-title article-title text-deep-gray my-8")

        for h3 in h3_tags:
            link = h3.find("a", href=True)
            if link:
                full_url = base_url + link["href"]
                article_links.append(full_url)
                print(f"Added: {full_url}")

    except Exception as e:
        print(f"Error fetching {issue_url}: {e}")

    # Insert into database
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Filter out already existing links
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")

        data = PsychiatryData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_ajp(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")


async def crawl_page_jama_psy(conn):
    base_url ="https://jamanetwork.com/rss/site_14/70.xml"
    name = "JAMA Psychiatry"
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
            data = PsychiatryData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_jama_psy(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_molecular_psychiatry(conn):
    name = 'Molecular Psychiatry'
    base_url = "https://www.nature.com/mp.rss"
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
            data = PsychiatryData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_molecular_psychiatry(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_schizophrenia(conn):
    name = 'Schizophrenia Bulletin'
    base_url ="https://academic.oup.com/rss/site_5240/3108.xml"
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
            data = PsychiatryData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_schizophrenia(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")
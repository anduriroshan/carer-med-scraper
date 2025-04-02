from bs4 import BeautifulSoup
from utils import (
    insert_into_database,
    fetch_page_with_zenrows,
    create_milvus_collection,
    setup_database
)
from crawl_article.crawl_article_orthopedics_sql import (
    crawl_article_jbjs,
    crawl_article_bone,
    crawl_article_clinical_ortho,
    crawl_article_jorunal_orthopaedic,
    crawl_article_jortho
)
from datetime import datetime
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from urllib.parse import urljoin
import requests
from urllib.parse import urljoin, urlparse

class OrthopedicsData(BaseModel):
    Journal: str
    Article: list

specialization = "orthopaedics"

async def crawl_page_jbjs(conn):
    name = 'The Journal of Bone and Joint Surgery'
    base_url = "https://journals.lww.com/jbjsjournal/pages/issuelist.aspx?year="
    req_str = "journals.lww.com"
    unique_links = set()
    start_year = 2025
    end_year = 2021
    issue_links = []
    issue_url='https://journals.lww.com/jbjsjournal/pages/currenttoc.aspx'
    print(f"Fetching Issue Page: {issue_url}")
    try:
        response = await fetch_page_with_zenrows(issue_url)
        soup = BeautifulSoup(response.html, "html.parser")
        sections = soup.find_all("section", id="wp-articles-navigator", class_="content-box")
        
        for section in sections:
            header = section.find("header")
            if header:
                h3_tag = header.find("h3")
                if h3_tag and "Scientific Articles" in h3_tag.get_text(strip=True):
                    h4_tags = section.find_all("h4")
                    for h4_tag in h4_tags:
                        link = h4_tag.find("a", href=True)
                        if link:
                            href = urljoin(issue_url, link["href"])
                            if href not in unique_links:
                                unique_links.add(href)
                                print(f"Found Article: {href}")
            else:
                continue
    except Exception as e:
        print(f"Error occurred while processing {issue_url}: {e}")
    
    # Step 3: Store extracted article links into the database
    article_links = list(unique_links)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}
        
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")
        
        data = OrthopedicsData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_jbjs(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_clinical_ortho(conn):
    name = 'Clinical Orthopaedics and Related Research'
    unique_links = set()
    issue_url='https://journals.lww.com/clinorthop/Pages/currenttoc.aspx'
    print(f"Fetching Issue Page: {issue_url}")
    try:
        response = await fetch_page_with_zenrows(issue_url)
        soup = BeautifulSoup(response.html, "html.parser")
        sections = soup.find_all("section", id="wp-articles-navigator", class_="content-box")
        
        for section in sections:
            header = section.find("header")
            if header:
                h3_tag = header.find("h3")
                if h3_tag and any(keyword in h3_tag.get_text(strip=True) for keyword in ["FEATURED ARTICLES", "CLINICAL RESEARCH", "BASIC RESEARCH"]):
                    h4_tags = section.find_all("h4")
                    for h4_tag in h4_tags:
                        link = h4_tag.find("a", href=True)
                        if link:
                            href = urljoin(issue_url, link["href"])
                            if href not in unique_links:
                                unique_links.add(href)
                                print(f"Found Article: {href}")
            else:
                continue
    except Exception as e:
        print(f"Error occurred while processing {issue_url}: {e}")
    
    # Step 3: Store extracted article links into the database
    article_links = list(unique_links)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}
        
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")
        
        data = OrthopedicsData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_clinical_ortho(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_bone(conn):
    base_url = "https://boneandjoint.org.uk/journal/BJJ/toc"
    name = "The Bone & Joint Journal"
    article_links = []

    url = 'https://boneandjoint.org.uk/journal/BJJ/current-issue'
    print(f"Fetching: {url}")
    response = await fetch_page_with_zenrows(url)
    soup = BeautifulSoup(response.html, "html.parser")
    row_divs = soup.find_all("div", class_="row")
    for row_div in row_divs:
        span_tag = row_div.find("span")
        if span_tag:
            link_tag = span_tag.find("a", href=True)
            if link_tag:
                full_url = urljoin(base_url, link_tag["href"])
                article_links.append(full_url)
                print(f"Found article: {full_url}")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")
        data = OrthopedicsData(Journal=name, Article=Links)
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_bone(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_jorunal_orthopaedic(conn):
    name = 'Journal of Orthopaedic Research'
    base_url = "https://onlinelibrary.wiley.com/feed/1554527x/most-recent"
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
            data = OrthopedicsData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_jorunal_orthopaedic(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")


async def crawl_page_jortho(conn):
    name = 'Journal of Orthopaedic Trauma'
    base_url = "https://journals.lww.com/jorthotrauma/_layouts/15/OAKS.Journals/feed.aspx?FeedType=CurrentIssue"
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
            data = OrthopedicsData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_jortho(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

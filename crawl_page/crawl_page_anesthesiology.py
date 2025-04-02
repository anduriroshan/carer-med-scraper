from utils import (
    insert_into_database,
    fetch_page_with_zenrows,
)
from crawl_article.crawl_article_anesthesiology_sql import (
    crawl_article_anesthesiology,
    crawl_article_bja,
    crawl_article_analgesia,
    crawl_article_clinical_anesthesia,
    crawl_article_rapm
)
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from datetime import datetime
from urllib.parse import urljoin,urlparse
import requests

class AnesthesiaData(BaseModel):
    Journal: str
    Article: list

specialization = "anesthesiology"

async def crawl_page_anesthesiology(conn):
    name = 'Anesthesiology'
    base_url = "https://journals.lww.com/anesthesiology/_layouts/15/OAKS.Journals/feed.aspx?FeedType=CurrentIssue"
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
            data = AnesthesiaData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            crawl_article_anesthesiology(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_bja(conn):
    name = "British Journal of Anaesthesia"
    base_url = "https://www.bjanaesthesia.org"
    main_url = f"{base_url}/issues"
    
    article_links = set()
    processed_article_ids = set()
    issue_link = 'https://www.bjanaesthesia.org/current'
    try:
        print(f"Fetching: {issue_link}")
        response = await fetch_page_with_zenrows(issue_link)
        page_soup = BeautifulSoup(response.html, "html.parser")
        # Iterate over sections with the required class
        sections = page_soup.find_all("section", {"class": "toc__section"})
        for section in sections:
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

        data = AnesthesiaData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_bja(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")


async def crawl_page_analgesia(conn):
    name = 'Anesthesia & Analgesia'
    base_url = "https://journals.lww.com/anesthesia-analgesia/_layouts/15/OAKS.Journals/feed.aspx?FeedType=CurrentIssue"
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
            data = AnesthesiaData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_analgesia(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_clinical_anesthesia(conn):
    name = "Journal of Clinical Anesthesia"
    base_url = "https://www.sciencedirect.com"
    main_url = "https://www.sciencedirect.com/journal/journal-of-clinical-anesthesia/vol"

    article_links = []

    url = "https://www.sciencedirect.com/journal/journal-of-clinical-anesthesia/latest/"
    print(f"Fetching articles from: {url}")
    response = await fetch_page_with_zenrows(url)
    soup = BeautifulSoup(response.html, "html.parser")
    section_items = soup.find_all("li", class_="js-section js-section-level-0 article-item toc-anchor")
    
    for section in section_items:
        h2_tag = section.find("h2", class_="section-title js-section-title-level-0 u-text-light u-margin-s-top u-margin-s-bottom u-text-italic text-m js-section-title")
        if h2_tag and h2_tag.get_text(strip=True) in ["Original Contributions", "Review"]:
            article_items = section.find_all("li", class_="js-article-list-item article-item u-padding-xs-top u-margin-l-bottom")
            for article in article_items:
                h3_tag = article.find("h3", class_="text-m u-font-serif u-display-inline")
                if h3_tag:
                    link_tag = h3_tag.find("a", href=True)
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
        
        data = AnesthesiaData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_clinical_anesthesia(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")
        
async def crawl_page_rapm(conn):
    base_url='https://rapm.bmj.com'
    main_url='https://rapm.bmj.com/content'
    name = "Regional Anesthesia & Pain Medicine"
    links = []
    current_year = datetime.now().year
    current_month = datetime.now().month

    try:
        volume = int(current_year) - 1975
        url = f"{main_url}/{volume}/{current_month}"
        print(f"Fetching: {url}")
        response = await fetch_page_with_zenrows(url)
        
        soup = BeautifulSoup(response.html, "html.parser")

        # Extract articles
        elements = soup.select(
            "body > div > section > div > div > div > div > div > div > div > div > div > div > div > div > div > div > div > div > div > ul > li > div > div > div"
        )
        for element in elements:
            link = element.find("a")
            if link and link.get("href"):
                links.append(base_url+ link.get("href").strip())
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}

            # Update the database with new links by comparing with existing data
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate the data using Pydantic model
            data =AnesthesiaData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_rapm(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")
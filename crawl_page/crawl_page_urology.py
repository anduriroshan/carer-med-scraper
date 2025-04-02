from utils import (
    insert_into_database,
    fetch_page_with_scraper_api,
    fetch_page_with_zenrows,
    create_milvus_collection
)
from crawl_article.crawl_article_urology_sql import (
    crawl_article_european_urology,
    crawl_article_aua,
    crawl_article_world_urology,
    crawl_article_urology,
    crawl_article_bjui
)
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from datetime import datetime
from urllib.parse import urljoin,urlparse

class UrologyData(BaseModel):
    Journal: str
    Article: list

specialization = "urology"

async def crawl_page_european_urology(conn):
    name = "European Urology"
    base_url = "https://www.europeanurology.com"
    issue_link = 'https://www.europeanurology.com/current'
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
            keywords = ["platinum priority papers", 'full length articles']
            if h2_tag and any(keyword in h2_tag.get_text(strip=True).lower() for keyword in keywords):
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

        data = UrologyData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_european_urology(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")


async def crawl_page_aua(conn):
    base_url = "https://www.auajournals.org/toc/juro"
    name = "The Journal of Urology"
    article_links = []
    url = 'https://www.auajournals.org/toc/juro/current'
    print(f"Fetching: {url}")
    response = await fetch_page_with_zenrows(url)

    soup = BeautifulSoup(response.html, "html.parser")
    issue_items = soup.find_all("div", class_="issue-item__title")
    for item in issue_items:
        link_tag = item.find("a", href=True)
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
        data = UrologyData(Journal=name, Article=Links)
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_aua(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_urology(conn):
    name = "Urology"
    base_url = "https://www.goldjournal.net"
    issue_link = 'https://www.goldjournal.net/current'
    article_links = set()
    processed_article_ids = set()

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

        data = UrologyData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_urology(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_world_urology(conn):
    name='World Journal of Urology'
    base_url='https://link.springer.com/journal/345/volumes-and-issues'
    req_str="https://link.springer.com/article"

    links = []
    volume = int(datetime.now().year) - 1982
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
            data = UrologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_world_urology(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")


async def crawl_page_bjui(conn):
    name = 'BJU International'
    base_url = "https://bjui-journals.onlinelibrary.wiley.com/feed/1464410x/most-recent"
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
            data = UrologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_bjui(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

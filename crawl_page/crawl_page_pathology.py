from utils import (
    insert_into_database,
    fetch_page_with_scraper_api,
    fetch_page_with_zenrows,
    create_milvus_collection
)
from crawl_article.crawl_article_pathology_sql import (
    crawl_article_ajp,
    crawl_article_journal_pathology,
    crawl_article_springer,
    crawl_article_histopathology
)
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from datetime import datetime
from urllib.parse import urljoin,urlparse
import requests

class PathologyData(BaseModel):
    Journal: str
    Article: list

specialization = "pathology"

async def crawl_page_ajp(conn):
    name = "The American Journal of Pathology"
    base_url = "https://ajp.amjpathol.org"
    main_url = f"{base_url}/issues"
    
    article_links = set()
    processed_article_ids = set()
    issue_link = 'https://ajp.amjpathol.org/current'
    try:
        print(f"Fetching: {issue_link}")
        response = await fetch_page_with_zenrows(issue_link)
        page_soup = BeautifulSoup(response.html, "html.parser")

        # Iterate over sections with the required class
        sections = page_soup.find_all("section", {"class": "toc__section"})
        for section in sections:
                h2_tag = section.find("h2")
                if h2_tag and h2_tag.get_text(strip=True) == "Regular Articles":
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

        data = PathologyData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_ajp(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_journal_pathology(conn):
    name = 'The Journal of Pathology'
    base = 'https://pathsocjournals.onlinelibrary.wiley.com'
    base_url = 'https://pathsocjournals.onlinelibrary.wiley.com/toc/10969896'
    article_links = []


    main_url ='https://pathsocjournals.onlinelibrary.wiley.com/toc/10969896/current'
    print(f"Fetching: {main_url}")

    response = await fetch_page_with_zenrows(main_url)


    soup = BeautifulSoup(response.html, "html.parser")
    
    # Find sections that contain articles
    sections = soup.find_all("div", class_="issue-items-container bulkDownloadWrapper")

    for section in sections:
        h3_tag = section.find("h3")
        if h3_tag and h3_tag.get_text(strip=True) in ["Original Articles", "Original Papers"]:

            # Find all articles in this section
            articles = section.find_all("div", class_="issue-item")
            
            for article in articles:
                link = article.find("a", href=True)
                if link:
                    full_url = base + link["href"]
                    article_links.append(full_url)
                    print(f"Added: {full_url}")
    # Insert into database
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Filter out already existing links
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")

        data = PathologyData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_journal_pathology(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_springer(conn):
    name='Virchows Archiv'
    base_url='https://link.springer.com/journal/428/volumes-and-issues'
    req_str="https://link.springer.com/article"
    current_month = datetime.now().month
    current_year = datetime.now().year
    links = []
    base_num = 2*(int(current_year)) - 3564
    volume = base_num if current_month < 7 else base_num + 1
    issue = current_month if current_month < 7 else current_month - 6

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
        data = PathologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database and write to CSV file
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_springer(specialization)
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database and CSV file.")

async def crawl_page_histopathology(conn):
    name = 'Histopathology'
    base = 'https://onlinelibrary.wiley.com'
    base_url = 'https://onlinelibrary.wiley.com/toc/13652559'
    article_links = []


    main_url =base_url+'/current'
    print(f"Fetching: {main_url}")

    response = await fetch_page_with_zenrows(main_url)


    soup = BeautifulSoup(response.html, "html.parser")
    article_list = soup.find_all("div", class_="issue-items-container bulkDownloadWrapper")

    for article in article_list:
        div = article.find("div", class_="issue-item")
        link = div.find("a")
        if link and link.get("href"):
            full_url = base + link["href"]
            article_links.append(full_url)
            print(full_url)
    # Insert into database
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Filter out already existing links
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")

        data = PathologyData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_histopathology(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")
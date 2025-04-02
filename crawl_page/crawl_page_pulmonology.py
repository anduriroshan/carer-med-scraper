from utils import (
    insert_into_database,
    fetch_page_with_scraper_api,
    fetch_page_with_zenrows,
    create_milvus_collection,
    connection_config
)
from crawl_article.crawl_article_pulmonology_sql import (
    crawl_article_ajrccm,
    crawl_article_erj,
    crawl_article_chest,
    crawl_article_thorax,
    crawl_article_respiratory,
)
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from datetime import datetime
from urllib.parse import urljoin,urlparse

class PulmonologyData(BaseModel):
    Journal: str
    Article: list

specialization = "pulmonology"

async def crawl_page_ajrccm(conn):
    base_url ="https://www.atsjournals.org/action/showFeed?type=etoc&feed=rss&jc=ajrccm"
    name = "American Journal of Respiratory and Critical Care Medicine"
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
            data = PulmonologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_ajrccm(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_erj(conn):
    name = "European Respiratory Journal"
    base_url = "https://publications.ersnet.org/content/erj"
    current_year = datetime.now().year
    current_month = datetime.now().month
    unique_links=set()
    # Calculate the base number based on the year
    base_num = 2 * (current_year) - 3985
    vol_num = base_num if current_month <= 6 else base_num + 1
    issue_num = (current_month - 1) % 6 + 1
    issue_url = f"{base_url}/{vol_num}/{issue_num}"
    print(f"Fetching: {issue_url}")
    try:
        response = await fetch_page_with_zenrows(issue_url)
        soup = BeautifulSoup(response.html, "html.parser")
        
        article_links = set()
        
        # Locate the LI containing "Original Research Articles"
        original_research_li = soup.find(
            "h3", class_="item-list__title",
            string=lambda text: text and text.lower() in ["original research articles", "original articles"]
        )
        if original_research_li:
            ul_tag = original_research_li.find_next_sibling("ul", class_="item-list__toc")
            if ul_tag:
                li_tags = ul_tag.find_all("li")
                for li in li_tags:
                    h3_tag = li.find("h3")
                    if h3_tag:
                        link = h3_tag.find("a", href=True)
                        if link:
                            href = urljoin(base_url, link["href"])
                            article_links.add(href)
        
        for href in article_links:
            if href not in unique_links:
                print(f"Found article: {href}")
                unique_links.add(href)
    except Exception as e:
        print(f"Error fetching data for ERJ: {e}")
    
    links = list(unique_links)
    # Insert into database
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Update the database with new links by comparing with existing data
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = PulmonologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database and write to CSV file
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_erj(specialization)
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")


async def crawl_page_chest(conn):
    name = "CHEST"
    base_url = "https://journal.chestnet.org"
    main_url = f"{base_url}/issues"
    unique_links=set()
    current_year = datetime.now().year
    current_month = datetime.now().month
    base_num = 2 * current_year - 3883
    volume = base_num if current_month <= 6 else base_num + 1
    group_id = f"d2020.v{volume}" 
    issue_url = f"{main_url}?publicationCode=chest&issueGroupId={group_id}"
    print(f"Fetching : {issue_url}")
    try:
        response = await fetch_page_with_zenrows(issue_url)
        soup = BeautifulSoup(response.html, "html.parser")

        div = soup.find("div", {
            "data-groupid": group_id,
            "class": "list-of-issues__group list-of-issues__group--issues js--open"
        })

        if div:
            links = div.find_all("a", href=True)
            for link in links:
                href = urljoin(base_url, link["href"])
                if href not in unique_links:  # Add only unique links
                    unique_links.add(href)

    except Exception as e:
        print(f"Error fetching data for group ID {group_id}: {e}")
    # Nested scraping for article links inside each unique issue page
    article_links = set()
    processed_article_ids = set()

    for issue_link in unique_links:
        try:
            print(f"Fetching: {issue_link}")
            response = await fetch_page_with_zenrows(issue_link)
            page_soup = BeautifulSoup(response.html, "html.parser")

            # Iterate over sections with the required class
            sections = page_soup.find_all("ul", {"class": "toc__body rlist clearfix"})
            for section in sections:
                # Check if the section contains "Original Articles"
                h3_tag = section.find("h3", class_="heading1")
                if h3_tag and "original research" in h3_tag.get_text(strip=True).lower():
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
    article_links = list(article_links)
    # Insert into database
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Update the database with new links by comparing with existing data
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = PulmonologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database and write to CSV file
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_chest(specialization)
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_thorax(conn):
    base_url='https://thorax.bmj.com'
    main_url='https://thorax.bmj.com/content'
    name = "Thorax"
    links = []
    current_year = datetime.now().year
    current_month = datetime.now().month

    try:
        volume = int(current_year) - 1945
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
            data = PulmonologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_thorax(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_respiratory(conn):
    connection = connection_config()
    cursor = connection.cursor()
    name = "Respiratory Medicine"
    base_url = "https://www.resmedjournal.com"

    article_links = set()
    processed_article_ids = set()
    issue_link = 'https://www.resmedjournal.com/current'
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
    article_links = list(article_links)
    # Insert into database
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Update the database with new links by comparing with existing data
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = PulmonologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database and write to CSV file
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_respiratory(specialization)
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")
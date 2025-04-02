from utils import (
    insert_into_database,
    fetch_page_with_scraper_api,
    fetch_page_with_zenrows,
    create_milvus_collection
)
from crawl_article.crawl_article_rheumatology_sql import (
    crawl_article_ard,
    crawl_article_arthritis,
    crawl_article_rheumatology,
    crawl_article_art,
    crawl_article_clinical_r,
)
from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from datetime import datetime
from urllib.parse import urljoin,urlparse

class RheumatologyData(BaseModel):
    Journal: str
    Article: list

specialization = "rheumatology"

async def crawl_page_ard(conn):
    base_url='https://ard.bmj.com'
    main_url='https://ard.bmj.com/content'
    name = "Annals of the Rheumatic Diseases"
    links = []
    current_year = datetime.now().year
    current_month = datetime.now().month

    try:
        volume = int(current_year) - 1941
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
            data = RheumatologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_ard(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_arthritis(conn):
    name = 'Arthritis & Rheumatology'
    base = 'https://acrjournals.onlinelibrary.wiley.com'
    base_url = 'https://acrjournals.onlinelibrary.wiley.com/toc/23265205'
    article_links=[]
    current_year = datetime.now().year
    current_month = datetime.now().month
    vol=int(current_year)-1948
    main_url=f"{base_url}/{current_year}/{vol}/{current_month}"
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

        # Update the database with new links by comparing with existing data
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = RheumatologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database and write to CSV file
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_arthritis(specialization)
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")


async def crawl_page_rheumatology(conn):
    name = 'Rheumatology'
    base_url = "https://academic.oup.com"
    main_url = f"{base_url}/rheumatology/issue"
    article_links = []

    current_year = datetime.now().year
    current_month = datetime.now().month
    volume = int(current_year)-1961

    issue_url = f"{main_url}/{volume}/{current_month}"
    print(f"Scraping: {issue_url}")
    try:
        # Fetch the issue page
        response = await fetch_page_with_zenrows(issue_url)
        soup = BeautifulSoup(response.html, "html.parser")

        # Locate all section tags inside section-container
        sections = soup.find_all("div", class_="section-container")

        for section in sections:
            # Find all div tags with the class "content al-article-list-group"
            content_divs = section.find_all("div", class_="content al-article-list-group")

            for content_div in content_divs:
                # Find all article items inside "al-article-item-wrap al-normal"
                article_divs = content_div.find_all("div", class_="al-article-item-wrap al-normal")

                for article_div in article_divs:
                    article_items = article_div.find("div", class_="al-article-items")
                    if article_items:
                        h5_tag = article_items.find("h5", class_="customLink item-title")
                        if h5_tag:
                            link = h5_tag.find("a", href=True)
                            if link:
                                full_article_link = urljoin(base_url, link["href"])
                                article_links.append(full_article_link)
                                print(f"Found article: {full_article_link}")

    except Exception as e:
        print(f"Error scraping {issue_url}: {e}")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Update the database with new links by comparing with existing data
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = RheumatologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database and write to CSV file
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_rheumatology(specialization)
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")


async def crawl_page_art(conn):
    base_url ="https://arthritis-research.biomedcentral.com/articles/most-recent/rss.xml"
    name = "Arthritis Research & Therapy"
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
            data = RheumatologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_art(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_clincal_r(conn):
    name='Clinical Rheumatology'
    base_url='https://link.springer.com/journal/10067/volumes-and-issues'
    req_str="https://link.springer.com/article"
    try:
        from datetime import datetime

        links = []
        issues = datetime.now().month
        volume = int(datetime.now().year) - 1981
        url = base_url + f"/{volume}" + f"-{issues}"
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
            data = RheumatologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_clinical_r(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")
from utils import (
    insert_into_database,
    fetch_page_with_scraper_api,
    fetch_page_with_zenrows,
    create_milvus_collection,
    connection_config
)
from crawl_article.crawl_article_hematology_sql import (
    crawl_article_blood,
    crawl_article_clincal_oncology,
    crawl_article_haematologica,
    crawl_article_leukemia,
    crawl_article_blood_advances

)

from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from datetime import datetime
from urllib.parse import urljoin,urlparse
from utils import setup_database
class HematologyData(BaseModel):
    Journal: str
    Article: list

specialization = "hematology"

async def crawl_page_blood(conn):
    name = 'blood'
    base_url = "https://ashpublications.org"
    main_url = f"{base_url}/blood/issue"
    article_links = []
    current_year = datetime.now().year
    current_month =datetime.now().month

    base_num = 2*(current_year)-3905
    volume = base_num if current_month <=6 else base_num+1 
    for issue in range(1, 27):  # Issues 1 to 26
        issue_url = f"{main_url}/{volume}/{issue}"
        print(f"Scraping :  {issue_url}")
        try:
            # Fetch the issue page
            response = await fetch_page_with_zenrows(issue_url)
            if not response or response.status_code != 200:
                continue
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

        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")

        data = HematologyData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_blood(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_clinical_oncology(conn):
    name = 'Journal of Clinical Oncology'
    base_url = 'https://ascopubs.org'
    main_url = 'https://ascopubs.org/toc/jco'
        
    links = []  # List to store article links
    month = datetime.now().month
    volume = int(datetime.now().year) - 1983  # Calculate volume based on the year
    start = (month - 1) * 3 + 1  # Calculate the start issue based on the month
    issues = [start, start + 1, start + 2]  # Get three consecutive issues

    # Extract article links from the journal issues
    for issue in issues:
        url = f"{main_url}/{volume}/{issue}"
            
        response = await fetch_page_with_zenrows(url)
        soup = BeautifulSoup(response.html, "html.parser")

        # Find all sections containing articles
        sections = soup.find_all("section", class_="toc__section ml-md-16")
        for section in sections:
            row_divs = section.find_all("div", class_="row")
            for row_div in row_divs:
                badge = row_div.find("span", class_="badge-type")
                if badge and badge.get_text(strip=True) == "ORIGINAL REPORTS":
                    a_tag = row_div.find("a", href=True)
                    if a_tag:
                        href = a_tag["href"]
                        full_link = base_url + href
                        links.append(full_link)

    # Insert the new links into the database if not already present
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        Links = [link for link in links if link not in existing_data]  # Filter out existing links
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = HematologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_clincal_oncology(specialization)
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database")

async def crawl_page_haematologica(conn):
    base_url = "https://haematologica.org/issue/view"
    name = "Haematologica"
    article_links = []

    url = 'https://haematologica.org/issue/current'
    print(f"Fetching: {url}")
    response = await fetch_page_with_zenrows(url)
    soup = BeautifulSoup(response.html, "html.parser")
    article_divs = soup.find_all("div", class_="card one-article-intoc")
    for article in article_divs:
        span_tag = article.find("span", class_="galley-doi-value")
        if span_tag:
            link = span_tag.find("a", href=True)
            if link:
                full_url = urljoin(base_url, link["href"])
                article_links.append(full_url)
                print(f"Found article DOI link: {full_url}")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")
        data = HematologyData(Journal=name, Article=Links)
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_haematologica(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_leukemia(conn):
    name = 'Leukemia'
    base_url = "https://www.nature.com/leu.rss"
    links = []

    try:
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html,  "html.parser")
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
            data = HematologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_leukemia(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_blood_advances(conn):
    name = 'Blood Advances'
    base_url = "https://ashpublications.org"
    main_url = f"{base_url}/bloodadvances/issue"
    article_links = []
    current_year = datetime.now().year
    current_month = datetime.now().month
    volume = current_year - 2016
    
    issue_start = (current_month - 1) * 2 + 1
    issue_end = issue_start + 1
    
    for issue in range(issue_start, issue_end + 1):
        issue_url = f"{main_url}/{volume}/{issue}"
        print(f"Scraping :  {issue_url}")
        try:
            response = await fetch_page_with_zenrows(issue_url)
            if not response or response.status_code != 200:
                continue
            soup = BeautifulSoup(response.html, "html.parser")
            sections = soup.find_all("div", class_="section-container")

            for section in sections:
                content_divs = section.find_all("div", class_="content al-article-list-group")
                for content_div in content_divs:
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
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")
        data = HematologyData(Journal=name, Article=Links)
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_blood_advances(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")


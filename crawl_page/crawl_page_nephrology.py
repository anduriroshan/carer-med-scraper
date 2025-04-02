from utils import (
    insert_into_database,  # Function to insert data into the database
    fetch_page_with_scraper_api,  # Function to fetch pages using Scraper API
    fetch_page_with_zenrows,  # Function to fetch pages using Zenrows
    create_milvus_collection  # Function to create a Milvus collection (not used in this code)
)
from crawl_article.crawl_article_nephrology_sql import (
    crawl_article_jasn,
    crawl_article_kidney,
    crawl_article_cjasn,
    crawl_article_ndt,
    crawl_article_ajkd,
)
from bs4 import BeautifulSoup  # Library for parsing HTML and XML
import re  # Regular expressions (not used in this code)
from pydantic import BaseModel, ValidationError  # Pydantic for data validation
from datetime import datetime  # For working with dates and times
from urllib.parse import urljoin, urlparse  # For URL manipulation
import requests

# Define Pydantic model for Oncology data
class NephrologyData(BaseModel):
    Journal: str  # The name of the journal
    Article: list  # List of article links

specialization = "nephrology"  # Define the specialization as 'nephrology'

async def crawl_page_jasn(conn):
    name = 'Journal of the American Society of Nephrology'
    base_url = "https://journals.lww.com/jasn/_layouts/15/OAKS.Journals/feed.aspx?FeedType=CurrentIssue"
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
            data = NephrologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_jasn(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_kidney_international(conn):
    base_url='https://www.kidney-international.org'
    name = "Kidney International"
    links = []
    current_year = datetime.now().year
    current_month = datetime.now().month
    unique_links = set() 

   
    base_num = 2 * current_year - 3943
    volume = base_num if current_month <= 6 else base_num + 1
    group_id = f"d2020.v{volume}"
    issue_url = f"{base_url}?publicationCode=kint&issueGroupId={group_id}"
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
                if href not in unique_links:  
                    unique_links.add(href)
                    print(f"Found: {href}")

    except Exception as e:
        print(f"Error fetching data for group ID {group_id}: {e}")

    article_links = set()
    processed_article_ids = set()

    for issue_link in unique_links:
        try:
            print(f"Fetching: {issue_link}")
            response = await fetch_page_with_zenrows(issue_link)
            page_soup = BeautifulSoup(response.html, "html.parser")

            sections = page_soup.find_all("section", {"class": "toc__section"})
            for section in sections:
                h2_tag = section.find("h2", class_="toc__heading__header top")
                if h2_tag and any(keyword in h2_tag.get_text(strip=True).lower() for keyword in ["basic research", "clinical investigation", "clincial investigation","research letters"]):
                    for li in section.find_all("li"):
                        article_link = li.find("a", href=True)
                        if article_link:
                            href = article_link["href"]
                            match = re.search(r"/(S\d+\-\d+\(\d+\)\d+\-\d+)/", href)
                            if match:
                                current_article_id = match.group(1)  
                                if current_article_id in processed_article_ids:
                                    continue
                                processed_article_ids.add(current_article_id)
                                full_article_link = urljoin(base_url, href)
                                article_links.add(full_article_link)
                                print(full_article_link)
        except Exception as e:
            print(f"Error fetching article links from {issue_link}: {e}")

    # Convert article links to a list
    links = list(article_links)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Update the database with new links by comparing with existing data
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = NephrologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database and write to CSV file
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_kidney(specialization)
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database and CSV file.")



async def crawl_page_cjasn(conn):
    name = 'Clinical Journal of the American Society of Nephrology'
    base_url = "https://journals.lww.com/cjasn/_layouts/15/OAKS.Journals/feed.aspx?FeedType=CurrentIssue"
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
            data = NephrologyData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_cjasn(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_ndt(conn):
    name = 'Nephrology Dialysis Transplantation'
    base_url = "https://academic.oup.com"
    main_url = f"{base_url}/ndt/issue"
    links = []
    
    current_year = datetime.now().year
    issue = datetime.now().month
    volume = int(current_year - 1985)
    issue_url = f"{main_url}/{volume}/{issue}"
    print(f"Scraping issue: Volume {volume}, Issue {issue} at {issue_url}")
    
    try:
        # Fetch the issue page
        response = await fetch_page_with_zenrows(issue_url)

        soup = BeautifulSoup(response.html, "html.parser")

        # Locate the section containing Original Articles
        h4_tag = soup.find("h4", class_="title articleClientType act-header", string=lambda text: text and text.lower() == "original articles")

        if not h4_tag:
            print("No 'Original Articles' section found.")
        else:
            # Find the div containing article links
            content_div = h4_tag.find_next("div", class_="content al-article-list-group")
            if content_div:
                # Extract articles from the div
                article_divs = content_div.find_all("div", class_="al-article-item-wrap al-normal")
                for article_div in article_divs:
                    h5_tag = article_div.find("h5", class_="customLink item-title")
                    if h5_tag:
                        link = h5_tag.find("a", href=True)
                        if link:
                            full_article_link = urljoin(base_url, link["href"])
                            print(f"Found: {full_article_link}")
                            links.append(full_article_link)
            else:
                print("No articles found.")
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {issue_url}: {e}")

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Update the database with new links by comparing with existing data
        new_links = [link for link in links if link not in existing_data]
        print(f"Updated links: {new_links}")

        if new_links:
            # Validate the data using Pydantic model
            data = NephrologyData(
                Journal=name,
                Article=new_links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(new_links))
            
            print(f"Successfully inserted {len(new_links)} articles into the database")
        else:
            print("No new links to insert.")

    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {new_links} into the database and CSV file.")
    except Exception as e:
        print(f"Error occurred: {e}")

    # **Ensure crawl_article_ndt runs even if no new links are found**
    crawl_article_ndt(specialization)

async def crawl_page_ajkd(conn):
    name = "American Journal of Kidney Diseases"
    base_url = "https://www.ajkd.org"
    main_url = f"{base_url}/issues"
    current_year = datetime.now().year
    current_month = datetime.now().month
    unique_links = set() 
    base_num = 2 * current_year - 3965
    volume = base_num if current_month <= 6 else base_num + 1
    group_id = f"d2020.v{volume}"
    unique_links = set()  
    issue_url = f"{main_url}?publicationCode=yajkd&issueGroupId={group_id}"
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
                if href not in unique_links:  
                    unique_links.add(href)
                    print(f"Found: {href}")

    except Exception as e:
        print(f"Error fetching data for group ID {group_id}: {e}")

    article_links = set()
    processed_article_ids = set()

    for issue_link in unique_links:
        try:
            print(f"Fetching: {issue_link}")
            response = await fetch_page_with_zenrows(issue_link)
            page_soup = BeautifulSoup(response.html, "html.parser")

            sections = page_soup.find_all("section", {"class": "toc__section"})
            for section in sections:
                h2_tag = section.find("h2", class_="toc__heading__header top")
                if h2_tag and any(keyword in h2_tag.get_text(strip=True).lower() for keyword in ["special report", "original investigations"]):
                    for li in section.find_all("li"):
                        article_link = li.find("a", href=True)
                        if article_link:
                            href = article_link["href"]
                            match = re.search(r"/(S\d+\-\d+\(\d+\)\d+\-\d+)/", href)
                            if match:
                                current_article_id = match.group(1)  
                                if current_article_id in processed_article_ids:
                                    continue
                                processed_article_ids.add(current_article_id)
                                full_article_link = urljoin(base_url, href)
                                article_links.add(full_article_link)
                                print(full_article_link)
        except Exception as e:
            print(f"Error fetching article links from {issue_link}: {e}")

    # Convert article links to a list
    links = list(article_links)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Update the database with new links by comparing with existing data
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = NephrologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database and write to CSV file
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_ajkd(specialization)
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database and CSV file.")
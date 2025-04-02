from bs4 import BeautifulSoup  # Importing BeautifulSoup for parsing HTML and XML documents
from utils import (  # Importing utility functions for database operations and web scraping
    insert_into_database,
    fetch_page_with_zenrows,
)
from crawl_article.crawl_article_dermatology_sql import (  # Importing functions for processing crawled articles
    crawl_article_derma_clinics,
    crawl_article_jaad,
    crawl_article_jama,
    crawl_article_jid,
    crawl_article_wiley
)
import re  # Importing regex for pattern matching
from pydantic import BaseModel, ValidationError  # Importing Pydantic for data validation
from urllib.parse import urljoin, urlparse  # Importing utilities for URL manipulation
from datetime import datetime  # Importing datetime for handling date and time
import requests  # Importing requests for HTTP requests

# Pydantic model to validate the structure of dermatology articles
class DermaData(BaseModel):
    Journal: str  # Journal name
    Article: list  # List of article URLs

# Define specialization for database categorization
specialization = "dermatology"


# Function to crawl articles from the Journal of the American Academy of Dermatology (JAAD)
async def crawl_page_jaad(conn):
    name = "Journal of the American Academy of Dermatology"  # Journal name
    base_url = "https://www.jaad.org"  # Base URL of the journal
    main_url = f"{base_url}/issues"  # URL for issue listings
    current_year = datetime.now().year  # Fetch current year
    current_month = datetime.now().month  # Fetch current month

    # Calculate volume number based on the year
    base_num = 88 + 2 * (current_year - 2023)
    num = base_num if current_month <= 6 else base_num + 1
    group_id = f"d2020.v{num}"  # Constructing issue group ID
    unique_links = set()  # Set to store unique issue links

    issue_url = f"{main_url}?publicationCode=ymjd&issueGroupId={group_id}"  # Constructing issue URL
    print(f"Fetching: {issue_url}")  # Debugging: Printing issue URL being fetched

    # Fetching the main issue page
    try:
        response = await fetch_page_with_zenrows(issue_url)  # Fetch the page
        if not response:  # Handling case where response is None
            raise ValueError("Fetch failed: No response returned.")
        if response.status_code != 200:  # Handling non-200 status codes
            raise ValueError(f"Non-200 status code received: {response.status_code}")

        soup = BeautifulSoup(response.html, "html.parser")  # Parsing HTML content

        # Extract issue links from the page
        div = soup.find("div", {
            "data-groupid": group_id,
            "class": "list-of-issues__group list-of-issues__group--issues js--open"
        })

        if div:
            links = div.find_all("a", href=True)
            for link in links:
                href = urljoin(base_url, link["href"])  # Construct full URL
                if href not in unique_links:  # Add only unique links
                    unique_links.add(href)
                    print(f"Found issue link: {href}")  # Debugging: Printing extracted issue links

    except (ValueError, requests.exceptions.RequestException) as e:
        print(f"Error fetching data for group ID {group_id}: {e}")
        return  # Exit function if an error occurs

    # Fetch articles from the extracted issue links
    article_links = set()  # Set to store article links
    for issue_link in unique_links:
        print(f"Fetching articles from: {issue_link}")  # Debugging
        try:
            response = await fetch_page_with_zenrows(issue_link)  # Fetch issue page
            if not response:  # Handling case where response is None
                print(f"Skipping {issue_link} due to fetch failure: No response returned.")
                continue
            if response.status_code != 200:  # Handling non-200 status codes
                print(f"Skipping {issue_link} due to non-200 status code: {response.status_code}")
                continue

            page_soup = BeautifulSoup(response.html, "html.parser")  # Parsing issue page content

            # Locate sections containing research articles
            sections = page_soup.find_all("section", {"class": "toc__section"})
            for section in sections:
                heading = section.find("h2", string="Original Articles")
                if heading:
                    divs = section.find_all("div", {
                        "class": "toc__item__cover col-md-3 col-lg-2 hidden-xs hidden-sm hidden-md"
                    })
                    for div in divs:
                        article_link = div.find("a", href=True)  # Finding article link
                        if article_link:
                            full_article_link = urljoin(base_url, article_link["href"])  # Constructing full article URL
                            print(full_article_link)  # Debugging
                            article_links.add(full_article_link)  # Adding to set of article links

        except requests.exceptions.RequestException as e:
            print(f"Error fetching article links from {issue_link}: {e}")  # Handling exceptions

    # Convert set to list for database insertion
    links = list(article_links)
    try:
        cursor = conn.cursor()  # Creating database cursor
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing links from DB

        # Filtering out already existing links
        Links = [link for link in links if link not in existing_data]  
        print(f"Updated links: {Links}")  # Debugging: Printing new links count

        # Validating data with Pydantic model
        data = DermaData(
            Journal=name,  # Journal name
            Article=Links,  # List of new articles
        )

        # Inserting validated data into the database
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))  
        await crawl_article_jaad(specialization)  # Crawling article content
        print(f"Successfully inserted {len(Links)} into the database")  # Success message

    except ValidationError as e:
        print(f"Validation error: {e}")  # Handling validation errors
        print(f"Failed to insert {Links} into the database and CSV file.")  # Debugging failure


# Function to crawl articles from JAMA Dermatology
async def crawl_page_jama_derma(conn):
    name = "JAMA Dermatology"  # Journal name
    base_url = "https://jamanetwork.com/rss/site_12/68.xml"  # RSS feed URL

    try:
        response = await fetch_page_with_zenrows(base_url)  # Fetch RSS feed
        soup = BeautifulSoup(response.html, "html.parser")  # Parse XML response

        # Extract article links from RSS feed
        links = [
            item.find("link").text
            for item in soup.find_all("item")
            if item.find("link")
        ]
        print(f"Found {len(links)} links in the RSS feed")  # Debugging: Printing extracted links

        try:
            cursor = conn.cursor()  # Creating a database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing article links

            # Filtering out already existing links
            Links = [link for link in links if link not in existing_data]  
            print(f"Updated links: {Links}")  # Debugging: Printing new links count

            # Validating data with Pydantic model
            data = DermaData(
                Journal=name,  # Journal name
                Article=Links,  # List of new articles
            )

            # Inserting validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))  
            await crawl_article_jama(specialization)  # Crawling article content
            print(f"Successfully inserted {len(Links)} into the database")  # Success message

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handling validation errors
            print(f"Failed to insert {Links} into the database and CSV file.")  # Debugging failure

    except Exception as e:
        print(f"Error occurred: {e}")  # Handling fetch errors

async def crawl_page_jid(conn):  # Function to fetch new articles from Journal of Investigative Dermatology
    name = "Journal of Investigative Dermatology"  # Journal name
    base_url = "https://www.jidonline.org"  # Base URL for the journal
    year = datetime.now().year  # Get the current year
    num = int(year - 1880)  # Compute the volume number
    main_url = f"{base_url}/issues"  # Issues listing page
    group_id = f"d2020.v{num}"  # Generate issue group ID
    unique_links = set()  # Set to store unique issue links

    issue_url = f"{main_url}?publicationCode=jid&issueGroupId={group_id}"  # Construct issue URL
    print(f"Fetching: {issue_url}")  # Debugging

    # Fetching issue listing page
    try:
        response = await fetch_page_with_zenrows(issue_url)  # Fetch page content
        soup = BeautifulSoup(response.html, "html.parser")  # Parse HTML content

        # Find issue links based on group ID
        div = soup.find("div", {
            "data-groupid": group_id,
            "class": "list-of-issues__group list-of-issues__group--issues js--open"
        })

        if div:
            links = div.find_all("a", href=True)
            for link in links:
                href = urljoin(base_url, link["href"])  # Construct full URL
                if href not in unique_links:  # Store only unique links
                    print(f"Found issue: {href}")  
                    unique_links.add(href)  # Add issue link to the set

    except Exception as e:
        print(f"Error fetching data for group ID {group_id}: {e}")  # Handle errors

    # Fetching articles from each issue
    article_links = set()  # Set to store unique article links
    processed_article_ids = set()  # Track already processed article IDs

    for issue_link in unique_links:
        try:
            print(f"Fetching: {issue_link}")  # Debugging
            response = await fetch_page_with_zenrows(issue_link)  # Fetch issue page
            page_soup = BeautifulSoup(response.html, "html.parser")  # Parse HTML content

            # Locate sections containing original articles
            sections = page_soup.find_all("section", {"class": "toc__section"})
            for section in sections:
                h2_tag = section.find("h2", class_="toc__heading__header top")
                if h2_tag and "original articles" in h2_tag.get_text(strip=True).lower():
                    for li in section.find_all("li"):
                        article_link = li.find("a", href=True)  # Extract article link
                        if article_link:
                            href = article_link["href"]

                            # Extract unique article identifier from the URL
                            parsed_url = urlparse(href)
                            path_parts = parsed_url.path.split("/")
                            if "article" in path_parts:
                                identifier_index = path_parts.index("article") + 1
                                if identifier_index < len(path_parts):
                                    current_article_id = path_parts[identifier_index]

                                    # Check if this article has been processed before
                                    if current_article_id in processed_article_ids:
                                        continue
                                    processed_article_ids.add(current_article_id)  # Track processed articles

                                    # Construct full article URL
                                    full_article_link = urljoin(base_url, href)
                                    article_links.add(full_article_link)  # Store article link
                                    print(f"Added: {full_article_link}")  # Debugging

        except Exception as e:
            print(f"Error fetching article links from {issue_link}: {e}")  # Handle errors

    # Convert set to list for database insertion
    links = list(article_links)
    try:
        cursor = conn.cursor()  # Create database cursor
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing article links

        # Filter out already stored links
        Links = [link for link in links if link not in existing_data]  
        print(f"Updated links: {Links}")  # Debugging

        # Validate data using Pydantic model
        data = DermaData(
            Journal=name,  # Journal name
            Article=Links,  # List of new articles
        )

        # Insert the validated data into the database
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))  
        await crawl_article_jid(specialization)  # Crawling article content
        print(f"Successfully inserted {len(Links)} into the database")  # Success message

    except ValidationError as e:
        print(f"Validation error: {e}")  # Handle validation errors
        print(f"Failed to insert {Links} into the database and CSV file.")  # Debugging

async def crawl_page_derma_clinics(conn):  # Function to fetch articles from Dermatologic Clinics journal
    name = "Dermatologic Clinics"  # Journal name
    base_url = "https://www.derm.theclinics.com"  # Base URL for the journal
    main_url = f"{base_url}/issues"  # Issues listing page
    year = datetime.now().year  # Get current year
    num = int(year - 1982)  # Compute the volume number
    group_id = f"d2020.v{num}"  # Generate issue group ID
    unique_links = set()  # Set to store unique issue links

    issue_url = f"{main_url}?publicationCode=det&issueGroupId={group_id}"  # Construct issue URL
    print(f"Fetching: {issue_url}")  # Debugging

    # Fetching issue listing page
    try:
        response = await fetch_page_with_zenrows(issue_url)  # Fetch page content
        soup = BeautifulSoup(response.html, "html.parser")  # Parse HTML content

        # Find issue links based on group ID
        div = soup.find("div", {
            "data-groupid": group_id,
            "class": "list-of-issues__group list-of-issues__group--issues js--open"
        })

        if div:
            links = div.find_all("a", href=True)
            for link in links:
                href = urljoin(base_url, link["href"])  # Construct full URL
                if href not in unique_links:  # Store only unique links
                    print(f"Found issue link: {href}")  
                    unique_links.add(href)  # Add issue link to the set

    except Exception as e:
        print(f"Error fetching data for group ID {group_id}: {e}")  # Handle errors

    # Fetching articles from each issue
    article_links = set()  # Set to store unique article links
    processed_article_ids = set()  # Track already processed article IDs

    for issue_link in unique_links:
        try:
            print(f"Fetching articles from: {issue_link}")  # Debugging
            response = await fetch_page_with_zenrows(issue_link)  # Fetch issue page
            page_soup = BeautifulSoup(response.html, "html.parser")  # Parse HTML content

            # Locate sections containing review articles
            sections = page_soup.find_all("section", {"class": "toc__section"})
            for section in sections:
                h2_tag = section.find("h2", class_="toc__heading top")
                if h2_tag and "review article" in h2_tag.get_text(strip=True).lower():
                    for li in section.find_all("li"):
                        article_link = li.find("a", href=True)  # Extract article link
                        if article_link:
                            href = article_link["href"]
                            full_article_link = urljoin(base_url, href)  # Construct full article URL
                            article_links.add(full_article_link)  # Store article link
                            print(full_article_link)  # Debugging

        except Exception as e:
            print(f"Error fetching article links from {issue_link}: {e}")  # Handle errors
    links = list(article_links)
    try:
        cursor = conn.cursor()  # Create database cursor
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing article links

        # Filter out already stored links
        Links = [link for link in links if link not in existing_data]  
        print(f"Updated links: {Links}")  # Debugging

        # Validate data using Pydantic model
        data = DermaData(
            Journal=name,  # Journal name
            Article=Links,  # List of new articles
        )

        # Insert the validated data into the database
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))  
        await crawl_article_derma_clinics(specialization)  # Crawling article content
        print(f"Successfully inserted {len(Links)} into the database")  # Success message

    except ValidationError as e:
        print(f"Validation error: {e}")  # Handle validation errors
        print(f"Failed to insert {Links} into the database and CSV file.")  # Debugging

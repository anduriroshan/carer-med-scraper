from utils import (
    insert_into_database,  # Function to insert data into the database
    fetch_page_with_scraper_api,  # Function to fetch pages using Scraper API (not used in this code)
    fetch_page_with_zenrows,  # Function to fetch pages using Zenrows
    create_milvus_collection  # Function to create a Milvus collection (not used in this code)
)
from crawl_article.crawl_article_pediatrics_sql import (
    crawl_article_adc_bmj,  # Function to crawl articles from Archives of Disease in Childhood (BMJ) (not used in this code)
    crawl_article_jama,  # Function to crawl articles from JAMA Pediatrics (not used in this code)
    crawl_article_jpeds,  # Function to crawl articles from Journal of Pediatrics (not used in this code)
    crawl_article_ped_research,  # Function to crawl articles from Pediatric Research (not used in this code)
    crawl_article_pediatrics  # Function to crawl articles from Pediatrics journal (not used in this code)
)
from bs4 import BeautifulSoup  # Library for parsing HTML and XML
import re  # Regular expressions (not used in this code)
from pydantic import BaseModel, ValidationError  # Pydantic for data validation
from datetime import datetime  # For working with dates and times
from urllib.parse import urljoin, urlparse  # For URL manipulation

# Define Pydantic model for Pediatrics data
class PediatricsData(BaseModel):
    Journal: str  # Journal name
    Article: list  # List of article links

specialization = "pediatrics"  # Define the specialization as 'pediatrics'

# Function to crawl articles from JAMA Pediatrics using RSS feed
async def crawl_page_jama_ped(conn):
    base_url ="https://jamanetwork.com/rss/site_19/75.xml"  # RSS feed URL for JAMA Pediatrics
    name = "JAMA Pediatrics"  # Journal name
    links = []  # List to store article links

    try:
        # Fetch the RSS feed using Zenrows
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")  # Parse the RSS XML content

        # Extract and process each item in the RSS feed to get the article links
        links = [
            item.find("link").text
            for item in soup.find_all("item")  # Iterate through each RSS feed item
            if item.find("link")  # Ensure the 'link' tag is present
        ]

        # Database operations to insert the new article links
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing links from the database

            # Filter out the links that already exist in the database
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate the data using Pydantic model
            data = PediatricsData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_jama(specialization)  # Crawl related articles for further processing
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle errors related to the fetching and parsing process

# Function to crawl articles from Pediatrics journal by building URLs based on the current year and month
async def crawl_page_pediatrics(conn):
    base_url='https://publications.aap.org/pediatrics/issue'  # Base URL for Pediatrics
    name = "Pediatrics"  # Journal name
    links = []  # List to store article links
    current_year = datetime.now().year  # Get current year
    current_month = datetime.now().month  # Get current month

    try:    
        base_num = 2 * current_year - 3895  # Calculate base volume number based on the year
        volume = base_num if current_month <= 6 else base_num + 1  # Determine volume number based on the month
        page = current_month if current_month <= 6 else current_month - 6  # Page number based on the month

        url = f"{base_url}/{volume}/{page}"  # Construct URL for the current issue

        print(f"Fetching: {url}")

        # Fetch the main issue page using Zenrows
        response_main = await fetch_page_with_zenrows(url)
        soup_main = BeautifulSoup(response_main.html, "html.parser")  # Parse the HTML content

        # Extract articles from the main issue page
        main_articles = soup_main.select(
            "#resourceTypeList-IssueOnRails_IssueArticleList > div > section > div > div > div > h5"
        )
        for element in main_articles:
            link = element.find("a")  # Find the <a> tag with the article link
            if link and link.get("href"):  # Ensure the <a> tag has an href attribute
                links.append(base_url[:28] + link.get("href").strip())  # Construct the full article link

        # Database operations to insert the new article links
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing links from the database

            # Filter out the links that already exist in the database
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate the data using Pydantic model
            data = PediatricsData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_pediatrics(specialization)  # Crawl related articles for further processing
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle errors related to the fetching and parsing process

# Function to crawl articles from Archives of Disease in Childhood (BMJ) using URLs constructed from volume and month
async def crawl_page_adc_bmj(conn):
    base_url='https://adc.bmj.com/content'  # Base URL for Archives of Disease in Childhood (BMJ)
    name = "Archives of Disease in Childhood (BMJ)"  # Journal name
    links = []  # List to store article links
    current_year = datetime.now().year  # Get current year
    current_month = datetime.now().month  # Get current month

    try:
        volume = int(current_year) - 1915  # Calculate volume based on the year
        url = f"{base_url}/{volume}/{current_month}"  # Construct URL for the current issue
        print(f"Fetching: {url}")
        response = await fetch_page_with_zenrows(url)  # Fetch the issue page using Zenrows # Ensure the request was successful
        soup = BeautifulSoup(response.html, "html.parser")  # Parse the HTML content

        # Extract articles from the issue page
        elements = soup.select(
            "body > div > section > div > div > div > div > div > div > div > div > div > div > div > div > div > div > div > div > div > ul > li > div > div > div"
        )
        for element in elements:
            link = element.find("a")  # Find the <a> tag with the article link
            if link and link.get("href"):  # Ensure the <a> tag has an href attribute
                links.append(base_url[:20] + link.get("href").strip())  # Construct the full article link

        # Database operations to insert the new article links
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing links from the database

            # Filter out the links that already exist in the database
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate the data using Pydantic model
            data = PediatricsData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_adc_bmj(specialization)  # Crawl related articles for further processing
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle errors related to the fetching and parsing process

# Function to crawl articles from Journal of Pediatrics by scraping the issues page
async def crawl_page_jpeds(conn):
    name = "Journal of Pediatrics"  # Journal name
    base_url = "https://www.jpeds.com"  # Base URL for Journal of Pediatrics
    main_url = f"{base_url}/issues"  # URL to fetch the issues page
    unique_links = set()  # Set to store unique issue links
    current_year = datetime.now().year  # Get current year
    current_month = datetime.now().month  # Get current month
    base_num = 12 * current_year - 24024  # Calculate base volume number based on the year
    volume = base_num if current_month <= 1 else base_num + current_month - 1  # Calculate volume based on the month
    group_id = f"d2020.v{volume}"  # Generate group ID for the issue

    issue_url = f"{main_url}?publicationCode=ympd&issueGroupId={group_id}"  # Construct URL for the issue group
    print(f"Fetching : {issue_url}")
    try:
        # Fetch the issue page using Zenrows
        response = await fetch_page_with_zenrows(issue_url)
        soup = BeautifulSoup(response.html, "html.parser")  # Parse the HTML content

        # Find the div containing the issue links
        div = soup.find("div", {
            "data-groupid": group_id,
            "class": "list-of-issues__group list-of-issues__group--issues js--open"
        })

        if div:
            links = div.find_all("a", href=True)
            for link in links:
                href = urljoin(base_url, link["href"])  # Build the full URL for each issue
                if href not in unique_links:
                    print(f"Found issue : {href}")  # Add only unique links
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
            page_soup = BeautifulSoup(response.html, "html.parser")  # Parse the issue page content

            # Iterate over sections containing articles
            sections = page_soup.find_all("section", {"class": "toc__section"})
            for section in sections:
                h2_tag = section.find("h2", class_="toc__heading__header top")
                if h2_tag and "original articles" in h2_tag.get_text(strip=True).lower():
                    for li in section.find_all("li"):
                        article_link = li.find("a", href=True)
                        if article_link:
                            href = article_link["href"]

                            # Extract the unique article identifier
                            parsed_url = urlparse(href)
                            path_parts = parsed_url.path.split("/")
                            if "article" in path_parts:
                                identifier_index = path_parts.index("article") + 1
                                if identifier_index < len(path_parts):
                                    current_article_id = path_parts[identifier_index]

                                    # Skip previously processed articles
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
    links = list(article_links)

    # Database operations to insert the new article links
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing links from the database

        # Filter out the links that already exist in the database
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = PediatricsData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_jpeds(specialization)  # Crawl related articles for further processing
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database and CSV file.")

# Function to crawl articles from Pediatric Research using RSS feed
async def crawl_page_nature_ped(conn):
    base_url ="https://www.nature.com/pr.rss"  # RSS feed URL for Pediatric Research
    name = "Pediatric Research"  # Journal name
    links = []  # List to store article links

    try:
        # Fetch the RSS feed using Zenrows
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")  # Parse the RSS XML content

        # Extract and process each item in the RSS feed to get the article links
        links = [
            item.find("link").text
            for item in soup.find_all("item")  # Iterate through each RSS feed item
            if item.find("link")  # Ensure the 'link' tag is present
        ]

        # Database operations to insert the new article links
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing links from the database

            # Filter out the links that already exist in the database
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate the data using Pydantic model
            data = PediatricsData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_ped_research(specialization)  # Crawl related articles for further processing
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle errors related to the fetching and parsing process

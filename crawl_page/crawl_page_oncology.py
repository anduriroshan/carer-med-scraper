from utils import (
    insert_into_database,  # Function to insert data into the database
    fetch_page_with_scraper_api,  # Function to fetch pages using Scraper API
    fetch_page_with_zenrows,  # Function to fetch pages using Zenrows
    create_milvus_collection  # Function to create a Milvus collection (not used in this code)
)
from crawl_article.crawl_article_oncology_sql import (
    crawl_article_cancer,  # Function to crawl articles related to cancer (not used in this code)
    crawl_article_cancer_cell,  # Function to crawl cancer cell articles
    crawl_article_Lancet,  # Function to crawl Lancet articles
    crawl_article_nature  # Function to crawl Nature articles
)
from bs4 import BeautifulSoup  # Library for parsing HTML and XML
import re  # Regular expressions (not used in this code)
from pydantic import BaseModel, ValidationError  # Pydantic for data validation
from datetime import datetime  # For working with dates and times
from urllib.parse import urljoin, urlparse  # For URL manipulation

# Define Pydantic model for Oncology data
class OncologyData(BaseModel):
    Journal: str  # The name of the journal
    Article: list  # List of article links

specialization = "oncology"  # Define the specialization as 'oncology'

# Function to crawl articles from the Cancer Cell journal
async def crawl_page_cancer_cell(conn):
    name = 'Cancer Cell'  # Journal name
    base_url = "https://www.cell.com"  # Base URL of the journal
    main_url = f"https://www.cell.com/cancer-cell/issues"  # URL to fetch the issues
    issue_links = []  # List to store issue links
    links = []  # List to store article links

    # Fetch the main issues page using Zenrows
    response = await fetch_page_with_zenrows(main_url)
    soup = BeautifulSoup(response.html, "html.parser")  # Parse the page content with BeautifulSoup

    # Extract issue links from the issues list
    issue_list = soup.find("ul", class_="rlist list-of-issues__list")
    if issue_list:
        for li in issue_list.find_all("li"):
            link = li.find("a")
            if link and link.get("href"):
                full_url = base_url + link["href"]  # Build full URL for the issue
                issue_links.append(full_url)

    # Extract article links for each issue
    for issue_url in issue_links:
        response = await fetch_page_with_zenrows(issue_url)
          # Ensure the request was successful
        soup = BeautifulSoup(response.html, "html.parser")

        toc_sections = soup.find_all("section", class_="toc__section")
        for section in toc_sections:
            h2_tag = section.find("h2", class_="toc__heading__header top")
            if h2_tag and "Articles" in h2_tag.get_text(strip=True):
                toc_body = section.find("ul", class_="toc__body rlist clearfix")
                if toc_body:
                    article_items = toc_body.find_all("li", class_=["articleCitation", "articleCitation freeFeaturedContent"])
                    for li in article_items:
                        link = li.find("a")
                        if link and link.get("href"):
                            full_url = base_url + link["href"]
                            links.append(full_url)  # Add article link to the list
                            
    # Insert the new links into the database if not already present
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Filter out the links that already exist in the database
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = OncologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_cancer_cell(specialization)  # Crawl related articles
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database")

# Function to crawl articles from The Lancet Oncology journal
async def crawl_page_lancet_onco(conn):
    name="The Lancet Oncology"
    base_url = "https://www.thelancet.com"
    main_url = f"{base_url}/journals/lanonc/issues"
    issue_links = []  # List to store issue links
    links = []  # List to store article links

    try:
        # Fetch the main issues page
        response = await fetch_page_with_zenrows(main_url)
        
        soup = BeautifulSoup(response.html, "html.parser")

        # Extract issue links
        issue_list = soup.find("ul", class_="rlist list-of-issues__list")
        if issue_list:
            for li in issue_list.find_all("li"):
                link = li.find("a")
                if link and link.get("href"):
                    full_url = base_url + link["href"]
                    issue_links.append(full_url)

        # Extract article links for each issue
        for issue_url in issue_links:
            count = 0
            response = await fetch_page_with_zenrows(issue_url)
            
            soup = BeautifulSoup(response.html, "html.parser")

            toc_sections = soup.find_all("section", class_="toc__section")
            for section in toc_sections:
                h2_tag = section.find("h2", class_="toc__section__header toc__section__header--A top")
                if h2_tag and "Articles" in h2_tag.get_text(strip=True):
                    toc_body = section.find("ul", class_="toc__body rlist clearfix")
                    if toc_body:
                        article_items = toc_body.find_all("li", class_=["articleCitation", "articleCitation freeFeaturedContent"])

                        for li in article_items:
                            link = li.find("a")
                            if link and link.get("href"):
                                full_url = base_url + link["href"]
                                links.append(full_url)
                                count += 1

            print(f"Extracted {count} articles from {issue_url}.")
    except Exception as e:
        print(f"Error: {e}")

    # Insert the new links into the database if not already present
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Filter out the links that already exist in the database
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = OncologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_Lancet(specialization)  # Crawl related articles
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database")

# Function to crawl articles from CA: A Cancer Journal for Clinicians
async def crawl_page_ascjournal(conn):
    name = 'CA: A Cancer Journal for Clinicians'
    base = 'https://acsjournals.onlinelibrary.wiley.com'
    base_url = 'https://acsjournals.onlinelibrary.wiley.com/toc/15424863'
    links=[]  # List to store article links
    year= datetime.now().year
    month = datetime.now().month
    vol = int(year-1950)  # Calculate volume number based on the year
    issue = (month + 1) // 2  # Calculate issue number based on the month
    main_url=f"{base_url}/{year}/{vol}/{issue}"

    # Fetch the issue page
    response = await fetch_page_with_zenrows(main_url)
    
    soup = BeautifulSoup(response.html, "html.parser")
    article_list = soup.find_all("div", class_="issue-items-container bulkDownloadWrapper")
    
    # Extract article links from the issue page
    for article in article_list:
        h3_tag = article.find("h3", class_="toc__heading section__header to-section")
        if h3_tag and "ARTICLE" in h3_tag.get_text().upper():
            link = article.find("a")
            if link and link.get("href"):
                full_url = base + link["href"]
                links.append(full_url)

    # Insert the new links into the database if not already present
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Filter out the links that already exist in the database
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = OncologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_cancer(specialization)  # Crawl related articles
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database")

# Function to crawl articles from Nature Reviews Cancer journal
async def crawl_page_nature_cancer(conn):
    name = 'Nature Reviews Cancer'
    base = 'https://www.nature.com/'
    base_url = 'https://www.nature.com/nrc/volumes'
    links = []  # List to store article links
    volume =int(datetime.now().year)-2000  # Calculate volume based on the year
    issue = int(datetime.now().month)  # Get current month for the issue

    url = f"{base_url}/{volume}/issues/{issue}"
        
    # Fetch the issue page
    response = await fetch_page_with_zenrows(url) 
    
    soup = BeautifulSoup(response.html, "html.parser")

    # Extract article links from the issue page
    section = soup.find("section", id="Reviews")
    if section:
        ul = section.find("ul", class_="app-article-list-row")
        if ul:
            list_items = ul.find_all("li", class_="app-article-list-row__item")
            for li in list_items:
                a_tag = li.find("a", class_="u-link-inherit")
                if a_tag and a_tag.has_attr("href"):
                    full_url = urljoin(base, a_tag["href"])
                    links.append(full_url)

    # Insert the new links into the database if not already present
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Filter out the links that already exist in the database
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")

        # Validate the data using Pydantic model
        data = OncologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_nature(specialization)  # Crawl related articles
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database")

# Function to crawl articles from the Journal of Clinical Oncology
async def crawl_page_jco(conn):
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
        data = OncologyData(
            Journal=name,
            Article=Links,
        )

        # Insert the validated data into the database
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        print(f"Successfully inserted {len(Links)} into the database")
    except ValidationError as e:
        print(f"Validation error: {e}")
        print(f"Failed to insert {Links} into the database")

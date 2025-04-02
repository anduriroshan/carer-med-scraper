from bs4 import BeautifulSoup  # For parsing HTML and XML documents
from utils import (
    insert_into_database,  # Utility to insert data into the database
    fetch_page_with_zenrows,  # Fetching web pages using the ZenRows API
    create_milvus_collection  # Create a Milvus collection to store article embeddings
)
from crawl_article.crawl_article_neuroscience_sql import (
    crawl_article_trends,  # Crawling articles from Trends in Neuroscience
    crawl_article_brain,  # Crawling articles from Brain journal
    crawl_article_jneuro,  # Crawling articles from Journal of Neuroscience
    crawl_article_neuron,  # Crawling articles from Neuron journal
    crawl_article_nature  # Crawling articles from Nature Reviews Neuroscience
)
from datetime import datetime  # For working with dates and times
import re  # Regular expressions for string pattern matching
from pydantic import BaseModel, ValidationError  # For data validation using Pydantic
from urllib.parse import urljoin  # For combining base URL and relative links
import requests  # For making HTTP requests

# Define the Pydantic model for data validation
class NeuroscienceData(BaseModel):
    Journal: str  # Journal name
    Article: list  # List of article URLs

specialization = "neuroscience"  # Define the specialization for the field

# Function to scrape articles from Nature Reviews Neuroscience
async def crawl_page_nature(conn):
    """
    Crawls articles from Nature Reviews Neuroscience, processes them, and inserts them into the database.
    """
    name = 'Nature Reviews Neuroscience'  # Journal name
    base = 'https://www.nature.com/'  # Base URL
    base_url = 'https://www.nature.com/nrn/volumes'  # URL for the volumes page

    try:
        # List to store article links
        links = []
        pages = datetime.now().month  # Get the current month
        year = datetime.now().year  # Get the current year
        volume = int(year - 1999)  # Calculate volume number based on the current year

        # Iterate through the issues based on current month
        for issues in range(1, pages + 1):
            url = f"{base_url}/{volume}/issues/{issues}"  # Construct the issue URL
            print(f"Fetching : {url}")  # Print the URL being fetched
            response = await fetch_page_with_zenrows(url)  # Fetch the issue page
              # Raise error for bad responses
            soup = BeautifulSoup(response.html, "html.parser")  # Parse the HTML content

            # Extract articles from the "Reviews" section
            section = soup.find("section", id="Reviews")
            if section:
                ul = section.find("ul", class_="app-article-list-row")
                if ul:
                    list_items = ul.find_all("li", class_="app-article-list-row__item")
                    for li in list_items:
                        a_tag = li.find("a", class_="u-link-inherit")
                        if a_tag and a_tag.has_attr("href"):
                            full_url = urljoin(base, a_tag["href"])  # Combine base URL with relative link
                            print(f"Found article : {full_url}")  # Debugging
                            links.append(full_url)  # Add to the list of links

        # Insert new articles into the database
        try:
            cursor = conn.cursor()  # Create database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing links from the database

            # Update the database with new links
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")  # Debugging

            # Validate and insert the data into the database
            data = NeuroscienceData(Journal=name, Article=Links)
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_nature(specialization)  # Crawl the article content and insert it into Milvus
            print(f"Successfully inserted {len(Links)} into the database")  # Success message

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handle validation errors
            print(f"Failed to insert {Links} into the database")  # Debugging message

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle general errors

# Function to scrape articles from Neuron journal
async def crawl_page_neuron(conn):
    """
    Crawls articles from the Neuron journal, processes them, and inserts them into the database.
    """
    name = 'Neuron'  # Journal name
    base_url = "https://www.cell.com"  # Base URL for Neuron
    main_url = f"{base_url}/neuron/issues"  # URL for the issues page

    issue_links = []  # List to store issue links
    links = []  # List to store article links

    # Fetch the issues page
    response = await fetch_page_with_zenrows(main_url)
      # Check for valid response
    soup = BeautifulSoup(response.html, "html.parser")  # Parse HTML content

    # Extract issue links
    issue_list = soup.find("ul", class_="rlist list-of-issues__list")
    if issue_list:
        for li in issue_list.find_all("li"):
            link = li.find("a")  # Find the link to the issue
            if link and link.get("href"):
                full_url = base_url + link["href"]  # Combine base URL with relative link
                issue_links.append(full_url)  # Add to issue links list

    # Iterate through issues and extract article links
    for issue_url in issue_links:
        response = await fetch_page_with_zenrows(issue_url)  # Fetch the issue page
          # Check for valid response
        soup = BeautifulSoup(response.html, "html.parser")  # Parse content

        toc_sections = soup.find_all("section", class_="toc__section")  # Find the TOC sections
        for section in toc_sections:
            h2_tag = section.find("h2", class_="toc__heading__header top")
            if h2_tag and "Articles" in h2_tag.get_text(strip=True):  # Check if it's the article section
                toc_body = section.find("ul", class_="toc__body rlist clearfix")
                if toc_body:
                    article_items = toc_body.find_all("li", class_=["articleCitation", "articleCitation freeFeaturedContent"])
                    for li in article_items:
                        link = li.find("a")  # Find the article link
                        if link and link.get("href"):
                            full_url = base_url + link["href"]  # Construct full article URL
                            links.append(full_url)  # Add to the list of article links

    try:
        cursor = conn.cursor()  # Create database cursor
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}  # Get existing article links from the database

        # Filter out existing links and insert new links
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")  # Debugging message

        # Validate data using Pydantic
        data = NeuroscienceData(Journal=name, Article=Links)
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))  # Insert into DB
        await crawl_article_neuron(specialization)  # Crawl article content

        print(f"Successfully inserted {len(Links)} into the database")  # Success message
    except ValidationError as e:
        print(f"Validation error: {e}")  # Handle validation errors
        print(f"Failed to insert {Links} into the database")  # Debugging message

# Function to scrape articles from Brain journal
async def crawl_page_brain(conn):
    """
    Crawls articles from the Brain journal, processes them, and inserts them into the database.
    """
    name = 'Brain'  # Journal name
    base_url = "https://academic.oup.com"  # Base URL for Brain journal
    main_url = f"{base_url}/brain/issue"  # URL for the issue page
    links = []  # List to store article links

    try:
        links = []  # Initialize list of links
        pages = datetime.now().month  # Get the current month
        year = datetime.now().year  # Get the current year
        volume = int(year - 1877)  # Calculate the volume number

        for issues in range(1, pages + 1):
            issue_url = f"{main_url}/{volume}/{issues}"  # Construct issue URL
            print(f"Scraping issue: Volume {volume}, Issue {issues} at {issue_url}")  # Debugging
            try:
                response = await fetch_page_with_zenrows(issue_url)  # Fetch issue page
                  # Check for valid response
                soup = BeautifulSoup(response.html, "html.parser")  # Parse the page content

                # Locate the "Original Articles" section
                h4_tag = soup.find("h4", class_="title articleClientType act-header", string="Original Articles")
                if not h4_tag:
                    continue  # Skip if section is not found

                # Find the div containing article links
                content_div = h4_tag.find_next("div", class_="content al-article-list-group")
                if not content_div:
                    continue  # Skip if content div is not found

                # Extract article links
                article_divs = content_div.find_all("div", class_="al-article-item-wrap al-normal")
                for article_div in article_divs:
                    h5_tag = article_div.find("h5", class_="customLink item-title")
                    if h5_tag:
                        link = h5_tag.find("a", href=True)  # Find the article link
                        if link:
                            full_article_link = urljoin(base_url, link["href"])  # Construct full URL
                            links.append(full_article_link)  # Add to the list

            except requests.exceptions.RequestException as e:
                print(f"Error fetching {issue_url}: {e}")  # Handle request errors
                continue  # Skip to next issue

        # Insert the crawled links into the database
        try:
            cursor = conn.cursor()  # Create database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Get existing article links

            # Filter out existing links and insert the new ones
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")  # Debugging message

            # Validate and insert the data
            data = NeuroscienceData(Journal=name, Article=Links)
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_brain(specialization)  # Crawl article content and insert into Milvus

            print(f"Successfully inserted {len(Links)} into the database")  # Success message
        except ValidationError as e:
            print(f"Validation error: {e}")  # Handle validation errors
            print(f"Failed to insert {Links} into the database")  # Debugging message
    except Exception as e:
        print(f"Error occurred: {e}")  # General error handling

async def crawl_page_jneuro(conn):
    """
    Crawls articles from the Journal of Neuroscience RSS feed and inserts them into the database.
    """
    name = 'Journal of Neuroscience'  # Journal name
    base_url = "https://www.jneurosci.org/rss/recent.xml"  # RSS feed URL for Journal of Neuroscience
    links = []  # List to store article links

    try:
        # Fetch the RSS feed and parse it
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")
        
        # Extract article links from the RSS feed
        links = [
            item.find("link").text
            for item in soup.find_all("item")
            if item.find("link")
        ]
        
        # Insert the links into the database
        try:
            cursor = conn.cursor()  # Create database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Get existing article links

            # Filter out existing links and insert the new ones
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")  # Debugging message

            # Validate and insert data into the database
            data = NeuroscienceData(Journal=name, Article=Links)
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_jneuro(specialization)  # Crawl the article content

            print(f"Successfully inserted {len(Links)} into the database")  # Success message
        except ValidationError as e:
            print(f"Validation error: {e}")  # Handle validation errors
            print(f"Failed to insert {Links} into the database and CSV file.")  # Debugging message
    except Exception as e:
        print(f"Error occurred: {e}")  # Handle general errors

async def crawl_page_trends(conn):
    """
    Crawls articles from Trends in Neuroscience and inserts the new links into the database.
    """
    name = 'Trends in Neuroscience'  # Journal name
    base_url = "https://www.cell.com"  # Base URL for Trends in Neuroscience
    main_url = f"https://www.cell.com/trends/neurosciences/archive#"  # URL for the archive

    issue_links = []  # List to store issue links
    links = []  # List to store article links

    try:
        # Fetch the main archive page
        response = await fetch_page_with_zenrows(main_url)
          # Check for valid response
        soup = BeautifulSoup(response.html, "html.parser")  # Parse the content

        # Extract issue links from the archive page
        issue_list = soup.find("ul", class_="rlist list-of-issues__list")
        if issue_list:
            for li in issue_list.find_all("li"):
                link = li.find("a")  # Find the article link
                if link and link.get("href"):
                    full_url = base_url + link["href"]  # Construct full URL
                    issue_links.append(full_url)  # Add to issue links

        # Iterate over each issue and extract article links
        for issue_url in issue_links:
            response = await fetch_page_with_zenrows(issue_url)  # Fetch the issue page
              # Check for valid response
            soup = BeautifulSoup(response.html, "html.parser")  # Parse the content

            # Extract articles from each issue's table of contents (TOC)
            toc_sections = soup.find_all("section", class_="toc__section")
            for section in toc_sections:
                h2_tag = section.find("h2", class_="toc__heading__header top")
                if h2_tag and any(keyword in h2_tag.get_text(strip=True) for keyword in ["Articles", "Reviews", "Feature Review"]):
                    toc_body = section.find("ul", class_="toc__body rlist clearfix")
                    if toc_body:
                        article_items = toc_body.find_all("li", class_=["articleCitation", "articleCitation freeFeaturedContent"])
                        for li in article_items:
                            link = li.find("a")  # Find the article link
                            if link and link.get("href"):
                                full_url = base_url + link["href"]  # Construct full article URL
                                links.append(full_url)  # Add to the list of article links
    except Exception as e:
        print(f"Error occurred while scraping Trends in Neuroscience: {e}")
        return  # Exit the function in case of any error in the first block

    # Insert the crawled links into the database
    try:
        cursor = conn.cursor()  # Create database cursor
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}  # Get existing article links from the database

        # Filter out existing links and insert the new ones
        Links = [link for link in links if link not in existing_data]
        print(f"Updated links: {Links}")  # Debugging message

        # Validate and insert data into the database
        data = NeuroscienceData(Journal=name, Article=Links)
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_trends(specialization)  # Crawl the article content and insert into Milvus

        print(f"Successfully inserted {len(Links)} into the database")  # Success message
    except ValidationError as e:
        print(f"Validation error: {e}")  # Handle validation errors
        print(f"Failed to insert {Links} into the database")  # Debugging message


from bs4 import BeautifulSoup  # Importing BeautifulSoup for parsing HTML and XML documents
from utils import (
    insert_into_database,  # Function to insert data into the database
    fetch_page_with_zenrows,  # Function to fetch pages using the ZenRows API
    create_milvus_collection  # Function to create a Milvus collection for storing embeddings
)
from crawl_article.crawl_article_immunology_sql import (
    crawl_article_immunity,  # Function to crawl articles from Immunity journal
    crawl_article_jaci,  # Function to crawl articles from Journal of Allergy and Clinical Immunology
    crawl_article_natural,  # Function to crawl articles from Nature Reviews Immunology
    crawl_article_trends  # Function to crawl articles from Trends in Immunology
)
import re  # Importing regular expressions for pattern matching
from pydantic import BaseModel, ValidationError  # Importing Pydantic for data validation
from urllib.parse import urljoin, urlparse  # URL utilities for combining URLs and parsing URLs
from datetime import datetime  # Importing datetime for date handling

class ImmunologyData(BaseModel):
    """Pydantic model for validating immunology data"""
    Journal: str  # Journal name
    Article: list  # List of article URLs

specialization = "immunology"  # Specialization category for database

async def crawl_page_natural(conn):
    """
    Crawls articles from Nature Reviews Immunology via its website.
    Compares with existing articles in the database and inserts new ones.
    """
    name = 'Nature Reviews Immunology'  # Journal name
    base = 'https://www.nature.com/'  # Base URL for Nature Reviews Immunology
    base_url = 'https://www.nature.com/ni/volumes'  # URL for issues of the journal

    try:
        # Fetching and processing issues based on current month
        from datetime import datetime
        links = []  # List to store article links
        pages = datetime.now().month  # Current month to determine issues
        year = datetime.now().year  # Current year
        volume = int(year - 1999)  # Calculate the volume number

        for issues in range(1, pages + 1):  # Iterate through issues of the current volume
            url = f"{base_url}/{volume}/issues/{issues}"  # Construct issue URL
            print(f"Fetching : {url}")  # Debugging
            response = await fetch_page_with_zenrows(url)  # Fetch page using ZenRows API
              # Raise an exception if response is not OK
            soup = BeautifulSoup(response.html, "html.parser")  # Parse the HTML content
            
            # Extract article links from the "Articles" section
            section = soup.find("section", id="Articles")
            if section:
                ul = section.find("ul", class_="app-article-list-row")
                if ul:
                    list_items = ul.find_all("li", class_="app-article-list-row__item")
                    for li in list_items:
                        a_tag = li.find("a", class_="u-link-inherit")
                        if a_tag and a_tag.has_attr("href"):
                            full_url = urljoin(base, a_tag["href"])  # Construct full URL
                            links.append(full_url)  # Add the link to the list

        # Insert the crawled links into the database and write to CSV file
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Fetch existing links

            # Filter out already existing links
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")  # Print the number of new links

            # Validate the data using Pydantic model
            data = ImmunologyData(Journal=name, Article=Links)

            # Insert the validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_natural(specialization)  # Crawl the article content and insert into Milvus

            print(f"Successfully inserted {len(Links)} into the database")  # Success message

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handle validation errors
            print(f"Failed to insert {Links} into the database")

    except Exception as e:
        print(f"Error occurred: {e}")  # General error handling

async def crawl_page_immunity(conn):
    """
    Crawls articles from Immunity journal, processes and inserts into the database.
    """
    name = 'Immunity'  # Journal name
    base_url = "https://www.cell.com"  # Base URL for Immunity
    main_url = f"{base_url}/immunity/issues"  # URL for issues

    issue_links = []  # List to store issue links
    links = []  # List to store article links

    try:
        # Fetch the main issues page
        response = await fetch_page_with_zenrows(main_url)
          # Check if the response is successful
        soup = BeautifulSoup(response.html, "html.parser")  # Parse the content
        
        # Extract all issue links
        issue_list = soup.find("ul", class_="rlist list-of-issues__list")
        if issue_list:
            for li in issue_list.find_all("li"):
                link = li.find("a")  # Find the article link
                if link and link.get("href"):
                    full_url = base_url + link["href"]  # Construct full URL for issue
                    issue_links.append(full_url)

        # Iterate over the issues and extract article links
        for issue_url in issue_links:
            response = await fetch_page_with_zenrows(issue_url)  # Fetch the issue page
              # Check for valid response
            soup = BeautifulSoup(response.html, "html.parser")  # Parse the content
            
            # Extract articles from the table of contents (TOC)
            toc_sections = soup.find_all("section", class_="toc__section")
            for section in toc_sections:
                h2_tag = section.find("h2", class_="toc__heading__header top")
                if h2_tag and "Articles" in h2_tag.get_text(strip=True):
                    toc_body = section.find("ul", class_="toc__body rlist clearfix")
                    if toc_body:
                        article_items = toc_body.find_all("li", class_=["articleCitation", "articleCitation freeFeaturedContent"])
                        for li in article_items:
                            link = li.find("a")  # Find the article link
                            if link and link.get("href"):
                                full_url = base_url + link["href"]  # Construct full URL for the article
                                links.append(full_url)

        # Insert the crawled links into the database
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Get existing article links

            # Filter out existing links and insert the new ones
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate and insert data
            data = ImmunologyData(Journal=name, Article=Links)
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_immunity(specialization)  # Crawl the article content

            print(f"Successfully inserted {len(Links)} into the database")  # Success message

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handle validation errors
            print(f"Failed to insert {Links} into the database")

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle fetch errors

async def crawl_page_annual(conn):
    """
    Crawls articles from the Annual Review of Immunology RSS feed and inserts them into the database.
    """
    name = 'Annual Review of Immunology'  # Journal name
    base_url = "https://www.annualreviews.org/rss/content/journals/immunol/latestarticles?fmt=rss"  # RSS feed URL
    links = []  # List to store article links

    try:
        # Fetch the RSS feed and parse it
        response = await fetch_page_with_zenrows(base_url)
        soup = BeautifulSoup(response.html, "html.parser")
        
        # Extract article links from the feed
        links = [
            item.find("link").text
            for item in soup.find_all("item")
            if item.find("link")
        ]
        
        # Insert the links into the database
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Get existing article links

            # Filter out existing links and insert the new ones
            Links = [link for link in links if link not in existing_data]
            print(f"Updated links: {Links}")

            # Validate and insert data
            data = ImmunologyData(Journal=name, Article=Links)
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_page_annual(specialization)
            print(f"Successfully inserted {len(Links)} into the database")  # Success message

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handle validation errors
            print(f"Failed to insert {Links} into the database and CSV file")

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle general errors

async def crawl_page_immu_trends(conn):
    """
    Crawls articles from Trends in Immunology, extracting article links and inserting them into the database.
    """
    name = 'Trends in Immunology'  # Journal name
    base_url = "https://www.cell.com"  # Base URL for Trends in Immunology
    main_url = f"https://www.cell.com/immunology/archive?isCoverWidget=true"  # URL for archived issues
    
    issue_links = []  # List to store issue links
    links = []  # List to store final article links

    try:
        # Fetch main page for issue links
        response = await fetch_page_with_zenrows(main_url)
          # Check for valid response
        soup = BeautifulSoup(response.html, "html.parser")  # Parse the content

        # Extract issue links
        issue_list = soup.find("ul", class_="rlist list-of-issues__list")
        if issue_list:
            for li in issue_list.find_all("li"):
                link = li.find("a")  # Find the article link
                if link and link.get("href"):
                    full_url = base_url + link["href"]  # Construct full URL for the issue
                    issue_links.append(full_url)

        # Extract articles from each issue
        for issue_url in issue_links:
            print(f"fetch issue: {issue_url}")  # Debugging
            response = await fetch_page_with_zenrows(issue_url)  # Fetch issue page
              # Check for valid response
            soup = BeautifulSoup(response.html, "html.parser")  # Parse the content

            # Extract articles from the table of contents (TOC)
            toc_sections = soup.find_all("section", class_="toc__section")
            for section in toc_sections:
                h2_tag = section.find("h2", class_="toc__heading__header top")
                if h2_tag and any(keyword in h2_tag.get_text(strip=True) for keyword in ["Articles", "Reviews", "Feature Review"]):
                    print(h2_tag.get_text(strip=True))  # Debugging
                    toc_body = section.find("ul", class_="toc__body rlist clearfix")
                    if toc_body:
                        article_items = toc_body.find_all("li", class_=["articleCitation", "articleCitation freeFeaturedContent"])
                        for li in article_items:
                            link = li.find("a")  # Find the article link
                            if link and link.get("href"):
                                full_url = base_url + link["href"]  # Construct full URL for the article
                                links.append(full_url)

        # Insert the crawled links into the database
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Get existing article links

            Links = [link for link in links if link not in existing_data]  # Filter out existing links
            print(f"Updated links: {Links}")  # Debugging

            # Validate and insert data
            data = ImmunologyData(Journal=name, Article=Links)
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_trends(specialization)  # Crawl article content and insert into Milvus

            print(f"Successfully inserted {len(Links)} into the database")  # Success message

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handle validation errors
            print(f"Failed to insert {Links} into the database")

    except Exception as e:
        print(f"Error occurred: {e}")  # Handle fetch errors

async def crawl_page_jaci(conn):
    """
    Crawls articles from Journal of Allergy and Clinical Immunology and inserts them into the database.
    """
    name = "Journal of Allergy and Clinical Immunology"  # Journal name
    base_url = "https://www.jacionline.org"  # Base URL for the journal
    main_url = f"{base_url}/issues"  # URL for issues page

    current_year = datetime.now().year  # Get current year
    current_month = datetime.now().month  # Get current month
    base_num = 151 + 2 * (current_year - 2023)  # Calculate the base volume number
    num = base_num if current_month <= 6 else base_num + 1  # Adjust volume number depending on the month
    group_id = f"d2020.v{num}"  # Construct group ID for the issue
    unique_links = set()  # Set to store unique issue links
    issue_url = f"{main_url}?publicationCode=ymai&issueGroupId={group_id}"  # Construct issue URL

    try:
        # Fetch the main page for issues
        response = await fetch_page_with_zenrows(issue_url)
        soup = BeautifulSoup(response.html, "html.parser")

        # Extract issue links from the page
        div = soup.find("div", {
            "data-groupid": group_id,
            "class": "list-of-issues__group list-of-issues__group--issues js--open"
        })

        if div:
            links = div.find_all("a", href=True)
            for link in links:
                href = urljoin(base_url, link["href"])  # Construct full URL for the issue
                if href not in unique_links:
                    print(f"Found issue : {href}")  # Debugging
                    unique_links.add(href)

    except Exception as e:
        print(f"Error fetching data for group ID {group_id}: {e}")  # Handle fetch errors

    # Crawl and extract articles from each issue
    article_links = set()
    processed_article_ids = set()  # Set to track processed articles

    stop_words = ["brief reports", "corrigendum", "frontmatter"]  # Stop words to halt scraping

    for issue_link in unique_links:
        try:
            print(f"Fetching: {issue_link}")  # Debugging
            response = await fetch_page_with_zenrows(issue_link)  # Fetch the issue page
            page_soup = BeautifulSoup(response.html, "html.parser")  # Parse the page content

            # Iterate over sections in the table of contents
            sections = page_soup.find_all("section", {"class": "toc__section"})
            for section in sections:
                h2_tag = section.find("h2", class_="toc__heading__header top")
                if h2_tag:
                    h2_text = h2_tag.get_text(strip=True).lower()

                    # Stop processing if the section matches a stop word
                    if any(re.search(rf"\b{word}s?\b", h2_text) for word in stop_words):
                        print(f"Stopping at section: {h2_text}")  # Debugging
                        break
                    print(f"Processing section: {h2_text}")  # Debugging
                    # Process articles under the current section
                    for li in section.find_all("li"):
                        article_link = li.find("a", href=True)  # Find the article link
                        if article_link:
                            href = article_link["href"]
                            parsed_url = urlparse(href)
                            path_parts = parsed_url.path.split("/")

                            # Process valid article links and track their identifiers
                            if "article" in path_parts:
                                identifier_index = path_parts.index("article") + 1
                                if identifier_index < len(path_parts):
                                    current_article_id = path_parts[identifier_index]

                                    if current_article_id in processed_article_ids:
                                        continue  # Skip if already processed
                                    processed_article_ids.add(current_article_id)

                                    full_article_link = urljoin(base_url, href)  # Construct full URL
                                    article_links.add(full_article_link)  # Add to the list of article links
                                    print(f"Added: {full_article_link}")  # Debugging
        except Exception as e:
            print(f"Error fetching article links from {issue_link}: {e}")  # Handle fetch errors

    # Convert the set of article links into a list
    links = list(article_links)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}  # Get existing article links

        Links = [link for link in links if link not in existing_data]  # Filter out existing links
        print(f"Updated links: {Links}")  # Debugging

        # Validate and insert data
        data = ImmunologyData(Journal=name, Article=Links)
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_jaci(specialization)  # Crawl article content and insert into Milvus

        print(f"Successfully inserted {len(Links)} into the database")  # Success message
    except ValidationError as e:
        print(f"Validation error: {e}")  # Handle validation errors
        print(f"Failed to insert {Links} into the database")

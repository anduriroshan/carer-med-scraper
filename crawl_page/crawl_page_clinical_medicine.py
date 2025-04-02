from bs4 import BeautifulSoup  # Importing BeautifulSoup for parsing HTML and XML documents
from utils import (  # Importing utility functions for database operations and web scraping
    insert_into_database,
    fetch_page_with_zenrows,
)
from crawl_article.crawl_article_clinical_medicine_sql import (  # Importing functions for crawling articles from specific journals
    crawl_article_aoim,
    crawl_article_bmj,
    crawl_article_JAMA,
    crawl_article_Lancet,
    crawl_article_nejm
)

from bs4 import BeautifulSoup  # Importing BeautifulSoup again (this is redundant and can be removed)
import re  # Importing regular expressions for string manipulation
from pydantic import BaseModel, ValidationError  # Importing Pydantic for data validation
from datetime import datetime  # Importing datetime for handling date and time

class ClinicalData(BaseModel):  # Defining a Pydantic model for clinical data
    Journal: str  # Journal name
    Article: list  # List of articles

specialization = "clinical_medicine"  # Defining the specialization


async def crawl_page_lancet(conn):  # Function to crawl articles from The Lancet
    base_url = "https://www.thelancet.com"  # Base URL for The Lancet
    name = 'The Lancet'  # Journal name
    main_url = f"{base_url}/journals/lancet/issues"  # URL for the main issues page

    issue_links = []  # List to store issue links
    article_links = []  # List to store article links

    try:
        # Step 1: Fetch the main issues page
        response = await fetch_page_with_zenrows(main_url)  # Fetching the main issues page
          # Raise an error for bad responses
        soup = BeautifulSoup(response.html, "html.parser")  # Parsing the HTML content

        # Step 2: Extract issue links
        issue_list = soup.find("ul", class_="rlist list-of-issues__list")  # Finding the list of issues
        if issue_list:
            for li in issue_list.find_all("li"):  # Iterating through each issue
                link = li.find("a")  # Finding the link for the issue
                if link and link.get("href"):
                    full_url = base_url + link["href"]  # Constructing the full URL
                    issue_links.append(full_url)  # Adding the link to the list
            print(f"Extracted {len(issue_links)} issue links.")  # Printing the number of extracted links
        else:
            print("No issues found on the main page.")  # Handling case where no issues are found
        print(issue_links)  # Printing the extracted issue links

        # Step 3: Extract article links for each issue
        for issue_url in issue_links:
            count = 0  # Counter for articles
                
            response = await fetch_page_with_zenrows(issue_url)  # Fetching the issue page
              # Raise an error for bad responses
            soup = BeautifulSoup(response.html, "html.parser")  # Parsing the HTML content

            # Locate the specific toc__section containing the h2 header with text "Articles"
            toc_sections = soup.find_all("section", class_="toc__section")  # Finding table of contents sections
            for section in toc_sections:
                h2_tag = section.find("h2", class_="toc__section__header toc__section__header--A top")  # Finding the header
                if h2_tag and "Articles" in h2_tag.get_text(strip=True):  # Checking if it's the correct section
                    # Found the correct section, get the list of articles
                    toc_body = section.find("ul", class_="toc__body rlist clearfix")  # Finding the body of the TOC
                    if toc_body:
                        article_items = toc_body.find_all("li", class_=["articleCitation", "articleCitation freeFeaturedContent"])  # Finding article items

                        for li in article_items:
                            # Extract the link from the <a> tag
                            link = li.find("a")  # Finding the article link
                            if link and link.get("href"):
                                full_url = base_url + link["href"]  # Constructing the full URL
                                print(full_url)  # Printing the article link
                                article_links.append(full_url)  # Adding the link to the list
                                count += 1  # Incrementing the counter

            print(f"Extracted {count} articles from {issue_url}.")  # Printing the number of articles extracted
        try:
            cursor = conn.cursor()  # Creating a database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = 'The Lancet'")  # Fetching existing article links
            existing_data = {row[0] for row in cursor.fetchall()}  # Storing existing links in a set

            # Update the database with new links by comparing with existing data
            Links = [link for link in article_links if link not in existing_data]  # Filtering new links
            print(f"Updated links: {Links}")  # Printing updated links

            # Validate the data using Pydantic model
            data = ClinicalData(
                Journal=name,  # Setting the journal name
                Article=Links,  # Setting the list of articles
            )
            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))  # Inserting data into the database

            # Crawling the article content and inserting into the milvus database
            await crawl_article_Lancet(specialization)  # Crawling articles from The Lancet
            print(f"Successfully inserted {len(Links)} into the database")  # Printing success message

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handling validation errors
            print(f"Failed to insert {article_links} into the database and CSV file.")  # Printing failure message
            return
    except Exception as e:
        print(f"Error occurred: {e}")  # Handling general errors

async def crawl_page_jama(conn):  # Function to crawl articles from JAMA
    base_url = "https://jamanetwork.com/journals/jama/issue"  # Base URL for JAMA
    name = "JAMA: Journal of the American Medical Association"  # Journal name

    try:
        links = []  # List to store article links
        current_year = datetime.now().year  # Getting the current year
        current_month = datetime.now().month  # Getting the current month
        base_num =  2 * (current_year) - 3717 # Calculating the base number for the volume
        volume = base_num if current_month <= 6 else base_num + 1 # Calculating the volume number
        start_issue = (current_month - 1) * 4 + 1 # Calculating the start issue number
        end_issue = start_issue + 3 # Calculating the end issue number
        print(f"Fetching issues {start_issue} to {end_issue} for Volume {volume}")

        for issue in range(start_issue, end_issue + 1):     # Looping through issues
            url = f"{base_url}/{volume}/{issue}"  # Constructing the URL for each issue
            print(f"Fetching: {url}")  # Printing the URL being fetched

            try:
                response = await fetch_page_with_zenrows(url)  # Fetching the issue page

                # Check if the response status is 200
                if response.status_code == 200:
                    soup = BeautifulSoup(response.html, "html.parser")  # Parsing the HTML content

                    # Extract article links
                    elements = soup.select("#original-investigation > div > div > h3")  # Selecting article elements
                    if elements:
                        for element in elements:
                            link = element.find("a")  # Finding the article link
                            if link:
                                article_url = link.get("href").strip()  # Getting the article URL
                                links.append(article_url)  # Adding the link to the list
                                print(f"Found article: {article_url}")  # Printing the found article link
                else:
                    print(f"Skipping {url}: Status code {response.status_code}")  # Handling non-200 responses
                    continue  # Skip to the next iteration if response is not 200
            except Exception as fetch_error:
                print(f"Error fetching {url}: {fetch_error}")  # Handling fetch errors
                continue  # Skip to the next iteration if an error occurs during the fetch

        # Print the total number of links found
        print(f"Total links found for {name}: {len(links)}")  # Printing the total number of links found
        try:
            cursor = conn.cursor()  # Creating a database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = 'The Lancet'")  # Fetching existing article links
            existing_data = {row[0] for row in cursor.fetchall()}  # Storing existing links in a set

            # Update the database with new links by comparing with existing data
            Links = [link for link in links if link not in existing_data]  # Filtering new links
            print(f"Updated links: {Links}")  # Printing updated links

            # Validate the data using Pydantic model
            data = ClinicalData(
                Journal=name,  # Setting the journal name
                Article=Links,  # Setting the list of articles
            )
            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))  # Inserting data into the database

            # Crawling the article content and inserting into the milvus database
            await crawl_article_JAMA(specialization)  # Crawling articles from The Lancet
            print(f"Successfully inserted {len(Links)} into the database")  # Printing success message

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handling validation errors
            print(f"Failed to insert {links} into the database and CSV file.")  # Printing failure message
            return
    except Exception as e:
        print(f"Error occurred: {e}")  # Handling general errors


async def crawl_page_nejm(conn):  # Function to crawl articles from NEJM
    name = "New England Journal of Medicine"  # Journal name
    base_url = "https://onesearch-rss.nejm.org/api/specialty/rss?context=nejm&specialty=clinical-medicine"  # Base URL for NEJM
    try:
        response = await fetch_page_with_zenrows(base_url)  # Fetching the RSS feed
        soup = BeautifulSoup(response.html, "html.parser")  # Parsing the XML content
        # Extract and process each item in the RSS feed
        links = [
            item.find("link").text  # Extracting links from the feed
            for item in soup.find_all("item")
            if item.find("link")  # Ensuring the link exists
        ]
        try:
            cursor = conn.cursor()  # Creating a database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))  # Fetching existing article links
            existing_data = {row[0] for row in cursor.fetchall()}  # Storing existing links in a set

            # Update the database with new links by comparing with existing data
            Links = [link for link in links if link not in existing_data]  # Filtering new links
            print(f"Updated links: {Links}")  # Printing updated links

            # Validate the data using Pydantic model
            data = ClinicalData(
                Journal=name,  # Setting the journal name
                Article=Links,  # Setting the list of articles
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))  # Inserting data into the database
            await crawl_article_nejm(specialization)  # Crawling articles from NEJM
            print(f"Successfully inserted {len(Links)} into the database")  # Printing success message
        except ValidationError as e:
            print(f"Validation error: {e}")  # Handling validation errors
            print(f"Failed to insert {Links} into the database and CSV file.")  # Printing failure message

    except Exception as e:
        print(f"Error occurred: {e}")  # Handling general errors


async def crawl_page_bmj(conn):  # Function to crawl articles from BMJ
    base_url = r"https://www.bmj.com/search/advanced/toc_section%3AResearch%20numresults%3A100%20sort%3Apublication-date%20direction%3Adescending%20format_result%3Astandard"  # Base URL for BMJ
    name = "BMJ (British Medical Journal)"  # Journal name
    
    links = []  # List to store article links

    try:
        for page in range(0, 1):  # Looping through pages
            url = f"{base_url}?page={page}"  # Constructing the URL for each page
            print(f"Fetching: {url}")  # Printing the URL being fetched

            response = await fetch_page_with_zenrows(url)  # Fetching the page
              # Raise an error for bad responses

            soup = BeautifulSoup(response.html, "html.parser")  # Parsing the HTML content

            # Find all `cite` tags with the specified class
            cite_tags = soup.find_all("cite", class_="highwire-cite highwire-citation-bmj-search")  # Finding citation tags
            for cite in cite_tags:
                h4_tag = cite.find("h4")  # Finding the header tag
                if h4_tag and "Research" in h4_tag.get_text(strip=True):  # Checking if it's a research article
                    link = cite.find("a")  # Finding the article link
                    if link and link.get("href"):
                        article_url = "https://www.bmj.com" + link.get("href").strip()  # Constructing the full URL
                        links.append(article_url)  # Adding the link to the list
                        print(f"Found article: {article_url}")  # Printing the found article link

        try:
            cursor = conn.cursor()  # Creating a database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))  # Fetching existing article links
            existing_data = {row[0] for row in cursor.fetchall()}  # Storing existing links in a set

            # Update the database with new links by comparing with existing data
            Links = [link for link in links if link not in existing_data]  # Filtering new links
            print(f"Updated links: {Links}")  # Printing updated links

            # Validate the data using Pydantic model
            data = ClinicalData(
                Journal=name,  # Setting the journal name
                Article=Links,  # Setting the list of articles
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))  # Inserting data into the database
            await crawl_article_bmj(specialization)  # Crawling articles from BMJ
            print(f"Successfully inserted {len(Links)} into the database")  # Printing success message
        except ValidationError as e:
            print(f"Validation error: {e}")  # Handling validation errors
            print(f"Failed to insert {Links} into the database and CSV file.")  # Printing failure message

    except Exception as e:
        print(f"Error occurred: {e}")  # Handling general errors
        return None


async def crawl_page_aoim(conn):  # Function to crawl articles from Annals of Internal Medicine
    name = "Annals of Internal Medicine"  # Journal name
    base_url = "https://www.acpjournals.org/action/showFeed?type=etoc&feed=rss&jc=aim"  # Base URL for Annals of Internal Medicine
    try:
        response = await fetch_page_with_zenrows(base_url)  # Fetching the RSS feed
        soup = BeautifulSoup(response.html, "html.parser")  # Parsing the XML content
        # Extract and process each item in the RSS feed
        links = [
            item.find("link").text  # Extracting links from the feed
            for item in soup.find_all("item")
            if item.find("link")  # Ensuring the link exists
        ]
        try:
            cursor = conn.cursor()  # Creating a database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))  # Fetching existing article links
            existing_data = {row[0] for row in cursor.fetchall()}  # Storing existing links in a set

            # Update the database with new links by comparing with existing data
            Links = [link for link in links if link not in existing_data]  # Filtering new links
            print(f"Updated links: {Links}")  # Printing updated links

            # Validate the data using Pydantic model
            data = ClinicalData(
                Journal=name,  # Setting the journal name
                Article=Links,  # Setting the list of articles
            )

            # Insert the validated data into the database
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))  # Inserting data into the database
            await crawl_article_aoim(specialization)  # Crawling articles from Annals of Internal Medicine
            print(f"Successfully inserted {len(Links)} into the database")  # Printing success message
        except ValidationError as e:
            print(f"Validation error: {e}")  # Handling validation errors
            print(f"Failed to insert {Links} into the database")  # Printing failure message

    except Exception as e:
        print(f"Error occurred: {e}")  # Handling general errors

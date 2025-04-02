from bs4 import BeautifulSoup  # Importing BeautifulSoup for parsing HTML and XML documents
from utils import (  # Importing utility functions for database operations and web scraping
    insert_into_database,
    fetch_page_with_zenrows,
)
from crawl_article.crawl_article_cardiology_sql import (  # Importing functions for processing crawled articles
    crawl_article_circulation,
    crawl_article_ehj,
    crawl_article_jacc,
    crawl_article_cardio_research,
    crawl_article_heart,
)
from pydantic import BaseModel, ValidationError  # Importing Pydantic for data validation
from datetime import datetime  # Importing datetime for handling date and time


class CirculationArticle(BaseModel):  # Defining a Pydantic model for validating article data
    Journal: str  # Journal name
    Articles: list  # List of article URLs


specialization = 'cardiology'  # Defining the specialization


async def fetch_new_circulation_articles(conn):  # Function to fetch new articles from Circulation Journal
    name = 'Circulation'  # Journal name
    base_url = "https://www.ahajournals.org/toc/circ"  # Base URL for Circulation journal

    try:
        links = []  # List to store extracted article links
        cursor = conn.cursor()  # Creating a database cursor
        year = datetime.now().year  # Getting the current year
        volume = int(year - 1874)  # Calculating the journal volume based on year

        for issues in range(1, 26):  # Iterating through 25 issues
            url = base_url + f"/{volume}" + f"/{issues}"  # Constructing the URL for each issue
            print(f"Fetching issue: {url}")  # Debugging: Printing the issue URL being fetched
            
            try:
                response = await fetch_page_with_zenrows(url)  # Fetching the issue page
                if response.status_code == 200:  # Proceed only if the response is successful
                    soup = BeautifulSoup(response.html, "html.parser")  # Parsing the HTML content
                    elements = soup.select("#frmIssueItems > section > div > div > div > h2")  # Selecting article elements
                    
                    if elements:
                        for element in elements:
                            link = element.find("a")  # Finding the article link
                            if link:
                                complete_link = base_url[:-9] + link.get("href")  # Constructing the full article URL
                                links.append(complete_link)  # Adding to the list of extracted links
                                print(complete_link)  # Debugging: Printing the article link
                else:
                    print(f"Skipping issue {issues} due to non-200 response code: {response.status_code}")  # Handling unsuccessful responses
            except Exception as issue_error:
                print(f"Error while fetching issue {issues}: {issue_error}")  # Handling exceptions during fetch

        print(f"Total links found for {name}: {len(links)}")  # Printing the total number of extracted links
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))  # Fetching existing article links from database
        existing_data = {row[0] for row in cursor.fetchall()}  # Storing existing links in a set

        # Filtering out already existing links
        Links = [link for link in links if link not in existing_data]  
        print(f"Updated links: {len(Links)}")  # Printing new links count

        # Inserting new article links into the database
        insert_into_database(conn, name, Links, specialization, len(Links))  
        await crawl_article_circulation(specialization)  # Crawling article content
    except Exception as e:
        print(f"Error occurred: {e}")  # Handling general errors


async def fetch_new_european_heart_journal_articles(conn):  # Function to fetch articles from European Heart Journal
    name = "European Heart Journal"  # Journal name
    base_url = "https://academic.oup.com/rss/site_5375/3236.xml"  # RSS feed URL

    try:
        links = set()  # Using a set to store unique links
        response = await fetch_page_with_zenrows(base_url)  # Fetching the RSS feed
        soup = BeautifulSoup(response.html, "html.parser")  # Parsing the XML response

        # Extracting article links from RSS feed
        links = {item.find("link").text for item in soup.find_all("item") if item.find("link")}  

        try:
            cursor = conn.cursor()  # Creating a database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))  # Fetching existing article links
            existing_data = {row[0] for row in cursor.fetchall()}  # Storing existing links in a set

            # Filtering out already existing links
            Links = [link for link in links if link not in existing_data]  
            print(f"Updated links: {len(Links)}")  # Printing new links count

            # Validating extracted links using Pydantic model
            data = CirculationArticle(Journal=name, Articles=Links)

            # Inserting the validated data into the database
            insert_into_database(conn, data.Journal, data.Articles, specialization, len(Links))  
            await crawl_article_ehj(specialization)  # Crawling article content
            print(f"Successfully inserted {len(Links)} new articles into the database")  

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handling validation errors
            print(f"Failed to insert {links} into the database")  
    except Exception as e:
        print(f"Error fetching or processing links for {name}: {e}")  # Handling fetch errors


async def fetch_new_jacc_articles(conn):  # Function to fetch new articles from JACC
    base_url = "https://www.jacc.org/toc/jacc"  # Base URL for JACC
    req_str = "https://www.jacc.org/doi/"  # DOI link pattern
    name = "JACC: Journal of the American College of Cardiology"  # Journal name
    
    try:
        links = []  # List to store extracted article links
        current_year = datetime.now().year  # Getting the current year
        current_month = datetime.now().month  # Getting the current month
        base_num = 2 * current_year - 3965  # Calculating volume number
        volume = base_num if current_month <= 6 else base_num + 1  # Adjusting volume based on month

        for issue in range(1, 26):  # Loop through 1 to 25 issues
            url = f"{base_url}/{volume}/{issue}"  # Constructing issue URL
            print(f"Fetching issue: {url}")  # Debugging: Printing the issue URL being fetched
            
            response = await fetch_page_with_zenrows(url)  # Fetching the issue page

            # Handling non-200 responses
            if response is None or response.status_code != 200:
                print(f"Skipping issue {issue} due to non-200 response: {response.status_code if response else 'No Response'}")
                break  

            soup = BeautifulSoup(response.html, "html.parser")  # Parsing the HTML content
            elements = soup.select(
                "body > div > div > div > main > div > div > div > div > section > section > div > div > h4"
            )  # Selecting article elements

            if elements:
                for element in elements:
                    link = element.find("a")  # Finding the article link
                    if link and link.get("href"):
                        updated_link = "https://www.jacc.org" + link.get("href")  # Constructing the full article URL
                        print("Link found: " + updated_link)  # Debugging: Printing the extracted link

                        if req_str in updated_link:  # Checking if the link follows DOI pattern
                            links.append(updated_link)  # Adding to the list of extracted links

        print(f"Total links found for {name}: {len(links)}")  # Printing total extracted links
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        # Filtering out already existing links
        Links = [link for link in links if link not in existing_data]  
        print(f"Updated links: {len(Links)}")  # Printing new links count

        # Inserting into database
        insert_into_database(conn, name, Links, specialization, len(Links))  
        await crawl_article_jacc(specialization)  # Crawling article content

    except Exception as e:
        print(f"Error fetching links for {name}: {e}")  # Handling fetch errors

async def fetch_new_academic_oup_articles(conn):  # Function to fetch new articles from Cardiovascular Research journal
    name = "Cardiovascular Research"  # Journal name
    base_url = "https://academic.oup.com/rss/site_5369/3230.xml"  # RSS feed URL for the journal

    try:
        links = set()  # Using a set to store unique links
        response = await fetch_page_with_zenrows(base_url)  # Fetching the RSS feed
        soup = BeautifulSoup(response.html, "html.parser")  # Parsing the XML response

        # Extracting article links from RSS feed
        links = {item.find("link").text for item in soup.find_all("item") if item.find("link")}

        try:
            cursor = conn.cursor()  # Creating a database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))  
            existing_data = {row[0] for row in cursor.fetchall()}  # Storing existing links in a set

            # Filtering out already existing links
            Links = [link for link in links if link not in existing_data]  
            print(f"Updated links: {len(Links)}")  # Printing new links count

            # Validating extracted links using Pydantic model
            data = CirculationArticle(Journal=name, Articles=Links)

            # Inserting the validated data into the database
            insert_into_database(conn, data.Journal, data.Articles, specialization, len(Links))  
            await crawl_article_cardio_research(specialization)  # Crawling article content
            print(f"Successfully inserted {len(Links)} new articles into the database")

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handling validation errors

    except Exception as e:
        print(f"Error fetching or processing links for {name}: {e}")  # Handling fetch errors


async def fetch_new_heart_bmj_articles(conn):  # Function to fetch new articles from the Heart journal (BMJ)
    name = "Heart"  # Journal name
    base_url = "https://heart.bmj.com/rss/current.xml"  # RSS feed URL for the journal

    try:
        links = set()  # Using a set to store unique links
        response = await fetch_page_with_zenrows(base_url)  # Fetching the RSS feed
        soup = BeautifulSoup(response.html, "html.parser")  # Parsing the XML response

        # Extracting article links from RSS feed
        links = {item.find("link").text for item in soup.find_all("item") if item.find("link")}

        try:
            cursor = conn.cursor()  # Creating a database cursor
            cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
            existing_data = {row[0] for row in cursor.fetchall()}  # Storing existing links in a set

            # Filtering out already existing links
            Links = [link for link in links if link not in existing_data]  
            print(f"Updated links: {len(Links)}")  # Printing new links count

            # Validating extracted links using Pydantic model
            data = CirculationArticle(Journal=name, Articles=Links)

            # Inserting the validated data into the database
            insert_into_database(conn, data.Journal, data.Articles, specialization, len(Links))  
            await crawl_article_heart(specialization)  # Crawling article content
            print(f"Successfully inserted {len(Links)} new articles into the database")  

        except ValidationError as e:
            print(f"Validation error: {e}")  # Handling validation errors

    except Exception as e:
        print(f"Error fetching or processing links for {name}: {e}")  # Handling fetch errors

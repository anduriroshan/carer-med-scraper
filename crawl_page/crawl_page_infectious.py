from utils import (
    insert_into_database,
    fetch_page_with_scraper_api,
    fetch_page_with_zenrows,
    create_milvus_collection,
    connection_config
)
from crawl_article.crawl_article_infectious_sql import (
    crawl_article_lancet_id,
    crawl_article_clincal_id,
    crawl_article_infection,
    crawl_article_cdc,
    crawl_article_journal_id

)

from bs4 import BeautifulSoup
import re
from pydantic import BaseModel, ValidationError
from datetime import datetime
from urllib.parse import urljoin,urlparse
from utils import setup_database
class InfectiousData(BaseModel):
    Journal: str
    Article: list

specialization = "infectious_diseases"

async def crawl_page_lancet_id(conn):
    name="The Lancet Infectious Diseases"
    base_url = "https://www.thelancet.com"
    main_url = f"{base_url}/journals/laninf/issues"
    issue_links = []
    article_links = []

    try:
        # Step 1: Fetch the main issues page
        response = await fetch_page_with_zenrows(main_url)
        
        soup = BeautifulSoup(response.html, "html.parser")

        # Step 2: Extract issue links
        issue_list = soup.find("ul", class_="rlist list-of-issues__list")
        if issue_list:
            for li in issue_list.find_all("li"):    
                link = li.find("a")
                if link and link.get("href"):
                    full_url = base_url + link["href"]
                    issue_links.append(full_url)
            print(f"Extracted {len(issue_links)} issue links.")
        else:
            print("No issues found on the main page.")

        print(f"Issue links: {issue_links}")

        # Step 3: Extract article links for each issue
        for issue_url in issue_links:
            count = 0
            response = await fetch_page_with_zenrows(issue_url)
            
            soup = BeautifulSoup(response.html, "html.parser")

            # Locate the specific toc__section containing the h2 header with text "Articles"
            toc_sections = soup.find_all("section", class_="toc__section")
            for section in toc_sections:
                h2_tag = section.find("h2", class_="toc__section__header toc__section__header--A top")
                if h2_tag and "Articles" in h2_tag.get_text(strip=True):
                    # Found the correct section, get the list of articles
                    toc_body = section.find("ul", class_="toc__body rlist clearfix")
                    if toc_body:
                        article_items = toc_body.find_all("li", class_=["articleCitation", "articleCitation freeFeaturedContent"])

                        for li in article_items:
                            # Extract the link from the <a> tag
                            link = li.find("a")
                            if link and link.get("href"):
                                full_url = base_url + link["href"]
                                article_links.append(full_url)
                                print(f"Found article : {full_url}")
                                count += 1

            print(f"Extracted {count} articles from {issue_url}.")
    except Exception as e:
        print(f"Error occurred: {e}")
    # Insert into database
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s", (name,))
        existing_data = {row[0] for row in cursor.fetchall()}

        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")

        data = InfectiousData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_lancet_id(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_clinical_id(conn):
    name = 'Clinical Infectious Diseases'
    base_url = "https://academic.oup.com"
    main_url = f"{base_url}/cid/issue"
    article_links = []
    current_year = datetime.now().year
    current_month = datetime.now().month
    base_num = 2*current_year - 3970
    volume = base_num if current_month <= 6 else base_num + 1

    for issue in range(1,current_month*2+1):
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

        data = InfectiousData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_clincal_id(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page__journal_id(conn):
    name = 'Journal of Infectious Diseases'
    base_url = "https://academic.oup.com"
    main_url = f"{base_url}/jid/issue"
    article_links = []
    current_year = datetime.now().year
    current_month = datetime.now().month
    base_num = 2*current_year - 3818
    volume = base_num if current_month <= 6 else base_num + 1 
    for issue in range(1, 7):  # Issues 1 to 12
        issue_url = f"{main_url}/{volume}/{issue}"
        print(f"Scraping :  {issue_url}")
        try:
            response = await fetch_page_with_zenrows(issue_url)
            if not response or response.status_code != 200:
                continue
            soup = BeautifulSoup(response.html, "html.parser")

            # Locate all section tags inside section-container
            sections = soup.find("div", class_="section-container")
            
            for section in sections.find_all("section"):
                h4_tag = section.find("h4")
                if h4_tag and any(keyword in h4_tag.text for keyword in ["MAJOR ARTICLES AND BRIEF REPORTS", "articles"]):
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

        data = InfectiousData(
            Journal=name,
            Article=Links,
        )
        
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_journal_id(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def crawl_page_infection(conn):
    name='Infection'
    base_url='https://link.springer.com/journal/15010/volumes-and-issues'
    req_str="https://link.springer.com/article"
    try:
        from datetime import datetime

        links = []
        volume = int(datetime.now().year) - 1972
        for issue in range(1,7):
            url = base_url + f"/{volume}" + f"-{issue}"
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
            data = InfectiousData(
                Journal=name,
                Article=Links,
            )

            # Insert the validated data into the database and write to CSV file
            insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
            await crawl_article_infection(specialization)
            print(f"Successfully inserted {len(Links)} into the database")
        except ValidationError as e:
            print(f"Validation error: {e}")
            print(f"Failed to insert {Links} into the database and CSV file.")

    except Exception as e:
        print(f"Error occurred: {e}")

async def crawl_page_cdc(conn):
    base_url = "https://wwwnc.cdc.gov/eid/rss/upcoming.xml"
    main_url= "https://wwwnc.cdc.gov"
    name = "Emerging Infectious Diseases"
    article_links = []
# Fetch the RSS feed
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
        Links = [link for link in article_links if link not in existing_data]
        print(f"Updated links: {Links}")
        data = InfectiousData(Journal=name, Article=Links)
        insert_into_database(conn, data.Journal, data.Article, specialization, len(Links))
        await crawl_article_cdc(specialization)
    except Exception as e:
        print(f"Error inserting data into database: {e}")

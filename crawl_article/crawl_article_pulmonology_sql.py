from utils import (
    insert_article_metadata,ensure_scraped_column_exists, connection_config, generate_embedding,  fetch_page_with_zenrows
)
from summarizer_keyword_generator import extract_keywords, summarize_text, extract_keywords_keybert
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime
import re
import json
import aiomysql
import asyncio
from urllib.parse import urljoin

specialization = 'pulmonology'

def extract_volume_and_issue(soup):
    """Extracts volume and issue numbers from breadcrumbs navigation."""
    breadcrumbs = soup.find("ul", class_="breadcrumbs")
    if breadcrumbs:
        items = breadcrumbs.find_all("li")
        for item in items:
            link = item.find("a", href=True)
            if link and "/toc/ajrccm/" in link["href"]:
                match = re.search(r"/toc/ajrccm/(\d+)/(\d+)", link["href"])
                if match:
                    return match.group(1), match.group(2)
    return "N/A", "N/A"

async def crawl_article_ajrccm(specialization):
    journal_name = 'American Journal of Respiratory and Critical Care Medicine'
    connection = connection_config()
    table_name = specialization
    

    try:
        if not connection.is_connected():
            print("Database connection failed.")
            return
        cursor = connection.cursor()
        ensure_scraped_column_exists(cursor)
        
        query = "SELECT article_link FROM article_links WHERE journal_name = %s AND scraped = 'pending'"
        cursor.execute(query, (journal_name,))
        links = cursor.fetchall()

        if not links:
            print(f"No pending links found for journal: {journal_name}")
            return

        print(f"Found {len(links)} pending links for journal: {journal_name}")

        for link_tuple in links:
            article_url = link_tuple[0]
            print(f"Scraping article: {article_url}")

            try:
                response = await fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.html, "lxml")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}
                og_tags = {tag.get("property"): tag.get("content") for tag in soup.find_all("meta", attrs={"property": True})}
                article_title = meta_tags.get("dc.Title", "N/A")
                article_journal_title = meta_tags.get("citation_journal_title", "N/A")
                article_language = meta_tags.get("dc.Language", "English")
                article_volume,article_issue = extract_volume_and_issue(soup)

                article_publication_date = meta_tags.get("dc.Date", "2001/01/01")

                article_publisher = meta_tags.get('dc.Publisher', 'N/A')
                article_full_pdf_link = re.sub(r"/doi/(abs|full)/([^/]+)", r"/doi/pdf/\2", article_url)
                article_doi = meta_tags.get("publication_doi", "N/A")
                article_issn = meta_tags.get("wkhealth_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "dc.Creator"})]
                article_author = ', '.join(authors) if authors else "N/A"
                article_abstract=''
                abstract_div = None
                for abstract_div in soup.find_all("div", class_="abstractSection abstractInFull"):
                    # Extract paragraphs if the abstract section is found
                    paragraphs = abstract_div.find_all("p") if abstract_div else []
                    if paragraphs != []:
                        article_abstract = " ".join(
                            p.get_text(strip=True) for p in paragraphs
                        )
                    else:
                        article_abstract = "N/A"

                article_speciality = specialization
                article_contributor = "N/A"
                if article_abstract != "N/A":
                    article_summary = summarize_text(article_abstract)
                else:
                    article_summary = "N/A"
                print(f"Summariaztion done")
                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"

                keywords = None 
                keywords = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "keywords"})
                ]
                article_keywords = str(
                    ", ".join(keywords) if keywords else None
                )  # Check if keywords list is not empty

                # If article_keywords is None, then extract keywords from the article_abstract
                if not article_keywords:
                    article_keywords = extract_keywords_keybert(article_abstract)
                
                article_keywords = keywords if keywords else extract_keywords_keybert(article_abstract)
                article_identifier = article_doi

                article_ingestion_date=datetime.now().strftime('%Y-%m-%d')

                # Convert lists to strings where necessary
                data = (
                    article_speciality,
                    article_url,
                    article_abstract,
                    article_title,
                    article_journal_title,
                    article_publisher,
                    article_volume,
                    article_issue,
                    article_publication_date,
                    article_issn,
                    article_language,
                    article_identifier,
                    article_author,
                    article_contributor,
                    article_full_pdf_link,
                    article_summary,
                    ", ".join(article_keywords) if isinstance(article_keywords, list) else article_keywords,
                    "pending",
                    article_ingestion_date
                )

                insert_article_metadata(table_name, data)
                
                update_query = (
                    "UPDATE article_links SET scraped = 'done' WHERE article_link = %s"
                )
                cursor.execute(update_query, (article_url,))
                connection.commit()

                print(f"Successfully scraped and updated: {article_url}")

            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")

async def crawl_article_erj(specialization):
    journal_name = 'European Respiratory Journal'
    connection = connection_config()
    table_name = specialization


    try:
        if not connection.is_connected():
            print("Database connection failed.")
            return
        cursor = connection.cursor()
        ensure_scraped_column_exists(cursor)
        # Fetch links that have not been scraped
        query = "SELECT article_link FROM article_links WHERE journal_name = %s AND scraped = 'pending'"
        cursor.execute(query, (journal_name,))
        links = cursor.fetchall()

        if not links:
            print(f"No pending links found for journal: {journal_name}")
            return

        print(f"Found {len(links)} pending links for journal: {journal_name}")

        for link_tuple in links:
            article_url = link_tuple[0]
            print(f"Scraping article: {article_url}")

            try:
                response = await fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.html, "html.parser")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}

                article_title = meta_tags.get("citation_title", "N/A")
                article_journal_title = meta_tags.get("citation_journal_title", "N/A")
                article_language = meta_tags.get("citation_language", "English")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get("citation_online_date", "N/A")
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_author"})]
                article_author = ', '.join(authors) if authors else "N/A"

                article_abstract = meta_tags.get("DC.Description","N/A")
                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "dc.contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"
                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_keyword"})]
                article_keywords = ', '.join(keywords) if keywords else None  # Check if keywords list is not empty

                # If article_keywords is None, then extract keywords from the article_abstract
                if not article_keywords:
                    article_keywords = extract_keywords_keybert(article_abstract)
                article_identifier = article_doi

                article_ingestion_date=datetime.now().strftime('%Y-%m-%d')

                # Convert lists to strings where necessary
                data = (
                    article_speciality,
                    article_url,
                    article_abstract,
                    article_title,
                    article_journal_title,
                    article_publisher,
                    article_volume,
                    article_issue,
                    article_publication_date,
                    article_issn,
                    article_language,
                    article_identifier,
                    article_author,
                    article_contributor,
                    article_full_pdf_link,
                    article_summary,
                    ", ".join(article_keywords) if isinstance(article_keywords, list) else article_keywords,
                    "pending",
                    article_ingestion_date
                )


                insert_article_metadata(table_name, data)
                
                update_query = (
                    "UPDATE article_links SET scraped = 'done' WHERE article_link = %s"
                )
                cursor.execute(update_query, (article_url,))
                connection.commit()

                print(f"Successfully scraped and updated: {article_url}")

            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
async def crawl_article_chest(specialization):
    journal_name = 'CHEST'
    connection = connection_config()
    table_name = specialization
    

    try:
        if not connection.is_connected():
            print("Database connection failed.")
            return
        cursor = connection.cursor()
        ensure_scraped_column_exists(cursor)
        # Fetch links that have not been scraped
        query = "SELECT article_link FROM article_links WHERE journal_name = %s AND scraped = 'pending'"
        cursor.execute(query, (journal_name,))
        links = cursor.fetchall()

        if not links:
            print(f"No pending links found for journal: {journal_name}")
            return

        print(f"Found {len(links)} pending links for journal: {journal_name}")

        for link_tuple in links:
            article_url = link_tuple[0]
            print(f"Scraping article: {article_url}")

            try:
                response = await fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.html, "lxml")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}

                article_title = meta_tags.get("citation_title", "N/A")
                article_journal_title = meta_tags.get("citation_journal_title", "N/A")
                article_language = meta_tags.get("citation_language", "English")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get("citation_date", "2001/01/01")
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_author"})]
                article_author = ', '.join(authors) if authors else "N/A"
                article_abstract=None
                abstract_html = meta_tags.get("citation_abstract", "N/A")
                if abstract_html != "N/A":
                    abstract_soup = BeautifulSoup(abstract_html, "html.parser")
                    abstract_paragraphs = [p.get_text(strip=True) for p in abstract_soup.find_all("p")]
                    article_abstract = " ".join(abstract_paragraphs) if abstract_paragraphs else "N/A"
                else:
                    article_abstract = "N/A"

                article_speciality = specialization
                article_contributor = 'N/A'
                article_summary = summarize_text(article_abstract)
                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_keyword"})]
                if keywords != []:
                    article_keywords = ', '.join(keywords)
                else:
                    article_keywords = extract_keywords_keybert(article_abstract)
                article_identifier = article_doi

                article_ingestion_date=datetime.now().strftime('%Y-%m-%d')

                # Convert lists to strings where necessary
                data = (
                    article_speciality,
                    article_url,
                    article_abstract,
                    article_title,
                    article_journal_title,
                    article_publisher,
                    article_volume,
                    article_issue,
                    article_publication_date,
                    article_issn,
                    article_language,
                    article_identifier,
                    article_author,
                    article_contributor,
                    article_full_pdf_link,
                    article_summary,
                    ", ".join(article_keywords) if isinstance(article_keywords, list) else article_keywords,
                    "pending",
                    article_ingestion_date
                )

                insert_article_metadata(table_name, data)
                
                update_query = (
                    "UPDATE article_links SET scraped = 'done' WHERE article_link = %s"
                )
                cursor.execute(update_query, (article_url,))
                connection.commit()

                print(f"Successfully scraped and updated: {article_url}")

            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")

async def crawl_article_thorax(specialization):
    journal_name = 'Thorax'
    connection = connection_config()
    table_name = specialization
    

    try:
        if not connection.is_connected():
            print("Database connection failed.")
            return
        cursor = connection.cursor()
        ensure_scraped_column_exists(cursor)
        # Fetch links that have not been scraped
        query = "SELECT article_link FROM article_links WHERE journal_name = %s AND scraped = 'pending'"
        cursor.execute(query, (journal_name,))
        links = cursor.fetchall()

        if not links:
            print(f"No pending links found for journal: {journal_name}")
            return

        print(f"Found {len(links)} pending links for journal: {journal_name}")

        for link_tuple in links:
            article_url = link_tuple[0]
            print(f"Scraping article: {article_url}")

            try:
                response = await fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.html, "lxml")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}

                article_title = meta_tags.get("citation_title", "N/A")
                article_journal_title = meta_tags.get("citation_journal_title", "N/A")
                article_language = meta_tags.get("citation_language", "English")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get("citation_publication_date", "2001/01/01")
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_author"})]
                article_author = ', '.join(authors) if authors else "N/A"

                article_abstract = meta_tags.get("DC.Description", "N/A")
                

                article_speciality = specialization
                article_contributor = 'N/A'
                article_summary = summarize_text(article_abstract)
                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_keyword"})]
                if keywords != []:
                    article_keywords = ', '.join(keywords)
                else:
                    article_keywords = extract_keywords_keybert(article_abstract)
                article_identifier = article_doi

                article_ingestion_date=datetime.now().strftime('%Y-%m-%d')

                # Convert lists to strings where necessary
                data = (
                    article_speciality,
                    article_url,
                    article_abstract,
                    article_title,
                    article_journal_title,
                    article_publisher,
                    article_volume,
                    article_issue,
                    article_publication_date,
                    article_issn,
                    article_language,
                    article_identifier,
                    article_author,
                    article_contributor,
                    article_full_pdf_link,
                    article_summary,
                    ", ".join(article_keywords) if isinstance(article_keywords, list) else article_keywords,
                    "pending",
                    article_ingestion_date
                )

                insert_article_metadata(table_name, data)
                
                update_query = (
                    "UPDATE article_links SET scraped = 'done' WHERE article_link = %s"
                )
                cursor.execute(update_query, (article_url,))
                connection.commit()

                print(f"Successfully scraped and updated: {article_url}")

            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")

async def crawl_article_respiratory(specialization):
    journal_name = 'Respiratory Medicine'
    connection = connection_config()
    table_name = specialization
    

    try:
        if not connection.is_connected():
            print("Database connection failed.")
            return
        cursor = connection.cursor()
        ensure_scraped_column_exists(cursor)
        # Fetch links that have not been scraped
        query = "SELECT article_link FROM article_links WHERE journal_name = %s AND scraped = 'pending'"
        cursor.execute(query, (journal_name,))
        links = cursor.fetchall()

        if not links:
            print(f"No pending links found for journal: {journal_name}")
            return

        print(f"Found {len(links)} pending links for journal: {journal_name}")

        for link_tuple in links:
            article_url = link_tuple[0]
            print(f"Scraping article: {article_url}")

            try:
                response = await fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.html, "lxml")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}

                article_title = meta_tags.get("citation_title", "N/A")
                article_journal_title = meta_tags.get("citation_journal_title", "N/A")
                article_language = meta_tags.get("citation_language", "English")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get("citation_date", "2001/01/01")
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_author"})]
                article_author = ', '.join(authors) if authors else "N/A"
                article_abstract=None
                abstract_html = meta_tags.get("citation_abstract", "N/A")
                if abstract_html != "N/A":
                    abstract_soup = BeautifulSoup(abstract_html, "html.parser")
                    abstract_paragraphs = [p.get_text(strip=True) for p in abstract_soup.find_all("p")]
                    article_abstract = " ".join(abstract_paragraphs) if abstract_paragraphs else "N/A"
                else:
                    article_abstract = "N/A"

                article_speciality = specialization
                article_contributor = 'N/A'
                article_summary = summarize_text(article_abstract)
                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_keyword"})]
                if keywords != []:
                    article_keywords = ', '.join(keywords)
                else:
                    article_keywords = extract_keywords_keybert(article_abstract)
                article_identifier = article_doi

                article_ingestion_date=datetime.now().strftime('%Y-%m-%d')

                # Convert lists to strings where necessary
                data = (
                    article_speciality,
                    article_url,
                    article_abstract,
                    article_title,
                    article_journal_title,
                    article_publisher,
                    article_volume,
                    article_issue,
                    article_publication_date,
                    article_issn,
                    article_language,
                    article_identifier,
                    article_author,
                    article_contributor,
                    article_full_pdf_link,
                    article_summary,
                    ", ".join(article_keywords) if isinstance(article_keywords, list) else article_keywords,
                    "pending",
                    article_ingestion_date
                )

                insert_article_metadata(table_name, data)
                
                update_query = (
                    "UPDATE article_links SET scraped = 'done' WHERE article_link = %s"
                )
                cursor.execute(update_query, (article_url,))
                connection.commit()

                print(f"Successfully scraped and updated: {article_url}")

            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")

async def main():
    specialization='pulmonology'
    await crawl_article_ajrccm(specialization)
    await crawl_article_erj(specialization)
    await crawl_article_chest(specialization)
    await crawl_article_thorax(specialization)
    await crawl_article_respiratory(specialization)

if __name__ == "__main__":
    asyncio.run(main())
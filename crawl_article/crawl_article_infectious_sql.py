from utils import (
    insert_article_metadata,ensure_scraped_column_exists, connection_config, generate_embedding, fetch_page_with_zenrows, 

)
from summarizer_keyword_generator import extract_keywords, summarize_text, extract_keywords_keybert
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime
from pymilvus import Collection, CollectionSchema, connections, FieldSchema, DataType, utility
import re
import json
import aiomysql
import asyncio
from urllib.parse import urljoin

specialization = 'infectious_diseases'

def extract_volume_issue(url):
    pattern = re.compile(r"/article/(\d+)/(\d+)/")
    match = pattern.search(url)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def convert_date_format(date_str):
    try:
        return datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
    except ValueError:
        return '2001/01/01'

def extract_issn(soup):
    issn_tag = soup.find("span", class_="issn")
    if issn_tag:
        match = re.search(r"\d{4}-\d{4}", issn_tag.text)
        if match:
            return match.group(0)
    return None

async def crawl_article_lancet_id(specialization):
    journal_name ="The Lancet Infectious Diseases"
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
                article_language = meta_tags.get("citation_language", "N/A")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get("citation_date", "N/A")
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_author"})]
                article_author = ', '.join(authors) if authors else "N/A"

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
                '''process_and_store_embeddings(
                    article_title, article_abstract, article_author, article_url, table_name, cursor, connection, specialization
                )'''
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

async def crawl_article_clincal_id(specialization):
    journal_name =  'Clinical Infectious Diseases'
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
                article_publication_date = meta_tags.get("citation_publication_date", "2001-01-01")
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_author"})]
                article_author = ', '.join(authors) if authors else "N/A"

                if soup.find('section', class_='abstract'):
                    abstract_section = soup.find('section', class_='abstract')
                else:
                    abstract_section = soup.find('section', class_='abstract extract')

                if abstract_section:
                    # If the abstract section exists, find all <p> tags within it
                    paragraphs = abstract_section.find_all('p', class_='chapter-para')
                else:
                    # If the abstract section does not exist, find all <p> tags in the entire document
                    paragraphs = soup.find_all('p', class_='chapter-para')

                # Check if there is a second paragraph and get its text
                if len(paragraphs) >= 1:
                    article_abstract = ' '.join(x.get_text(strip=True) for x in paragraphs)
                else:
                    article_abstract = "N/A"
                if article_abstract != "N/A":
                    article_summary = summarize_text(article_abstract) 
                else:
                    article_summary = "N/A"
                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"

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
                '''process_and_store_embeddings(
                    article_title, article_abstract, article_author, article_url, table_name, cursor, connection, specialization
                )'''
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

async def crawl_article_journal_id(specialization):
    journal_name =  'Journal of Infectious Diseases'
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
                article_publication_date = meta_tags.get("citation_publication_date", "2001-01-01")
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_author"})]
                article_author = ', '.join(authors) if authors else "N/A"

                if soup.find('section', class_='abstract'):
                    abstract_section = soup.find('section', class_='abstract')
                else:
                    abstract_section = soup.find('section', class_='abstract extract')

                if abstract_section:
                    # If the abstract section exists, find all <p> tags within it
                    paragraphs = abstract_section.find_all('p', class_='chapter-para')
                else:
                    # If the abstract section does not exist, find all <p> tags in the entire document
                    paragraphs = soup.find_all('p', class_='chapter-para')

                # Check if there is a second paragraph and get its text
                if len(paragraphs) >= 1:
                    article_abstract = ' '.join(x.get_text(strip=True) for x in paragraphs)
                else:
                    article_abstract = "N/A"
                if article_abstract != "N/A":
                    article_summary = summarize_text(article_abstract) 
                else:
                    article_summary = "N/A"
                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"

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
                '''process_and_store_embeddings(
                    article_title, article_abstract, article_author, article_url, table_name, cursor, connection, specialization
                )'''
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

async def crawl_article_infection(specialization):
    journal_name = "Infection"
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

                meta_tags = {
                    tag.get("name"): tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": True})
                }

                article_title = meta_tags.get("citation_title", "N/A")
                article_journal_title = meta_tags.get("citation_journal_title", "N/A")
                article_language = meta_tags.get("dc.language", "N/A")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get(
                    "prism.publicationDate", "2001/01/01"
                )
                article_publisher = meta_tags.get("citation_publisher", "N/A")
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "citation_author"})
                ]
                article_author = ", ".join(authors) if authors else "N/A"

                article_abstract = meta_tags.get("dc.description", "N/A")

                article_speciality = specialization
                contributor = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "DC.Contributor"})
                ]
                article_contributor = ", ".join(contributor) if contributor else "N/A"
                article_summary = (
                    summarize_text(article_abstract)
                    if article_abstract != "N/A" or article_abstract != ""
                    else "N/A"
                )
                keywords = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "dc.subject"})
                ]
                article_keywords = ", ".join(keywords) if keywords else None
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
                '''process_and_store_embeddings(
                    article_title, article_abstract, article_author, article_url, table_name, cursor, connection, specialization
                )'''
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

async def crawl_article_cdc(specialization):
    journal_name = "Emerging Infectious Diseases journal"
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

                meta_tags = {
                    tag.get("name"): tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": True})
                }
                og_tags = {
                    tag.get("property"): tag.get("content")
                    for tag in soup.find_all("meta", attrs={"property": True})
                }

                article_title = meta_tags.get("citation_title", "N/A")
                article_journal_title = og_tags.get("og:site_name", "Emerging Infectious Disease journal")
                article_language = meta_tags.get("dc.language", "EN")
                article_volume,article_issue = extract_volume_issue(article_url)
                article_date = og_tags.get(
                    "cdc:last_updated", "2001/01/01"
                )
                article_publication_date=convert_date_format(article_date)
                article_publisher = meta_tags.get("citation_publisher", "N/A")
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = extract_issn(soup)

                authors = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "citation_author"})
                ]
                article_author = ", ".join(authors) if authors else "N/A"

                abstract_div = soup.find("div", id="abstract")
                if abstract_div:
                    abstract_text = abstract_div.find("p")
                    if abstract_text:
                        article_abstract=abstract_text.text.strip()
                    else:
                        article_abstract = 'N/A'

                article_speciality = specialization
                contributor = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "DC.Contributor"})
                ]
                article_contributor = ", ".join(contributor) if contributor else "N/A"
                article_summary = (
                    summarize_text(article_abstract)
                    if article_abstract != "N/A" or article_abstract != ""
                    else "N/A"
                )
                keywords = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "keywords"})
                ]
                article_keywords = ", ".join(keywords) if keywords else None
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
                '''process_and_store_embeddings(
                    article_title, article_abstract, article_author, article_url, table_name, cursor, connection, specialization
                )'''
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
    specialization = 'infectious_diseases'
    await crawl_article_cdc(specialization)
    await crawl_article_clincal_id(specialization)
    await crawl_article_infection(specialization)
    await crawl_article_journal_id(specialization)
    await crawl_article_lancet_id(specialization)

if __name__ == "__main__":
    asyncio.run(main())
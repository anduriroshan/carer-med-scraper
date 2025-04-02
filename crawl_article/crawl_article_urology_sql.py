from utils import (
    insert_article_metadata,ensure_scraped_column_exists, connection_config, generate_embedding,  fetch_page_with_zenrows, 

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

specialization = "urology"

async def crawl_article_european_urology(specialization):
    journal_name = "European Urology"
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
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "DC.Contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"

                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_keywords"})]
                article_keywords = ', '.join(keywords) if keywords else None  # Check if keywords list is not empty

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



async def crawl_article_aua(specialization):
    journal_name = "The Journal of Urology"
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
                response =await  fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.html, "html.parser")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}

                article_title = meta_tags.get("dc.Title", "N/A")
                article_journal_title = meta_tags.get("citation_journal_title", "N/A")
                article_language = meta_tags.get("dc.Language", "English")
                article_publisher = meta_tags.get("dc.Publisher", "N/A")
                article_doi = meta_tags.get("publication_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                # Construct PDF URL
                article_full_pdf_link = f"https://www.auajournals.org/doi/epdf/{article_doi}" if article_doi != "N/A" else "N/A"

                # Extract Authors
                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "dc.Creator"})]
                article_author = ', '.join(authors) if authors else "N/A"

                # Extract Abstract
                abstract_section = soup.find("div", class_="abstractSection abstractInFull")
                if abstract_section:
                    abstract_paragraphs = [p.get_text(strip=True) for p in abstract_section.find_all("p")]
                    article_abstract = " ".join(abstract_paragraphs) if abstract_paragraphs else "N/A"
                else:
                    article_abstract = "N/A"

                # Extract Volume & Issue from JSON
                article_volume, article_issue = "N/A", "N/A"
                script_tags = soup.find_all("script", type='text/javascript')

                json_text = None
                for script in script_tags:
                    match = re.search(r"digitalData\s*=\s*({.*?});", script.text, re.DOTALL)
                    if match:
                        json_text = match.group(1).strip()
                        break  # Stop after finding the first match

                if json_text:
                    try:
                        digital_data = json.loads(json_text)
                        
                        # Extract required fields
                        article_volume = digital_data["page"]["journalInfo"].get("journalVolume", "N/A")
                        article_issue = digital_data["page"]["journalInfo"].get("journalIssue", "N/A")
                        article_issn = digital_data["page"]["journalInfo"].get("journalISSN", "N/A")
                        
                        print("Volume:", article_volume)
                        print("Issue:", article_issue)
                        print("ISSN:", article_issn)
                        
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON: {e}")

                # Extract Article Date from Citation Div
                article_publication_date = "N/A"
                citation_div = soup.find("div", class_="citation__top")
                if citation_div:
                    citation_spans = citation_div.find_all("span", class_="citation__top__item")
                    if citation_spans:
                        article_publication_date = citation_spans[-1].get_text(strip=True)  # Last span contains the date

                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "DC.Contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"

                # Extract Keywords
                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_keywords"})]
                article_keywords = ', '.join(keywords) if keywords else extract_keywords_keybert(article_abstract)

                article_identifier = article_doi
                article_ingestion_date = datetime.now().strftime('%Y-%m-%d')

                # Prepare Data for Database
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

                # Update article as scraped
                update_query = "UPDATE article_links SET scraped = 'done' WHERE article_link = %s"
                cursor.execute(update_query, (article_url,))
                connection.commit()

                print(f"Successfully scraped and updated: {article_url}")

            except Exception as e:
                print(f"Error scraping {article_url}: {e}")

    except mysql.connector.Error as e:
        print(f"Database error: {e}")


async def crawl_article_urology(specialization):
    journal_name = "Urology"
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
                article_publication_date = meta_tags.get("citation_date", "2001/01/01")
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
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "DC.Contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"

                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_keywords"})]
                article_keywords = ', '.join(keywords) if keywords else None  # Check if keywords list is not empty

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


async def crawl_article_world_urology(specialization):
    journal_name = "World Journal of Urology"
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

async def crawl_article_bjui(specialization):
    journal_name = 'BJU International'
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
                response = await  fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.html, "html.parser")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}

                article_title = meta_tags.get("citation_title", "N/A")
                article_journal_title = 'Journal of Orthopaedic Research'
                article_language = meta_tags.get("citation_language", "English")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get("citation_online_date", "2001/01/01")
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_author"})]
                article_author = ', '.join(authors) if authors else "N/A"
                article_abstract=None
                abstract_texts = []
                abstract_div = soup.find('div', class_='article-section__content en main')
                if abstract_div:
                    paragraphs = abstract_div.find_all('p')
                    for paragraph in paragraphs:
                        abstract_texts.append(paragraph.get_text(strip=True))
                article_abstract=' '.join(abstract_texts) if abstract_texts!=[] else "N/A"

                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "dc.contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"
                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_keywords"})]
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
    specialization='urology'
    await crawl_article_european_urology(specialization)
    await crawl_article_aua(specialization)
    await crawl_article_urology(specialization)
    await crawl_article_world_urology(specialization)
    await crawl_article_bjui(specialization)
if __name__ == "__main__":
    asyncio.run(main())
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
specialization = 'psychiatry'

def extract_authors(citation_data):
    """
    Extracts authors from citation data (RIS, BibTeX, or Medlars).
    """
    authors = []

    # Extract from BibTeX format
    if "bibtex" in citation_data:
        bibtex = citation_data["bibtex"]
        match = re.search(r"author\s*=\s*\{(.+?)\}", bibtex)
        if match:
            authors = match.group(1).split(" and ")  # BibTeX uses "and" as a separator

    # If BibTeX fails, extract from RIS format
    if not authors and "ris" in citation_data:
        ris = citation_data["ris"]
        authors = re.findall(r"A1\s+-\s+(.+)", ris)  # RIS uses "A1 - Author Name"

    # If RIS fails, extract from Medlars format
    if not authors and "medlars" in citation_data:
        medlars = citation_data["medlars"]
        authors = re.findall(r"FAU - (.+)", medlars)  # Medlars uses "FAU - Author Name"

    return ", ".join(authors) if authors else "N/A"


def generate_pdf_url(article_url):
    if "/doi/abs/" in article_url:
        return article_url.replace("/doi/abs/", "/doi/pdf/")
    elif "/doi/" in article_url:
        parts = article_url.split("/doi/")
        return f"{parts[0]}/doi/pdf/{parts[1]}"
    return article_url

async def crawl_article_ajp(specialization):
    journal_name='American Journal of Psychiatry'
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

                article_title = meta_tags.get("dc.Title", "N/A")
                article_journal_title = meta_tags.get("citation_journal_title", "N/A")
                article_language = meta_tags.get("dc.Language", "N/A")
                article_volume = soup.find('span',property='volumeNumber').get_text()
                article_issue = soup.find('span',property='issueNumber').get_text()
                article_publication_date = datetime.strptime(meta_tags.get("dc.Date", "N/A"), "%d %B %Y").strftime("%Y-%m-%d")
                article_publisher = meta_tags.get("dc.Publisher", "N/A")
                article_full_pdf_link = generate_pdf_url(article_url)
                article_doi = meta_tags.get("publication_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "dc.Creator"})
                ]
                article_author = ", ".join(authors) if authors != [None] else "N/A"

                article_abstract = og_tags.get("og:description", "N/A")

                article_speciality = specialization
                contributor = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "dc.Contributor"})
                ]
                article_contributor = ", ".join(contributor) if contributor else "N/A"
                article_summary = (
                    summarize_text(article_abstract)
                    if article_abstract != "N/A" or article_abstract != ""
                    else "N/A"
                )
                keywords = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "dc.Subject"})
                ]
                article_keywords = ", ".join(keywords) if keywords!=[None] else None
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

async def crawl_article_jama_psy(specialization):
    journal_name = "JAMA Psychiatry"
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
                article_publication_date = meta_tags.get("citation_publication_date", "2001/01/01")
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


async def crawl_article_molecular_psychiatry(specialization):
    journal_name = 'Molecular Psychiatry'
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
                article_publication_date = meta_tags.get("citation_online_date", "2001/01/01")
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_author"})]
                article_author = ', '.join(authors) if authors else "N/A"

                article_abstract = meta_tags.get("dc.description","N/A")

                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "dc.contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"
                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "dc.subject"})]
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

async def crawl_article_schizophrenia(specialization):
    journal_name =   'Schizophrenia Bulletin'
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
    specialization = 'psychiatry'
    await crawl_article_ajp(specialization)
    await crawl_article_jama_psy(specialization)
    await crawl_article_molecular_psychiatry(specialization)
    await crawl_article_schizophrenia(specialization)

if __name__ == "__main__":
    asyncio.run(main())
    
    
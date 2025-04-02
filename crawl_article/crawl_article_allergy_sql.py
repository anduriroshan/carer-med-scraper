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
specialization = 'allergy_immunology'

def parse_publication_date(date_str):
    """Parse publication date with multiple formats."""
    formats = ["%B %Y", "%B %d, %Y", "%b %y"]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue  # Try the next format
    
    return "2001/01/01"

async def crawl_article_allergy(specialization):
    journal_name = 'Allergy'
    table_name = specialization
    try:
        connection = connection_config()
        cursor= connection.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s AND scraped = 'pending'", (journal_name,))
        links = cursor.fetchall()

        if not links:
            print(f"No pending links found for journal: {journal_name}")
            return

        print(f"Found {len(links)} pending links for journal: {journal_name}")

        for link_tuple in links:
            article_url = link_tuple[0]
            print(f"Scraping article: {article_url}")
            try:
                response_text = await fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response_text.html, "html.parser")

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
                
                abstract_div = soup.find('div', class_='article-section__content en main')
                article_abstract = ' '.join([p.get_text(strip=True) for p in abstract_div.find_all('p')]) if abstract_div else "N/A"
                article_speciality = specialization
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"
                article_keywords = extract_keywords_keybert(article_abstract) if article_abstract != "N/A" else "N/A"
                article_identifier = article_doi
                article_ingestion_date = datetime.now().strftime('%Y-%m-%d')
                article_contributor = 'N/A'
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
                cursor.execute("UPDATE article_links SET scraped = 'done' WHERE article_link = %s", (article_url,))
                connection.commit()
                print(f"Successfully scraped and updated: {article_url}")
            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except aiomysql.Error as e:
        print(f"Database error: {e}")


async def crawl_article_cea(specialization):
    journal_name = 'Clinical & Experimental Allergy'
    table_name = specialization
    try:
        connection = connection_config()
        cursor= connection.cursor()
        cursor.execute("SELECT article_link FROM article_links WHERE journal_name = %s AND scraped = 'pending'", (journal_name,))
        links = cursor.fetchall()

        if not links:
            print(f"No pending links found for journal: {journal_name}")
            return

        print(f"Found {len(links)} pending links for journal: {journal_name}")


        for link_tuple in links:
            article_url = link_tuple[0]
            print(f"Scraping article: {article_url}")
            try:
                response_text = await fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response_text, "html.parser")

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
                
                abstract_div = soup.find('div', class_='article-section__content en main')
                article_abstract = ' '.join([p.get_text(strip=True) for p in abstract_div.find_all('p')]) if abstract_div else "N/A"
                article_speciality = specialization
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"
                article_keywords = extract_keywords_keybert(article_abstract) if article_abstract != "N/A" else "N/A"
                article_identifier = article_doi
                article_ingestion_date = datetime.now().strftime('%Y-%m-%d')

                article_contributor = 'N/A'
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
                cursor.execute("UPDATE article_links SET scraped = 'done' WHERE article_link = %s", (article_url,))
                connection.commit()
                print(f"Successfully scraped and updated: {article_url}")
            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except aiomysql.Error as e:
        print(f"Database error: {e}")


async def crawl_article_iaa(specialization):
    journal_name = 'International Archives of Allergy and Immunology'
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
                soup = BeautifulSoup(response.content, "html.parser")

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

                article_abstract = soup.find("meta", attrs={"property": "og:description"}).get("content", "N/A")

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

async def crawl_article_co_allergy(specialization):
    journal_name = 'Current Opinion in Allergy and Clinical Immunology'
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
                soup = BeautifulSoup(response.content, "lxml")

                meta_tags = {
                    tag.get("name"): tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": True})
                }
                og_tags = {
                    tag.get("property"): tag.get("content")
                    for tag in soup.find_all("meta", attrs={"property": True})
                }
                article_title = meta_tags.get("wkhealth_title", "N/A")
                article_journal_title = meta_tags.get(
                    "wkhealth_journal_title_legacy", "N/A"
                )
                article_language = meta_tags.get("wkhealth_language", "English")
                article_volume = meta_tags.get("wkhealth_volume", "N/A")
                article_issue = meta_tags.get("wkhealth_issue", "N/A")
                article_publication_date = parse_publication_date(meta_tags.get("wkhealth_article_publication_date", "2001/01/01"))
                article_publisher = 'LWW'
                article_full_pdf_link = meta_tags.get("wkhealth_pdf_url", "N/A")
                article_doi = meta_tags.get("wkhealth_doi", "N/A")
                article_issn = meta_tags.get("wkhealth_issn", "N/A")

                authors = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "wkhealth_authors"})
                ]
                article_author = ", ".join(authors) if authors!=[None] else "N/A"

                article_abstract=None
                abstract_texts = []
                abstract_div = soup.find('div', class_='ejp-article-text-abstract')
                if abstract_div:
                    paragraphs = abstract_div.find_all('p')
                    for paragraph in paragraphs:
                        abstract_texts.append(paragraph.get_text(strip=True))
                article_abstract=' '.join(abstract_texts) if abstract_texts!=[] else "N/A"

                if article_abstract != "N/A":
                    article_summary = summarize_text(article_abstract)
                else:
                    article_summary = "N/A"
                article_speciality = specialization
                contributor = [
                    tag.get("content")
                    for tag in soup.find_all(
                        "meta", attrs={"name": "citation_contributor"}
                    )
                ]
                article_contributor = ", ".join(contributor) if contributor !=[None] else "N/A"
                keywords=None
                fulltext_div = soup.find(
                    "div", class_="ejp-fulltext-content js-ejp-fulltext-content"
                )
                if fulltext_div:
                    divs = fulltext_div.find_all("div")
                    for div in divs:
                        strong_tag = div.find("strong")
                        if strong_tag and "Keywords" in strong_tag.text:
                            p_tag = div.find("p")
                            if p_tag:
                                keywords = p_tag.get_text(strip=True)

                article_keywords = (   
                    keywords if keywords else extract_keywords_keybert(article_abstract)
                )
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
    specialization='allergy_immunology'
    await crawl_article_allergy(specialization)
    await crawl_article_cea(specialization)
    await crawl_article_iaa(specialization)
    await crawl_article_co_allergy(specialization)
if __name__ == "__main__":
    asyncio.run(main())
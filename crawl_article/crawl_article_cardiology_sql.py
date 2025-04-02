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

specialization = "cardiology"

async def crawl_article_circulation(specialization):
    journal_name = "Circulation"
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
                article_language = meta_tags.get("dc.Language", "English")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get("dc.Date", "N/A")
                article_publisher = meta_tags.get("dc.Publisher", "N/A")
                article_full_pdf_link = article_url.replace("/full/", "/pdf/")
                article_doi = meta_tags.get("publication_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "dc.Creator"})
                ]
                article_author = ", ".join(authors) if authors else "N/A"

                article_abstract = og_tags.get("og:description", "N/A")

                volume_element = soup.find("span", property="volumeNumber")
                article_volume = (
                    volume_element.get_text(strip=True) if volume_element else "N/A"
                )
                issue_element = soup.find("span", property="issueNumber")
                article_issue = (
                    issue_element.get_text(strip=True) if issue_element else "N/A"
                )

                article_speciality = specialization

                contributor = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "dc.Contributor"})
                ]
                article_contributor = ", ".join(contributor) if contributor else "N/A"

                if article_abstract == "N/A":
                    article_summary = "N/A"
                else:
                    article_summary = summarize_text(article_abstract)
                # Extract keywords from meta tags
                keywords = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "citation_keyword"})
                ]
                article_keywords = (
                    ", ".join(keywords) if keywords else None
                )  # Check if keywords list is not empty

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
                
                update_query = f"UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s"
                cursor.execute(update_query, (article_url,))
                connection.commit()
                print(f"Successfully scraped and updated {specialization} table: {article_url}\n")
            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")


async def crawl_article_ehj(specialization):
    journal_name = "European Heart Journal"
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
                article_language = meta_tags.get("citation_language", "English")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get(
                    "citation_publication_date", "N/A"
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

                abstract_section = soup.find("section", class_="abstract")

                if abstract_section:
                    # If the abstract section exists, find all <p> tags within it
                    paragraphs = abstract_section.find_all("p", class_="chapter-para")
                else:
                    # If the abstract section does not exist, find all <p> tags in the entire document
                    paragraphs = soup.find_all("p", class_="chapter-para")

                # Check if there is a second paragraph and get its text
                if len(paragraphs) > 1:
                    article_abstract = paragraphs[1].get_text(strip=True)
                else:
                    article_abstract = "N/A"
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
                article_contributor = ", ".join(contributor) if contributor else "N/A"

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
                
                update_query = f"UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s"
                cursor.execute(update_query, (article_url,))
                connection.commit()
                print(f"Successfully scraped and updated {specialization} table: {article_url}\n")
            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")


async def crawl_article_jacc(specialization):
    journal_name = "JACC: Journal of the American College of Cardiology"
    connection = connection_config()
    table_name = specialization
    

    base_url = "https://www.jacc.org"

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

                epub_section_text = soup.find("div", class_="epub-section").get_text(
                    strip=True
                )

                # Use regular expressions to extract volume and issue
                volume_issue_match = re.search(r"(\d+)\s*\((\d+)\)", epub_section_text)

                if volume_issue_match:
                    volume = volume_issue_match.group(1)  # Extract the volume
                    issue = volume_issue_match.group(2)  # Extract the issue
                else:
                    volume = "N/A"
                    issue = "N/A"

                article_title = meta_tags.get("dc.Title", "N/A")
                article_journal_title = meta_tags.get("citation_journal_title", "N/A")
                article_language = meta_tags.get("dc.Language", "English")
                article_volume = volume
                article_issue = issue
                article_publication_date = meta_tags.get("dc.Date", "N/A")
                article_publisher = meta_tags.get("dc.Publisher", "N/A")
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("publication_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "dc.Creator"})
                ]
                article_author = ", ".join(authors) if authors else "N/A"

                li_tags = soup.find_all("li", class_="article__navbar__col")

                # Loop through each <li> tag to find the one containing "PDF"
                pdf_link = None
                for li in li_tags:
                    # Check if there's an <a> tag with "PDF" text inside <span>
                    a_tag = li.find("a", href=True)
                    if (
                        a_tag
                        and a_tag.find("span", class_="format-icon")
                        and a_tag.find("span", class_="format-icon").get_text(
                            strip=True
                        )
                        == "PDF"
                    ):
                        pdf_link = urljoin(base_url, a_tag["href"])
                        break
                article_full_pdf_link = pdf_link
                abstract_div = None
                for section in soup.find_all("div", class_="article-section__content"):
                    h2_tag = section.find("h2", class_="article-section__title")
                    if h2_tag and h2_tag.get_text(strip=True).lower() == "abstract":
                        abstract_div = section
                        break
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
                # Extract keywords from meta tags
                keywords = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "keywords"})
                ]
                article_keywords = (
                    ", ".join(keywords) if keywords else None
                )  # Check if keywords list is not empty

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
                
                update_query = f"UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s"
                cursor.execute(update_query, (article_url,))
                connection.commit()
                print(f"Successfully scraped and updated {specialization} table: {article_url}\n")
            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")

async def crawl_article_cardio_research(specialization):
    journal_name = "Cardiovascular Research"
    connection = connection_config()
    table_name = specialization
    

    base_url = "https://academic.oup.com"

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
                article_language = meta_tags.get("citation_language", "English")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get(
                    "citation_publication_date", "N/A"
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

                a_tag = soup.find("a", class_="al-link pdf article-pdfLink")
                if a_tag and "href" in a_tag.attrs:
                    # Extract the href value
                    href_value = a_tag["href"]
                    article_full_pdf_link = urljoin(base_url, href_value)
                else:
                    article_full_pdf_link = "N/A"

                abstract_section = soup.find("section", class_="abstract")

                if abstract_section:
                    # If the abstract section exists, find all <p> tags within it
                    paragraphs = abstract_section.find_all("p", class_="chapter-para")
                else:
                    # If the abstract section does not exist, find all <p> tags in the entire document
                    paragraphs = soup.find_all("p", class_="chapter-para")

                # Check if there is a second paragraph and get its text
                if len(paragraphs) >= 1:
                    article_abstract = " ".join([p.get_text(strip=True) for p in paragraphs])
                else:
                    article_abstract = "N/A"
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
                article_contributor = ", ".join(contributor) if contributor else "N/A"

                keywords = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "citation_keyword"})
                ]
                if keywords != []:
                    article_keywords = ", ".join(keywords)
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
                
                update_query = f"UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s"
                cursor.execute(update_query, (article_url,))
                connection.commit()
                print(f"Successfully scraped and updated {specialization} table: {article_url}\n")
            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")


async def crawl_article_heart(specialization):
    journal_name = "Heart"
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
                article_language = meta_tags.get("DC.Language", "N/A")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")
                article_publication_date = meta_tags.get(
                    "citation_publication_date", "N/A"
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

                article_abstract = meta_tags.get("DC.Description", "N/A")

                article_speciality = specialization
                contributor = [
                    tag.get("content")
                    for tag in soup.find_all("meta", attrs={"name": "DC.Contributor"})
                ]
                article_contributor = ", ".join(contributor) if contributor else "N/A"
                article_summary = (
                    summarize_text(article_abstract)
                    if article_abstract != "N/A"
                    else "N/A"
                )
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
                
                update_query = f"UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s"
                cursor.execute(update_query, (article_url,))
                connection.commit()
                print(f"Successfully scraped and updated {specialization} table: {article_url}\n")
            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")

async def main():
    specialization='cardiology'
    await crawl_article_circulation(specialization)
    await crawl_article_ehj(specialization)
    await crawl_article_jacc(specialization)
    await crawl_article_cardio_research(specialization)
    await crawl_article_heart(specialization)

if __name__ == "__main__":
    asyncio.run(main())
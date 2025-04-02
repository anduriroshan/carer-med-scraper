from utils import (
    fetch_page_with_scraper_api, insert_article_metadata, create_index,
    ensure_scraped_column_exists, connection_config, generate_embedding, 
    add_to_milvus, create_milvus_collection, fetch_page_with_zenrows, 
    initialize_milvus_merged,
    initialize_milvus, print_sample_from_milvus,process_and_store_embeddings
)
from summarizer_keyword_generator import extract_keywords, summarize_text, extract_keywords_keybert
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime
from pymilvus import Collection, CollectionSchema, connections, FieldSchema, DataType, utility

specialization = 'nephrology'
create_milvus_collection(specialization)

def crawl_article_jasn(specialization):
    journal_name = 'Journal of the American Society of Nephrology'
    connection = connection_config()
    table_name = specialization
    milvus_collection = initialize_milvus(specialization) 

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
                response = fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.content, "lxml")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}
                og_tags = {tag.get("property"): tag.get("content") for tag in soup.find_all("meta", attrs={"property": True})}
                article_title = meta_tags.get("wkhealth_title", "N/A")
                article_journal_title = meta_tags.get("wkhealth_journal_title_legacy", "N/A")
                article_language = meta_tags.get("wkhealth_language", "English")
                article_volume = meta_tags.get("wkhealth_volume", "N/A")
                article_issue = meta_tags.get("wkhealth_issue", "N/A")

                raw_publication_date = meta_tags.get("wkhealth_article_publication_date", "2001/01/01")
                if raw_publication_date != "N/A":
                    try:
                        parsed_date = datetime.strptime(raw_publication_date, "%B %d, %Y")
                        article_publication_date = parsed_date.strftime("%Y-%m-%d")
                    except ValueError:
                        article_publication_date = "N/A"
                else:
                    article_publication_date = "N/A"
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("wkhealth_pdf_url", "N/A")
                article_doi = meta_tags.get("wkhealth_doi", "N/A")
                article_issn = meta_tags.get("wkhealth_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "wkhealth_authors"})]
                article_author = ', '.join(authors) if authors else "N/A"

                article_abstract = og_tags.get("og:description", "N/A")
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"
                print(f"Summariaztion done")
                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"

                keywords = None 
                fulltext_div = soup.find("div", class_="ejp-fulltext-content js-ejp-fulltext-content")
                if fulltext_div:
                    divs = fulltext_div.find_all("div")
                    for div in divs:
                        strong_tag = div.find("strong")
                        if strong_tag and "Keywords" in strong_tag.text:
                            p_tag = div.find("p")
                            if p_tag:
                                keywords = p_tag.get_text(strip=True)
                                break
                
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
                '''process_and_store_embeddings(
                    article_title, article_abstract, article_author, article_url, table_name, cursor, connection, specialization
                )'''
                update_query = f"UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s"
                cursor.execute(update_query, (article_url,))
                connection.commit()
                print(f"Successfully scraped and updated {specialization} table: {article_url}\n")
            except Exception as e:
                print(f"Error scraping {article_url}: {e}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")

def crawl_article_kidney(specialization):
    journal_name = 'Kidney International'
    connection = connection_config()
    table_name = specialization
    milvus_collection = initialize_milvus(specialization) 

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
                response = fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.content, "lxml")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}
                og_tags = {tag.get("property"): tag.get("content") for tag in soup.find_all("meta", attrs={"property": True})}
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

                article_abstract = og_tags.get("og:description", "N/A")
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"
                print(f"Summariaztion done")
                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"

                keywords = None 
                fulltext_div = soup.find("div", class_="ejp-fulltext-content js-ejp-fulltext-content")
                if fulltext_div:
                    divs = fulltext_div.find_all("div")
                    for div in divs:
                        strong_tag = div.find("strong")
                        if strong_tag and "Keywords" in strong_tag.text:
                            p_tag = div.find("p")
                            if p_tag:
                                keywords = p_tag.get_text(strip=True)
                                break
                
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
                try:
                    title_embedding = [generate_embedding(article_title)]
                    print("-> Title embeddings done")
                    abstract_embedding = [generate_embedding(article_abstract)]
                    print("-> Abstract embeddings done")
                    authors_embedding = [generate_embedding(article_author)]
                    print("-> Authors embeddings done")

                    article_metadata = {
                        "title_text": article_title,
                        "abstract_text": article_abstract,
                        "authors_text": article_author,
                        "article_url": article_url,
                        "title_embedding": title_embedding,
                        "abstract_embedding": abstract_embedding,
                        "authors_embedding": authors_embedding,
                    }
                    add_to_milvus(milvus_collection, article_metadata)
                    merged = initialize_milvus('merged_collection')
                    add_to_milvus(merged, article_metadata)

                    update_query = f"UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s"
                    cursor.execute(update_query, (article_url,))
                    connection.commit()
                    print(f"Successfully scraped and updated {specialization} table: {article_url}\n")

                except Exception as e:
                    print(f"Error processing article: {e}")
                    continue
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

def crawl_article_cjasn(specialization):
    journal_name = 'Clinical Journal of the American Society of Nephrology'
    connection = connection_config()
    table_name = specialization
    milvus_collection = initialize_milvus(specialization) 

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
                response = fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.content, "lxml")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}
                og_tags = {tag.get("property"): tag.get("content") for tag in soup.find_all("meta", attrs={"property": True})}
                article_title = meta_tags.get("wkhealth_title", "N/A")
                article_journal_title = meta_tags.get("wkhealth_journal_title_legacy", "N/A")
                article_language = meta_tags.get("wkhealth_language", "English")
                article_volume = meta_tags.get("wkhealth_volume", "N/A")
                article_issue = meta_tags.get("wkhealth_issue", "N/A")

                raw_publication_date = meta_tags.get("wkhealth_article_publication_date", "N/A")
                if raw_publication_date != "N/A":
                    try:
                        parsed_date = datetime.strptime(f"01 {raw_publication_date}", "%d %B %Y")
                        article_publication_date = parsed_date.strftime("%Y-%m-%d")
                    except ValueError:
                        article_publication_date = "N/A"
                else:
                    article_publication_date = "N/A"
                article_publisher = meta_tags.get('citation_publisher', 'LWW')
                article_full_pdf_link = meta_tags.get("wkhealth_pdf_url", "N/A")
                article_doi = meta_tags.get("wkhealth_doi", "N/A")
                article_issn = meta_tags.get("wkhealth_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "wkhealth_authors"})]
                article_author = ', '.join(authors) if authors else "N/A"

                article_abstract = og_tags.get("og:description", "N/A")
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"
                print(f"Summariaztion done")
                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"

                keywords = None 
                fulltext_div = soup.find("div", class_="ejp-fulltext-content js-ejp-fulltext-content")
                if fulltext_div:
                    divs = fulltext_div.find_all("div")
                    for div in divs:
                        strong_tag = div.find("strong")
                        if strong_tag and "Keywords" in strong_tag.text:
                            p_tag = div.find("p")
                            if p_tag:
                                keywords = p_tag.get_text(strip=True)
                                break
                
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
                try:
                    title_embedding = [generate_embedding(article_title)]
                    print("-> Title embeddings done")
                    abstract_embedding = [generate_embedding(article_abstract)]
                    print("-> Abstract embeddings done")
                    authors_embedding = [generate_embedding(article_author)]
                    print("-> Authors embeddings done")

                    article_metadata = {
                        "title_text": article_title,
                        "abstract_text": article_abstract,
                        "authors_text": article_author,
                        "article_url": article_url,
                        "title_embedding": title_embedding,
                        "abstract_embedding": abstract_embedding,
                        "authors_embedding": authors_embedding,
                    }
                    add_to_milvus(milvus_collection, article_metadata)
                    merged = initialize_milvus('merged_collection')
                    add_to_milvus(merged, article_metadata)

                    update_query = f"UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s"
                    cursor.execute(update_query, (article_url,))
                    connection.commit()
                    print(f"Successfully scraped and updated {specialization} table: {article_url}\n")

                except Exception as e:
                    print(f"Error processing article: {e}")
                    continue
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

def crawl_article_ndt(specialization):
    journal_name = 'Nephrology Dialysis Transplantation'
    connection = connection_config()
    table_name = specialization
    milvus_collection = initialize_milvus(specialization) 

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
                response = fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.content, "lxml")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}
                og_tags = {tag.get("property"): tag.get("content") for tag in soup.find_all("meta", attrs={"property": True})}
                article_title = meta_tags.get("citation_title", "N/A")
                article_journal_title = meta_tags.get("citation_journal_title", "N/A")
                article_language = meta_tags.get("citation_language", "English")
                article_volume = meta_tags.get("citation_volume", "N/A")
                article_issue = meta_tags.get("citation_issue", "N/A")

                article_publication_date = meta_tags.get("citation_publication_date", "N/A")
                article_publisher = meta_tags.get('citation_publisher', 'N/A')
                article_full_pdf_link = meta_tags.get("citation_pdf_url", "N/A")
                article_doi = meta_tags.get("citation_doi", "N/A")
                article_issn = meta_tags.get("citation_issn", "N/A")

                authors = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_author"})]
                article_author = ', '.join(authors) if authors else "N/A"

                abstract_section = soup.find('section', class_='abstract')

                if abstract_section:
                    paragraphs = abstract_section.find_all('p', class_='chapter-para')
                else:
                    paragraphs = soup.find_all('p', class_='chapter-para')

                if len(paragraphs) >= 1:
                    article_abstract = ' '.join(x.get_text(strip=True) for x in paragraphs)
                else:
                    article_abstract = "N/A"
                if article_abstract != "N/A":
                    article_summary = summarize_text(article_abstract) 
                else:
                    article_summary = "N/A"
                print(f"Summariaztion done")
                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"

                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_keyword"})]
                article_keywords = ', '.join(keywords) if keywords else None  
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
                try:
                    title_embedding = [generate_embedding(article_title)]
                    print("-> Title embeddings done")
                    abstract_embedding = [generate_embedding(article_abstract)]
                    print("-> Abstract embeddings done")
                    authors_embedding = [generate_embedding(article_author)]
                    print("-> Authors embeddings done")

                    article_metadata = {
                        "title_text": article_title,
                        "abstract_text": article_abstract,
                        "authors_text": article_author,
                        "article_url": article_url,
                        "title_embedding": title_embedding,
                        "abstract_embedding": abstract_embedding,
                        "authors_embedding": authors_embedding,
                    }
                    add_to_milvus(milvus_collection, article_metadata)
                    merged = initialize_milvus('merged_collection')
                    add_to_milvus(merged, article_metadata)

                    update_query = f"UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s"
                    cursor.execute(update_query, (article_url,))
                    connection.commit()
                    print(f"Successfully scraped and updated {specialization} table: {article_url}\n")

                except Exception as e:
                    print(f"Error processing article: {e}")
                    continue
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

def crawl_article_ajkd(specialization):
    journal_name = 'American Journal of Kidney Diseases'
    connection = connection_config()
    table_name = specialization
    milvus_collection = initialize_milvus(specialization) 

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
                response = fetch_page_with_zenrows(article_url)
                soup = BeautifulSoup(response.content, "lxml")

                meta_tags = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta", attrs={"name": True})}
                og_tags = {tag.get("property"): tag.get("content") for tag in soup.find_all("meta", attrs={"property": True})}
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
                article_summary = summarize_text(article_abstract) if article_abstract != "N/A" else "N/A"
                print(f"Summariaztion done")
                article_speciality = specialization
                contributor = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_contributor"})]
                article_contributor = ', '.join(contributor) if contributor else "N/A"

                keywords = [tag.get("content") for tag in soup.find_all("meta", attrs={"name": "citation_keywords"})]
                article_keywords = ', '.join(keywords) if keywords else None  
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
                try:
                    title_embedding = [generate_embedding(article_title)]
                    print("-> Title embeddings done")
                    abstract_embedding = [generate_embedding(article_abstract)]
                    print("-> Abstract embeddings done")
                    authors_embedding = [generate_embedding(article_author)]
                    print("-> Authors embeddings done")

                    article_metadata = {
                        "title_text": article_title,
                        "abstract_text": article_abstract,
                        "authors_text": article_author,
                        "article_url": article_url,
                        "title_embedding": title_embedding,
                        "abstract_embedding": abstract_embedding,
                        "authors_embedding": authors_embedding,
                    }
                    add_to_milvus(milvus_collection, article_metadata)
                    merged = initialize_milvus_merged('merged_collection')
                    add_to_milvus(merged, article_metadata)
                    update_query = f"UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s"
                    cursor.execute(update_query, (article_url,))
                    connection.commit()
                    print(f"Successfully scraped and updated {specialization} table: {article_url}\n")

                except Exception as e:
                    print(f"Error processing article: {e}")
                    continue
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

import mysql.connector
import csv
from mysql.connector import Error
import configparser
import requests
from pymilvus import (
    CollectionSchema,
    FieldSchema,
    DataType,
    Collection,
    connections,
    utility,
)
from sentence_transformers import SentenceTransformer
from crawl4ai import *

config_path = "/home/ds/config.ini"


def generate_embedding(text):
    """
    Generates an embedding for the given text using a pre-trained model.
    Returns a list of floats representing the embedding vector.
    """
    model = SentenceTransformer(
        "all-mpnet-base-v2"
    )  # Model that returns 768-dimensional vectors
    if text == "N/A" or not text.strip():
        return [0.0] * 768  # Return a zero vector if no valid text
    embedding = model.encode(text)
    return embedding.tolist()  # Ensure we return a flat list of floats


###################################################################################################################################################################################################


def add_to_milvus(collection, article_metadata):
    """
    Add article metadata to Milvus collection.
    Args:
        collection (Collection): Milvus collection object.
        article_metadata (dict): Metadata including embeddings and text fields.
    """
    # Prepare the data for each field in the schema
    data_to_insert = [
        [article_metadata["title_text"]],  # Title text
        [article_metadata["abstract_text"]],  # Abstract text
        [article_metadata["authors_text"]],  # Authors text
        [article_metadata["article_url"]],  # Article URL
        article_metadata["title_embedding"],  # Title embedding
        article_metadata["abstract_embedding"],  # Abstract embedding
        article_metadata["authors_embedding"],  # Authors embedding
    ]

    # Insert into Milvus
    collection.insert(data_to_insert)
    print(f"Inserted metadata into Milvus: {article_metadata['title_text']}")


###################################################################################################################################################################################################
def create_index(collection_name):
    # Connect to Milvus
    connections.connect(host="localhost", port="19530")

    # Load the collection
    collection = Collection(collection_name)

    # Create an index on the embedding fields (example for 'title_embedding')
    index_params = {
        "metric_type": "L2",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128},
    }
    collection.create_index("title_embedding", index_params)
    collection.create_index("abstract_embedding", index_params)
    collection.create_index("authors_embedding", index_params)

    # Load the collection into memory
    collection.load()

    print(f"Index created and collection '{collection_name}' loaded successfully.")


###################################################################################################################################################################################################
def initialize_milvus(collection_name):
    connections.connect(host="49.75.129.99", port="19530")
    if collection_name not in utility.list_collections():
        create_milvus_collection(collection_name)
    collection = Collection(collection_name)
    return collection


###################################################################################################################################################################################################
def create_milvus_collection(collection_name):
    """Creates a collection in Milvus with the specified schema."""
    connections.connect(host="49.75.129.99", port="19530")  # Connect to Milvus

    # Define the schema with exactly seven fields if that's the desired configuration
    fields = [
        FieldSchema(
            name="id", dtype=DataType.INT64, is_primary=True, auto_id=True
        ),  # Auto ID field
        FieldSchema(
            name="title_text", dtype=DataType.VARCHAR, max_length=65535
        ),  # Title text field
        FieldSchema(
            name="abstract_text", dtype=DataType.VARCHAR, max_length=65535
        ),  # Abstract text field
        FieldSchema(
            name="authors_text", dtype=DataType.VARCHAR, max_length=65535
        ),  # Authors text field
        FieldSchema(
            name="article_url", dtype=DataType.VARCHAR, max_length=65535
        ),  # Article URL field
        FieldSchema(
            name="title_embedding", dtype=DataType.FLOAT_VECTOR, dim=768
        ),  # Embedding for title
        FieldSchema(
            name="abstract_embedding", dtype=DataType.FLOAT_VECTOR, dim=768
        ),  # Embedding for abstract
        FieldSchema(
            name="authors_embedding", dtype=DataType.FLOAT_VECTOR, dim=768
        ),  # Embedding for authors
    ]
    schema = CollectionSchema(
        fields=fields, description="Collection for articles metadata and embeddings"
    )

    # Create the collection
    Collection(name=collection_name, schema=schema)
    print(f"Collection '{collection_name}' created successfully.")


###################################################################################################################################################################################################
def print_sample_from_milvus(collection_name):
    """Print sample data from the specified Milvus collection."""
    try:
        connections.connect(host="49.75.129.99", port="19530")
        collection = Collection(collection_name)

        # Load the collection into memory
        collection.load()

        # Perform a simple query to retrieve a few records (limit 5)
        results = collection.query(
            expr="",
            output_fields=["title_text", "abstract_text", "authors_text"],
            limit=5,
        )

        # Print the results with proper encoding
        for i, result in enumerate(results):
            print(f"Record {i + 1}:")
            # Encode and decode the strings using UTF-8
            print(
                f"Title: {result['title_text'].encode('utf-8', errors='replace').decode('utf-8')}"
            )
            print(
                f"Abstract: {result['abstract_text'].encode('utf-8', errors='replace').decode('utf-8')}"
            )
            print(
                f"Authors: {result['authors_text'].encode('utf-8', errors='replace').decode('utf-8')}"
            )
            print("-" * 50)
            print("\n")

    except Exception as e:
        print(f"Error querying Milvus: {e}")
    finally:
        connections.disconnect(alias="default")


###################################################################################################################################################################################################
def connection_config():
    # Read database configuration from the config file
    config = configparser.ConfigParser()
    config.read(config_path)

    db_config = {
        "host": config["mysql"]["host"],
        "user": config["mysql"]["user"],
        "password": config["mysql"]["password"],
        "database": config["mysql"]["database"],
        "port": int(config["mysql"]["port"]),
        "charset": config["mysql"]["charset"],
    }

    # Establish connection
    conn = mysql.connector.connect(**db_config)
    return conn


###################################################################################################################################################################################################
async def fetch_page_with_zenrows(url):
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url)
        return result if result else None
'''
def fetch_page_with_zenrows(url):
    config = configparser.ConfigParser()
    config.read(config_path)
    zenrows_api_key = config["zen_row"]["zen_row_key"]
    """
    Fetches a web page using ZenRows API.

    Args:
        url (str): The URL of the page to fetch.
        zenrows_api_key (str): Your ZenRows API key.

    Returns:
        str: The HTML content of the page, or None if an error occurs.
    """
    try:
        params = {"url": url, "apikey": zenrows_api_key}
        response = requests.get("https://api.zenrows.com/v1", params=params)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None'''




###################################################################################################################################################################################################


def fetch_page_with_scraper_api(url):
    config = configparser.ConfigParser()
    config.read(config_path)
    scraper_api_key = config["scraper_api"]["scraper_api"]
    """
    Fetches a web page using ZenRows API.

    Args:
        url (str): The URL of the page to fetch.
        zenrows_api_key (str): Your ZenRows API key.

    Returns:
        str: The HTML content of the page, or None if an error occurs.
    """
    try:
        params = {"apikey": scraper_api_key, "url": url}

        response = requests.get("https://api.scraperapi.com/", params=params)
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None


###################################################################################################################################################################################################


def setup_database():
    """
    Sets up the database connection and creates the necessary table if it doesn't exist.

    Args:
        config_path (str): Path to the configuration file with database credentials.

    Returns:
        mysql.connector.connection.MySQLConnection: The database connection object, or None if an error occurs.
    """
    try:
        # Read database configuration from the config file
        config = configparser.ConfigParser()
        config.read(config_path)

        db_config = {
            "host": config["mysql"]["host"],
            "user": config["mysql"]["user"],
            "password": config["mysql"]["password"],
            "database": config["mysql"]["database"],
            "port": int(config["mysql"]["port"]),
            "charset": config["mysql"]["charset"],
        }

        # Establish connection
        conn = mysql.connector.connect(**db_config)

        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS article_links (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    journal_name VARCHAR(255),
                    article_link VARCHAR(255),
                    specialization TEXT,
                    scraped TEXT,
                    UNIQUE (article_link)
                )
            """)
            conn.commit()
            print("Database setup successful!")
            return conn
    except Error as e:
        print(f"Error: {e}")
        return None
    except KeyError as e:
        print(f"Missing configuration key: {e}")
        return None


###################################################################################################################################################################################################


def insert_into_database(conn, journal_name, article_links, specialization, len_0f_new_links):
    """
    Inserts article links into the database.

    Args:
        conn (mysql.connector.connection.MySQLConnection): The database connection object.
        journal_name (str): The name of the journal.
        article_links (list): A list of article links to insert.

    Returns:
        bool: True if insertion was successful, False otherwise.
    """
    try:
        cursor = conn.cursor()
        for link in article_links:
            cursor.execute(
                "INSERT IGNORE INTO article_links (journal_name, article_link, specialization, scraped) VALUES (%s, %s, %s, %s)",
                (journal_name, link, specialization, "pending"),
            )
        conn.commit()
        print(f"Inserted {len_0f_new_links} links into the database!")
        return True
    except Error as e:
        print(f"Error inserting data: {e}")
        return False


###################################################################################################################################################################################################


def write_to_csv(journal_name, article_links, output_file="article_links_dump.csv"):
    """
    Writes article links to a CSV file.

    Args:
        journal_name (str): The name of the journal.
        article_links (list): A list of article links to write.
        output_file (str): The name of the output CSV file.

    Returns:
        bool: True if writing was successful, False otherwise.
    """
    try:
        with open(output_file, mode="w+", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Journal Name", "Article Link"])
            for link in article_links:
                writer.writerow([journal_name, link])
        print(f"Saved {len(article_links)} links to {output_file}!")
        return True
    except Exception as e:
        print(f"Error writing to CSV: {e}")
        return False


###################################################################################################################################################################################################


def ensure_table_exists(connection, table_name):
    """
    Ensures the specified table exists. If not, creates it.

    Args:
        connection: Active MySQL connection object.
        table_name (str): Name of the table to ensure existence.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        article_speciality VARCHAR(255),
        article_url VARCHAR(255),
        article_abstract TEXT,
        article_title TEXT,
        article_journal_title TEXT,
        article_publisher TEXT,
        article_volume VARCHAR(50),
        article_issue VARCHAR(50),
        article_publication_date DATE,
        article_issn VARCHAR(50),
        article_language VARCHAR(50),
        article_identifier VARCHAR(255),
        article_author TEXT,
        article_contributor TEXT,
        article_full_pdf_link TEXT,
        article_summary TEXT,
        article_keywords TEXT,
        embeddings TEXT,
        article_ingestion_date DATE,
        UNIQUE (article_url)
    )
    """
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table '{table_name}' ensured to exist.")
    except mysql.connector.Error as e:
        print(f"Error ensuring table {table_name} exists: {e}")


###################################################################################################################################################################################################


def ensure_scraped_column_exists(cursor):
    """Ensure the 'scraped' column exists in the 'article_links' table."""
    try:
        # Check if 'scraped' column exists
        cursor.execute("SHOW COLUMNS FROM article_links LIKE 'scraped'")
        result = cursor.fetchone()
        if not result:
            # Add 'scraped' column if it does not exist
            cursor.execute(
                "ALTER TABLE article_links ADD COLUMN scraped VARCHAR(10) DEFAULT 'pending'"
            )
    except mysql.connector.Error as e:
        print(f"Error ensuring 'scraped' column exists: {e}")


###################################################################################################################################################################################################


def insert_article_metadata(table_name, data):
    """
    Inserts article metadata into the database, creating the table if it doesn't exist.

    Args:
        connection_config (dict): MySQL connection configuration.
        table_name (str): The name of the table to insert into.
        data (tuple): A tuple containing article metadata values.
    """
    try:
        config = configparser.ConfigParser()
        config.read(config_path)

        db_config = {
            "host": config["mysql"]["host"],
            "user": config["mysql"]["user"],
            "password": config["mysql"]["password"],
            "database": config["mysql"]["database"],
            "port": int(config["mysql"]["port"]),
            "charset": config["mysql"]["charset"],
        }
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            # Ensure table exists
            ensure_table_exists(connection, table_name)

            # Insert data
            cursor = connection.cursor()
            insert_query = f"""
            INSERT IGNORE INTO {table_name} (
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
                article_keywords,
                embeddings,
                article_ingestion_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, data)
            connection.commit()
            print(f"Article metadata for {data[3]} saved successfully.")

            # Close cursor and connection
            cursor.close()
            connection.close()

    except ValueError as ve:
        print(f"Value Error: {ve}")
    except mysql.connector.Error as e:
        print(f"Error inserting article metadata: {e}")

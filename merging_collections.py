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
from utils import generate_embedding, connection_config


def create_milvus_collection_merged(collection_name):
    """Creates a collection in Milvus with the specified schema."""
    connections.connect(host="localhost", port="19530")  # Connect to Milvus

    # Define the schema
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="title_text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="abstract_text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="authors_text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="article_url", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="specialization", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="title_embedding", dtype=DataType.FLOAT_VECTOR, dim=768),
        FieldSchema(name="abstract_embedding", dtype=DataType.FLOAT_VECTOR, dim=768),
        FieldSchema(name="authors_embedding", dtype=DataType.FLOAT_VECTOR, dim=768),
    ]
    schema = CollectionSchema(fields=fields, description="Collection for articles metadata and embeddings")

    # Create the collection
    Collection(name=collection_name, schema=schema)
    print(f"Collection '{collection_name}' created successfully.")


def create_index(collection_name):
    """Creates indexes on embedding fields in Milvus."""
    connections.connect(host="localhost", port="19530")
    collection = Collection(collection_name)

    index_params = {"metric_type": "L2", "index_type": "IVF_FLAT", "params": {"nlist": 128}}

    collection.create_index("title_embedding", index_params)
    collection.create_index("abstract_embedding", index_params)
    collection.create_index("authors_embedding", index_params)

    collection.load()
    print(f"Index created and collection '{collection_name}' loaded successfully.")


def initialize_milvus_merged(collection_name):
    """Initializes Milvus collection if not already present."""
    connections.connect(host="localhost", port="19530")
    if collection_name not in utility.list_collections():
        create_milvus_collection_merged(collection_name)
        create_index(collection_name)
    collection = Collection(collection_name)
    return collection


def insert_table_to_milvus(collection_name, table_name):
    """Fetches only articles with pending embeddings from a MySQL table and inserts them into Milvus."""
    # Connect to MySQL
    connection = connection_config()
    cursor = connection.cursor()

    # Fetch data count for tracking progress
    count_query = f"""
        SELECT COUNT(*) FROM {table_name} 
        WHERE embeddings IS NULL OR embeddings != 'done'
    """
    cursor.execute(count_query)
    total_rows = cursor.fetchone()[0]

    # Fetch only records that are missing embeddings
    query = f"""
        SELECT article_title, article_abstract, article_author, article_url 
        FROM {table_name} 
        WHERE embeddings IS NULL OR embeddings != 'done'
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    # Connect to Milvus collection
    collection = Collection(collection_name)

    inserted_count = 0  # Track successfully inserted records

    # Process each row
    for row in rows:
        article_title, article_abstract, article_author, article_url = row

        # Generate embeddings
        title_embedding = generate_embedding(article_title)
        print("--> Title embeddings done")
        abstract_embedding = generate_embedding(article_abstract)
        print("--> Abstract embeddings done")
        authors_embedding = generate_embedding(article_author)
        print("--> Author embeddings done")

        # Prepare data for insertion
        insert_data = [
            [article_title],
            [article_abstract],
            [article_author],
            [article_url],
            [table_name],  # Specialization = Table name
            [title_embedding],
            [abstract_embedding],
            [authors_embedding],
        ]

        # Insert into Milvus
        collection.insert(insert_data)
        inserted_count += 1

        # Update MySQL record to mark embedding as done
        update_query = f"""
            UPDATE {table_name} SET embeddings = 'done' WHERE article_url = %s
        """
        cursor.execute(update_query, (article_url,))
        connection.commit()

        print(f"Inserted {inserted_count}/{total_rows} rows from '{table_name}' into Milvus and updated MySQL.")

    print(f"Finished inserting {inserted_count}/{total_rows} rows from '{table_name}' into Milvus and updated MySQL.")



def insert_multiple_tables_to_milvus(collection_name, table_names):
    """Insert data from multiple tables (specializations) into Milvus."""
    for table_name in table_names:
        print(f"\n Processing table: {table_name} ...")
        insert_table_to_milvus(collection_name, table_name)


def main():
    collection_name = "merged_specializations"
    initialize_milvus_merged(collection_name)

    # List of MySQL tables to insert (these are also the specializations)
    table_names = ['psychiatry','geriatrics','allergy_immunology','pathology','anesthesiology','radiology',
                   'otolarynology','urology','orthopaedics','bstetrics_gynecology','hematology','infectious_diseases','rheumatology',
                   'pulmonology','nephrology','opthalmology','gastroenterology','diabetes_endocrinology','cardiology',
                   'dermatology','immunology','clinical_medicine','neuroscience','oncology','pediatrics']  # Add more as needed

    insert_multiple_tables_to_milvus(collection_name, table_names)

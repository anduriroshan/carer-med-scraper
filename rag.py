import json
import dateparser
from datetime import datetime, timedelta
from fastapi import FastAPI, Request
from pymilvus import connections, Collection
from sentence_transformers import SentenceTransformer
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain_openai import ChatOpenAI
from langchain_community.cache import InMemoryCache
from constants import *
app = FastAPI()

# Milvus & Database Configuration
MILVUS_HOST = "45.79.125.99"
MILVUS_PORT = "19530"

COLLECTION_JOURNAL_NAME = "merged_specializations"
COLLECTION_NEWS_NAME = "article_news"

DB_CONFIG = {
    "host": "45.79.125.99",
    "user": "root",
    "password": "Help@carer123",
    "database": "articles_data"
}

# Load embedding model
model = SentenceTransformer("all-mpnet-base-v2")

# Connect to MySQL
db = SQLDatabase.from_uri("mysql+mysqlconnector://root:Help%40carer123@45.79.125.99:3306/articles_data")

# Load OpenAI Model for Text-to-SQL
api_key = OPENAI_API
llm = ChatOpenAI(temperature=0, openai_api_key=api_key, model_name='gpt-4-turbo')

# SQL Query Chain
SQLDatabaseChain.cache = InMemoryCache()
SQLDatabaseChain.model_rebuild()
db_chain = SQLDatabaseChain.from_llm(llm=llm, db=db, verbose=True, return_intermediate_steps=True)
def get_journal_details(query, collection_name):
    """Retrieve top 2-3 relevant journal articles from Milvus using title, abstract, and author embeddings."""
    embedding = model.encode([query])[0]

    # Connect to Milvus
    connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
    collection = Collection(collection_name)
    collection.load()

    search_params = {"metric_type": "L2", "nprobe": 20}

    # Search in title embeddings
    results_title = collection.search(
        data=[embedding],
        anns_field="title_embedding",
        param=search_params,
        limit=2,
        output_fields=["title_text", "abstract_text", "authors_text", "article_url"]
    )

    # Search in abstract embeddings
    results_abstract = collection.search(
        data=[embedding],
        anns_field="abstract_embedding",
        param=search_params,
        limit=2,
        output_fields=["title_text", "abstract_text", "authors_text", "article_url"]
    )

    # Search in authors embeddings
    results_authors = collection.search(
        data=[embedding],
        anns_field="authors_embedding",
        param=search_params,
        limit=2,
        output_fields=["title_text", "abstract_text", "authors_text", "article_url"]
    )

    # Combine and sort results by vector distance
    combined_results = results_title[0] + results_abstract[0] + results_authors[0]
    sorted_results = sorted(combined_results, key=lambda x: x.distance)

    retrieved_articles = []
    for result in sorted_results[:3]:  # Get top 3 results after sorting
        retrieved_articles.append({
            "vector_distance": result.distance,
            "title_text": result.entity.get("title_text"),
            "article_url": result.entity.get("article_url"),
            "abstract_text": result.entity.get("abstract_text"),
            "authors_text": result.entity.get("authors_text"),
        })

    return retrieved_articles if retrieved_articles else None

def parse_time_expression(query):
    """Extract and convert time expressions into date ranges."""
    now = datetime.now()
    start_date, end_date = None, None

    if "last week" in query:
        start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")
    elif "last month" in query:
        start_date = (now.replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")
    elif "last year" in query:
        start_date = (now.replace(year=now.year - 1, month=1, day=1)).strftime("%Y-%m-%d")
        end_date = (now.replace(year=now.year - 1, month=12, day=31)).strftime("%Y-%m-%d")
    else:
        parsed_date = dateparser.parse(query, settings={'PREFER_DATES_FROM': 'past'})
        if parsed_date:
            start_date = parsed_date.strftime("%Y-%m-%d")
            end_date = start_date  

    return start_date, end_date


@app.post("/custom-journal-query")
async def chat(request: Request):
    """Fetch journal articles and execute Text-to-SQL query with context."""
    input_json = await request.json()
    queryData = input_json["queryPrompt"]

    # Extract time range
    start_date, end_date = parse_time_expression(queryData)

    # Get journal details
    journal_results = get_journal_details(queryData, COLLECTION_JOURNAL_NAME)

    # If relevant results are found in journal collection
    if journal_results and journal_results[0]["vector_distance"] < 1.0:
        retrieved_context = json.dumps(journal_results[:2])
    else:
        # Fetch from news collection if no journal articles are found
        news_results = get_journal_details(queryData, COLLECTION_NEWS_NAME)
        if news_results:
            retrieved_context = json.dumps(news_results[:2])
        else:
            return {"message": "No relevant articles found."}

    # Modify query to include date filters if applicable
    if start_date and end_date:
        date_filter = f"Filter results where `publication_date` BETWEEN '{start_date}' AND '{end_date}'. "
    else:
        date_filter = ""

    query_with_context = (
        f"Convert the following natural language request into a MySQL query. "
        f"Ensure to handle time expressions like 'last week' or 'December 2024'. "
        f"{date_filter}"
        f"User request: {queryData}. Context: {retrieved_context}."
    )

    try:
        promptResponse = db_chain.run(query_with_context)
    except Exception as e:
        print(f"SQL Query Execution Error: {e}")
        promptResponse = "Please provide a more precise query."

    return {"response": promptResponse, "context": retrieved_context}

from pymilvus import Collection, connections, utility
from sentence_transformers import SentenceTransformer
from utils import create_index

def search_milvus(query):
    """Search Milvus collection using a natural language text query."""
    # Connect to Milvus
    connections.connect(host='localhost', port='19530')
    collections = utility.list_collections()

    # Print the collections
    print("Collections in Milvus:")
    for collection in collections:
        print(collection)

    
    # Load the collection
    collection = Collection("merged_collection")
    collection.load()
    
    # Use a pre-trained model to generate embeddings from the natural language query
    model = SentenceTransformer("all-mpnet-base-v2")  # You can use a different model if needed
    embedding = model.encode([query])[0]  # Generate the embedding of the query
    
    # Define search parameters
    search_params = {
        "metric_type": "L2",  # L2 distance (Euclidean) for comparing embeddings
        "params": {"nprobe": 20}  # Set nprobe for IVF index to control the search scope
    }

    # Perform the search for the title embeddings
    results_title = collection.search(
        data=[embedding],
        anns_field="title_embedding",  # Search in the title embedding
        param=search_params,
        limit=2,  # Limit the number of results
        output_fields=["title_text", "abstract_text", "authors_text", "article_url"]
    )
    
    # Perform the search for the abstract embeddings
    results_abstract = collection.search(
        data=[embedding],
        anns_field="abstract_embedding",  # Search in the abstract embedding
        param=search_params,
        limit=2,  # Limit the number of results
        output_fields=["title_text", "abstract_text", "authors_text", "article_url"]
    )

    # Combine the results from both searches (title and abstract)
    combined_results = results_title + results_abstract
    
    # Remove duplicates (using article URL as a unique identifier)
    unique_results = {}
    for result in combined_results:
        for hit in result:
            article_url = hit.entity.get('article_url')  # Access the 'article_url' field
            if article_url not in unique_results:
                unique_results[article_url] = hit  # Keep only the first occurrence
    
    # Process and print the search results
    for result in unique_results.values():  # Iterate through unique results
        print(f"Title: {result.entity.get('title_text')}")
        print(f"Abstract: {result.entity.get('abstract_text')}")
        print(f"Authors: {result.entity.get('authors_text')}")
        print(f"URL: {result.entity.get('article_url')}")
        print(f"Distance: {result.distance}")
        print("-" * 50)

# Example query
search_milvus("Give articles related to lungs")

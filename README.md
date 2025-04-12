# Retrieval-Augmented Search for Medical Research Articles Integrating Web Scraping and Vector-Based Retrieval


## Overview

This project is a comprehensive system for aggregating, processing, and searching medical journal articles across multiple specialties. It consists of several components that work together to:

1. Crawl and collect articles from various medical journals
2. Store article metadata in a structured database
3. Generate embeddings for semantic search
4. Provide a search interface with natural language query capabilities

The system covers 25 medical specialties with dedicated crawlers for each specialty's top journals.

## System Architecture

```
├── crawl_article/          # Article content scrapers for each specialty
├── crawl_page/             # Journal page crawlers for each specialty
├── app.py                  # Streamlit UI for search interface
├── merging_collections.py  # Milvus vector database operations
├── rag.py                  # FastAPI backend for search functionality
├── update.py               # Main update script for all specialties
├── utils.py                # Shared utility functions
└── constants.py            # API keys and configuration
```

## Key Components

### 1. Crawling System

The system uses a two-phase crawling approach:

1. **Journal Crawlers** (`crawl_page/`):
   - Discover new articles from journal websites/RSS feeds
   - Store article links in MySQL database
   - Organized by medical specialty (25 specialties)

2. **Article Crawlers** (`crawl_article/`):
   - Scrape detailed metadata from individual article pages
   - Extract title, authors, abstract, keywords, etc.
   - Store full metadata in MySQL

### 2. Data Processing Pipeline

- **MySQL Database**:
  - Stores all article metadata and links
  - Tracks scraping status
  - Organized by medical specialty

- **Milvus Vector Database**:
  - Stores embeddings for semantic search
  - Indexes title, abstract, and author embeddings
  - Enables similarity search across all specialties

### 3. Search System

- **FastAPI Backend** (`rag.py`):
  - Processes natural language queries
  - Retrieves relevant articles using vector similarity
  - Supports date-based filtering
  - Integrates with MySQL for structured queries

- **Streamlit UI** (`app.py`):
  - User-friendly search interface
  - Displays search results with article details
  - Provides links to original articles

## Installation and Setup

### Prerequisites

- Python 3.8+
- MySQL server
- Milvus vector database
- Redis (for caching)
- API keys for:
  - ZenRows (web scraping)
  - OpenAI (embeddings and text-to-SQL)

### Installation Steps

1. Clone the repository:
   ```bash
   git clone [repository-url]
   cd [repository-name]
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up configuration:
   - Create `config.ini` with database credentials
   - Update API keys in `constants.py`

4. Initialize databases:
   - Run MySQL setup queries from `utils.py`
   - Initialize Milvus collections with `merging_collections.py`

## Usage

### Updating Article Database

Run the main update script to fetch new articles:
```bash
python update.py
```

This will:
1. Crawl all configured journals
2. Scrape new articles
3. Update MySQL database
4. Generate embeddings and update Milvus

### Running the Search Interface

1. Start the FastAPI backend:
   ```bash
   uvicorn rag:app --reload
   ```

2. Start the Streamlit UI:
   ```bash
   streamlit run app.py
   ```

## Supported Medical Specialties

The system currently supports 25 medical specialties:

1. Anesthesiology
2. Allergy & Immunology
3. Cardiology
4. Clinical Medicine
5. Dermatology
6. Diabetes & Endocrinology
7. Gastroenterology
8. Geriatrics
9. Hematology
10. Infectious Diseases
11. Nephrology
12. Neuroscience
13. Obstetrics & Gynecology
14. Oncology
15. Ophthalmology
16. Orthopedics
17. Otolaryngology (ENT)
18. Pathology
19. Pediatrics
20. Psychiatry
21. Pulmonology
22. Radiology
23. Rheumatology
24. Urology
25. Immunology

## Technical Details

### Database Schema

**article_links table**:
- Stores discovered article links before scraping
- Tracks scraping status (`pending`/`done`)

**Specialty tables**:
- One table per specialty (e.g., `anesthesiology`)
- Stores complete article metadata including:
  - Title, authors, abstract
  - Journal information
  - Publication dates
  - Keywords and summaries
  - PDF links

### Embedding Generation

- Uses `all-mpnet-base-v2` sentence transformer model
- Generates 768-dimensional embeddings for:
  - Article titles
  - Abstracts
  - Author lists

### Search Features

1. **Semantic Search**:
   - Finds articles similar to query meaning
   - Combines title/abstract/author similarity

2. **Temporal Filtering**:
   - Understands natural language time expressions
   - "last week", "last month", specific dates

3. **Hybrid Search**:
   - Combines vector search with SQL queries
   - Filters by specialty, journal, date ranges

## Customization

To add support for new journals or specialties:

1. Create new crawler modules in:
   - `crawl_page/` for journal discovery
   - `crawl_article/` for article scraping

2. Add the new specialty to:
   - `update.py` main update loop
   - `merging_collections.py` Milvus integration

3. Update the Streamlit UI if new search filters are needed

## Performance Considerations

- The system is designed for asynchronous operation
- Rate limiting is handled by ZenRows proxy service
- Milvus indexes are optimized for medical text search
- Caching is implemented for frequent queries

## Limitations

- Dependent on journal website structures (may break if sites change)
- Some journals may block automated scraping
- Embedding quality depends on the transformer model

# DocuFetch

DocuFetch is an intelligent document harvesting tool designed to automatically discover, track, and download academic papers and news articles based on user-specified keywords.

## Features

- Keyword-based document discovery
- Multiple source support:
  - **Academic Sources**:
    - arXiv
    - Google Scholar
    - Semantic Scholar
    - CORE
    - Crossref
    - Unpaywall
    - PubMed
    - DOAJ (Directory of Open Access Journals)
    - OpenAIRE
  - **News Sources**:
    - Various news websites via newspaper3k
- **Selective API Search**: Choose to search only academic sources, only news sources, or both
- Advanced deduplication mechanism
- Configurable update intervals
- Document statistics tracking
- Terminal-based interaction
- Automatic PDF downloads for academic papers
- Preview mode to check document counts before downloading

## Installation

```bash
pip install -r requirements.txt
```

## Usage

DocuFetch is a command-line tool with several commands:

```bash
# Add keywords to monitor
python src/main.py add "machine learning" "artificial intelligence"

# Remove keywords
python src/main.py remove "machine learning"

# Clear all keywords
python src/main.py clear

# List current keywords
python src/main.py list

# Configure document sources
python src/main.py sources --arxiv --no-scholar --news
python src/main.py sources --semantic-scholar --core --crossref
python src/main.py sources --unpaywall --pubmed --doaj --openaire
python src/main.py sources --list

# Configure API keys for sources that require them
python src/main.py api core YOUR_CORE_API_KEY
python src/main.py api crossref your.email@example.com
python src/main.py api unpaywall your.email@example.com
python src/main.py api pubmed YOUR_PUBMED_API_KEY
python src/main.py api doaj YOUR_DOAJ_API_KEY

# Set update interval (in hours)
python src/main.py interval 24

# Manually trigger document discovery (with preview)
python src/main.py fetch
python src/main.py fetch --academic-only  # Search only academic sources
python src/main.py fetch --news-only      # Search only news sources

# Preview documents without downloading
python src/main.py preview
python src/main.py preview --academic-only  # Preview only academic sources
python src/main.py preview --news-only      # Preview only news sources

# Open downloads directory
python src/main.py open

# Start continuous monitoring
python src/main.py monitor
python src/main.py monitor --academic-only  # Monitor only academic sources
python src/main.py monitor --news-only      # Monitor only news sources

# Show download statistics
python src/main.py stats
```

## Project Structure

```
docuFetch/
├── src/                       # Source code
│   ├── main.py                # Entry point and CLI interface
│   ├── sources/               # Document source implementations
│   │   ├── __init__.py
│   │   ├── academic.py        # Consolidated academic paper sources
│   │   ├── news.py            # News article sources
│   │   └── manager.py         # Source management
│   └── utils.py               # Utility functions
├── logs/                      # Application logs
├── requirements.txt           # Project dependencies
└── README.md                  # Project documentation
```

## Configuration

DocuFetch stores its configuration in `~/.docufetch/config.json`. The default configuration is:

```json
{
  "keywords": [],
  "sources": {
    "arxiv": true,
    "scholar": true,
    "news": true,
    "semantic_scholar": false,
    "core": false,
    "crossref": false,
    "unpaywall": false,
    "pubmed": false,
    "doaj": false,
    "openaire": false
  },
  "api_keys": {
    "core": "",
    "crossref_email": "",
    "unpaywall_email": "",
    "pubmed_api_key": "",
    "doaj_api_key": "",
    "openaire_api_key": ""
  },
  "update_interval": 12,
  "max_results_per_source": 50,
  "download_pdfs": true
}
```

## Downloads

Downloaded documents are stored in `~/DocuFetch_Downloads/`:
- Academic papers: `~/DocuFetch_Downloads/academic/`
- News articles: `~/DocuFetch_Downloads/news/`

## Deduplication

DocuFetch implements an advanced deduplication mechanism to prevent downloading the same document from different sources:

1. Each document is assigned a unique ID based on its title and authors
2. When a document is discovered, its ID is checked against previously downloaded documents
3. If a match is found, the document is skipped to avoid duplication
4. Deduplication metadata is stored in `~/DocuFetch_Downloads/academic/dedup/`

## API Keys

Some sources require API keys or email addresses for better rate limits:

- **CORE API**: Requires an API key. Register at [CORE API](https://core.ac.uk/services/api)
- **Crossref API**: Benefits from an email address for better rate limits
- **Unpaywall API**: Requires an email address for better rate limits
- **PubMed API**: Requires an API key. Register at [PubMed API](https://www.ncbi.nlm.nih.gov/books/NBK25500/)
- **DOAJ API**: Requires an API key. Register at [DOAJ API](https://doaj.org/api)
- **OpenAIRE API**: Requires an API key. Register at [OpenAIRE API](https://api.openaire.eu/)

## Recent Updates

- **Consolidated Academic Sources**: All academic source implementations have been merged into a single file (`academic.py`) for better maintainability
- **Selective API Search**: Added the ability to search only academic sources, only news sources, or both using the `--academic-only` and `--news-only` flags
- **Improved Code Organization**: Streamlined the codebase by removing redundant files and improving the overall structure

## Dependencies

- `requests`: HTTP requests
- `beautifulsoup4`: HTML parsing
- `scholarly`: Google Scholar API
- `newspaper3k`: News article scraping
- `arxiv`: arXiv API
- `tqdm`: Progress bars
- `colorama`: Terminal colors
- `pyfiglet`: ASCII art banners
- `schedule`: Task scheduling
- `python-dotenv`: Environment variable management

## License

MIT

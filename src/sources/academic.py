"""
Academic paper sources for DocuFetch.
This module contains all academic paper sources including base sources and extended sources.
"""

import os
import logging
import time
import json
import hashlib
from pathlib import Path
from datetime import datetime
import requests
from tqdm import tqdm
import arxiv
from scholarly import scholarly
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class BaseSource:
    """Base class for document sources."""
    
    def __init__(self, download_dir, max_results=50, download_pdfs=True, timeout=30):
        """
        Initialize the source.
        
        Args:
            download_dir: Directory to save downloaded documents
            max_results: Maximum number of results to return
            download_pdfs: Whether to download PDF files
            timeout: Request timeout in seconds
        """
        self.download_dir = Path(download_dir)
        self.max_results = max_results
        self.download_pdfs = download_pdfs
        self.timeout = timeout
        
        # Create download directory
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Create metadata directory
        self.metadata_dir = self.download_dir / "metadata"
        os.makedirs(self.metadata_dir, exist_ok=True)
        
        # Create deduplication directory
        self.dedup_dir = self.download_dir / "dedup"
        os.makedirs(self.dedup_dir, exist_ok=True)
    
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch documents for a keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of document metadata
        """
        raise NotImplementedError("Subclasses must implement fetch method")
    
    def _save_metadata(self, document, filename):
        """
        Save document metadata to a file.
        
        Args:
            document: Document metadata
            filename: Filename to save metadata to
        """
        metadata_path = self.metadata_dir / filename
        
        try:
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(document, f, indent=2)
            logger.debug(f"Saved metadata to {metadata_path}")
        except Exception as e:
            logger.error(f"Error saving metadata: {str(e)}")
    
    def _download_file(self, url, filename):
        """
        Download a file from a URL.
        
        Args:
            url: URL to download from
            filename: Filename to save to
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            response = requests.get(url, stream=True, timeout=self.timeout)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024
            
            with open(filename, 'wb') as f:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {Path(filename).name}") as pbar:
                    for data in response.iter_content(block_size):
                        f.write(data)
                        pbar.update(len(data))
            
            return True
        except requests.exceptions.Timeout:
            logger.error(f"Timeout downloading file from {url}")
            return False
        except Exception as e:
            logger.error(f"Error downloading file: {str(e)}")
            return False
    
    def _generate_unique_id(self, document):
        """
        Generate a unique ID for a document based on its content.
        
        Args:
            document: Document metadata
            
        Returns:
            str: Unique ID
        """
        # Use title and authors to create a unique ID
        content = (document["title"] + "".join(document["authors"])).lower()
        return hashlib.md5(content.encode()).hexdigest()
    
    def _is_duplicate(self, document):
        """
        Check if a document is a duplicate.
        
        Args:
            document: Document metadata
            
        Returns:
            bool: True if duplicate, False otherwise
        """
        # Generate unique ID if not already present
        if "unique_id" not in document:
            document["unique_id"] = self._generate_unique_id(document)
        
        # Check if unique ID already exists
        dedup_file = self.dedup_dir / f"{document['unique_id']}.json"
        
        if dedup_file.exists():
            # Load existing document to compare
            try:
                with open(dedup_file, 'r', encoding='utf-8') as f:
                    existing_doc = json.load(f)
                
                logger.info(f"Duplicate document found: '{document['title']}' from {document['source']} "
                           f"matches existing document from {existing_doc['source']}")
                return True
            except Exception as e:
                logger.error(f"Error reading deduplication file: {str(e)}")
                return False
        else:
            # Save document for future deduplication
            try:
                with open(dedup_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        "title": document["title"],
                        "authors": document["authors"],
                        "source": document["source"],
                        "id": document["id"],
                        "url": document["url"]
                    }, f, indent=2)
                return False
            except Exception as e:
                logger.error(f"Error saving deduplication file: {str(e)}")
                return False


class ArxivSource(BaseSource):
    """arXiv paper source."""
    
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch papers from arXiv.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        # Handle keyword as a list
        if isinstance(keyword, list):
            # Search for each keyword separately and combine results
            all_results = []
            for k in keyword:
                all_results.extend(self._fetch_single_keyword(k, preview_mode))
            return all_results
        else:
            return self._fetch_single_keyword(keyword, preview_mode)
    
    def _fetch_single_keyword(self, keyword, preview_mode=False):
        """
        Fetch papers from arXiv for a single keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        logger.info(f"Searching arXiv for '{keyword}'")
        
        try:
            # Search arXiv
            search = arxiv.Search(
                query=keyword,
                max_results=self.max_results,
                sort_by=arxiv.SortCriterion.Relevance
            )
            
            results = []
            
            for paper in search.results():
                # Create document metadata
                document = {
                    "id": paper.get_short_id(),
                    "title": paper.title,
                    "authors": [author.name for author in paper.authors],
                    "abstract": paper.summary,
                    "url": paper.entry_id,
                    "pdf_url": paper.pdf_url,
                    "published": paper.published.isoformat() if paper.published else None,
                    "source": "arxiv",
                    "keyword": keyword,
                    "fetched_at": datetime.now().isoformat(),
                    "categories": paper.categories
                }
                
                # Check for duplicates
                if self._is_duplicate(document):
                    continue
                
                # Generate filename for metadata
                metadata_filename = f"arxiv_{document['id']}.json"
                
                # Save metadata
                self._save_metadata(document, metadata_filename)
                
                # Download PDF if enabled
                if not preview_mode and self.download_pdfs:
                    pdf_filename = self.download_dir / f"arxiv_{document['id']}.pdf"
                    
                    # Check if file already exists
                    if not pdf_filename.exists():
                        logger.info(f"Downloading PDF for arXiv paper {document['id']}")
                        self._download_file(document['pdf_url'], pdf_filename)
                    else:
                        logger.info(f"PDF for arXiv paper {document['id']} already exists")
                
                results.append(document)
            
            logger.info(f"Found {len(results)} papers from arXiv for '{keyword}'")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from arXiv: {str(e)}")
            return []


class ScholarSource(BaseSource):
    """Google Scholar paper source."""
    
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch papers from Google Scholar.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        # Handle keyword as a list
        if isinstance(keyword, list):
            # Search for each keyword separately and combine results
            all_results = []
            for k in keyword:
                all_results.extend(self._fetch_single_keyword(k, preview_mode))
            return all_results
        else:
            return self._fetch_single_keyword(keyword, preview_mode)
    
    def _fetch_single_keyword(self, keyword, preview_mode=False):
        """
        Fetch papers from Google Scholar for a single keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        logger.info(f"Searching Google Scholar for '{keyword}'")
        
        try:
            # Search Google Scholar
            search_query = scholarly.search_pubs(keyword)
            results = []
            
            for i in range(self.max_results):
                try:
                    paper = next(search_query)
                    
                    # Create document metadata
                    document = {
                        "id": f"scholar_{i}_{hashlib.md5(paper.get('bib', {}).get('title', '').encode()).hexdigest()[:8]}",
                        "title": paper.get('bib', {}).get('title', 'Unknown Title'),
                        "authors": paper.get('bib', {}).get('author', []),
                        "abstract": paper.get('bib', {}).get('abstract', ''),
                        "url": paper.get('pub_url', ''),
                        "pdf_url": paper.get('eprint_url', ''),
                        "published": paper.get('bib', {}).get('pub_year', ''),
                        "source": "scholar",
                        "keyword": keyword,
                        "fetched_at": datetime.now().isoformat(),
                        "citations": paper.get('num_citations', 0)
                    }
                    
                    # Ensure authors is always a list
                    if not isinstance(document["authors"], list):
                        if document["authors"]:
                            document["authors"] = [document["authors"]]
                        else:
                            document["authors"] = []
                    
                    # Check for duplicates
                    if self._is_duplicate(document):
                        continue
                    
                    # Generate filename for metadata
                    metadata_filename = f"scholar_{document['id']}.json"
                    
                    # Save metadata
                    if not preview_mode:
                        self._save_metadata(document, metadata_filename)
                        
                        # Download PDF if enabled and URL available
                        if self.download_pdfs and document['pdf_url']:
                            pdf_filename = self.download_dir / f"scholar_{document['id']}.pdf"
                            
                            # Check if file already exists
                            if not pdf_filename.exists():
                                logger.info(f"Downloading PDF for Google Scholar paper {document['id']}")
                                self._download_file(document['pdf_url'], pdf_filename)
                            else:
                                logger.info(f"PDF for Google Scholar paper {document['id']} already exists")
                    
                    results.append(document)
                    
                except StopIteration:
                    break
                except Exception as e:
                    logger.error(f"Error processing Google Scholar result: {str(e)}")
                    continue
            
            logger.info(f"Found {len(results)} papers from Google Scholar for '{keyword}'")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from Google Scholar: {str(e)}")
            return []


class SemanticScholarSource(BaseSource):
    """Semantic Scholar paper source."""
    
    def __init__(self, download_dir, max_results=50, download_pdfs=True, api_key=None):
        """
        Initialize the Semantic Scholar source.
        
        Args:
            download_dir: Directory to save downloaded documents
            max_results: Maximum number of results to return
            download_pdfs: Whether to download PDF files
            api_key: Optional API key for Semantic Scholar
        """
        super().__init__(download_dir, max_results, download_pdfs)
        self.api_url = "https://api.semanticscholar.org/graph/v1"
        self.api_key = api_key
        self.headers = {}
        if api_key:
            self.headers = {"x-api-key": api_key}
        self.retry_delay = 2  # seconds
        self.max_retries = 3
        
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch papers from Semantic Scholar.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        # Handle keyword as a list
        if isinstance(keyword, list):
            # Search for each keyword separately and combine results
            all_results = []
            for k in keyword:
                try:
                    results = self._fetch_single_keyword(k, preview_mode)
                    all_results.extend(results)
                except Exception as e:
                    logger.error(f"Error fetching from Semantic Scholar for keyword '{k}': {str(e)}")
            return all_results
        else:
            return self._fetch_single_keyword(keyword, preview_mode)
    
    def _fetch_single_keyword(self, keyword, preview_mode=False):
        """
        Fetch papers from Semantic Scholar for a single keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        logger.info(f"Searching Semantic Scholar for '{keyword}'")
        
        for attempt in range(self.max_retries):
            try:
                # Search Semantic Scholar
                search_url = f"{self.api_url}/paper/search"
                params = {
                    "query": keyword,
                    "limit": self.max_results,
                    "fields": "paperId,title,abstract,authors,year,url,venue,publicationDate,externalIds,openAccessPdf"
                }
                
                response = requests.get(search_url, params=params, headers=self.headers, timeout=self.timeout)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', self.retry_delay))
                    logger.warning(f"Rate limited by Semantic Scholar. Retrying after {retry_after} seconds. Attempt {attempt+1}/{self.max_retries}")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                
                data = response.json()
                results = []
                
                # Check if data has the expected structure
                if not data or "data" not in data:
                    logger.warning(f"Unexpected response structure from Semantic Scholar: {data}")
                    return []
                
                for paper in data.get("data", []):
                    # Skip if paper is None or not a dict
                    if not paper or not isinstance(paper, dict):
                        continue
                    
                    # Create document metadata with safer gets
                    document = {
                        "id": paper.get("paperId", ""),
                        "title": paper.get("title", "Unknown Title"),
                        "authors": [],
                        "abstract": paper.get("abstract", ""),
                        "url": paper.get("url", ""),
                        "pdf_url": "",
                        "published": paper.get("year", ""),
                        "venue": paper.get("venue", ""),
                        "source": "semantic_scholar",
                        "keyword": keyword,
                        "fetched_at": datetime.now().isoformat(),
                        "external_ids": paper.get("externalIds", {})
                    }
                    
                    # Safely extract authors
                    if "authors" in paper and isinstance(paper["authors"], list):
                        document["authors"] = [author.get("name", "") for author in paper["authors"] if isinstance(author, dict)]
                    
                    # Safely extract PDF URL
                    if "openAccessPdf" in paper and isinstance(paper["openAccessPdf"], dict):
                        document["pdf_url"] = paper["openAccessPdf"].get("url", "")
                    
                    # Check for duplicates
                    if self._is_duplicate(document):
                        continue
                    
                    # Generate filename for metadata
                    metadata_filename = f"semantic_{document['id']}.json"
                    
                    # Save metadata
                    if not preview_mode:
                        self._save_metadata(document, metadata_filename)
                        
                        # Download PDF if enabled and URL available
                        if self.download_pdfs and document['pdf_url']:
                            pdf_filename = self.download_dir / f"semantic_{document['id']}.pdf"
                            
                            # Check if file already exists
                            if not pdf_filename.exists():
                                logger.info(f"Downloading PDF for Semantic Scholar paper {document['id']}")
                                self._download_file(document['pdf_url'], pdf_filename)
                            else:
                                logger.info(f"PDF for Semantic Scholar paper {document['id']} already exists")
                    
                    results.append(document)
                
                return results
                
            except requests.exceptions.HTTPError as e:
                logger.error(f"Error fetching from Semantic Scholar: {str(e)}")
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {self.retry_delay} seconds... (Attempt {attempt+1}/{self.max_retries})")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to fetch from Semantic Scholar after {self.max_retries} attempts")
                    return []
            except Exception as e:
                logger.error(f"Unexpected error fetching from Semantic Scholar: {str(e)}")
                return []
        
        return []


class CoreSource(BaseSource):
    """CORE API paper source."""
    
    def __init__(self, download_dir, max_results=50, download_pdfs=True, api_key=None):
        """
        Initialize the CORE source.
        
        Args:
            download_dir: Directory to save downloaded documents
            max_results: Maximum number of results to return
            download_pdfs: Whether to download PDF files
            api_key: CORE API key
        """
        super().__init__(download_dir, max_results, download_pdfs)
        self.api_key = api_key or os.getenv("CORE_API_KEY", "")
        self.api_url = "https://api.core.ac.uk/v3"
        
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch papers from CORE.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        # Handle keyword as a list
        if isinstance(keyword, list):
            # Search for each keyword separately and combine results
            all_results = []
            for k in keyword:
                all_results.extend(self._fetch_single_keyword(k, preview_mode))
            return all_results
        else:
            return self._fetch_single_keyword(keyword, preview_mode)
    
    def _fetch_single_keyword(self, keyword, preview_mode=False):
        """
        Fetch papers from CORE for a single keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        logger.info(f"Searching CORE for '{keyword}'")
        
        if not self.api_key:
            logger.warning("No API key provided for CORE. Set it with 'api core your-api-key'")
            return []
        
        try:
            # Search CORE
            search_url = f"{self.api_url}/search/works"
            headers = {"Authorization": f"Bearer {self.api_key}"}
            data = {
                "q": keyword,
                "limit": self.max_results,
                "offset": 0
            }
            
            response = requests.post(search_url, json=data, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            # Check if data has the expected structure
            if not data or "results" not in data:
                logger.warning(f"Unexpected response structure from CORE: {data}")
                return []
            
            for paper in data.get("results", []):
                # Skip if paper is None or not a dict
                if not paper or not isinstance(paper, dict):
                    continue
                
                # Create document metadata
                document = {
                    "id": paper.get("id", ""),
                    "title": paper.get("title", "Unknown Title"),
                    "authors": [],
                    "abstract": paper.get("abstract", ""),
                    "url": paper.get("sourceFulltextUrls", [""])[0] if paper.get("sourceFulltextUrls") else "",
                    "pdf_url": "",
                    "published": paper.get("publishedDate", ""),
                    "source": "core",
                    "keyword": keyword,
                    "fetched_at": datetime.now().isoformat(),
                    "doi": paper.get("doi", "")
                }
                
                # Safely extract authors
                if "authors" in paper and isinstance(paper["authors"], list):
                    document["authors"] = [author.get("name", "") for author in paper["authors"] if isinstance(author, dict)]
                
                # Safely extract PDF URL
                if "downloadUrl" in paper and paper["downloadUrl"]:
                    document["pdf_url"] = paper["downloadUrl"]
                
                # Check for duplicates
                if self._is_duplicate(document):
                    continue
                
                # Generate filename for metadata
                metadata_filename = f"core_{document['id']}.json"
                
                # Save metadata
                if not preview_mode:
                    self._save_metadata(document, metadata_filename)
                    
                    # Download PDF if enabled and URL available
                    if self.download_pdfs and document['pdf_url']:
                        pdf_filename = self.download_dir / f"core_{document['id']}.pdf"
                        
                        # Check if file already exists
                        if not pdf_filename.exists():
                            logger.info(f"Downloading PDF for CORE paper {document['id']}")
                            self._download_file(document['pdf_url'], pdf_filename)
                        else:
                            logger.info(f"PDF for CORE paper {document['id']} already exists")
                
                results.append(document)
            
            logger.info(f"Found {len(results)} papers from CORE for '{keyword}'")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from CORE: {str(e)}")
            return []


class CrossrefSource(BaseSource):
    """Crossref API paper source."""
    
    def __init__(self, download_dir, max_results=50, download_pdfs=True, email=None):
        """
        Initialize the Crossref source.
        
        Args:
            download_dir: Directory to save downloaded documents
            max_results: Maximum number of results to return
            download_pdfs: Whether to download PDF files
            email: Email for Crossref API (for better rate limits)
        """
        super().__init__(download_dir, max_results, download_pdfs)
        self.email = email or os.getenv("CROSSREF_EMAIL", "")
        self.api_url = "https://api.crossref.org/works"
        
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch papers from Crossref.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        # Handle keyword as a list
        if isinstance(keyword, list):
            # Search for each keyword separately and combine results
            all_results = []
            for k in keyword:
                all_results.extend(self._fetch_single_keyword(k, preview_mode))
            return all_results
        else:
            return self._fetch_single_keyword(keyword, preview_mode)
    
    def _fetch_single_keyword(self, keyword, preview_mode=False):
        """
        Fetch papers from Crossref for a single keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        logger.info(f"Searching Crossref for '{keyword}'")
        
        try:
            # Search Crossref
            params = {
                "query": keyword,
                "rows": self.max_results,
                "sort": "relevance",
                "order": "desc"
            }
            
            if self.email:
                params["mailto"] = self.email
            
            response = requests.get(self.api_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            # Check if data has the expected structure
            if not data or "message" not in data or "items" not in data["message"]:
                logger.warning(f"Unexpected response structure from Crossref")
                return []
            
            for paper in data["message"]["items"]:
                # Skip if paper is None or not a dict
                if not paper or not isinstance(paper, dict):
                    continue
                
                # Create document metadata
                document = {
                    "id": paper.get("DOI", "").replace("/", "_"),
                    "title": paper.get("title", ["Unknown Title"])[0] if paper.get("title") else "Unknown Title",
                    "authors": [],
                    "abstract": "",
                    "url": f"https://doi.org/{paper.get('DOI', '')}" if paper.get("DOI") else "",
                    "pdf_url": "",
                    "published": paper.get("created", {}).get("date-time", "") if paper.get("created") else "",
                    "source": "crossref",
                    "keyword": keyword,
                    "fetched_at": datetime.now().isoformat(),
                    "doi": paper.get("DOI", "")
                }
                
                # Safely extract authors
                if "author" in paper and isinstance(paper["author"], list):
                    document["authors"] = [
                        f"{author.get('given', '')} {author.get('family', '')}" 
                        for author in paper["author"] 
                        if isinstance(author, dict)
                    ]
                
                # Safely extract PDF URL
                if "link" in paper and isinstance(paper["link"], list):
                    for link in paper["link"]:
                        if isinstance(link, dict) and link.get("content-type") == "application/pdf":
                            document["pdf_url"] = link.get("URL", "")
                            break
                
                # Check for duplicates
                if self._is_duplicate(document):
                    continue
                
                # Generate filename for metadata
                metadata_filename = f"crossref_{document['id']}.json"
                
                # Save metadata
                if not preview_mode:
                    self._save_metadata(document, metadata_filename)
                    
                    # Download PDF if enabled and URL available
                    if self.download_pdfs and document['pdf_url']:
                        pdf_filename = self.download_dir / f"crossref_{document['id']}.pdf"
                        
                        # Check if file already exists
                        if not pdf_filename.exists():
                            logger.info(f"Downloading PDF for Crossref paper {document['id']}")
                            self._download_file(document['pdf_url'], pdf_filename)
                        else:
                            logger.info(f"PDF for Crossref paper {document['id']} already exists")
                
                results.append(document)
            
            logger.info(f"Found {len(results)} papers from Crossref for '{keyword}'")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from Crossref: {str(e)}")
            return []


class UnpaywallSource(BaseSource):
    """Unpaywall API paper source."""
    
    def __init__(self, download_dir, max_results=50, download_pdfs=True, email=None):
        """
        Initialize the Unpaywall source.
        
        Args:
            download_dir: Directory to save downloaded documents
            max_results: Maximum number of results to return
            download_pdfs: Whether to download PDF files
            email: Email for Unpaywall API (required)
        """
        super().__init__(download_dir, max_results, download_pdfs)
        self.email = email or os.getenv("UNPAYWALL_EMAIL", "")
        self.api_url = "https://api.unpaywall.org/v2"
        
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch papers from Unpaywall.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        # Unpaywall requires DOIs, so we need to get them from another source
        # We'll use Crossref to get DOIs
        crossref = CrossrefSource(self.download_dir, self.max_results, False)
        crossref_results = crossref.fetch(keyword, True)
        
        # Extract DOIs
        dois = [paper.get("doi", "") for paper in crossref_results if paper.get("doi")]
        
        # Limit to max_results
        dois = dois[:self.max_results]
        
        # Fetch papers from Unpaywall
        results = []
        for doi in dois:
            try:
                paper = self._fetch_by_doi(doi, preview_mode)
                if paper:
                    results.append(paper)
            except Exception as e:
                logger.error(f"Error fetching from Unpaywall for DOI {doi}: {str(e)}")
        
        return results
    
    def _fetch_by_doi(self, doi, preview_mode=False):
        """
        Fetch a paper from Unpaywall by DOI.
        
        Args:
            doi: DOI to fetch
            preview_mode: If True, only count documents without downloading
            
        Returns:
            dict: Paper metadata or None if not found
        """
        if not self.email:
            logger.warning("No email provided for Unpaywall. Set it with 'api unpaywall your-email'")
            return None
        
        logger.info(f"Fetching from Unpaywall for DOI {doi}")
        
        try:
            # Fetch from Unpaywall
            url = f"{self.api_url}/{doi}?email={self.email}"
            response = requests.get(url, timeout=self.timeout)
            
            # Check if paper was found
            if response.status_code == 404:
                logger.info(f"Paper with DOI {doi} not found in Unpaywall")
                return None
            
            response.raise_for_status()
            
            data = response.json()
            
            # Check if open access version is available
            if not data.get("is_oa", False):
                logger.info(f"No open access version available for DOI {doi}")
                return None
            
            # Find best open access PDF
            pdf_url = None
            if data.get("best_oa_location") and data["best_oa_location"].get("url_for_pdf"):
                pdf_url = data["best_oa_location"]["url_for_pdf"]
            
            # Create document metadata
            document = {
                "id": doi.replace("/", "_"),
                "title": data.get("title", "Unknown Title"),
                "authors": [],
                "abstract": "",
                "url": data.get("doi_url", ""),
                "pdf_url": pdf_url,
                "published": data.get("published_date", ""),
                "source": "unpaywall",
                "keyword": "",
                "fetched_at": datetime.now().isoformat(),
                "doi": doi,
                "journal": data.get("journal_name", ""),
                "is_open_access": data.get("is_oa", False)
            }
            
            # Extract authors if available
            if "z_authors" in data and isinstance(data["z_authors"], list):
                document["authors"] = [
                    f"{author.get('given', '')} {author.get('family', '')}" 
                    for author in data["z_authors"] 
                    if isinstance(author, dict)
                ]
            
            # Check for duplicates
            if self._is_duplicate(document):
                return None
            
            # Generate filename for metadata
            metadata_filename = f"unpaywall_{document['id']}.json"
            
            # Save metadata
            if not preview_mode:
                self._save_metadata(document, metadata_filename)
                
                # Download PDF if enabled and URL available
                if self.download_pdfs and document['pdf_url']:
                    pdf_filename = self.download_dir / f"unpaywall_{document['id']}.pdf"
                    
                    # Check if file already exists
                    if not pdf_filename.exists():
                        logger.info(f"Downloading PDF for Unpaywall paper {document['id']}")
                        self._download_file(document['pdf_url'], pdf_filename)
                    else:
                        logger.info(f"PDF for Unpaywall paper {document['id']} already exists")
            
            return document
            
        except Exception as e:
            logger.error(f"Error fetching from Unpaywall: {str(e)}")
            return None


class PubMedSource(BaseSource):
    """PubMed paper source."""
    
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch papers from PubMed.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        # Handle keyword as a list
        if isinstance(keyword, list):
            # Search for each keyword separately and combine results
            all_results = []
            for k in keyword:
                all_results.extend(self._fetch_single_keyword(k, preview_mode))
            return all_results
        else:
            return self._fetch_single_keyword(keyword, preview_mode)
    
    def _fetch_single_keyword(self, keyword, preview_mode=False):
        """
        Fetch papers from PubMed for a single keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        logger.info(f"Searching PubMed for '{keyword}'")
        
        try:
            # Search PubMed
            search_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
            search_params = {
                "db": "pubmed",
                "term": keyword,
                "retmode": "json",
                "retmax": self.max_results
            }
            
            search_response = requests.get(search_url, params=search_params, timeout=self.timeout)
            search_response.raise_for_status()
            
            search_data = search_response.json()
            
            # Check if search was successful
            if "esearchresult" not in search_data or "idlist" not in search_data["esearchresult"]:
                logger.warning(f"Unexpected response structure from PubMed search")
                return []
            
            # Get PMIDs
            pmids = search_data["esearchresult"]["idlist"]
            
            if not pmids:
                logger.info(f"No results found in PubMed for '{keyword}'")
                return []
            
            # Fetch details for each PMID
            fetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
            fetch_params = {
                "db": "pubmed",
                "id": ",".join(pmids),
                "retmode": "xml"
            }
            
            fetch_response = requests.get(fetch_url, params=fetch_params, timeout=self.timeout)
            fetch_response.raise_for_status()
            
            # Parse XML
            soup = BeautifulSoup(fetch_response.content, "xml")
            
            results = []
            
            # Process each article
            for article in soup.find_all("PubmedArticle"):
                try:
                    # Extract PMID
                    pmid = article.find("PMID").text if article.find("PMID") else ""
                    
                    # Extract title
                    title = article.find("ArticleTitle").text if article.find("ArticleTitle") else "Unknown Title"
                    
                    # Extract abstract
                    abstract = ""
                    abstract_elem = article.find("AbstractText")
                    if abstract_elem:
                        abstract = abstract_elem.text
                    
                    # Extract authors
                    authors = []
                    author_list = article.find("AuthorList")
                    if author_list:
                        for author in author_list.find_all("Author"):
                            last_name = author.find("LastName").text if author.find("LastName") else ""
                            fore_name = author.find("ForeName").text if author.find("ForeName") else ""
                            if last_name or fore_name:
                                authors.append(f"{fore_name} {last_name}".strip())
                    
                    # Extract publication date
                    pub_date = ""
                    pub_date_elem = article.find("PubDate")
                    if pub_date_elem:
                        year = pub_date_elem.find("Year").text if pub_date_elem.find("Year") else ""
                        month = pub_date_elem.find("Month").text if pub_date_elem.find("Month") else ""
                        day = pub_date_elem.find("Day").text if pub_date_elem.find("Day") else ""
                        pub_date = f"{year}-{month}-{day}" if year else ""
                    
                    # Extract DOI
                    doi = ""
                    article_id_list = article.find("ArticleIdList")
                    if article_id_list:
                        for article_id in article_id_list.find_all("ArticleId"):
                            if article_id.get("IdType") == "doi":
                                doi = article_id.text
                                break
                    
                    # Create document metadata
                    document = {
                        "id": pmid,
                        "title": title,
                        "authors": authors,
                        "abstract": abstract,
                        "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                        "pdf_url": "",
                        "published": pub_date,
                        "source": "pubmed",
                        "keyword": keyword,
                        "fetched_at": datetime.now().isoformat(),
                        "doi": doi
                    }
                    
                    # Check for duplicates
                    if self._is_duplicate(document):
                        continue
                    
                    # Generate filename for metadata
                    metadata_filename = f"pubmed_{document['id']}.json"
                    
                    # Save metadata
                    if not preview_mode:
                        self._save_metadata(document, metadata_filename)
                    
                    results.append(document)
                    
                except Exception as e:
                    logger.error(f"Error processing PubMed article: {str(e)}")
                    continue
            
            logger.info(f"Found {len(results)} papers from PubMed for '{keyword}'")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from PubMed: {str(e)}")
            return []


class DOAJSource(BaseSource):
    """Directory of Open Access Journals (DOAJ) paper source."""
    
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch papers from DOAJ.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        # Handle keyword as a list
        if isinstance(keyword, list):
            # Search for each keyword separately and combine results
            all_results = []
            for k in keyword:
                all_results.extend(self._fetch_single_keyword(k, preview_mode))
            return all_results
        else:
            return self._fetch_single_keyword(keyword, preview_mode)
    
    def _fetch_single_keyword(self, keyword, preview_mode=False):
        """
        Fetch papers from DOAJ for a single keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        logger.info(f"Searching DOAJ for '{keyword}'")
        
        try:
            # Search DOAJ
            search_url = "https://doaj.org/api/v2/search/articles"
            params = {
                "query": keyword,
                "pageSize": self.max_results
            }
            
            response = requests.get(search_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            # Check if data has the expected structure
            if not data or "results" not in data:
                logger.warning(f"Unexpected response structure from DOAJ")
                return []
            
            for paper in data.get("results", []):
                # Skip if paper is None or not a dict
                if not paper or not isinstance(paper, dict):
                    continue
                
                # Extract bibjson data
                bibjson = paper.get("bibjson", {})
                
                # Create document metadata
                document = {
                    "id": paper.get("id", ""),
                    "title": bibjson.get("title", "Unknown Title"),
                    "authors": [],
                    "abstract": bibjson.get("abstract", ""),
                    "url": "",
                    "pdf_url": "",
                    "published": "",
                    "source": "doaj",
                    "keyword": keyword,
                    "fetched_at": datetime.now().isoformat(),
                    "doi": "",
                    "journal": bibjson.get("journal", {}).get("title", "")
                }
                
                # Safely extract authors
                if "author" in bibjson and isinstance(bibjson["author"], list):
                    document["authors"] = [
                        f"{author.get('name', '')}" 
                        for author in bibjson["author"] 
                        if isinstance(author, dict) and "name" in author
                    ]
                
                # Safely extract publication date
                if "year" in bibjson and "month" in bibjson:
                    document["published"] = f"{bibjson['year']}-{bibjson['month']}"
                
                # Safely extract DOI
                if "identifier" in bibjson and isinstance(bibjson["identifier"], list):
                    for identifier in bibjson["identifier"]:
                        if isinstance(identifier, dict) and identifier.get("type") == "doi":
                            document["doi"] = identifier.get("id", "")
                            document["url"] = f"https://doi.org/{document['doi']}"
                            break
                
                # Safely extract PDF URL
                if "link" in bibjson and isinstance(bibjson["link"], list):
                    for link in bibjson["link"]:
                        if isinstance(link, dict) and link.get("type") == "fulltext":
                            document["pdf_url"] = link.get("url", "")
                            break
                
                # Check for duplicates
                if self._is_duplicate(document):
                    continue
                
                # Generate filename for metadata
                metadata_filename = f"doaj_{document['id']}.json"
                
                # Save metadata
                if not preview_mode:
                    self._save_metadata(document, metadata_filename)
                    
                    # Download PDF if enabled and URL available
                    if self.download_pdfs and document['pdf_url']:
                        pdf_filename = self.download_dir / f"doaj_{document['id']}.pdf"
                        
                        # Check if file already exists
                        if not pdf_filename.exists():
                            logger.info(f"Downloading PDF for DOAJ paper {document['id']}")
                            self._download_file(document['pdf_url'], pdf_filename)
                        else:
                            logger.info(f"PDF for DOAJ paper {document['id']} already exists")
                
                results.append(document)
            
            logger.info(f"Found {len(results)} papers from DOAJ for '{keyword}'")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from DOAJ: {str(e)}")
            return []


class OpenAIRESource(BaseSource):
    """OpenAIRE paper source."""
    
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch papers from OpenAIRE.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        # Handle keyword as a list
        if isinstance(keyword, list):
            # Search for each keyword separately and combine results
            all_results = []
            for k in keyword:
                all_results.extend(self._fetch_single_keyword(k, preview_mode))
            return all_results
        else:
            return self._fetch_single_keyword(keyword, preview_mode)
    
    def _fetch_single_keyword(self, keyword, preview_mode=False):
        """
        Fetch papers from OpenAIRE for a single keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        logger.info(f"Searching OpenAIRE for '{keyword}'")
        
        try:
            # Search OpenAIRE
            search_url = "https://api.openaire.eu/search/publications"
            params = {
                "keywords": keyword,
                "size": self.max_results,
                "format": "json"
            }
            
            response = requests.get(search_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            # Check if data has the expected structure
            if not data or "response" not in data or "results" not in data["response"]:
                logger.warning(f"Unexpected response structure from OpenAIRE")
                return []
            
            # Extract results
            results_data = data["response"]["results"]
            
            if "result" not in results_data:
                logger.info(f"No results found in OpenAIRE for '{keyword}'")
                return []
            
            # Ensure result is a list
            result_list = results_data["result"]
            if not isinstance(result_list, list):
                result_list = [result_list]
            
            for paper in result_list:
                try:
                    # Extract metadata
                    metadata = paper.get("metadata", {}).get("oaf:entity", {}).get("oaf:result", {})
                    
                    # Skip if metadata is missing
                    if not metadata:
                        continue
                    
                    # Extract title
                    title = "Unknown Title"
                    title_elem = metadata.get("title")
                    if title_elem:
                        if isinstance(title_elem, dict) and "$" in title_elem:
                            title = title_elem["$"]
                        elif isinstance(title_elem, str):
                            title = title_elem
                    
                    # Extract authors
                    authors = []
                    creator_elem = metadata.get("creator")
                    if creator_elem:
                        if isinstance(creator_elem, list):
                            for creator in creator_elem:
                                if isinstance(creator, dict) and "$" in creator:
                                    authors.append(creator["$"])
                                elif isinstance(creator, str):
                                    authors.append(creator)
                        elif isinstance(creator_elem, dict) and "$" in creator_elem:
                            authors.append(creator_elem["$"])
                        elif isinstance(creator_elem, str):
                            authors.append(creator_elem)
                    
                    # Extract date
                    date = ""
                    date_elem = metadata.get("dateofacceptance")
                    if date_elem:
                        if isinstance(date_elem, dict) and "$" in date_elem:
                            date = date_elem["$"]
                        elif isinstance(date_elem, str):
                            date = date_elem
                    
                    # Extract DOI
                    doi = ""
                    pid_elem = metadata.get("pid")
                    if pid_elem and isinstance(pid_elem, list):
                        for pid in pid_elem:
                            if isinstance(pid, dict) and pid.get("@classid") == "doi":
                                doi = pid.get("$", "")
                                break
                    
                    # Extract URL
                    url = ""
                    web_url_elem = metadata.get("webresource")
                    if web_url_elem and isinstance(web_url_elem, list):
                        for web_url in web_url_elem:
                            if isinstance(web_url, dict) and "$" in web_url:
                                url = web_url["$"]
                                break
                    
                    # Create document metadata
                    document = {
                        "id": paper.get("header", {}).get("dri:objIdentifier", ""),
                        "title": title,
                        "authors": authors,
                        "abstract": "",
                        "url": url or (f"https://doi.org/{doi}" if doi else ""),
                        "pdf_url": "",
                        "published": date,
                        "source": "openaire",
                        "keyword": keyword,
                        "fetched_at": datetime.now().isoformat(),
                        "doi": doi
                    }
                    
                    # Check for duplicates
                    if self._is_duplicate(document):
                        continue
                    
                    # Generate filename for metadata
                    metadata_filename = f"openaire_{document['id']}.json"
                    
                    # Save metadata
                    if not preview_mode:
                        self._save_metadata(document, metadata_filename)
                    
                    results.append(document)
                    
                except Exception as e:
                    logger.error(f"Error processing OpenAIRE paper: {str(e)}")
                    continue
            
            logger.info(f"Found {len(results)} papers from OpenAIRE for '{keyword}'")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from OpenAIRE: {str(e)}")
            return []

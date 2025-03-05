"""
Extended academic paper sources for DocuFetch.
"""

import os
import json
import requests
import logging
import time
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm
from .academic import BaseSource

logger = logging.getLogger(__name__)


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
                
                response = requests.get(search_url, params=params, headers=self.headers)
                
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
                    self._save_metadata(document, metadata_filename)
                    
                    # Download PDF if enabled and URL available
                    if not preview_mode and self.download_pdfs and document['pdf_url']:
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
            logger.warning("No CORE API key provided. Set the CORE_API_KEY environment variable.")
            return []
        
        try:
            # Search CORE
            search_url = f"{self.api_url}/search/works"
            headers = {
                "Authorization": f"Bearer {self.api_key}"
            }
            
            payload = {
                "q": keyword,
                "limit": self.max_results,
                "scroll": True
            }
            
            response = requests.post(search_url, json=payload, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for paper in data.get("results", []):
                # Create document metadata
                document = {
                    "id": paper.get("id", ""),
                    "title": paper.get("title", "Unknown Title"),
                    "authors": [author.get("name", "") for author in paper.get("authors", [])],
                    "abstract": paper.get("abstract", ""),
                    "url": paper.get("downloadUrl", ""),
                    "pdf_url": paper.get("downloadUrl", ""),
                    "published": paper.get("publishedDate", ""),
                    "source": "core",
                    "keyword": keyword,
                    "fetched_at": datetime.now().isoformat(),
                    "doi": paper.get("doi", "")
                }
                
                # Check for duplicates
                if self._is_duplicate(document):
                    continue
                
                # Generate filename for metadata
                metadata_filename = f"core_{document['id']}.json"
                
                # Save metadata
                self._save_metadata(document, metadata_filename)
                
                # Download PDF if enabled and URL available
                if not preview_mode and self.download_pdfs and document['pdf_url']:
                    pdf_filename = self.download_dir / f"core_{document['id']}.pdf"
                    
                    # Check if file already exists
                    if not pdf_filename.exists():
                        logger.info(f"Downloading PDF for CORE paper {document['id']}")
                        self._download_file(document['pdf_url'], pdf_filename)
                    else:
                        logger.info(f"PDF for CORE paper {document['id']} already exists")
                
                results.append(document)
            
            logger.info(f"Found {len(results)} papers on CORE for '{keyword}'")
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
            
            # Add email for better rate limits if available
            if self.email:
                params["mailto"] = self.email
            
            response = requests.get(self.api_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for item in data.get("message", {}).get("items", []):
                # Extract authors
                authors = []
                for author in item.get("author", []):
                    name_parts = []
                    if "given" in author:
                        name_parts.append(author["given"])
                    if "family" in author:
                        name_parts.append(author["family"])
                    authors.append(" ".join(name_parts))
                
                # Extract PDF URL if available
                pdf_url = ""
                for link in item.get("link", []):
                    if link.get("content-type", "").lower() == "application/pdf":
                        pdf_url = link.get("URL", "")
                        break
                
                # Create document metadata
                document = {
                    "id": item.get("DOI", "").replace("/", "_"),
                    "title": item.get("title", ["Unknown Title"])[0] if item.get("title") else "Unknown Title",
                    "authors": authors,
                    "abstract": item.get("abstract", ""),
                    "url": f"https://doi.org/{item.get('DOI', '')}" if item.get("DOI") else "",
                    "pdf_url": pdf_url,
                    "published": item.get("published", {}).get("date-parts", [[""]])[0][0],
                    "source": "crossref",
                    "keyword": keyword,
                    "fetched_at": datetime.now().isoformat(),
                    "doi": item.get("DOI", "")
                }
                
                # Check for duplicates
                if self._is_duplicate(document):
                    continue
                
                # Generate filename for metadata
                metadata_filename = f"crossref_{document['id']}.json"
                
                # Save metadata
                self._save_metadata(document, metadata_filename)
                
                # Download PDF if enabled and URL available
                if not preview_mode and self.download_pdfs and document['pdf_url']:
                    pdf_filename = self.download_dir / f"crossref_{document['id']}.pdf"
                    
                    # Check if file already exists
                    if not pdf_filename.exists():
                        logger.info(f"Downloading PDF for Crossref paper {document['id']}")
                        self._download_file(document['pdf_url'], pdf_filename)
                    else:
                        logger.info(f"PDF for Crossref paper {document['id']} already exists")
                
                results.append(document)
            
            logger.info(f"Found {len(results)} papers on Crossref for '{keyword}'")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching from Crossref: {str(e)}")
            return []

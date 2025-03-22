"""
Academic paper sources for DocuFetch.
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

logger = logging.getLogger(__name__)


class BaseSource:
    """Base class for document sources."""
    
    def __init__(self, download_dir, max_results=50, download_pdfs=True):
        """
        Initialize the source.
        
        Args:
            download_dir: Directory to save downloaded documents
            max_results: Maximum number of results to return
            download_pdfs: Whether to download PDF files
        """
        self.download_dir = Path(download_dir)
        self.max_results = max_results
        self.download_pdfs = download_pdfs
        
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
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024
            
            with open(filename, 'wb') as f:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {Path(filename).name}") as pbar:
                    for data in response.iter_content(block_size):
                        f.write(data)
                        pbar.update(len(data))
            
            return True
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
                sort_by=arxiv.SortCriterion.SubmittedDate,
                sort_order=arxiv.SortOrder.Descending
            )
            
            results = []
            
            for paper in search.results():
                # Generate unique ID
                paper_id = paper.get_short_id()
                
                # Check if already downloaded
                if self._is_duplicate(paper_id, paper.title):
                    logger.debug(f"Skipping duplicate paper: {paper.title}")
                    continue
                
                # Create document metadata
                document = {
                    'id': paper_id,
                    'title': paper.title,
                    'authors': [author.name for author in paper.authors],
                    'abstract': paper.summary,
                    'url': paper.entry_id,
                    'pdf_url': paper.pdf_url,
                    'published': paper.published.strftime('%Y-%m-%d'),
                    'source': 'arxiv',
                    'keyword': keyword
                }
                
                # Record as processed to avoid duplicates
                self._record_processed(paper_id, paper.title)
                
                # Generate metadata filename
                metadata_filename = f"arxiv_{paper_id}.json"
                
                # Save metadata
                self._save_metadata(document, metadata_filename)
                
                # Download PDF if enabled and not in preview mode
                if self.download_pdfs and not preview_mode:
                    pdf_filename = self.download_dir / f"arxiv_{document['id']}.pdf"
                    
                    # Check if file already exists
                    if not pdf_filename.exists():
                        logger.info(f"Downloading PDF for {document['title']}")
                        self._download_file(document['pdf_url'], pdf_filename)
                        document['local_pdf'] = str(pdf_filename)
                    else:
                        logger.debug(f"PDF already exists for {document['title']}")
                        document['local_pdf'] = str(pdf_filename)
                
                results.append(document)
            
            logger.info(f"Found {len(results)} papers on arXiv for '{keyword}'")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from arXiv: {str(e)}")
            return []
    
    def _is_duplicate(self, paper_id, title):
        """
        Check if a paper is a duplicate.
        
        Args:
            paper_id: Paper ID
            title: Paper title
            
        Returns:
            bool: True if duplicate, False otherwise
        """
        # Check if paper ID already exists
        dedup_file = self.dedup_dir / f"{paper_id}.json"
        
        if dedup_file.exists():
            # Load existing document to compare
            try:
                with open(dedup_file, 'r', encoding='utf-8') as f:
                    existing_doc = json.load(f)
                
                logger.info(f"Duplicate document found: '{title}' matches existing document")
                return True
            except Exception as e:
                logger.error(f"Error reading deduplication file: {str(e)}")
                return False
        else:
            # Save document for future deduplication
            try:
                with open(dedup_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        "title": title,
                        "id": paper_id
                    }, f, indent=2)
                return False
            except Exception as e:
                logger.error(f"Error saving deduplication file: {str(e)}")
                return False
    
    def _record_processed(self, paper_id, title):
        """
        Record a paper as processed.
        
        Args:
            paper_id: Paper ID
            title: Paper title
        """
        # Save document for future deduplication
        try:
            with open(self.dedup_dir / f"{paper_id}.json", 'w', encoding='utf-8') as f:
                json.dump({
                    "title": title,
                    "id": paper_id
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving deduplication file: {str(e)}")


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
            query = scholarly.search_pubs(keyword)
            
            results = []
            count = 0
            
            for result in query:
                if count >= self.max_results:
                    break
                
                # Get full publication data
                try:
                    pub = scholarly.fill(result)
                except Exception as e:
                    logger.warning(f"Error filling publication data: {str(e)}")
                    continue
                
                try:
                    # Create document metadata
                    document = {
                        "id": pub.get('pub_url', '').split('=')[-1] if pub.get('pub_url') else f"scholar_{count}",
                        "title": pub.get('bib', {}).get('title', 'Unknown Title'),
                        "authors": pub.get('bib', {}).get('author', []) if isinstance(pub.get('bib', {}).get('author', []), list) else [pub.get('bib', {}).get('author', 'Unknown')],
                        "abstract": pub.get('bib', {}).get('abstract', ''),
                        "url": pub.get('pub_url', ''),
                        "pdf_url": pub.get('eprint_url', ''),
                        "published": pub.get('bib', {}).get('pub_year', ''),
                        "source": "scholar",
                        "keyword": keyword,
                        "fetched_at": datetime.now().isoformat(),
                        "citations": pub.get('num_citations', 0)
                    }
                    
                    # Generate unique ID for deduplication
                    document["unique_id"] = self._generate_unique_id(document)
                    
                    # Check for duplicates
                    if self._is_duplicate(document):
                        count += 1
                        continue
                    
                    # Generate filename for metadata
                    metadata_filename = f"scholar_{document['id']}.json"
                    
                    # Save metadata if not in preview mode
                    if not preview_mode:
                        self._save_metadata(document, metadata_filename)
                    
                    # Download PDF if enabled, not in preview mode, and URL available
                    if self.download_pdfs and not preview_mode and document['pdf_url']:
                        pdf_filename = self.download_dir / f"scholar_{document['id']}.pdf"
                        
                        # Check if file already exists
                        if not pdf_filename.exists():
                            logger.info(f"Downloading PDF for Scholar paper {document['id']}")
                            self._download_file(document['pdf_url'], pdf_filename)
                        else:
                            logger.info(f"PDF for Scholar paper {document['id']} already exists")
                    
                    results.append(document)
                    count += 1
                except Exception as e:
                    logger.warning(f"Error processing Scholar result: {str(e)}")
                    continue
                
                # Add a small delay to avoid rate limiting
                time.sleep(2)
            
            logger.info(f"Found {len(results)} papers on Google Scholar for '{keyword}'")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching from Google Scholar: {str(e)}")
            return []

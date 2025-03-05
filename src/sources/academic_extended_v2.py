"""
Additional academic paper sources for DocuFetch.
This module implements various free academic APIs with deduplication handling.
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
from bs4 import BeautifulSoup

from .academic import BaseSource

logger = logging.getLogger(__name__)


class UnpaywallSource(BaseSource):
    """Unpaywall API source for finding open access versions of papers by DOI."""
    
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
        self.api_url = "https://api.unpaywall.org/v2/"
        
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch papers from Unpaywall based on DOIs extracted from keyword.
        
        Note: Unpaywall API requires DOIs, so we extract DOIs from the keyword.
        
        Args:
            keyword: Keyword to search for (or DOI)
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
        Fetch papers from Unpaywall for a single keyword or DOI.
        
        Args:
            keyword: Keyword to search for (or DOI)
            preview_mode: If True, only count documents without downloading
            
        Returns:
            list: List of paper metadata
        """
        logger.info(f"Searching Unpaywall for '{keyword}'")
        
        if not self.email:
            logger.warning("No email provided for Unpaywall API. Set it with 'api unpaywall your.email@example.com'")
            return []
        
        # Check if keyword looks like a DOI
        if keyword.startswith("10.") and "/" in keyword:
            doi = keyword
            logger.info(f"Searching Unpaywall for DOI: {doi}")
            
            try:
                # Query Unpaywall for this DOI
                url = f"{self.api_url}{doi}?email={self.email}"
                response = requests.get(url)
                response.raise_for_status()
                
                data = response.json()
                results = []
                
                # Check if we got a valid response
                if "title" in data:
                    # Create document metadata
                    document = {
                        "id": data.get("doi", "").replace("/", "_"),
                        "title": data.get("title", "Unknown Title"),
                        "authors": [author.get("given", "") + " " + author.get("family", "") 
                                   for author in data.get("z_authors", [])],
                        "abstract": "",  # Unpaywall doesn't provide abstracts
                        "url": data.get("doi_url", ""),
                        "pdf_url": "",
                        "published": data.get("year", ""),
                        "source": "unpaywall",
                        "keyword": keyword,
                        "fetched_at": datetime.now().isoformat(),
                        "doi": data.get("doi", "")
                    }
                    
                    # Find best open access PDF URL if available
                    best_oa_location = None
                    for location in data.get("oa_locations", []):
                        if location.get("url_for_pdf"):
                            if best_oa_location is None or location.get("version") == "publishedVersion":
                                best_oa_location = location
                    
                    if best_oa_location:
                        document["pdf_url"] = best_oa_location.get("url_for_pdf", "")
                    
                    # Check for duplicates
                    if self._is_duplicate(document):
                        return []
                    
                    # Generate filename for metadata
                    metadata_filename = f"unpaywall_{document['id']}.json"
                    
                    # Save metadata
                    self._save_metadata(document, metadata_filename)
                    
                    if not preview_mode:
                        # Download PDF if enabled and URL available
                        if self.download_pdfs and document['pdf_url']:
                            pdf_filename = f"unpaywall_{document['id']}.pdf"
                            self._download_pdf(document['pdf_url'], pdf_filename)
                            document['local_pdf'] = str(self.download_dir / pdf_filename)
                    
                    results.append(document)
                
                return results
                
            except Exception as e:
                logger.error(f"Error fetching from Unpaywall: {str(e)}")
                return []
        else:
            logger.info(f"Unpaywall requires a DOI, not a keyword. Skipping search for: {keyword}")
            return []


class PubMedSource(BaseSource):
    """PubMed/NCBI E-utilities API source."""
    
    def __init__(self, download_dir, max_results=50, download_pdfs=True, email=None):
        """
        Initialize the PubMed source.
        
        Args:
            download_dir: Directory to save downloaded documents
            max_results: Maximum number of results to return
            download_pdfs: Whether to download PDF files
            email: Email for NCBI E-utilities (recommended for better rate limits)
        """
        super().__init__(download_dir, max_results, download_pdfs)
        self.email = email or os.getenv("NCBI_EMAIL", "")
        self.api_base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
        
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
            # Step 1: Search PubMed for the keyword
            search_url = f"{self.api_base}esearch.fcgi"
            params = {
                "db": "pubmed",
                "term": keyword,
                "retmax": self.max_results,
                "retmode": "json",
                "sort": "relevance"
            }
            
            if self.email:
                params["email"] = self.email
                
            response = requests.get(search_url, params=params)
            response.raise_for_status()
            
            search_data = response.json()
            pmids = search_data.get("esearchresult", {}).get("idlist", [])
            
            if not pmids:
                logger.info(f"No results found in PubMed for '{keyword}'")
                return []
                
            # Step 2: Fetch details for the found PMIDs
            fetch_url = f"{self.api_base}efetch.fcgi"
            fetch_params = {
                "db": "pubmed",
                "id": ",".join(pmids),
                "retmode": "xml"
            }
            
            if self.email:
                fetch_params["email"] = self.email
                
            fetch_response = requests.get(fetch_url, params=fetch_params)
            fetch_response.raise_for_status()
            
            # Parse XML response
            soup = BeautifulSoup(fetch_response.text, 'xml')
            articles = soup.find_all("PubmedArticle")
            
            results = []
            
            for article in articles:
                # Extract article details
                pmid = article.find("PMID").text if article.find("PMID") else ""
                
                # Get article title
                title_element = article.find("ArticleTitle")
                title = title_element.text if title_element else "Unknown Title"
                
                # Get authors
                author_list = article.find_all("Author")
                authors = []
                for author in author_list:
                    last_name = author.find("LastName")
                    fore_name = author.find("ForeName")
                    if last_name and fore_name:
                        authors.append(f"{fore_name.text} {last_name.text}")
                    elif last_name:
                        authors.append(last_name.text)
                
                # Get abstract
                abstract_text = ""
                abstract_element = article.find("AbstractText")
                if abstract_element:
                    abstract_text = abstract_element.text
                
                # Get DOI
                doi = ""
                article_id_list = article.find("ArticleIdList")
                if article_id_list:
                    for article_id in article_id_list.find_all("ArticleId"):
                        if article_id.get("IdType") == "doi":
                            doi = article_id.text
                            break
                
                # Get publication date
                pub_date = ""
                pub_date_element = article.find("PubDate")
                if pub_date_element:
                    year = pub_date_element.find("Year")
                    if year:
                        pub_date = year.text
                
                # Create document metadata
                document = {
                    "id": pmid,
                    "title": title,
                    "authors": authors,
                    "abstract": abstract_text,
                    "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                    "pdf_url": "",  # PubMed doesn't provide direct PDF links
                    "published": pub_date,
                    "source": "pubmed",
                    "keyword": keyword,
                    "fetched_at": datetime.now().isoformat(),
                    "doi": doi,
                    "pmid": pmid
                }
                
                # Check for duplicates
                if self._is_duplicate(document):
                    continue
                
                # Generate filename for metadata
                metadata_filename = f"pubmed_{document['id']}.json"
                
                # Save metadata
                self._save_metadata(document, metadata_filename)
                
                if not preview_mode:
                    # We don't have direct PDF links from PubMed, so we can't download PDFs
                    pass
                
                results.append(document)
            
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from PubMed: {str(e)}")
            return []


class DOAJSource(BaseSource):
    """Directory of Open Access Journals (DOAJ) API source."""
    
    def __init__(self, download_dir, max_results=50, download_pdfs=True, api_key=None):
        """
        Initialize the DOAJ source.
        
        Args:
            download_dir: Directory to save downloaded documents
            max_results: Maximum number of results to return
            download_pdfs: Whether to download PDF files
            api_key: DOAJ API key (optional, for higher rate limits)
        """
        super().__init__(download_dir, max_results, download_pdfs)
        self.api_key = api_key or os.getenv("DOAJ_API_KEY", "")
        self.api_url = "https://doaj.org/api/search/articles"
        
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
            params = {
                "q": keyword,
                "pageSize": self.max_results
            }
            
            headers = {}
            if self.api_key:
                headers["x-api-key"] = self.api_key
                
            response = requests.get(self.api_url, params=params, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for item in data.get("results", []):
                bibjson = item.get("bibjson", {})
                
                # Extract authors
                authors = []
                for author in bibjson.get("author", []):
                    name = author.get("name", "")
                    if name:
                        authors.append(name)
                
                # Extract DOI
                doi = ""
                for identifier in bibjson.get("identifier", []):
                    if identifier.get("type") == "doi":
                        doi = identifier.get("id", "")
                        break
                
                # Extract PDF URL
                pdf_url = ""
                for link in bibjson.get("link", []):
                    if link.get("type") == "fulltext" and link.get("content_type", "").lower() == "application/pdf":
                        pdf_url = link.get("url", "")
                        break
                
                # Create document metadata
                document = {
                    "id": item.get("id", ""),
                    "title": bibjson.get("title", "Unknown Title"),
                    "authors": authors,
                    "abstract": bibjson.get("abstract", ""),
                    "url": "",
                    "pdf_url": pdf_url,
                    "published": bibjson.get("year", ""),
                    "source": "doaj",
                    "keyword": keyword,
                    "fetched_at": datetime.now().isoformat(),
                    "doi": doi,
                    "journal": bibjson.get("journal", {}).get("title", "")
                }
                
                # Set URL (prefer DOI URL, fallback to first link)
                if doi:
                    document["url"] = f"https://doi.org/{doi}"
                elif bibjson.get("link") and len(bibjson.get("link")) > 0:
                    document["url"] = bibjson.get("link")[0].get("url", "")
                
                # Check for duplicates
                if self._is_duplicate(document):
                    continue
                
                # Generate filename for metadata
                metadata_filename = f"doaj_{document['id']}.json"
                
                # Save metadata
                self._save_metadata(document, metadata_filename)
                
                if not preview_mode:
                    # Download PDF if enabled and URL available
                    if self.download_pdfs and document['pdf_url']:
                        pdf_filename = f"doaj_{document['id']}.pdf"
                        self._download_pdf(document['pdf_url'], pdf_filename)
                        document['local_pdf'] = str(self.download_dir / pdf_filename)
                
                results.append(document)
            
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from DOAJ: {str(e)}")
            return []


class OpenAIRESource(BaseSource):
    """OpenAIRE API source."""
    
    def __init__(self, download_dir, max_results=50, download_pdfs=True):
        """
        Initialize the OpenAIRE source.
        
        Args:
            download_dir: Directory to save downloaded documents
            max_results: Maximum number of results to return
            download_pdfs: Whether to download PDF files
        """
        super().__init__(download_dir, max_results, download_pdfs)
        self.api_url = "https://api.openaire.eu/search/publications"
        
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
            params = {
                "keywords": keyword,
                "size": self.max_results,
                "format": "json"
            }
            
            response = requests.get(self.api_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            # OpenAIRE response structure can be complex
            response_data = data.get("response", {}).get("results", {}).get("result", [])
            
            if not response_data:
                logger.info(f"No results found in OpenAIRE for '{keyword}'")
                return []
            
            for item in response_data:
                metadata = item.get("metadata", {}).get("oaf:entity", {}).get("oaf:result", {})
                
                # Extract title
                title = "Unknown Title"
                title_element = metadata.get("title")
                if title_element and isinstance(title_element, dict) and "$" in title_element:
                    title = title_element.get("$", "Unknown Title")
                elif title_element and isinstance(title_element, list) and len(title_element) > 0:
                    if isinstance(title_element[0], dict) and "$" in title_element[0]:
                        title = title_element[0].get("$", "Unknown Title")
                
                # Extract authors
                authors = []
                creator_element = metadata.get("creator")
                if creator_element:
                    if isinstance(creator_element, list):
                        for creator in creator_element:
                            if isinstance(creator, dict) and "$" in creator:
                                authors.append(creator.get("$", ""))
                    elif isinstance(creator_element, dict) and "$" in creator_element:
                        authors.append(creator_element.get("$", ""))
                
                # Extract DOI
                doi = ""
                pid_element = metadata.get("pid")
                if pid_element and isinstance(pid_element, list):
                    for pid in pid_element:
                        if isinstance(pid, dict) and pid.get("@classid") == "doi":
                            doi = pid.get("$", "")
                            break
                
                # Extract URL and PDF URL
                url = ""
                pdf_url = ""
                instance_element = metadata.get("instance")
                if instance_element and isinstance(instance_element, list):
                    for instance in instance_element:
                        if isinstance(instance, dict):
                            # Get URL
                            webresource = instance.get("webresource")
                            if webresource and isinstance(webresource, dict) and "url" in webresource:
                                url_value = webresource.get("url", {}).get("$", "")
                                if url_value:
                                    url = url_value
                                    # Check if it's a PDF
                                    if url_value.lower().endswith(".pdf"):
                                        pdf_url = url_value
                
                # If we have a DOI but no URL, create a DOI URL
                if doi and not url:
                    url = f"https://doi.org/{doi}"
                
                # Create document metadata
                document = {
                    "id": doi.replace("/", "_") if doi else hashlib.md5(title.encode()).hexdigest(),
                    "title": title,
                    "authors": authors,
                    "abstract": "",  # OpenAIRE doesn't consistently provide abstracts
                    "url": url,
                    "pdf_url": pdf_url,
                    "published": metadata.get("dateofacceptance", {}).get("$", ""),
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
                self._save_metadata(document, metadata_filename)
                
                if not preview_mode:
                    # Download PDF if enabled and URL available
                    if self.download_pdfs and document['pdf_url']:
                        pdf_filename = f"openaire_{document['id']}.pdf"
                        self._download_pdf(document['pdf_url'], pdf_filename)
                        document['local_pdf'] = str(self.download_dir / pdf_filename)
                
                results.append(document)
            
            return results
            
        except Exception as e:
            logger.error(f"Error fetching from OpenAIRE: {str(e)}")
            return []

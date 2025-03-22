"""
Source manager for DocuFetch.
"""

import os
import logging
from pathlib import Path
from .academic import (
    ArxivSource, ScholarSource, 
    SemanticScholarSource, CoreSource, CrossrefSource,
    UnpaywallSource, PubMedSource, DOAJSource, OpenAIRESource
)
from .news import NewsSource

logger = logging.getLogger(__name__)


class SourceManager:
    """
    Manages document sources and coordinates fetching.
    """
    
    def __init__(self, config):
        """
        Initialize the source manager.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.base_download_dir = Path(os.path.expanduser("~/DocuFetch_Downloads"))
        
        # Create base download directory
        os.makedirs(self.base_download_dir, exist_ok=True)
        
        # Initialize sources
        self.sources = {}
        self._init_sources()
    
    def _init_sources(self):
        """Initialize document sources based on configuration."""
        max_results = self.config.get("max_results_per_source", 50)
        download_pdfs = self.config.get("download_pdfs", True)
        
        # Academic sources
        academic_dir = self.base_download_dir / "academic"
        
        if self.config.get("sources", {}).get("arxiv", True):
            self.sources["arxiv"] = ArxivSource(
                download_dir=academic_dir,
                max_results=max_results,
                download_pdfs=download_pdfs
            )
        
        if self.config.get("sources", {}).get("scholar", True):
            self.sources["scholar"] = ScholarSource(
                download_dir=academic_dir,
                max_results=max_results,
                download_pdfs=download_pdfs
            )
        
        # Extended academic sources
        if self.config.get("sources", {}).get("semantic_scholar", False):
            api_key = self.config.get("api_keys", {}).get("semantic_scholar", "")
            self.sources["semantic_scholar"] = SemanticScholarSource(
                download_dir=academic_dir,
                max_results=max_results,
                download_pdfs=download_pdfs,
                api_key=api_key
            )
        
        if self.config.get("sources", {}).get("core", False):
            api_key = self.config.get("api_keys", {}).get("core", "")
            self.sources["core"] = CoreSource(
                download_dir=academic_dir,
                max_results=max_results,
                download_pdfs=download_pdfs,
                api_key=api_key
            )
        
        if self.config.get("sources", {}).get("crossref", False):
            email = self.config.get("api_keys", {}).get("crossref_email", "")
            self.sources["crossref"] = CrossrefSource(
                download_dir=academic_dir,
                max_results=max_results,
                download_pdfs=download_pdfs,
                email=email
            )
            
        # Additional academic sources (v2)
        if self.config.get("sources", {}).get("unpaywall", False):
            email = self.config.get("api_keys", {}).get("unpaywall_email", "")
            self.sources["unpaywall"] = UnpaywallSource(
                download_dir=academic_dir,
                max_results=max_results,
                download_pdfs=download_pdfs,
                email=email
            )
            
        if self.config.get("sources", {}).get("pubmed", False):
            email = self.config.get("api_keys", {}).get("ncbi_email", "")
            self.sources["pubmed"] = PubMedSource(
                download_dir=academic_dir,
                max_results=max_results,
                download_pdfs=download_pdfs,
                email=email
            )
            
        if self.config.get("sources", {}).get("doaj", False):
            api_key = self.config.get("api_keys", {}).get("doaj_api_key", "")
            self.sources["doaj"] = DOAJSource(
                download_dir=academic_dir,
                max_results=max_results,
                download_pdfs=download_pdfs,
                api_key=api_key
            )
            
        if self.config.get("sources", {}).get("openaire", False):
            self.sources["openaire"] = OpenAIRESource(
                download_dir=academic_dir,
                max_results=max_results,
                download_pdfs=download_pdfs
            )
        
        # News sources
        if self.config.get("sources", {}).get("news", True):
            news_dir = self.base_download_dir / "news"
            news_sources_count = min(10, max(1, self.config.get("news_sources_count", 5)))
            self.sources["news"] = NewsSource(
                download_dir=news_dir,
                max_results=max_results,
                news_sources_count=news_sources_count
            )
    
    def preview_documents(self, keywords, source_type=None):
        """
        Preview documents from sources without downloading.
        
        Args:
            keywords: List of keywords to search for
            source_type: Type of sources to search (None for all, 'academic' for academic sources, 'news' for news sources)
            
        Returns:
            dict: Dictionary with source names as keys and number of documents as values
        """
        if not keywords:
            logger.warning("No keywords provided for preview")
            return {}
        
        preview_results = {}
        
        for source_name, source in self.sources.items():
            # Skip sources that don't match the requested type
            if source_type == 'academic' and source_name == 'news':
                continue
            if source_type == 'news' and source_name != 'news':
                continue
                
            try:
                # Set preview mode to True to count documents without downloading
                documents = source.fetch(keywords, preview_mode=True)
                preview_results[source_name] = len(documents)
                logger.info(f"Found {len(documents)} documents from {source_name}")
            except Exception as e:
                logger.error(f"Error previewing documents from {source_name}: {str(e)}")
                preview_results[source_name] = 0
        
        return preview_results
    
    def fetch_documents(self, keywords, preview_first=False, source_type=None):
        """
        Fetch documents from sources.
        
        Args:
            keywords: List of keywords to search for
            preview_first: If True, show preview and ask for confirmation before downloading
            source_type: Type of sources to search (None for all, 'academic' for academic sources, 'news' for news sources)
            
        Returns:
            dict: Dictionary with source names as keys and lists of documents as values
        """
        if not keywords:
            logger.warning("No keywords provided for document discovery")
            return {}
        
        if preview_first:
            preview_results = self.preview_documents(keywords, source_type)
            total_documents = sum(preview_results.values())
            
            if total_documents == 0:
                logger.info("No documents found for the provided keywords")
                return {}
            
            return preview_results
        
        results = {}
        
        for source_name, source in self.sources.items():
            # Skip sources that don't match the requested type
            if source_type == 'academic' and source_name == 'news':
                continue
            if source_type == 'news' and source_name != 'news':
                continue
                
            try:
                documents = source.fetch(keywords)
                results[source_name] = documents
                logger.info(f"Found {len(documents)} documents from {source_name}")
            except Exception as e:
                logger.error(f"Error fetching documents from {source_name}: {str(e)}")
                results[source_name] = []
        
        return results
    
    def get_enabled_sources(self):
        """
        Get a list of enabled sources.
        
        Returns:
            list: List of enabled source names
        """
        return list(self.sources.keys())
    
    def enable_source(self, source_name):
        """
        Enable a source.
        
        Args:
            source_name: Name of the source to enable
            
        Returns:
            bool: True if successful, False otherwise
        """
        valid_sources = ["arxiv", "scholar", "news", "semantic_scholar", "core", "crossref", "unpaywall", "pubmed", "doaj", "openaire"]
        if source_name not in valid_sources:
            logger.error(f"Invalid source name: {source_name}")
            return False
        
        if source_name in self.sources:
            logger.info(f"Source {source_name} is already enabled")
            return True
        
        # Update config
        if "sources" not in self.config:
            self.config["sources"] = {}
        self.config["sources"][source_name] = True
        
        # Reinitialize sources
        self._init_sources()
        
        logger.info(f"Enabled source: {source_name}")
        return True
    
    def disable_source(self, source_name):
        """
        Disable a source.
        
        Args:
            source_name: Name of the source to disable
            
        Returns:
            bool: True if successful, False otherwise
        """
        if source_name not in self.sources:
            logger.warning(f"Source {source_name} is not enabled")
            return True
        
        # Update config
        self.config["sources"][source_name] = False
        
        # Remove source
        del self.sources[source_name]
        
        logger.info(f"Disabled source: {source_name}")
        return True
    
    def get_stats(self):
        """
        Get statistics about downloaded documents.
        
        Returns:
            dict: Dictionary of statistics
        """
        stats = {
            "total_documents": 0,
            "by_source": {},
            "by_keyword": {}
        }
        
        # All possible sources
        all_sources = ["arxiv", "scholar", "semantic_scholar", "core", "crossref", "unpaywall", "pubmed", "doaj", "openaire", "news"]
        
        # Count documents by source
        for source_name in all_sources:
            if source_name in ["arxiv", "scholar", "semantic_scholar", "core", "crossref", "unpaywall", "pubmed", "doaj", "openaire"]:
                dir_path = self.base_download_dir / "academic" / "metadata"
                if dir_path.exists():
                    prefix = source_name
                    if source_name == "semantic_scholar":
                        prefix = "semantic"
                    files = [f for f in dir_path.glob(f"{prefix}_*.json")]
                    stats["by_source"][source_name] = len(files)
                    stats["total_documents"] += len(files)
                else:
                    stats["by_source"][source_name] = 0
            else:
                dir_path = self.base_download_dir / "news" / "metadata"
                if dir_path.exists():
                    files = [f for f in dir_path.glob("news_*.json")]
                    stats["by_source"][source_name] = len(files)
                    stats["total_documents"] += len(files)
                else:
                    stats["by_source"][source_name] = 0
        
        # Count documents by keyword
        keywords = self.config.get("keywords", [])
        for keyword in keywords:
            stats["by_keyword"][keyword] = 0
            
            # Academic sources
            academic_dir = self.base_download_dir / "academic" / "metadata"
            if academic_dir.exists():
                for source_name in ["arxiv", "scholar", "semantic_scholar", "core", "crossref", "unpaywall", "pubmed", "doaj", "openaire"]:
                    # This is a simplification - in a real implementation we would
                    # need to read each JSON file to check the keyword
                    prefix = source_name
                    if source_name == "semantic_scholar":
                        prefix = "semantic"
                    files = [f for f in academic_dir.glob(f"{prefix}_*.json")]
                    stats["by_keyword"][keyword] += len(files) // max(1, len(keywords))
            
            # News sources
            news_dir = self.base_download_dir / "news" / "metadata"
            if news_dir.exists():
                files = [f for f in news_dir.glob("news_*.json")]
                stats["by_keyword"][keyword] += len(files) // max(1, len(keywords))
        
        return stats
    
    def set_api_key(self, source_name, api_key):
        """
        Set API key for a source.
        
        Args:
            source_name: Name of the source
            api_key: API key or email
            
        Returns:
            bool: True if successful, False otherwise
        """
        if source_name not in ["core", "crossref", "unpaywall", "pubmed", "doaj", "semantic_scholar"]:
            logger.error(f"API key not required for source: {source_name}")
            return False
        
        # Update config
        if "api_keys" not in self.config:
            self.config["api_keys"] = {}
        
        if source_name == "core":
            self.config["api_keys"]["core"] = api_key
        elif source_name == "crossref":
            self.config["api_keys"]["crossref_email"] = api_key
        elif source_name == "unpaywall":
            self.config["api_keys"]["unpaywall_email"] = api_key
        elif source_name == "pubmed":
            self.config["api_keys"]["ncbi_email"] = api_key
        elif source_name == "doaj":
            self.config["api_keys"]["doaj_api_key"] = api_key
        elif source_name == "semantic_scholar":
            self.config["api_keys"]["semantic_scholar"] = api_key
        
        # Reinitialize sources
        self._init_sources()
        
        logger.info(f"Set API key for source: {source_name}")
        return True

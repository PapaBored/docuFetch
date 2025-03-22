"""
News article sources for DocuFetch.
"""

import os
import logging
import json
from pathlib import Path
from datetime import datetime
import newspaper
from newspaper import Article
from newspaper import news_pool

logger = logging.getLogger(__name__)


class NewsSource:
    """News article source using newspaper3k."""
    
    def __init__(self, download_dir, max_results=50):
        """
        Initialize the news source.
        
        Args:
            download_dir: Directory to save downloaded articles
            max_results: Maximum number of results to return
        """
        self.download_dir = Path(download_dir)
        self.max_results = max_results
        
        # Create download directory
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Create metadata directory
        self.metadata_dir = self.download_dir / "metadata"
        os.makedirs(self.metadata_dir, exist_ok=True)
        
        # Popular news sources
        self.news_sources = [
            'https://www.bbc.com',
            'https://www.cnn.com',
            'https://www.reuters.com',
            'https://www.nytimes.com',
            'https://www.theguardian.com',
            'https://www.washingtonpost.com',
            'https://www.aljazeera.com',
            'https://www.bloomberg.com',
            'https://www.forbes.com',
            'https://techcrunch.com'
        ]
    
    def fetch(self, keyword, preview_mode=False):
        """
        Fetch news articles for a keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count articles without downloading content
            
        Returns:
            list: List of article metadata
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
        Fetch news articles for a single keyword.
        
        Args:
            keyword: Keyword to search for
            preview_mode: If True, only count articles without downloading content
            
        Returns:
            list: List of article metadata
        """
        logger.info(f"Searching news sources for '{keyword}'")
        
        results = []
        
        try:
            # Build newspaper objects for each source
            papers = [newspaper.build(source, memoize_articles=False) for source in self.news_sources]
            
            # Download articles in parallel
            news_pool.set(papers, threads_per_source=2)
            news_pool.join()
            
            # Search for articles containing the keyword
            for paper in papers:
                for article in paper.articles:
                    if len(results) >= self.max_results:
                        break
                    
                    try:
                        # Download and parse article
                        article.download()
                        article.parse()
                        
                        # Check if article contains the keyword
                        if keyword.lower() in article.title.lower() or (
                                article.text and keyword.lower() in article.text.lower()):
                            
                            # Try to extract publish date and other metadata
                            try:
                                article.nlp()
                            except Exception:
                                pass
                            
                            # Create article metadata
                            article_id = article.url.split('/')[-1].split('.')[0]
                            if not article_id:
                                article_id = f"news_{len(results)}"
                            
                            document = {
                                "id": article_id,
                                "title": article.title,
                                "authors": article.authors,
                                "summary": article.summary if hasattr(article, 'summary') else '',
                                "text": article.text,
                                "url": article.url,
                                "source": article.source_url,
                                "published": article.publish_date.isoformat() if article.publish_date else None,
                                "keyword": keyword,
                                "fetched_at": datetime.now().isoformat()
                            }
                            
                            # If preview mode, just add to results without saving
                            if not preview_mode:
                                # Generate filename for metadata
                                metadata_filename = f"news_{document['id']}.json"
                                
                                # Save metadata
                                self._save_metadata(document, metadata_filename)
                                
                                # Save article text
                                article_filename = self.download_dir / f"news_{document['id']}.txt"
                                self._save_article_text(document['text'], article_filename)
                            
                            results.append(document)
                    
                    except Exception as e:
                        logger.warning(f"Error processing article {article.url}: {str(e)}")
                        continue
            
            logger.info(f"Found {len(results)} news articles for '{keyword}'")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching news articles: {str(e)}")
            return []
    
    def _save_metadata(self, document, filename):
        """
        Save article metadata to a file.
        
        Args:
            document: Article metadata
            filename: Filename to save metadata to
        """
        metadata_path = self.metadata_dir / filename
        
        try:
            # Remove full text from metadata to save space
            metadata = document.copy()
            metadata['text'] = metadata['text'][:500] + '...' if len(metadata['text']) > 500 else metadata['text']
            
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            logger.debug(f"Saved metadata to {metadata_path}")
        except Exception as e:
            logger.error(f"Error saving metadata: {str(e)}")
    
    def _save_article_text(self, text, filename):
        """
        Save article text to a file.
        
        Args:
            text: Article text
            filename: Filename to save text to
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(text)
            logger.debug(f"Saved article text to {filename}")
        except Exception as e:
            logger.error(f"Error saving article text: {str(e)}")

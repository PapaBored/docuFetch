#!/usr/bin/env python3
"""
DocuFetch - Intelligent Document Harvesting Tool
"""

import os
import sys
import argparse
import logging
import json
import time
from pathlib import Path
from datetime import datetime
import schedule
import pyfiglet
from colorama import init, Fore, Style
from dotenv import load_dotenv
import subprocess

# Initialize colorama
init(autoreset=True)

# Load environment variables
load_dotenv()

# Import DocuFetch modules
from sources.manager import SourceManager
from utils import setup_logging, create_directory_if_not_exists, format_file_size

# Setup logging
logger = setup_logging()

# Configuration
CONFIG_DIR = Path.home() / ".docufetch"
CONFIG_FILE = CONFIG_DIR / "config.json"
DOWNLOADS_DIR = Path(os.getenv("DOWNLOADS_DIR", str(Path.home() / "DocuFetch_Downloads")))


class DocuFetch:
    """Main DocuFetch application class."""
    
    def __init__(self):
        """Initialize DocuFetch."""
        self.config = self._load_config()
        
        # Create downloads directory
        create_directory_if_not_exists(DOWNLOADS_DIR)
        create_directory_if_not_exists(DOWNLOADS_DIR / "academic")
        create_directory_if_not_exists(DOWNLOADS_DIR / "news")
        
        # Initialize source manager
        self.source_manager = SourceManager(self.config)
        
    def _load_config(self):
        """Load configuration from file."""
        # Create config directory if it doesn't exist
        create_directory_if_not_exists(CONFIG_DIR)
        
        # Create default config if it doesn't exist
        if not CONFIG_FILE.exists():
            default_config = {
                "keywords": [],
                "sources": {
                    "arxiv": True,
                    "scholar": True,
                    "news": True,
                    "semantic_scholar": False,
                    "core": False,
                    "crossref": False,
                    "unpaywall": False,
                    "pubmed": False,
                    "doaj": False,
                    "openaire": False
                },
                "api_keys": {
                    "core": "",
                    "crossref_email": "",
                    "unpaywall_email": "",
                    "ncbi_email": "",
                    "doaj_api_key": "",
                    "semantic_scholar": ""
                },
                "update_interval": 12,  # hours
                "max_results_per_source": 50,
                "download_pdfs": True
            }
            
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2)
            
            logger.info(f"Created default configuration at {CONFIG_FILE}")
            return default_config
        
        # Load existing config
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Add new sources if they don't exist (for backward compatibility)
            if "sources" in config:
                for source in ["unpaywall", "pubmed", "doaj", "openaire"]:
                    if source not in config["sources"]:
                        config["sources"][source] = False
            
            # Add API keys section if it doesn't exist
            if "api_keys" not in config:
                config["api_keys"] = {
                    "core": "",
                    "crossref_email": "",
                    "unpaywall_email": "",
                    "ncbi_email": "",
                    "doaj_api_key": "",
                    "semantic_scholar": ""
                }
            
            logger.info(f"Loaded configuration from {CONFIG_FILE}")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            sys.exit(1)
    
    def _save_config(self):
        """Save configuration to file."""
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2)
            logger.info(f"Saved configuration to {CONFIG_FILE}")
        except Exception as e:
            logger.error(f"Error saving configuration: {str(e)}")
    
    def add_keywords(self, keywords):
        """Add keywords to the configuration."""
        for keyword in keywords:
            if keyword not in self.config["keywords"]:
                self.config["keywords"].append(keyword)
        
        self._save_config()
        print(f"{Fore.GREEN}Added keywords: {', '.join(keywords)}")
    
    def remove_keywords(self, keywords):
        """Remove keywords from the configuration."""
        for keyword in keywords:
            if keyword in self.config["keywords"]:
                self.config["keywords"].remove(keyword)
        
        self._save_config()
        print(f"{Fore.YELLOW}Removed keywords: {', '.join(keywords)}")
    
    def clear_keywords(self):
        """Clear all keywords from the configuration."""
        removed_keywords = self.config["keywords"].copy()
        self.config["keywords"] = []
        self._save_config()
        if removed_keywords:
            print(f"{Fore.YELLOW}Cleared all keywords: {', '.join(removed_keywords)}")
        else:
            print(f"{Fore.YELLOW}No keywords to clear.")
    
    def list_keywords(self):
        """List all configured keywords."""
        if not self.config["keywords"]:
            print(f"{Fore.YELLOW}No keywords configured.")
            return
        
        print(f"{Fore.CYAN}Configured keywords:")
        for i, keyword in enumerate(self.config["keywords"], 1):
            print(f"{Fore.CYAN}{i}. {keyword}")
    
    def update_sources(self, sources_config):
        """Update source configuration."""
        for source, enabled in sources_config.items():
            if source in self.config["sources"]:
                self.config["sources"][source] = enabled
                
                if enabled:
                    self.source_manager.enable_source(source)
                else:
                    self.source_manager.disable_source(source)
        
        self._save_config()
        print(f"{Fore.GREEN}Updated source configuration.")
    
    def list_sources(self):
        """List all configured sources."""
        print(f"{Fore.CYAN}Configured sources:")
        
        # Group sources by type
        print(f"{Fore.CYAN}Academic sources:")
        for source in ["arxiv", "scholar", "semantic_scholar", "core", "crossref", "unpaywall", "pubmed", "doaj", "openaire"]:
            if source in self.config["sources"]:
                status = f"{Fore.GREEN}Enabled" if self.config["sources"][source] else f"{Fore.RED}Disabled"
                print(f"{Fore.CYAN}  - {source}: {status}")
                
                # Show API key status for sources that require it
                if source == "core" and self.config["sources"][source]:
                    api_key = self.config.get("api_keys", {}).get("core", "")
                    key_status = f"{Fore.GREEN}Configured" if api_key else f"{Fore.RED}Not configured"
                    print(f"{Fore.CYAN}    API Key: {key_status}")
                elif source == "crossref" and self.config["sources"][source]:
                    email = self.config.get("api_keys", {}).get("crossref_email", "")
                    email_status = f"{Fore.GREEN}Configured" if email else f"{Fore.RED}Not configured"
                    print(f"{Fore.CYAN}    Email: {email_status}")
                elif source == "unpaywall" and self.config["sources"][source]:
                    api_key = self.config.get("api_keys", {}).get("unpaywall_email", "")
                    key_status = f"{Fore.GREEN}Configured" if api_key else f"{Fore.RED}Not configured"
                    print(f"{Fore.CYAN}    API Key: {key_status}")
                elif source == "pubmed" and self.config["sources"][source]:
                    api_key = self.config.get("api_keys", {}).get("ncbi_email", "")
                    key_status = f"{Fore.GREEN}Configured" if api_key else f"{Fore.RED}Not configured"
                    print(f"{Fore.CYAN}    API Key: {key_status}")
                elif source == "doaj" and self.config["sources"][source]:
                    api_key = self.config.get("api_keys", {}).get("doaj_api_key", "")
                    key_status = f"{Fore.GREEN}Configured" if api_key else f"{Fore.RED}Not configured"
                    print(f"{Fore.CYAN}    API Key: {key_status}")
        
        print(f"{Fore.CYAN}News sources:")
        if "news" in self.config["sources"]:
            status = f"{Fore.GREEN}Enabled" if self.config["sources"]["news"] else f"{Fore.RED}Disabled"
            print(f"{Fore.CYAN}  - news: {status}")
    
    def set_api_key(self, source, api_key):
        """Set API key for a source."""
        if source not in ["core", "crossref", "unpaywall", "pubmed", "doaj", "semantic_scholar"]:
            print(f"{Fore.RED}API key not required for source: {source}")
            return
        
        result = self.source_manager.set_api_key(source, api_key)
        
        if result:
            self._save_config()
            print(f"{Fore.GREEN}Set API key for {source}.")
        else:
            print(f"{Fore.RED}Failed to set API key for {source}.")
    
    def set_update_interval(self, hours):
        """Set the update interval in hours."""
        self.config["update_interval"] = hours
        self._save_config()
        print(f"{Fore.GREEN}Set update interval to {hours} hours.")
    
    def preview_documents(self):
        """Preview documents from all sources based on keywords."""
        if not self.config.get("keywords", []):
            print(f"{Fore.YELLOW}No keywords configured. Use 'add' command to add keywords.")
            return
        
        print(f"{Fore.CYAN}Previewing documents for keywords: {', '.join(self.config['keywords'])}")
        
        # Preview documents
        preview_results = self.source_manager.preview_documents(self.config["keywords"])
        
        # Display results
        total_documents = sum(preview_results.values())
        
        if total_documents == 0:
            print(f"{Fore.YELLOW}No documents found for the configured keywords.")
            return
        
        print(f"{Fore.GREEN}Found {total_documents} documents across all sources:")
        
        # Group sources by type
        academic_sources = ["arxiv", "scholar", "semantic_scholar", "core", "crossref", 
                           "unpaywall", "pubmed", "doaj", "openaire"]
        news_sources = ["news"]
        
        print(f"{Fore.CYAN}Academic sources:")
        for source in academic_sources:
            if source in preview_results:
                count = preview_results[source]
                if count > 0:
                    print(f"{Fore.CYAN}  - {source}: {Fore.GREEN}{count} documents")
                else:
                    print(f"{Fore.CYAN}  - {source}: {Fore.YELLOW}No documents found")
        
        print(f"{Fore.CYAN}News sources:")
        for source in news_sources:
            if source in preview_results:
                count = preview_results[source]
                if count > 0:
                    print(f"{Fore.CYAN}  - {source}: {Fore.GREEN}{count} articles")
                else:
                    print(f"{Fore.CYAN}  - {source}: {Fore.YELLOW}No articles found")
        
        # Ask for confirmation
        user_input = input(f"{Fore.YELLOW}Do you want to download these documents? (y/n): ")
        
        if user_input.lower() in ["y", "yes"]:
            print(f"{Fore.GREEN}Starting download...")
            self.fetch_documents(skip_preview=True)
        else:
            print(f"{Fore.YELLOW}Download canceled.")
    
    def fetch_documents(self, skip_preview=False):
        """Fetch documents from all sources based on keywords."""
        if not self.config.get("keywords", []):
            print(f"{Fore.YELLOW}No keywords configured. Use 'add' command to add keywords.")
            return
        
        if not skip_preview:
            # Show preview first
            self.preview_documents()
            return
        
        print(f"{Fore.CYAN}Fetching documents for keywords: {', '.join(self.config['keywords'])}")
        
        # Fetch documents
        results = self.source_manager.fetch_documents(self.config["keywords"])
        
        # Display results
        total_documents = sum(len(docs) for source_name, docs in results.items())
        
        if total_documents == 0:
            print(f"{Fore.YELLOW}No documents found for the configured keywords.")
        else:
            print(f"{Fore.GREEN}Downloaded {total_documents} documents.")
    
    def start_monitoring(self):
        """Start monitoring for new documents at regular intervals."""
        interval_hours = self.config["update_interval"]
        
        print(f"{Fore.CYAN}Starting document monitoring. Will check every {interval_hours} hours.")
        print(f"{Fore.CYAN}Press Ctrl+C to stop.")
        
        # Schedule the fetch job
        schedule.every(interval_hours).hours.do(self.fetch_documents)
        
        # Run once immediately
        self.fetch_documents()
        
        # Keep running until interrupted
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"{Fore.YELLOW}Monitoring stopped.")
    
    def show_stats(self):
        """Show document statistics."""
        stats = self.source_manager.get_stats()
        
        print(f"{Fore.CYAN}DocuFetch Statistics:")
        print(f"{Fore.CYAN}Total documents: {stats['total_documents']}")
        
        print(f"\n{Fore.CYAN}Documents by source:")
        for source, count in stats["by_source"].items():
            print(f"{Fore.CYAN}  - {source}: {count}")
        
        print(f"\n{Fore.CYAN}Documents by keyword:")
        for keyword, count in stats["by_keyword"].items():
            print(f"{Fore.CYAN}  - {keyword}: {count}")
        
        # Calculate total size of downloads
        total_size = 0
        for root, _, files in os.walk(DOWNLOADS_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)
        
        print(f"\n{Fore.CYAN}Total download size: {format_file_size(total_size)}")
    
    def get_stats(self):
        """Get statistics about downloaded documents."""
        return self.source_manager.get_stats()
    
    def open_downloads_directory(self):
        """Open the downloads directory in the file explorer."""
        download_dir = Path(DOWNLOADS_DIR)
        
        if not download_dir.exists():
            os.makedirs(download_dir, exist_ok=True)
            print(f"{Fore.YELLOW}Created downloads directory: {download_dir}")
        
        try:
            if os.name == 'nt':  # Windows
                os.startfile(download_dir)
            elif os.name == 'posix':  # macOS or Linux
                if sys.platform == 'darwin':  # macOS
                    subprocess.run(['open', download_dir])
                else:  # Linux
                    subprocess.run(['xdg-open', download_dir])
            print(f"{Fore.GREEN}Opened downloads directory: {download_dir}")
        except Exception as e:
            print(f"{Fore.RED}Error opening downloads directory: {str(e)}")
    
    def add_keywords(self, keywords):
        """Add keywords to the configuration."""
        for keyword in keywords:
            if keyword not in self.config["keywords"]:
                self.config["keywords"].append(keyword)
        
        self._save_config()
        print(f"{Fore.GREEN}Added keywords: {', '.join(keywords)}")


def display_banner():
    """Display the DocuFetch banner."""
    banner = pyfiglet.figlet_format("DocuFetch", font="slant")
    print(f"{Fore.CYAN}{banner}")
    print(f"{Fore.CYAN}Intelligent Document Harvesting Tool")
    print(f"{Fore.CYAN}Version 1.0.0")
    print(f"{Fore.CYAN}=" * 50)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="DocuFetch - Intelligent Document Harvesting Tool")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Add keywords
    add_parser = subparsers.add_parser("add", help="Add keywords to monitor")
    add_parser.add_argument("keywords", nargs="+", help="Keywords to add")
    
    # Remove keywords
    remove_parser = subparsers.add_parser("remove", help="Remove keywords")
    remove_parser.add_argument("keywords", nargs="+", help="Keywords to remove")
    
    # Clear keywords
    subparsers.add_parser("clear", help="Clear all keywords")
    
    # List keywords
    subparsers.add_parser("list", help="List current keywords")
    
    # Configure sources
    sources_parser = subparsers.add_parser("sources", help="Configure document sources")
    sources_parser.add_argument("--arxiv", dest="arxiv", action="store_true", help="Enable arXiv")
    sources_parser.add_argument("--no-arxiv", dest="arxiv", action="store_false", help="Disable arXiv")
    sources_parser.add_argument("--scholar", dest="scholar", action="store_true", help="Enable Google Scholar")
    sources_parser.add_argument("--no-scholar", dest="scholar", action="store_false", help="Disable Google Scholar")
    sources_parser.add_argument("--news", dest="news", action="store_true", help="Enable news sources")
    sources_parser.add_argument("--no-news", dest="news", action="store_false", help="Disable news sources")
    sources_parser.add_argument("--semantic-scholar", dest="semantic_scholar", action="store_true", help="Enable Semantic Scholar")
    sources_parser.add_argument("--no-semantic-scholar", dest="semantic_scholar", action="store_false", help="Disable Semantic Scholar")
    sources_parser.add_argument("--core", dest="core", action="store_true", help="Enable CORE")
    sources_parser.add_argument("--no-core", dest="core", action="store_false", help="Disable CORE")
    sources_parser.add_argument("--crossref", dest="crossref", action="store_true", help="Enable Crossref")
    sources_parser.add_argument("--no-crossref", dest="crossref", action="store_false", help="Disable Crossref")
    sources_parser.add_argument("--unpaywall", dest="unpaywall", action="store_true", help="Enable Unpaywall")
    sources_parser.add_argument("--no-unpaywall", dest="unpaywall", action="store_false", help="Disable Unpaywall")
    sources_parser.add_argument("--pubmed", dest="pubmed", action="store_true", help="Enable PubMed")
    sources_parser.add_argument("--no-pubmed", dest="pubmed", action="store_false", help="Disable PubMed")
    sources_parser.add_argument("--doaj", dest="doaj", action="store_true", help="Enable DOAJ")
    sources_parser.add_argument("--no-doaj", dest="doaj", action="store_false", help="Disable DOAJ")
    sources_parser.add_argument("--openaire", dest="openaire", action="store_true", help="Enable OpenAIRE")
    sources_parser.add_argument("--no-openaire", dest="openaire", action="store_false", help="Disable OpenAIRE")
    sources_parser.add_argument("--list", action="store_true", help="List current source configuration")
    
    # Configure API keys
    api_parser = subparsers.add_parser("api", help="Configure API keys")
    api_parser.add_argument("source", choices=["core", "crossref", "unpaywall", "pubmed", "doaj", "semantic_scholar"], 
                           help="Source to configure API key for")
    api_parser.add_argument("key", help="API key or email")
    
    # Set update interval
    interval_parser = subparsers.add_parser("interval", help="Set update interval")
    interval_parser.add_argument("hours", type=int, help="Update interval in hours")
    
    # Fetch documents
    subparsers.add_parser("fetch", help="Manually trigger document discovery")
    
    # Start monitoring
    subparsers.add_parser("monitor", help="Start continuous monitoring")
    
    # Show statistics
    subparsers.add_parser("stats", help="Show download statistics")
    
    # Preview documents
    subparsers.add_parser("preview", help="Preview documents")
    
    # Open downloads directory
    subparsers.add_parser("open", help="Open downloads directory")
    
    return parser.parse_args()


def main():
    """Main entry point."""
    # Display banner
    display_banner()
    
    # Parse arguments
    args = parse_args()
    
    # Initialize DocuFetch
    docufetch = DocuFetch()
    
    # Execute command
    if args.command == "add":
        docufetch.add_keywords(args.keywords)
    
    elif args.command == "remove":
        docufetch.remove_keywords(args.keywords)
    
    elif args.command == "clear":
        docufetch.clear_keywords()
    
    elif args.command == "list":
        docufetch.list_keywords()
    
    elif args.command == "sources":
        if args.list:
            docufetch.list_sources()
        else:
            sources_config = {}
            
            for source in ["arxiv", "scholar", "news", "semantic_scholar", "core", "crossref", "unpaywall", "pubmed", "doaj", "openaire"]:
                if hasattr(args, source):
                    sources_config[source] = getattr(args, source)
            
            if sources_config:
                docufetch.update_sources(sources_config)
            else:
                docufetch.list_sources()
    
    elif args.command == "api":
        docufetch.set_api_key(args.source, args.key)
    
    elif args.command == "interval":
        docufetch.set_update_interval(args.hours)
    
    elif args.command == "fetch":
        docufetch.fetch_documents()
    
    elif args.command == "monitor":
        docufetch.start_monitoring()
    
    elif args.command == "stats":
        docufetch.show_stats()
    
    elif args.command == "preview":
        docufetch.preview_documents()
    
    elif args.command == "open":
        docufetch.open_downloads_directory()
    
    else:
        print(f"{Fore.YELLOW}No command specified. Use --help for usage information.")


if __name__ == "__main__":
    main()

import requests
import hashlib
import threading
import time
import os
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime


class Queue:
    """A thread-safe queue for storing URLs to be crawled."""
    
    def __init__(self):
        self.total_queued = 0
        self.number = 0
        self.elements = []
        self.lock = threading.Lock()
    
    def enqueue(self, url):
        """Add a URL to the queue."""
        with self.lock:
            self.elements.append(url)
            self.total_queued += 1
            self.number += 1
    
    def dequeue(self):
        """Remove and return the next URL from the queue."""
        with self.lock:
            url = self.elements[0]
            self.elements = self.elements[1:]
            self.number -= 1
            return url
    
    def size(self):
        """Return the current size of the queue."""
        with self.lock:
            return self.number


class CrawledSet:
    """A thread-safe set for storing URLs that have been crawled."""
    
    def __init__(self):
        self.data = {}
        self.number = 0
        self.lock = threading.Lock()
    
    def add(self, url):
        """Add a URL to the set of crawled URLs."""
        with self.lock:
            self.data[self._hash_url(url)] = True
            self.number += 1
    
    def contains(self, url):
        """Check if a URL has been crawled."""
        with self.lock:
            return self._hash_url(url) in self.data
    
    def size(self):
        """Return the number of URLs that have been crawled."""
        with self.lock:
            return self.number
    
    def _hash_url(self, url):
        """Hash a URL to a 64-bit integer."""
        return hashlib.md5(url.encode()).hexdigest()


class DatabaseConnection:
    """A connection to a MongoDB database for storing crawled webpages."""
    
    def __init__(self, access=True):
        self.access = access
        self.uri = None
        self.client = None
        self.collection = None
    
    def connect(self):
        """Connect to the MongoDB database."""
        if self.access:
            self.uri = os.getenv("MONGODB_URI")
            self.client = MongoClient(self.uri)
            self.collection = self.client["webCrawlerArchive"]["webpages"]
            # Clear the collection
            self.collection.delete_many({})
    
    def disconnect(self):
        """Disconnect from the MongoDB database."""
        if self.access and self.client:
            self.client.close()
    
    def insert_webpage(self, webpage):
        """Insert a webpage into the database."""
        if self.access and self.collection:
            self.collection.insert_one(webpage)


class CrawlerStats:
    """Statistics for the web crawler."""
    
    def __init__(self):
        self.pages_per_minute = "0 0\n"
        self.crawled_ratio_per_minute = "0 0\n"
        self.start_time = datetime.now()
    
    def update(self, crawled, queue):
        """Update the crawler statistics."""
        minutes = (datetime.now() - self.start_time).total_seconds() / 60
        self.pages_per_minute += f"{minutes:.6f} {crawled.size()}\n"
        
        queue_size = queue.size()
        ratio = float(crawled.size()) / max(1, queue_size)  # Avoid division by zero
        self.crawled_ratio_per_minute += f"{minutes:.6f} {ratio:.6f}\n"
    
    def print(self):
        """Print the crawler statistics."""
        print("Pages crawled per minute:")
        print(self.pages_per_minute)
        print("Crawl to Queued Ratio per minute:")
        print(self.crawled_ratio_per_minute)


def get_href(tag):
    """Extract the href attribute from an anchor tag."""
    href = tag.get("href")
    if not href or not href.startswith("http"):
        return False, href
    return True, href


def fetch_page(url):
    """Fetch the content of a webpage."""
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()  # Raise an exception for 4XX/5XX responses
        return response.content
    except (requests.RequestException, Exception):
        return b""


def parse_html(curr_url, content, queue, crawled, db):
    """Parse HTML content and extract links and content."""
    soup = BeautifulSoup(content, "html.parser")
    token_count = 0
    page_content_length = 0
    webpage = {"Url": curr_url, "Title": "", "Content": ""}
    
    # Extract title
    title_tag = soup.find("title")
    if title_tag:
        webpage["Title"] = title_tag.text.strip()
        print(f"Count: {crawled.size()} | {curr_url} -> {webpage['Title']}")
    
    # Extract content (first 500 characters after body tag)
    body = soup.find("body")
    if body:
        # Remove script and style tags
        for script in body.find_all(["script", "style"]):
            script.decompose()
        
        # Get text content
        text = body.get_text(strip=True)
        webpage["Content"] = text[:500]
    
    # Extract links (limit to 500 tokens)
    for a_tag in soup.find_all("a", limit=500):
        ok, href = get_href(a_tag)
        if not ok:
            continue
        
        if not crawled.contains(href):
            queue.enqueue(href)
    
    # Insert webpage into database if we haven't reached the limit
    if crawled.size() < 1000:
        db.insert_webpage(webpage)
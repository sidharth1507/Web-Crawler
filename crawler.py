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
    
    def __init__(self):
        self.total_queued = 0
        self.number = 0
        self.elements = []
        self.lock = threading.Lock()
    
    def enqueue(self, url):
        with self.lock:
            self.elements.append(url)
            self.total_queued += 1
            self.number += 1
    
    def dequeue(self):
        with self.lock:
            url = self.elements[0]
            self.elements = self.elements[1:]
            self.number -= 1
            return url
    
    def size(self):
        with self.lock:
            return self.number


class CrawledSet:
    
    def __init__(self):
        self.data = {}
        self.number = 0
        self.lock = threading.Lock()
    
    def add(self, url):
        with self.lock:
            self.data[self._hash_url(url)] = True
            self.number += 1
    
    def contains(self, url):
        with self.lock:
            return self._hash_url(url) in self.data
    
    def size(self):
        with self.lock:
            return self.number
    
    def _hash_url(self, url):
        return hashlib.md5(url.encode()).hexdigest()


class DatabaseConnection:
    
    def __init__(self, access=True):
        self.access = access
        self.uri = None
        self.client = None
        self.collection = None
    
    def connect(self):
        if self.access:
            self.uri = os.getenv("MONGODB_URI")
            self.client = MongoClient(self.uri)
            self.collection = self.client["webCrawlerArchive"]["webpages"]
            self.collection.delete_many({})
    
    def disconnect(self):
        if self.access and self.client:
            self.client.close()
    
    def insert_webpage(self, webpage):
        if self.access and self.collection:
            self.collection.insert_one(webpage)


class CrawlerStats:
    
    def __init__(self):
        self.pages_per_minute = "0 0\n"
        self.crawled_ratio_per_minute = "0 0\n"
        self.start_time = datetime.now()
    
    def update(self, crawled, queue):
        minutes = (datetime.now() - self.start_time).total_seconds() / 60
        self.pages_per_minute += f"{minutes:.6f} {crawled.size()}\n"
        
        queue_size = queue.size()
        ratio = float(crawled.size()) / max(1, queue_size)
        self.crawled_ratio_per_minute += f"{minutes:.6f} {ratio:.6f}\n"
    
    def print(self):
        print("Pages crawled per minute:")
        print(self.pages_per_minute)
        print("Crawl to Queued Ratio per minute:")
        print(self.crawled_ratio_per_minute)


def get_href(tag):
    href = tag.get("href")
    if not href or not href.startswith("http"):
        return False, href
    return True, href


def fetch_page(url):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.content
    except (requests.RequestException, Exception):
        return b""


def parse_html(curr_url, content, queue, crawled, db):
    soup = BeautifulSoup(content, "html.parser")
    token_count = 0
    page_content_length = 0
    webpage = {"Url": curr_url, "Title": "", "Content": ""}
    
    title_tag = soup.find("title")
    if title_tag:
        webpage["Title"] = title_tag.text.strip()
        print(f"Count: {crawled.size()} | {curr_url} -> {webpage['Title']}")
    
    body = soup.find("body")
    if body:
        for script in body.find_all(["script", "style"]):
            script.decompose()
        
        text = body.get_text(strip=True)
        webpage["Content"] = text[:500]
    
    for a_tag in soup.find_all("a", limit=500):
        ok, href = get_href(a_tag)
        if not ok:
            continue
        
        if not crawled.contains(href):
            queue.enqueue(href)
    
    if crawled.size() < 1000:
        db.insert_webpage(webpage)

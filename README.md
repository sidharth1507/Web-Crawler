# Web Crawler

## Overview
This is a Python implementation of a web crawler that can crawl websites and store the content in a MongoDB database. The crawler is designed to be efficient, using threading for concurrent operations, and includes statistics tracking to monitor performance.

![Web Crawler Diagram](images/web-crawler-diagram.png)

## Features
- Concurrent fetching and parsing of web pages
- MongoDB integration for storing crawled content
- Avoids loops and dead ends
- Ignores script and style tags
- Gracefully handles invalid URLs and errors
- Performance statistics tracking

## Requirements
- Python 3.7+
- MongoDB (optional, for web archive functionality)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/web-crawler.git
   cd web-crawler
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. (Optional) Set up MongoDB for web archive functionality:
   - Create a MongoDB Atlas account or use a local MongoDB instance
   - Create a `.env` file in the project root with your MongoDB connection string:
     ```
     MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
     ```

## Usage

Run the crawler with the default seed URL (https://www.cc.gatech.edu/):
```
python main.py
```

## How it works

### Web Crawler Components

1. **Queue**: A thread-safe queue for storing URLs to be crawled.
2. **CrawledSet**: A thread-safe set for storing URLs that have been crawled.
3. **DatabaseConnection**: A connection to a MongoDB database for storing crawled webpages.
4. **CrawlerStats**: Statistics tracking for the web crawler.

### Crawling Process

1. The crawler starts with a seed URL and adds it to the queue.
2. It dequeues a URL, marks it as crawled, and fetches its content.
3. It parses the HTML content to extract:
   - The page title
   - The first 500 characters of text content after the body tag
   - Links to other pages
4. It adds new links to the queue and stores the page content in the database.
5. It repeats steps 2-4 until the queue is empty or the maximum number of pages has been crawled.

### Web Archive

The crawler stores the following information for each crawled page:
- URL
- Title
- Content (first 500 characters after the body tag)

This information can be searched using MongoDB's search functionality.

## Performance

The crawler tracks the following statistics:
- Crawl Speed: Pages per minute
- Crawled to Queued Ratio per minute

## Future Enhancements

- Allow the user to specify the seed URL
- Implement different crawling algorithms (BFS, DFS, hybrids)
- Improve content extraction for better representation of webpage content
- Handle relative links
- Implement a more sophisticated throttling mechanism to respect robots.txt




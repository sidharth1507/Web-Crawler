import time
import threading
from dotenv import load_dotenv
from crawler import Queue, CrawledSet, DatabaseConnection, CrawlerStats, fetch_page, parse_html


def main():
    # Check for MongoDB access
    web_archive_access = True
    if not load_dotenv():
        print("Error loading .env file. No access to web archive.")
        web_archive_access = False
    
    # Initialize database connection
    db = DatabaseConnection(access=web_archive_access)
    db.connect()
    
    # Initialize crawler components
    crawled = CrawledSet()
    seed = "https://www.manipal.edu/"
    queue = Queue()
    
    # Initialize statistics tracking
    crawler_stats = CrawlerStats()
    ticker = threading.Event()
    done = threading.Event()
    
    # Start statistics update thread
    def update_stats():
        while not done.is_set():
            if ticker.wait(60):  # Wait for 60 seconds or until ticker is set
                break
            crawler_stats.update(crawled, queue)
    
    stats_thread = threading.Thread(target=update_stats)
    stats_thread.daemon = True
    stats_thread.start()
    
    # Start crawling with the seed URL
    queue.enqueue(seed)
    url = queue.dequeue()
    crawled.add(url)
    
    content = fetch_page(url)
    parse_html(url, content, queue, crawled, db)
    
    # Main crawling loop
    while queue.size() > 0 and crawled.size() < 5000:
        url = queue.dequeue()
        crawled.add(url)
        
        content = fetch_page(url)
        if not content:
            continue
        
        # Use a thread pool for parsing to improve performance
        threading.Thread(
            target=parse_html, 
            args=(url, content, queue, crawled, db)
        ).start()
        
        # Throttle the crawling speed to be respectful
        time.sleep(0.1)
    
    # Clean up
    done.set()
    ticker.set()
    stats_thread.join()
    db.disconnect()
    
    # Print final statistics
    print("\n------------------CRAWLER STATS------------------")
    print(f"Total queued: {queue.total_queued}")
    print(f"To be crawled (Queue) size: {queue.size()}")
    print(f"Crawled size: {crawled.size()}")
    crawler_stats.print()


if __name__ == "__main__":
    main()
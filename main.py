import time
import threading
from dotenv import load_dotenv
from crawler import Queue, CrawledSet, DatabaseConnection, CrawlerStats, fetch_page, parse_html


def main():
    web_archive_access = True
    if not load_dotenv():
        print("Error loading .env file. No access to web archive.")
        web_archive_access = False
    
    db = DatabaseConnection(access=web_archive_access)
    db.connect()
    
    crawled = CrawledSet()
    seed = "https://www.manipal.edu/"
    queue = Queue()
    
    crawler_stats = CrawlerStats()
    ticker = threading.Event()
    done = threading.Event()
    
    def update_stats():
        while not done.is_set():
            if ticker.wait(60):
                break
            crawler_stats.update(crawled, queue)
    
    stats_thread = threading.Thread(target=update_stats)
    stats_thread.daemon = True
    stats_thread.start()
    
    queue.enqueue(seed)
    url = queue.dequeue()
    crawled.add(url)
    
    content = fetch_page(url)
    parse_html(url, content, queue, crawled, db)
    
    while queue.size() > 0 and crawled.size() < 5000:
        url = queue.dequeue()
        crawled.add(url)
        
        content = fetch_page(url)
        if not content:
            continue
        
        threading.Thread(
            target=parse_html, 
            args=(url, content, queue, crawled, db)
        ).start()
        
        time.sleep(0.1)
    
    done.set()
    ticker.set()
    stats_thread.join()
    db.disconnect()
    
    print("\n------------------CRAWLER STATS------------------")
    print(f"Total queued: {queue.total_queued}")
    print(f"To be crawled (Queue) size: {queue.size()}")
    print(f"Crawled size: {crawled.size()}")
    crawler_stats.print()


if __name__ == "__main__":
    main()

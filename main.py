from database import DbCreator
from crawler import Crawler

DbCreator.initialize_db()

crawler = Crawler()

crawler.start_crawl()

from database import DbCreator
from crawler import Crawler


import os
try:
    os.remove("lab1.db")
except FileNotFoundError:
    pass
DbCreator.initialize_db()

crawler = Crawler()

crawler.start_crawl()

from database import DbCreator
from crawler import Crawler

import os
try:
    os.remove("lab1.db")
except FileNotFoundError:
    pass

crawler = Crawler()
crawler.start_crawl()
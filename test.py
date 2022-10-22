import bs4 as bs

import requests
from crawler import ParseUtils, OmegaParser3000

# WIKI_URL = "https://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0"
# URL = "https://nstu.ru/"
# # URL = "http://example.com/"
# URL = "https://ngs.ru/"
# # URL = "http://deb.debian.org/"

# response = requests.get(URL)
# content = response.text
# soup = bs.BeautifulSoup(content, "html.parser")

# omega = OmegaParser3000()

# omega.merge_text_and_links(soup.text, soup.find_all("a"), URL)

# from database import DbActor, DbCreator
# from searcher import PageRankerer
# pr = PageRankerer()

# creator = DbCreator()
# creator.initialize_db()
# pr.calculate_ranks()

# pr.close()

# import sqlite3

# conn = sqlite3.connect("lab1.db")

# cur = conn.cursor()

# res = cur.execute("SELECT urlId from url_list")

# res = res.fetchall()

# for r in res:
#     link_to = r[0] 
#     rrr = cur.execute(f"SELECT fkFromUrlId FROM link_between_url WHERE fkToUrlId = {link_to};")
#     rr = rrr.fetchall()
#     print(f"link_to = {link_to} - {rr}")
from searcher import PageRankerer

rp = PageRankerer()
rp.calculate_ranks()
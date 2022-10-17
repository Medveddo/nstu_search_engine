import bs4 as bs

import requests
from crawler import ParseUtils, OmegaParser3000

# WIKI_URL = "https://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0"
# URL = "https://nstu.ru/"
# URL = "http://example.com/"
URL = "https://ngs.ru/"
# URL = "http://deb.debian.org/"

response = requests.get(URL)
content = response.text
soup = bs.BeautifulSoup(content, "html.parser")

omega = OmegaParser3000()

omega.merge_text_and_links(soup.text, soup.find_all("a"), URL)

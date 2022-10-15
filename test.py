import bs4 as bs

import requests
from crawler import ParseUtils
# WIKI_URL = "https://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0"
URL = "https://nstu.ru/"

response = requests.get(URL)
content = response.text
soup = bs.BeautifulSoup(content, 'html.parser')

# links = ParseUtils.get_all_urls(content, URL)

# for link in links:
#     print(link)
print(ParseUtils.get_text_only(content))

# links
# for link in soup.find_all('a'):
#     print(link.get('href'))

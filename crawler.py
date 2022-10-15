from bs4 import BeautifulSoup
from typing import List, Optional


class Crawler:
    START_URL_LIST = [
        "https://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0",
    ]
    DEFAULT_DEPTH = 2
    
    def __init__(self, url_list = START_URL_LIST, depth = DEFAULT_DEPTH) -> None:
        self.urls_to_crawl = url_list
        self.depth = depth
    
    
class ParseUtils:
    
    @staticmethod
    def get_all_urls(text: str, root_url: Optional[None] = None) -> List[str]:
        soup = BeautifulSoup(text, "html.parser")
        links = soup.find_all("a")
        if not root_url:
            return [link.get("href") for link in links]
        
        href_list = []

        for link in links:
            href: str = link.get("href")
            if href.startswith("http"):
                href_list.append(href)
                continue
            href_list.append(f"{root_url}{href}")

        return href_list
    
    @staticmethod
    def get_test_only(text: str) -> str:
        soup = BeautifulSoup(text, "html.parser")
        
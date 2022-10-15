from bs4 import BeautifulSoup
from typing import List, Optional


class Crawler:
    START_URL_LIST = [
        "https://nstu.ru/",
    ]
    DEFAULT_DEPTH = 2

    def __init__(self, url_list=START_URL_LIST, depth=DEFAULT_DEPTH) -> None:
        self.urls_to_crawl = url_list
        self.depth = depth

    def start_crawl():
        pass


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

            continue
            href_list.append(f"{root_url}{href}")

        return href_list

    @staticmethod
    def get_text_only(text: str) -> str:
        texts = ParseUtils._get_childs_texts(text)
        return " ".join(texts)

    @staticmethod
    def get_separated_words(text: str) -> List[str]:
        texts = ParseUtils._get_childs_texts(text)
        separate_words = []
        for element in texts:
            separate_words.extend(element.split(" "))
        print(separate_words)
        return separate_words

    @staticmethod
    def _get_childs_texts(text: str) -> List[str]:
        soup = BeautifulSoup(text, "html.parser")
        texts = []
        for element in soup.find_all():
            if element.name == "script":
                continue
            try:
                content = " ".join(element.contents)
            except TypeError:
                continue
            if "<" not in content and ">" not in content:
                text = element.text.strip().replace("\n", "")
                if text:
                    texts.append(text)
        return texts

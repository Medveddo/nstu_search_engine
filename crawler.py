from dataclasses import dataclass
from bs4 import BeautifulSoup
from typing import List, Optional
from loguru import logger
import requests
from database import DbActor
from dataclasses import dataclass

@dataclass
class Word:
    word: str
    location: int
    id_: int = 0

class Crawler:
    START_URL_LIST = [
        "http://deb.debian.org/",
    ]
    DEFAULT_DEPTH = 2

    def __init__(self, url_list=START_URL_LIST, depth=DEFAULT_DEPTH) -> None:
        self.urls_to_crawl: List[str] = url_list
        self.depth = depth
        self.db = DbActor()

    def start_crawl(self):
        self._crawl_iteration(self.urls_to_crawl.pop())

    def _crawl_iteration(self, url_to_process: str, current_depth = 0):
        logger.debug(f"Crawling {url_to_process} ...")


        response = requests.get(url_to_process)
        content = response.text

        word_list = ParseUtils.get_separated_words(content)
        links = ParseUtils.get_all_urls(content)

        word_list = [Word(word=word, location=i) for i, word in enumerate(word_list)]
        
        # 1
        # Добавляем ссылку в url_list
        current_url_id = self.db.insert_url(url_to_process)
        logger.debug(f"{current_url_id=} {url_to_process=}")

        # 2
        # Вставляем все слова в word_list (filtered = 0)
        words = []
        for word in word_list:
            id_ = self.db.insert_word(word.word)
            word.id_ = id_
        logger.debug(word_list)
        # 3 
        # Заполняем word_location
        # TODO: one insert with many VALUES

        # 4
        # Получаем все ссылки

        # 5
        # Заполянем link_between_url

        # 6 
        # Заполняем link_word

        # Получаем все ссылки
        # Заполняем link_between_url
        # Берём все слова нас транице
        # Заполняем
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
        # print(separate_words)
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

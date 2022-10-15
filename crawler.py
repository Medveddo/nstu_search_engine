from dataclasses import dataclass
import datetime
from bs4 import BeautifulSoup
from typing import List, Optional
from loguru import logger
import requests
from database import DbActor
from dataclasses import dataclass
from entites import Word, Link, Element, LinkToGo
from utils import Decorators
from sqlalchemy.exc import SQLAlchemyError

class Crawler:
    START_URL_LIST = [
        # LinkToGo("https://ngs.ru/"),
        # LinkToGo("http://deb.debian.org/"),
        LinkToGo("https://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0"),
    ]
    DEFAULT_DEPTH = 2

    def __init__(self, url_list=START_URL_LIST, depth=DEFAULT_DEPTH) -> None:
        self.urls_to_crawl: List[LinkToGo] = url_list
        self.crawled_urls: List[str] = []
        self.depth = depth
        self.db = DbActor()
        self.crawl_count = 0
        self.start_time = datetime.datetime.utcnow()

    @Decorators.timing
    def start_crawl(self):
        self._crawl_iteration(self.urls_to_crawl.pop(0))

    @staticmethod
    @Decorators.timing
    def _get_page(url: str) -> str:
        try:
            response = requests.get(url)
        except Exception:
            return ""
        return response.text

    @Decorators.timing
    def _crawl_iteration(self, link_to_process: LinkToGo):
        try:
            current_depth = link_to_process.depth
            url_to_process = link_to_process.link
            if self.crawl_count % 10 == 0:
                logger.error(f"Crawled count: {self.crawl_count}")
                self.db.get_stat()
            self.crawl_count += 1
            logger.debug(f"Crawling {url_to_process} - {link_to_process.depth} ...")
            
            # 1
            # Добавляем текущую ссылку в url_list
            current_url_id = self.db.insert_url(url_to_process)
            # logger.debug(f"{current_url_id=} {url_to_process=}")

            # 2
            # Получаем web-страницу
            content = self._get_page(url_to_process)

            # 3
            # Парсим страничку
            elements = ParseUtils._get_childs_texts_turbo(content)

            location = 0
            for element in elements:
                if not element.word:
                    continue
                element.location = location
                location += 1

            self.db.insert_links_from_elements(elements)
            self.db.insert_words_from_elements(elements)
            self.db.insert_links_between_by_elements(elements, current_url_id)
            self.db.fill_words_locations_by_elements(elements, current_url_id)
            self.db.fill_link_words_by_elements(elements)

            self.crawled_urls.append(url_to_process)

            links_to_go_next = [
                element.href for element in elements 
                if element.href and element.href not in self.crawled_urls
            ]

            if current_depth + 1 <= self.DEFAULT_DEPTH:
                self.urls_to_crawl.extend([LinkToGo(link, current_depth + 1) for link in links_to_go_next])
                self.urls_to_crawl = list(set(self.urls_to_crawl))
        except SQLAlchemyError:
            logger.warning(f"Broken HTML with SQLLite {link_to_process.link} {link_to_process.depth}")
            pass
        try:
            link_to_go = self.urls_to_crawl.pop(0)
        except IndexError:
            logger.success(f"Finished crawl. Crawled pages: {self.crawl_count}. Time ellapsed: {(datetime.datetime.utcnow() - self.start_time).seconds / 60} min.")
            self.db.close()
            return
        
        self._crawl_iteration(link_to_go)

class ParseUtils:
    @staticmethod
    def get_text_only(text: str) -> str:
        texts = ParseUtils._get_childs_texts(text)
        return " ".join(texts)
    
    @staticmethod
    def _get_childs_texts_turbo(text: str) -> List[Element]:
        soup = BeautifulSoup(text, "html.parser")
        elements = []
        for element in soup.find_all():
            if element.name == "head":
                continue
            if element.name == "style":
                continue
            if element.name == "script":
                continue
            try:
                content = " ".join(element.contents)
            except TypeError:
                continue
            if "<" in content or ">"in content:
                continue

            text = element.text.strip().replace("\n", "")
            
            words = text.split(" ")
            

            for word in words:
                # Исключить числа
                if word.replace(" ", "").isnumeric():
                    continue

                if element.name == "a":
                    href: str = element.get("href")
                    if not href:
                        continue
                    if not href and not word or "mailto:" in href or "tel:" in href:
                        continue
                    if not href.startswith("http"):
                        continue
                    elements.append(Element(word=word, href=href))
                    continue
                if not word:
                    continue
                elements.append(Element(word=word))
        
        return elements
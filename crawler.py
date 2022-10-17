import datetime
import time
from typing import List
import bs4

import requests
from bs4 import BeautifulSoup
from loguru import logger
from sqlalchemy.exc import SQLAlchemyError

import csv

from database import DbActor
from entites import Element, LinkToGo
from utils import Decorators


class Crawler:
    START_URL_LIST = [
        LinkToGo("https://ngs.ru/"),
        # LinkToGo("http://deb.debian.org/"),
        # LinkToGo("http://example.com/"),
        # LinkToGo(
        #     "https://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0"
        # ),
    ]
    MAX_DEPTH = 2
    SLEEP_TIMEOUT = 0.25
    MAX_RETRIES_COUNT = 3

    def __init__(self, url_list=START_URL_LIST, depth=MAX_DEPTH) -> None:
        self.start_url_list = url_list[:]
        self.urls_to_crawl: List[LinkToGo] = url_list[:]
        self.crawled_urls: List[str] = []
        self.depth = depth
        self.db = DbActor()
        self.crawl_count = 0
        self.start_time = datetime.datetime.utcnow()
        self.error_processed_urls: List[str] = []

    @staticmethod
    def create_stat_csv():
        with open("stat.csv", "w") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(
                [
                    "iterations_count",
                    "link_between",
                    "link_word",
                    "url_list",
                    "word_list",
                    "word_location",
                ]
            )

    @Decorators.timing
    def start_crawl(self):
        self.create_stat_csv()
        try:
            while self.urls_to_crawl:
                self._crawl_iteration(self.urls_to_crawl.pop(0))
        except KeyboardInterrupt:
            logger.info("Crawler was stoped by user.")
        except Exception as e:
            logger.critical(f"unexpected end of crawling - {e}")

        logger.success(
            f"Finished crawl. Crawled pages: {self.crawl_count}. Time ellapsed: {(datetime.datetime.utcnow() - self.start_time).seconds / 60 :.2f} min. Started from: {self.start_url_list}"
        )
        if self.error_processed_urls:
            logger.warning(f"Unprocessed urls: {self.error_processed_urls}")
        self.db.close()

    @Decorators.timing
    def _get_page(self, url: str) -> str:
        retries_count = 0
        while retries_count < self.MAX_RETRIES_COUNT:
            try:
                response = requests.get(url, timeout=10)
                return response.text
            except (requests.HTTPError, requests.ConnectionError) as e:
                retries_count += 1
                logger.warning(f"fetch '{url}' - {e} {retries_count=}")
                time.sleep(self.SLEEP_TIMEOUT)
                continue

        logger.error(f"Max retries count exceeded - {url}")
        self.error_processed_urls.append(url)
        return ""

    @Decorators.timing
    def _crawl_iteration(self, link_to_process: LinkToGo):
        current_depth = link_to_process.depth
        url_to_process = link_to_process.link
        if self.crawl_count and self.crawl_count % 10 == 0:
            self.db.get_stat(self.crawl_count)
        self.crawl_count += 1
        logger.debug(
            f"{self.crawl_count} - Crawling {url_to_process} ({link_to_process.depth}) ..."
        )

        # 2
        # Получаем web-страницу
        content = self._get_page(url_to_process)
        if not content:
            return
        # 3
        # Парсим страничку
        elements = ParseUtils._get_childs_texts_turbo(content, link_to_process.link)

        try:
            # 1
            # Добавляем текущую ссылку в url_list
            current_url_id = self.db.insert_url(url_to_process)
            self.db.insert_links_from_elements(elements)
            self.db.insert_words_from_elements(elements)
            self.db.insert_links_between_by_elements(elements, current_url_id)
            self.db.fill_words_locations_by_elements(elements, current_url_id)
            self.db.fill_link_words_by_elements(elements)

            # Mark page as crawled
            self.crawled_urls.append(url_to_process)

            if current_depth + 1 > self.MAX_DEPTH:
                return

            links_to_go_next = [
                LinkToGo(element.href, current_depth + 1)
                for element in elements
                if element.href and element.href not in self.crawled_urls
            ]

            # https://stackoverflow.com/questions/480214/how-do-i-remove-duplicates-from-a-list-while-preserving-order
            self.urls_to_crawl.extend(links_to_go_next)
            self.urls_to_crawl = list(dict.fromkeys(self.urls_to_crawl))
        except SQLAlchemyError as e:
            # logger.exception(e)
            logger.warning(
                f"Broken HTML with SQLLite {link_to_process.link} {link_to_process.depth}"
            )
            self.error_processed_urls.append(link_to_process.link)


class ParseUtils:
    @staticmethod
    def _get_childs_texts_turbo(text: str, base_url: str) -> List[Element]:
        soup = BeautifulSoup(text, "html.parser")

        # listUnwantedItems = ('script', 'style')
        # for script in soup.find_all(listUnwantedItems):
        #     script.decompose()

        return OmegaParser3000.merge_text_and_links(soup.text, soup.find_all("a"), base_url)


class OmegaParser3000:
    # STRIP_CHARACTERS = ":,«».\"/()-!?'"
    STRIP_CHARACTERS = ":,«».\"/()-!?'0123456789"

    @classmethod
    def merge_text_and_links(
        cls, original_text: str, a_tags: List[bs4.Tag], base_url: str
    ) -> List[Element]:
        base_url = base_url.strip("/")
        clean_words = cls._text_to_clean_words(original_text)
        output_elements: List[Element] = []
        for i, word in enumerate(clean_words):
            output_elements.append(Element(word=word, location=i))

        for a in a_tags:
            a_href = a.get("href")
            if not a_href:
                continue
            if "mailto:" in a_href or "tel:" in a_href:
                continue
            if not a_href.startswith("http"):
                # TODO: to remove the same site - uncomment
                # continue
                a_href = f"{base_url}{a_href}"
            a_words = cls._text_to_clean_words(a.text)
            for i, a_word in enumerate(a_words, start=len(output_elements)):
                output_elements.append(Element(word=a_word, location=i, href=a_href))

        return output_elements

    @classmethod
    def _text_to_clean_words(cls, text: str) -> List[str]:
        text = cls._clean_up_input_text(text)
        words = text.split(" ")
        cls._cleanup_and_lower_words(words)
        cls._remove_numbers(words)
        return [word for word in words if word]

    @staticmethod
    def _clean_up_input_text(input_text: str) -> str:
        clean_text = input_text.replace("\n", " ")
        clean_text = " ".join(clean_text.split())
        return clean_text

    @classmethod
    def _cleanup_and_lower_words(cls, words: List[str]) -> None:
        for i, word in enumerate(words):
            words[i] = word.strip(cls.STRIP_CHARACTERS).lower()

    @staticmethod
    def _remove_numbers(words: List[str]) -> None:
        new_words = []
        for word in words:
            try:
                float(word.replace(",", "."))
            except ValueError:
                new_words.append(word)

        words.clear()
        words.extend(new_words)

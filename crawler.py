import asyncio
import datetime
import time
from typing import List
import bs4
import aiohttp
import re

import requests
from bs4 import BeautifulSoup
from loguru import logger
from sqlalchemy.exc import SQLAlchemyError

import csv

from database import DbActor
from entities import Element, LinkToGo, FetchedUrl
from utils import Decorators
import threading

class Crawler:
    START_URL_LIST = [
        LinkToGo("https://ngs.ru/"),
        LinkToGo("https://lenta.ru/"),
        LinkToGo("https://news.mail.ru/"),
        # LinkToGo("http://deb.debian.org/"),
        # LinkToGo("http://example.com/"),
        # LinkToGo(
        #     "https://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0"
        # ),
    ]
    MAX_DEPTH = 2
    SLEEP_TIMEOUT = 0.25
    MAX_RETRIES_COUNT = 3

    def __init__(self, url_list: List[LinkToGo]=START_URL_LIST, depth=MAX_DEPTH) -> None:
        for url in url_list:
            url.link = url.link.strip("/")
        self.start_url_list = url_list[:]
        self.urls_to_crawl: List[LinkToGo] = url_list[:]
        self.crawled_urls: List[str] = []
        self.depth = depth
        self.db = DbActor()
        self.crawl_count = 0
        self.start_time = datetime.datetime.utcnow()
        self.error_processed_urls: List[str] = []
        self.pages_to_process: List[FetchedUrl] = []
        self.stop_flag = False

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
                    "unique_words_count",
                ]
            )

    @Decorators.timing
    def start_crawl(self):
        self.create_stat_csv()
        try:
            fetch_thread = threading.Thread(target=self.async_fetch_urls)
            fetch_thread.start()
            time.sleep(2)
            idle_counter = 0
            while True:
                if not self.pages_to_process:
                    logger.debug(f"Empty pages to process - ({idle_counter}) Sleep ...")
                    time.sleep(5)
                    idle_counter += 1
                    if idle_counter >= 8:
                        break
                    continue
                try:
                    page_to_process = self.pages_to_process.pop(0)
                    idle_counter = 0
                except IndexError:
                    continue
                self._crawl_iteration(page_to_process)
        except KeyboardInterrupt:
            logger.info("Crawler was stoped by user.")
            self.stop_flag = True
        except Exception as e:
            logger.exception(e)
            logger.critical(f"unexpected end of crawling - {e}")
        finally:
            logger.debug("Wait fetch thread to end ...")
            fetch_thread.join()
            logger.success(
                f"Finished crawl. Crawled pages: {self.crawl_count}. Time ellapsed: {(datetime.datetime.utcnow() - self.start_time).seconds / 60 :.2f} min. Started from: {self.start_url_list}"
            )
            if self.error_processed_urls:
                logger.warning(f"Unprocessed urls ({len(self.error_processed_urls)}): {self.error_processed_urls[:3]} ... {self.error_processed_urls[-3:]}")
            self.db.save_to_db_to_disk()
            self.db.close()

    def async_fetch_urls(self):
        logger.debug("Starting fetch thread")
        try:
            asyncio.run(self.fetch_urls())
        except (KeyboardInterrupt, RuntimeError):
            logger.info("Finished fetch thread")
        
    async def fetch_urls(self):
        idle_counter = 0
        batch_size = 60
        while True:
            if self.stop_flag:
                self.error_processed_urls.extend(self.urls_to_crawl)
                break

            if not self.urls_to_crawl:
                logger.debug(f"Empty urls to fetch ({idle_counter}). Sleeping ...")
                await asyncio.sleep(5)
                idle_counter += 1
                if idle_counter >= 5:
                    break
                continue
            
            urls_batch: List[LinkToGo] = self.urls_to_crawl[:batch_size]
            self.urls_to_crawl = self.urls_to_crawl[batch_size:]

            async def fetch(session: aiohttp.ClientSession, link: LinkToGo):
                retries_count = 0
                while retries_count < 3:
                    try:
                        async with session.get(link.link) as response:
                            text = await response.text()
                            logger.debug(f"fetched {link.link}")
                            return FetchedUrl(url=link.link, text=text, depth=link.depth)
                    except (aiohttp.ServerTimeoutError, aiohttp.ServerConnectionError, aiohttp.ClientConnectionError, asyncio.exceptions.TimeoutError) as e:
                        retries_count += 1
                        logger.warning(f"{link.link} - {repr(e)} - {retries_count}")
                        await asyncio.sleep(0.25)
                    except (aiohttp.TooManyRedirects, UnicodeDecodeError):
                        break
                    except Exception as e:
                        logger.critical(e)
                        logger.exception(e)
                        break

                self.error_processed_urls.append(link.link)
                logger.error(f"Max retries exceed - {link.link}")
                return FetchedUrl(url="", text="")

            timeout = aiohttp.ClientTimeout(total=10, connect=2)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                results: List[FetchedUrl] = await asyncio.gather(*[fetch(session, url) for url in urls_batch], return_exceptions=True)
                logger.success(f"Fetched {len(results)} urls")
                results = [result for result in results if result.text]
                self.pages_to_process.extend(results)
            
            logger.debug(f"End fetch iteration. urls_to_fetch={len(self.urls_to_crawl)} pages_to_process={len(self.pages_to_process)}")
            idle_counter = 0
            await asyncio.sleep(5)          

        logger.info("Finishing fetch thread ...")

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
    def _crawl_iteration(self, fetched_url: FetchedUrl):
        current_depth = fetched_url.depth
        url_to_process = fetched_url.url
        if self.crawl_count and self.crawl_count % 10 == 0:
            self.db.get_stat(self.crawl_count)
        self.crawl_count += 1
        logger.debug(
            f"{self.crawl_count} - Processing {url_to_process} ({fetched_url.depth}) ..."
        )

        if not fetched_url.text:
            return

        elements = ParseUtils._get_childs_texts_turbo(fetched_url.text, fetched_url.url)

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
                f"Broken HTML with SQLLite {fetched_url.url} {fetched_url.depth} - {e}"
            )
            self.error_processed_urls.append(fetched_url.url)


class ParseUtils:
    @staticmethod
    def _get_childs_texts_turbo(text: str, base_url: str) -> List[Element]:
        soup = BeautifulSoup(text, "html.parser")
        return OmegaParser3000.merge_text_and_links(soup.get_text(separator=" ", strip=True), soup.find_all("a"), base_url)


class OmegaParser3000:
    STRIP_CHARACTERS = ":,«».\"/|()-!?'0123456789"

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
            if a_href.endswith((".jpg", ".png", ".gif", ".jpeg", ".pdf")):
                continue
            
            if not a_href.startswith("http"):
                # TODO: to remove the same site - uncomment
                continue
                a_href = f"{base_url}{a_href}"
            a_words = cls._text_to_clean_words(a.text)
            for i, a_word in enumerate(a_words, start=len(output_elements)):
                output_elements.append(Element(word=a_word, location=i, href=a_href.strip("/")))

        return output_elements

    @classmethod
    def _text_to_clean_words(cls, text: str) -> List[str]:
        text = cls._clean_up_input_text(text)
        words = re.split("[\W\d]+", text, flags=re.UNICODE)
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

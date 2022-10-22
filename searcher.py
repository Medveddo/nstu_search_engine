# Нужен сикули запрос на популярные слова - ???

# Получить Ids слов

# Получить странички слов и их позиции с полным перебором

# Rank main

# Rank temp

from typing import List

from loguru import logger
from database import DbActor


class Searcher:
    def __init__(self) -> None:
        self.db = DbActor()

    def search(self, query: str):
        pass

    def close(self) -> None:
        self.db.close()

class PageRankerer:
    def __init__(self) -> None:
        self.db = DbActor(in_memory=False)

    def close(self) -> None:
        self.db.close()

    def calculate_ranks(self):
        url_ids = self.db.get_unique_urls_ids()
        self.insert_fresh_page_rank(url_ids)
        # set
        # | url_fk      | rank |
        # |-------------|------|
        # | 1(ngs.ru)   | 1.0  |
        # |2(rambler.ru)| 1.0  |
        #
        
        # for each url_fk go to urls-between and find where URL_TO == url_fk
        for url_id in url_ids[1:]:
            from_url_fks = self.db.get_references_by_url_to_fk(url_id)
            logger.debug(f"{url_id} - {from_url_fks}")
            break

    def insert_fresh_page_rank(self, url_fks: List[str]) -> None:
        self.db.fill_page_rank(url_fks)
        
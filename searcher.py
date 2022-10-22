# Нужен сикули запрос на популярные слова - ???

# Получить Ids слов

# Получить странички слов и их позиции с полным перебором

# Rank main

# Rank temp

from dataclasses import dataclass
from typing import List

from loguru import logger
from database import DbActor
from entities import PageRankURL
from utils import Decorators


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

    @Decorators.timing
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
        for url_id in url_ids:
            from_url_fks = self.db.get_references_by_url_to_fk(url_id)
            logger.debug(f"TO URL ID: {url_id}, FROM LIST: {from_url_fks}")
            
            other_links_sum = 0
            
            for url_fk in from_url_fks:
                links_count_on_url = self.db.get_url_links_count(url_fk)
                rank = self.db.get_page_rank_by_id(url_fk)
                logger.debug(f"URL ID {url_fk} (Rank: {rank}) has {links_count_on_url} LINKS")
                
                try:
                    other_links_sum += rank/links_count_on_url 
                except TypeError:
                    logger.error(f"NO RANK URL ID: {url_fk}")
                
            default_coeef = 0.85
            # old_page_rank = self.db.get_page_rank_by_id(url_id)
            new_rank = (1 - default_coeef) + default_coeef * other_links_sum
            self.db.set_page_rank_by_id(url_id, new_rank)
        
        self.db.sync_main_and_temp_rank_tables()
        self.db.db.execute("DELETE FROM page_rank_temp")
        self.db.db.commit()


    def insert_fresh_page_rank(self, url_fks: List[str]) -> None:
        self.db.fill_page_rank(url_fks)
        
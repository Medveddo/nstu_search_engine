# Нужен сикули запрос на популярные слова - ???

# Получить Ids слов

# Получить странички слов и их позиции с полным перебором

# Rank main

# Rank temp

from pydoc import doc
from turtle import distance
from typing import List

from loguru import logger
from database import DbActor
from entities import WordLocationsCombination
from entities import PageRankURL
from utils import Decorators
from dataclasses import dataclass
from airium import Airium
from random import randint

class Searcher:
    def __init__(self) -> None:
        self.db = DbActor()

    def search(self, query: str):
        pass

    def create_marked_html_file(self, marked_html_filename, words, marked_words):
        marked_set = {}
        for i in tuple(marked_words):
            rand_color = "%06x" % randint(0, 0xFFFFFF)
            marked_set[i] = rand_color
       
        doc_gen = Airium(source_minify=True)

        with doc_gen.html("lang=ru"):
            with doc_gen.head():
                doc_gen.meta(charset="utf-8")
                doc_gen.title(_t="Marked Words Test")
            with doc_gen.body():
                with doc_gen.p():
                    for i in words:
                        if i not in marked_words:
                            doc_gen(f"{i}")
                        else:
                            with doc_gen.span(style=f"background-color:#{marked_set[i]}"):
                                doc_gen(f"{i}")
                        doc_gen(" ")
                        
        
        html = str(doc_gen)
        with open(marked_html_filename, "wb") as f:
            f.write(bytes(html, encoding='utf8'))

    def close(self) -> None:
        self.db.close()

class PageRankerer:
    def __init__(self) -> None:
        self.db = DbActor(in_memory=False)

    def close(self) -> None:
        self.db.close()

    def distance_score(self, words):
        combinations: List[WordLocationsCombination] = self.db.get_words_location_combinations(words)
        if len(combinations) == 0:
            return []

        unique_ids = set() 
        min_distance_list = []

        if len(combinations[0].word_locations) == 1:
            for i in min_distance_list:
                min_distance_list[i][1] = 1.0
        else:
            for i in combinations:
                if i.url not in unique_ids:
                    min_distance_list.append([i.url, 999999.9])
                    unique_ids.add(i.url)

        for i in min_distance_list:
            for j in combinations:
                if (j.url == i[0]):
                    local_distance = 0
                    for k in range(len(j.word_locations) - 1):
                        diff = abs(j.word_locations[k] - j.word_locations[k - 1])
                        local_distance += diff
                if (i[1] > local_distance):
                    i[1] = local_distance
        
        return self.normalized_score(min_distance_list)

    def normalized_score(self, distance_list, is_small_better = 0):
        columns = list(zip(*distance_list))

        min_score = min(columns[1])
        max_score = max(columns[1])

        if is_small_better:
            for i in distance_list:
                i[1] = float(min_score) / i[1]
        else:
            for i in distance_list:
                i[1] = float(i[1]) / max_score
        
        return distance_list


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
                
            default_coef = 0.85
            # old_page_rank = self.db.get_page_rank_by_id(url_id)
            new_rank = (1 - default_coef) + default_coef * other_links_sum
            self.db.set_page_rank_by_id(url_id, new_rank)
        
        self.db.sync_main_and_temp_rank_tables()
        self.db.db.execute("DELETE FROM page_rank_temp")
        self.db.db.commit()


    def insert_fresh_page_rank(self, url_fks: List[str]) -> None:
        self.db.fill_page_rank(url_fks)
        
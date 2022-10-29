# Нужен сикули запрос на популярные слова - ???

# Получить Ids слов

# Получить странички слов и их позиции с полным перебором

# Rank main

# Rank temp

from typing import Any, Dict, Iterable, List
from loguru import logger
from sqlalchemy import false
from database import DbActor
from entities import ResultURL, WordLocationsCombination
from entities import PageRankURL
from utils import Decorators
from dataclasses import dataclass
from airium import Airium
from random import randint

class Searcher:
    def __init__(self, in_memory=True) -> None:
        self.db = DbActor()

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
    def __init__(self, in_memory=False) -> None:
        self.db = DbActor()

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

    def normalized_score(self, distance_list, is_small_better = True):
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
        logger.info("Start calculating page ranks ...")
        url_ids = self.db.get_unique_urls_ids()

        page_ranks: Dict[int, PageRankURL] = dict()

        for url_id in url_ids:
            links_count = self.db.get_from_url_count(url_id)
            references = self.db.get_from_urls_by_to(url_id)
            page_ranks.setdefault(
                url_id,
                PageRankURL(
                    id=url_id,
                    links_count=links_count,
                    rank=1.0,
                    ratio=1.0/links_count if links_count else 1.0,
                    references=references
                )
            )
        
        iterations_count = 25

        for i in range(iterations_count):
            logger.debug(f"Iteration #{i}")
            for page in page_ranks.values():
                other_links_sum = 0
                for ref in page.references:
                    other_links_sum += page_ranks.get(ref).ratio
                page.rank = (1 - 0.85) + 0.85 * other_links_sum

            for page in page_ranks.values():
                page.ratio = page.rank / page.links_count if page.links_count else page.rank

            pages = list(page_ranks.values())
            logger.debug(pages[30])

        self.db.fill_page_rank(list(page_ranks.values()))


        self.db.save_to_db_to_disk()
        logger.success("Page ranks are calculated!")

    def get_normalized_page_ranks_by_result_urls(self, urls: List[ResultURL]) -> List[ResultURL]:
        urls_dict = {
            url.url_id: url
            for url 
            in urls
        }

        # logger.success(max_rank)
        # get url_ids ranks
        urls_with_page_rank = self.db.get_urls_with_page_ranks(
            [url.url_id for url in urls]
        )
        # logger.debug(urls_with_page_rank)

        max_rank = max([url.page_rank_raw_metric for url in urls_with_page_rank])

        # normalize
        ratio = 1 / max_rank 
        logger.warning(max_rank)
        logger.warning(f"{ratio=}")
        for url in urls_with_page_rank:
            url.page_rank_normalized_metric = url.page_rank_raw_metric * ratio

        # logger.debug(urls_with_page_rank)
        # return 


        # modify and calc summary
        for url in urls_with_page_rank:
            url.distance_normalized_metric = urls_dict[url.url_id].distance_normalized_metric
            url.total_rating = (url.page_rank_normalized_metric + url.distance_normalized_metric) / 2
            
        logger.success(urls_with_page_rank)
        return urls_with_page_rank
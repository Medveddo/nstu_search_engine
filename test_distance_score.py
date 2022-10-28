from typing import List, Tuple
from loguru import logger
from searcher import PageRankerer, Searcher
from operator import itemgetter
from entities import ResultURL

ranker = PageRankerer()
searcher = Searcher()

# input_string = input("Enter search words separated by space: ")
# try:
#     htmls_number = int(input("Enter max number of htmls to create: "))
# except:
#     htmls_number = 1
htmls_number = 2

# search_words = input_string.split(" ")
search_words = ["человек", "новосибирск"]
distanced_urls = ranker.distance_score(search_words)

if htmls_number > len(distanced_urls):
    htmls_number = len(distanced_urls)

if len(distanced_urls) > 0:
    # [url_id, distance_rank]
    distanced_urls: List[Tuple[int, float]] = sorted(distanced_urls, key=itemgetter(1), reverse=True)
    
    result_urls = [
        ResultURL(
            url_id=element[0],
            distance_normalized_metric=element[1],
        )
        for element
        in distanced_urls
    ]

    result_urls = ranker.get_normalized_page_ranks_by_result_urls(result_urls)

    # sort by total rating

    def total_rating_getter(url: ResultURL):
        return url.total_rating

    result_urls = sorted(result_urls, key=total_rating_getter, reverse=True)
    
    for url in result_urls[:htmls_number]:
        print(f"URL ({url.url_id}): {url.url_name}, total score: {url.total_rating:.3f} (page_rank={url.page_rank_normalized_metric:.3f}, distance={url.distance_normalized_metric:.3f})")
        words = ranker.db.get_words_by_url(url.url_id)
        searcher.create_marked_html_file(f"result_{url.url_id}.html", words, search_words)
else:
    logger.info("No URS found :(")

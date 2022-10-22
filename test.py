from searcher import PageRankerer, Searcher
from database import DbActor
from operator import itemgetter

ranker = PageRankerer()
actor = DbActor(False)
searcher = Searcher()

input_string = input("Enter search words separated by space: ")
search_words = input_string.split(" ")
distanced_urls = ranker.distance_score(search_words)

if len(distanced_urls) > 0:
    distanced_urls = sorted(distanced_urls, key=itemgetter(1), reverse=True)
    words = actor.get_words_by_url(distanced_urls[0][0])
    searcher.create_marked_html_file("test.html", words, search_words)
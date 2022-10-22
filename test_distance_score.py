from searcher import PageRankerer, Searcher
from operator import itemgetter

ranker = PageRankerer()
searcher = Searcher()

input_string = input("Enter search words separated by space: ")
try:
    htmls_number = int(input("Enter max number of htmls to create: "))
except:
    htmls_number = 1

search_words = input_string.split(" ")
distanced_urls = ranker.distance_score(search_words)

if len(distanced_urls) > 0:
    distanced_urls = sorted(distanced_urls, key=itemgetter(1), reverse=True)

    if htmls_number > len(distanced_urls):
        htmls_number = len(distanced_urls)

    for i in range(0, htmls_number):
        print(f"URL: {distanced_urls[i][0]}, metric: {distanced_urls[i][1]}")
        words = ranker.db.get_words_by_url(distanced_urls[i][0])
        searcher.create_marked_html_file(f"distance_{i}_{distanced_urls[i][0]}.html", words, search_words)

class Crawler:
    START_URL_LIST = [
        "https://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0",
    ]
    DEFAULT_DEPTH = 2
    
    def __init__(self, url_list = START_URL_LIST, depth = DEFAULT_DEPTH) -> None:
        self.urls_to_crawl = url_list
        self.depth = depth
    
    
    
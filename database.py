import csv

from typing import List, Tuple
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from entites import Element
from utils import Decorators

from tabulate import tabulate

SQLALCHEMY_DATABASE_URL = "sqlite:///lab1.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
DbSession = sessionmaker(autoflush=False, bind=engine)
Base = declarative_base()


class DbCreator:
    CREATE_TABLE_WORD_LIST = """
    CREATE TABLE IF NOT EXISTS word_list (
        wordId INTEGER PRIMARY KEY, -- id слова
        word TEXT, -- само слово
        isFiltered INT -- magic flag
    )
    """
    CREATE_TABLE_URL_LIST = """
    CREATE TABLE IF NOT EXISTS url_list (
        urlId INTEGER PRIMARY KEY,
        url TEXT -- ссылка на ресурс
    )
    """

    CREATE_TABLE_WORD_LOCATION = """
    -- Положение слов на странице
    CREATE TABLE IF NOT EXISTS word_location (
        id INTEGER PRIMARY KEY,
        fkWordId INT, -- FK на word_list:wordId
        fkUrlId INT, -- FK на url_list:urlId
        location INT -- порядковый номер слова на странице
    )
    """
    CREATE_TABLE_LINK_BETWEEN_URL = """
    -- откуда-куда ссылки
    CREATE TABLE IF NOT EXISTS link_between_url (
        linkId INTEGER PRIMARY KEY,
        fkFromUrlId INT, -- FK url_list:urlId
        fkToUrlId INT -- FK url_list:urlId
    )
    """
    CREATE_TABLE_LINK_WORD = """
    -- таблица слов использующихся в ссылках (tag 'a')
    CREATE TABLE IF NOT EXISTS link_word (
        id INTEGER PRIMARY KEY,
        fkWordId INT,
        fkLinkId INT
    )
    """

    SELECT_TABLES_COUNT = """
    SELECT COUNT(name) FROM sqlite_master WHERE type='table'
    """

    @classmethod
    def initialize_db(cls) -> None:
        session: Session = DbSession()
        session.execute(cls.CREATE_TABLE_WORD_LIST)
        session.execute(cls.CREATE_TABLE_URL_LIST)
        session.execute(cls.CREATE_TABLE_WORD_LOCATION)
        session.execute(cls.CREATE_TABLE_LINK_BETWEEN_URL)
        session.execute(cls.CREATE_TABLE_LINK_WORD)

        result = session.execute(cls.SELECT_TABLES_COUNT)
        tables_count = result.fetchall()[0][0]
        logger.debug(f"Tables count: {tables_count}")
        if tables_count != 5:
            logger.critical("Not enougth tables!!!")
            exit(1)

        session.close()


class DbActor:
    INSERT_INTO_URL_LIST = """
    INSERT INTO url_list(url) VALUES ('{url}')
    """

    INSERT_INTO_URL_LIST_TURBO = """
    INSERT INTO url_list(url) VALUES {list_of_values}
    """

    INSERT_INTO_WORD_LIST = """
    INSERT INTO word_list(word, isFiltered) VALUES ('{word}', {is_filtered})
    """

    INSERT_INTO_WORD_LIST_TURBO_FAST = """
    INSERT INTO word_list(word, isFiltered) VALUES {list_of_values}
    """

    INSERT_INTO_WORD_LOCATIONS = """
    INSERT INTO word_location(fkWordId, fkUrlId, location) VALUES {list_of_values}
    """

    INSERT_INTO_LINKS_BETWEEN = """
    INSERT INTO link_between_url(fkFromUrlId, fkToUrlId) VALUES {list_of_values}
    """

    INSERT_INTO_LINK_WORD = """
    INSERT INTO link_word(fkWordId, fkLinkId) VALUES {list_of_values}
    """

    SELECT_LAST_WORD_ID = """
    SELECT MAX(wordId) FROM word_list
    """

    SELECT_LAST_LINK_ID = """
    SELECT MAX(urlId) FROM url_list
    """

    SELECT_TABLE_SIZE_STATS = """
    SELECT COUNT(*), 'link_between' as temp_field FROM link_between_url 
    UNION 
    SELECT COUNT(*), 'link_word' as temp_field FROM link_word
    UNION 
    SELECT COUNT(*), 'url_list' as temp_field FROM url_list
    UNION 
    SELECT COUNT(*), 'word_list' as temp_field FROM word_list
    UNION 
    SELECT COUNT(*), 'word_location' as temp_field FROM word_location
    """

    def __init__(self) -> None:
        self.db = DbSession()

    def close(self):
        self.db.close()

    @staticmethod
    def append_csv_stat(data: List[Tuple[str, int]], urls_crawled: int):
        with open("stat.csv", "a") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(
                (
                    urls_crawled,
                    data[0][1],
                    data[1][1],
                    data[2][1],
                    data[3][1],
                    data[4][1],
                )
            )

    def get_stat(self, urls_crawled: int):
        result = self.db.execute(self.SELECT_TABLE_SIZE_STATS)
        result = result.fetchall()
        data = []
        for row in result:
            data.append((row[1], row[0]))
        logger.success(
            f'\n\tCrawled count: {urls_crawled}\n{tabulate(data, headers=["table_name", "rows"])}'
        )
        self.append_csv_stat(data, urls_crawled)

    def _get_last_word_id(self) -> int:
        result = self.db.execute(self.SELECT_LAST_WORD_ID)
        result = result.fetchone()[0]
        return result

    def _get_last_url_id(self) -> int:
        result = self.db.execute(self.SELECT_LAST_LINK_ID)
        result = result.fetchone()[0]
        return result

    def insert_url(self, url: str) -> int:
        query = self.INSERT_INTO_URL_LIST.format(url=url)
        self.db.execute(query)
        row_id = self._get_last_insert_rowid()
        self.db.commit()
        return row_id

    def _get_last_insert_rowid(self) -> int:
        return self.db.execute("SELECT last_insert_rowid();").fetchall()[0][0]

    @Decorators.timing
    def insert_links_from_elements(self, elements: List[Element]) -> None:
        last_url_id = self._get_last_url_id() or 0
        list_of_values = ""
        for element in elements:
            if not element.href:
                continue
            list_of_values += f"('{element.href}'),"
            last_url_id += 1
            element.link_id = last_url_id
        list_of_values = list_of_values.strip(",")
        if not list_of_values:
            return
        self.db.execute(
            self.INSERT_INTO_URL_LIST_TURBO.format(list_of_values=list_of_values)
        )
        self.db.commit()

    @Decorators.timing
    def insert_words_from_elements(self, elements: List[Element]) -> None:
        last_word_id = self._get_last_word_id() or 0
        values_list = ""
        for element in elements:
            if not element.word:
                continue
            safe_word = element.word.replace("'", "")
            values_list += f"('{safe_word}', 0),"
            last_word_id += 1
            element.word_id = last_word_id
        values_list = values_list.strip(",")
        self.db.execute(
            self.INSERT_INTO_WORD_LIST_TURBO_FAST.format(list_of_values=values_list)
        )
        self.db.commit()

    @Decorators.timing
    def insert_links_between_by_elements(
        self, elements: List[Element], original_link_id: int
    ) -> None:
        values_list = ""
        for element in elements:
            if not element.href:
                continue
            values_list += f"({original_link_id}, {element.link_id}),"
        values_list = values_list.strip(",")
        if not values_list:
            return
        query = self.INSERT_INTO_LINKS_BETWEEN.format(list_of_values=values_list)
        self.db.execute(query)
        self.db.commit()

    @Decorators.timing
    def fill_words_locations_by_elements(self, elements: List[Element], url_id: int):
        values_list = ""
        for element in elements:
            values_list += f"({element.word_id}, {url_id}, {element.location}),"
        values_list = values_list.strip(",")
        query = self.INSERT_INTO_WORD_LOCATIONS.format(list_of_values=values_list)
        self.db.execute(query)
        self.db.commit()

    @Decorators.timing
    def fill_link_words_by_elements(self, elements: List[Element]):
        list_of_values = ""
        for element in elements:
            if element.word and element.href:
                list_of_values += f"({element.word_id}, {element.link_id}),"
        list_of_values = list_of_values.strip(",")
        if not list_of_values:
            return
        query = self.INSERT_INTO_LINK_WORD.format(list_of_values=list_of_values)
        self.db.execute(query)
        self.db.commit()

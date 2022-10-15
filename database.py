from typing import List
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from entites import Word, Link
from utils import Decorators

SQLALCHEMY_DATABASE_URL = "sqlite:///lab1.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
DbSession = sessionmaker(autoflush=False, bind=engine)
Base = declarative_base()

class DbSelecter:
    SELECT_URL_LIST = """
    SELECT * FROM url_list;
    """


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
    INSERT INTO url_list(url) VALUES('{url}')
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

    SELECT_LAST_WORD_ID = """
    SELECT MAX(wordId) FROM word_list
    """
    
    def __init__(self) -> None:
        self.db = DbSession()
    def close(self):
        self.db.close()

    def _get_last_word_id(self) -> int:
        result = self.db.execute(self.SELECT_LAST_WORD_ID)
        result = result.fetchone()[0]
        return result

    @Decorators.timing
    def insert_urls(self, links: List[Link]) -> None:
        for link in links:
            id_ = self.insert_url(link.link)
            link.id_ = id_

    @Decorators.timing
    def insert_words(self, words: List[Word]) -> None:
        last_word_id = self._get_last_word_id()
        values_list = ""
        for word in words:
            values_list += f"('{word.word}', 0),"
            last_word_id += 1
            word.id_ = last_word_id
        values_list = values_list.strip(",")
        self.db.execute(self.INSERT_INTO_WORD_LIST_TURBO_FAST.format(list_of_values=values_list))
        self.db.commit()
        logger.critical(words[-2])

    def insert_url(self, url: str) -> int:
        query = self.INSERT_INTO_URL_LIST.format(url=url)
        self.db.execute(query)
        row_id = self._get_last_insert_rowid()
        self.db.commit()
        return row_id
    
    @Decorators.timing
    def insert_word(self, word: str, is_filtered: int = 0) -> int:
        query = self.INSERT_INTO_WORD_LIST.format(word=word, is_filtered=is_filtered)
        self.db.execute(query)
        row_id = self._get_last_insert_rowid()
        self.db.commit()
        return row_id

    @Decorators.timing
    def fill_words_locations(self, words: List[Word], url_id: int):
        values_list = ""
        for word in words:
            values_list += f"({word.id_}, {url_id}, {word.location}),"
        values_list = values_list.strip(",")
        query = self.INSERT_INTO_WORD_LOCATIONS.format(list_of_values=values_list)
        self.db.execute(query)
        self.db.commit()

    @Decorators.timing
    def fill_links_between(self, links: List[Link], original_link_id: int):
        values_list = ""
        for link in links:
            values_list += f"({original_link_id}, {link.id_}),"
        values_list = values_list.strip(",")
        query = self.INSERT_INTO_LINKS_BETWEEN.format(list_of_values=values_list)
        self.db.execute(query)
        self.db.commit()

    def _get_last_insert_rowid(self) -> int:
        return self.db.execute('SELECT last_insert_rowid();').fetchall()[0][0]

    

    
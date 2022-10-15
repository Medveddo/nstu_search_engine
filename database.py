from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

SQLALCHEMY_DATABASE_URL = "sqlite:///lab1.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
DbSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)


Base = declarative_base()


class DbSelecter:
    SELECT_URL_LIST = """
    SELECT * FROM url_list;
    """


class DbCreator:
    CREATE_TABLE_WORD_LIST = """
    CREATE TABLE IF NOT EXISTS word_list (
        wordId INTEGER PRIMARY KEY,
        word TEXT,
        isFiltered INT
    )
    """
    CREATE_TABLE_URL_LIST = """
    CREATE TABLE IF NOT EXISTS url_list (
        urlId INTEGER PRIMARY KEY,
        url TEXT
    )
    """

    CREATE_TABLE_WORD_LOCATION = """
    CREATE TABLE IF NOT EXISTS word_location (
        id INTEGER PRIMARY KEY,
        fkWordId INT,
        fkUrlId INT,
        location INT
    )
    """
    CREATE_TABLE_LINK_BETWEEN_URL = """
    CREATE TABLE IF NOT EXISTS link_between_url (
        linkId INTEGER PRIMARY KEY,
        fkFromUrlId INT,
        fkToUrlId INT
    )
    """
    CREATE_TABLE_LINK_WORD = """
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


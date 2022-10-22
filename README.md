# Web Crawler + DB index

Implementation of web crawler that saves indexed data to relational database for future use.

## Usage

1. Edit START_URL_LIST (crawler.py#L16)

2. Then run main.py

```shell
python main.py
```

## SQL Queries

Get words count:

```sql
-- SQLite
SELECT word, COUNT(wordId) AS word_count
FROM word_list GROUP BY word ORDER BY word_count DESC;
```

Get unique words count:

```sql
-- SQLite
SELECT COUNT(unique_word) from
(SELECT DISTINCT word as unique_word FROM word_list)
```

Get joined links from to:

```sql
-- SQLite
SELECT u1.url as FROM_URL, u2.url AS TO_URL
FROM link_between_url 
INNER JOIN url_list u1 ON u1.urlId = fkFromUrlId
INNER JOIN url_list u2 ON u2.urlId = fkToUrlId
```

Get unique urls

```sql
SELECT urlId, url FROM url_list GROUP BY url ORDER BY urlId
```

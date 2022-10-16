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

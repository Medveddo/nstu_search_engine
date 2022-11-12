import sqlite3
from collections import defaultdict
conn = sqlite3.connect("lab1.db")

cur = conn.cursor()

result = cur.execute("SELECT url FROM url_list")
result = result.fetchall()

DOMAIN_COUNT = defaultdict(lambda: 0)

for url in result:
    url_: str = url[0]
    domain = url_.removeprefix("http://").removeprefix("https://").split("/")[0]
    DOMAIN_COUNT[domain] += 1

def get_max_value_key():
    v = list(DOMAIN_COUNT.values())
    k = list(DOMAIN_COUNT.keys())

    return k[v.index(max(v))]

RATING = []

for i in range(20):
    key = get_max_value_key()
    value = DOMAIN_COUNT.get(key)
    DOMAIN_COUNT.pop(key)
    RATING.append((key, value))

print(RATING)

for record in RATING:
    print(f"{record[0]};{record[1]}")
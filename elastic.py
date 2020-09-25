import sqlite3

import requests
from elasticsearch import Elasticsearch

# подключение к бд sqlite
conn = sqlite3.connect('C:/sqlite/task_ETL/db.sqlite')
res = requests.get('http://localhost:9200')
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# Создаем курсор - это специальный объект который делает запросы и получает их результаты
cursor = conn.cursor()
# Делаем SELECT запрос к базе данных, используя обычный SQL-синтаксис
movies = cursor.execute("""
    SELECT id, title
    FROM movies
    LIMIT 3
""")
movie_list = [
    {'id': movie[0],
     'title': movie[1]}
    for movie in movies
]
print(movie_list)

# Получаем результат сделанного запроса
# results = cursor.fetchall()
# print(results)
# Не забываем закрыть соединение с базой данных
conn.close()

# make sure ES is up and running
res = requests.get('http://localhost:9200')
print(res.content)
# connect to our cluster
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# index some test data
es.index(index='moviestest', doc_type='', body={'id': 'test2'})
# # delete test data and try with something more interesting
# # es.delete(index='test-index', doc_type='test', id=1)
#
# # es.index(index='sw', doc_type='people', id=1, body={
# #     "name": "Luke Skywalker",
# #     "height": "172",
# #     "mass": "77",
# #     "hair_color": "blond",
# #     "birth_year": "19BBY",
# #     "gender": "male",
# # })
# print(es.get(index='test-index', doc_type='test', id='IhVnvXQBbE_rId2hF9VL'))

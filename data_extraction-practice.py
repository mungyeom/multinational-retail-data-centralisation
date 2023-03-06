from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy import text
import sqlalchemy
from sklearn.datasets import load_iris



# Pyscopg2

HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'zbflxhsktmrkdkfn'
DATABASE = 'Pagila'
PORT = 5432

with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT) as conn:
    with conn.cursor() as cur:
        cur.execute('''CREATE TABLE actor_2 AS (
                    SELECT * FROM actor
                    LIMIT 10);

                    SELECT * FROM actor_2''')
        print(type(cur))
        records = cur.fetchall()
        print(records)

with psycopg2.connect(host='localhost', user='postgres', password='zbflxhsktmrkdkfn', dbname='Pagila', port=5432) as conn:
    with conn.cursor() as cur:
        cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
        for table in cur.fetchall():
            print(table)
 
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'zbflxhsktmrkdkfn'
DATABASE = 'Pagila'
PORT = 5432

engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
engine.connect()
engine.execute('SELECT * FROM actor').fetchall()


# with engine.connect() as conn:
#     with conn.execute(text('SELECT * FROM actor')) as actor:
#         for partition in actor.partitions():
#             for row in partition:
#                 print(f"{row}")


inspector = inspect(engine)
inspector.get_table_names()


# making use of the ORM
actors = pd.read_sql_table('actor', engine)
actors.head(10)

actors = pd.read_sql_query('''SELECT * FROM actor LIMIT 10''', engine).set_index('actor_id')
actors

data = load_iris()
data.keys()
iris = pd.DataFrame(data['data'], columns=data['feature_names'])
iris.head()

iris.to_sql('iris_dataset', engine, if_exists='replace')

class DataExtractor:

    def __init__(self) -> None:
        pass
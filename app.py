import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text


def process_data (engine):
   conn = engine.connect()
   sql_query = """
          SELECT MIN(age), MAX(age)
          FROM test_table
          WHERE LENGTH(name) < 6
      """


   cur = conn.execute(text(sql_query))

   result1 = cur.fetchone()

   if result1 is None or result1[0] is None or result1[1] is None:
       print("В таблице нет записей, где длина имени меньше 6 символов")
   else:
       print(f"Минимальный возраст для имен, длина которых меньше 6 символов: {result1[0]}")
       print(f"Максимальный возраст для имен, длина которых меньше 6 символов: {result1[1]}")


   data = pd.read_sql(text('SELECT * FROM test_table'), conn)

   means = data.mean()

   conn.close()

   return means

if __name__ == "__main__":
    db_user = 'postgres'
    db_password = 'password'
    db_host = 'db'
    db_port = '5432'
    db_name = 'test_db'

    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    result = process_data(engine)






import sqlite3
import os
import pandas as pd
from sqlalchemy import create_engine

def load_credentials_from_sqlite(db_path="creds.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT url, port, user, pass FROM access;")
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise Exception("Не найдены учетные данные в таблице access")
    url, port, user, password = row

    os.environ["DB_USER"] = user
    os.environ["DB_PASSWORD"] = password
    os.environ["DB_URL"] = url
    os.environ["DB_PORT"] = port
    os.environ["DB_ROOT_BASE"] = "homeworks"  # ваш database name

def get_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    url = os.getenv("DB_URL")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_ROOT_BASE")
    if not all([user, password, url, port, dbname]):
        raise Exception("Не все переменные среды заданы!")

    engine_url = f"postgresql+psycopg2://{user}:{password}@{url}:{port}/{dbname}"
    engine = create_engine(engine_url)
    return engine

if __name__ == "__main__":
    load_credentials_from_sqlite()
    engine = get_engine()
    table_name = "demidova"  # замените на вашу фамилию латиницей

    df = pd.read_sql_table(table_name, con=engine, schema="public")
    print(f"Содержимое таблицы public.{table_name}:")
    print(df)

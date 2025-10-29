"""
Модуль для загрузки данных в БД и файловые хранилища.
"""

import pandas as pd
import os
import sqlite3
from sqlalchemy import create_engine
from .validate import validate_loaded_data


def load_to_parquet(df: pd.DataFrame, output_dir: str = "data/processed",
                    filename: str = "processed_data.parquet") -> str:
    """
    Сохраняет данные в формате Parquet

    Args:
        df: DataFrame для сохранения
        output_dir: Директория для сохранения
        filename: Имя файла

    Returns:
        Путь к сохраненному файлу
    """
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    df.to_parquet(output_path, index=False, compression='snappy')
    print(f"✅ Данные сохранены в Parquet: {output_path}")

    return output_path


def load_to_database(df: pd.DataFrame, table_name: str = "demidova", max_rows: int = 100) -> None:
    """
    Загружает данные в PostgreSQL базу данных

    Args:
        df: DataFrame для загрузки
        table_name: Название таблицы
        max_rows: Максимальное количество строк для загрузки
    """
    print(f"\n2️⃣ Загрузка данных в PostgreSQL (таблица: {table_name})...")

    try:
        # Ограничиваем количество строк
        df_to_load = df.head(max_rows)
        print(f"   Отобрано для загрузки: {len(df_to_load)} строк (max: {max_rows})")

        # Загрузка credentials из SQLite
        conn = sqlite3.connect("creds.db")
        cursor = conn.cursor()
        cursor.execute("SELECT url, port, user, pass FROM access;")
        row = cursor.fetchone()
        conn.close()

        if not row:
            raise Exception("Не найдены учетные данные в creds.db")

        url, port, user, password = row

        # Создание engine для PostgreSQL
        engine_url = f"postgresql+psycopg2://{user}:{password}@{url}:{port}/homeworks"
        engine = create_engine(engine_url)

        # Загрузка в БД
        df_to_load.to_sql(
            name=table_name,
            con=engine,
            schema="public",
            if_exists="replace",
            index=False
        )

        print(f"✅ Данные загружены в БД: public.{table_name} ({len(df_to_load)} строк)")

    except FileNotFoundError:
        print("⚠️  Файл creds.db не найден. Пропуск загрузки в БД.")
    except Exception as e:
        print(f"⚠️  Ошибка при загрузке в БД: {e}")


def load_data(df: pd.DataFrame, table_name: str = "demidova", max_rows: int = 100,
              output_dir: str = "data/processed") -> None:
    """
    Загружает данные в БД и сохраняет в Parquet

    Args:
        df: DataFrame для загрузки
        table_name: Название таблицы в БД
        max_rows: Максимальное количество строк для БД
        output_dir: Директория для Parquet файла
    """
    print("\n" + "=" * 70)
    print("LOAD: Загрузка данных")
    print("=" * 70)

    try:
        # Валидация перед загрузкой
        print("\n1️⃣ Валидация данных перед загрузкой...")
        validate_loaded_data(df)
        print("✅ Валидация пройдена")

        # Сохранение в Parquet
        print("\n2️⃣ Сохранение в Parquet...")
        parquet_path = load_to_parquet(df, output_dir)

        # Загрузка в БД
        print("\n3️⃣ Загрузка в базу данных...")
        load_to_database(df, table_name, max_rows)

        print("\n" + "=" * 70)
        print("✅ LOAD ЗАВЕРШЕН УСПЕШНО")
        print("=" * 70)

    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        raise


if __name__ == "__main__":
    # Тестовый запуск
    df = pd.read_csv("data/raw/raw_data.csv")
    load_data(df)

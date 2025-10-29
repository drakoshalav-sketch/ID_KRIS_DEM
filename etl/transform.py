"""
Модуль для трансформации данных.
"""

import pandas as pd
import os
from .validate import validate_transformed_data


def transform_data(input_path: str, output_dir: str = "data/processed") -> pd.DataFrame:
    """
    Трансформирует данные: приводит типы, очищает, обрабатывает

    Args:
        input_path: Путь к сырым данным
        output_dir: Директория для промежуточных результатов

    Returns:
        Трансформированный DataFrame
    """
    print("\n" + "=" * 70)
    print("TRANSFORM: Трансформация данных")
    print("=" * 70)

    # Создаем директорию если её нет
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Загружаем сырые данные
        print(f"\n1️⃣ Загрузка сырых данных из {input_path}")
        df = pd.read_csv(input_path)
        print(f"✅ Загружено: {df.shape[0]} строк")

        # Приведение типов данных
        print("\n2️⃣ Приведение типов данных...")

        # Текстовые поля
        text_cols = [
            "country", "country_code", "job_board", "job_title", "job_type",
            "location", "organization", "page_url", "sector", "uniq_id"
        ]

        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype("string")

        # Категориальные поля
        category_cols = ["has_expired", "job_type", "sector"]
        for col in category_cols:
            if col in df.columns:
                df[col] = df[col].astype("category")

        # Числовые поля
        if "salary" in df.columns:
            df["salary"] = pd.to_numeric(df["salary"], errors="coerce")

        # Дата
        if "date_added" in df.columns:
            df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce")

        print("✅ Типы данных приведены")

        # Очистка данных (удаление полных дубликатов)
        print("\n3️⃣ Очистка данных...")
        initial_rows = len(df)
        df = df.drop_duplicates()
        removed_duplicates = initial_rows - len(df)
        print(f"✅ Удалено дубликатов: {removed_duplicates}")

        # Валидация трансформированных данных
        print("\n4️⃣ Валидация трансформированных данных...")
        validate_transformed_data(df)
        print("✅ Валидация пройдена")

        print(f"\n✅ Трансформация завершена: {df.shape[0]} строк готовы к загрузке")

        return df

    except Exception as e:
        print(f"❌ Ошибка при трансформации данных: {e}")
        raise


if __name__ == "__main__":
    # Тестовый запуск
    df = transform_data("data/raw/raw_data.csv")
    print(df.head())

"""
Модуль для извлечения данных из источников.
"""

import pandas as pd
import os
from .validate import validate_raw_data


def extract_data(file_id: str, output_dir: str = "data/raw") -> str:
    """
    Извлекает данные из Google Drive и сохраняет в data/raw

    Args:
        file_id: Google Drive FILE_ID
        output_dir: Директория для сохранения сырых данных

    Returns:
        Путь к сохраненному файлу
    """
    print("=" * 70)
    print("EXTRACT: Загрузка данных из источника")
    print("=" * 70)

    # Создаем директорию если её нет
    os.makedirs(output_dir, exist_ok=True)

    # URL для загрузки
    file_url = f"https://drive.google.com/uc?export=download&id={file_id}"

    try:
        # Загружаем данные
        print(f"\n1️⃣ Загрузка данных из Google Drive (FILE_ID: {file_id[:10]}...)")
        raw_data = pd.read_csv(
            file_url,
            sep=",",
            quotechar='"',
            skipinitialspace=True,
            encoding="utf-8",
            on_bad_lines="skip"
        )

        print(f"✅ Данные загружены: {raw_data.shape[0]} строк, {raw_data.shape[1]} столбцов")

        # Валидация сырых данных
        print("\n2️⃣ Валидация сырых данных...")
        validate_raw_data(raw_data)
        print("✅ Валидация пройдена")

        # Сохраняем в data/raw
        output_path = os.path.join(output_dir, "raw_data.csv")
        raw_data.to_csv(output_path, index=False)
        print(f"\n3️⃣ Сырые данные сохранены: {output_path}")

        return output_path

    except Exception as e:
        print(f"❌ Ошибка при извлечении данных: {e}")
        raise


if __name__ == "__main__":
    # Тестовый запуск
    FILE_ID = "17jS24dobHhStIKS0M1m9kdGf4qST3r35"
    extract_data(FILE_ID)

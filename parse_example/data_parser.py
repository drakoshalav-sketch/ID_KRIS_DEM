import pandas as pd
import os


def parse_and_show(filename, description=""):
    """
    Парсит и показывает данные из указанного CSV файла
    """
    # Проверяем возможные пути к файлу
    possible_paths = [
        filename,
        os.path.join("..", "api_example", filename),
        os.path.join("api_example", filename)
    ]

    path = None
    for p in possible_paths:
        if os.path.exists(p):
            path = p
            break

    if not path:
        print(f"\n❌ Файл {filename} не найден!")
        return None

    try:
        # Читаем файл
        df = pd.read_csv(path, encoding="utf-8")

        # Проверяем, что файл не пустой
        if df.empty:
            print(f"\n⚠️  Файл {filename} пустой!")
            return None

        # Выводим информацию
        print("\n" + "=" * 70)
        if description:
            print(f"📊 {description}")
        else:
            print(f"📊 Анализ файла: {filename}")
        print("=" * 70)

        print(f"Размер датасета: {df.shape[0]} строк, {df.shape[1]} столбцов")
        print(f"Заголовки: {df.columns.tolist()}")

        # Проверка на пропущенные значения
        missing = df.isnull().sum()
        if missing.any():
            print(f"\n⚠️  Пропущенные значения:")
            for col, count in missing[missing > 0].items():
                print(f"  - {col}: {count}")

        # Выводим первые строки
        print("\n📋 Первые 5 строк:")
        key_cols = [c for c in ["position", "title", "link", "snippet", "source"]
                    if c in df.columns]
        if key_cols:
            print(df[key_cols].head())
        else:
            print(df.head())

        return df

    except pd.errors.EmptyDataError:
        print(f"\n⚠️  Файл {filename} пустой или поврежден!")
        return None
    except Exception as e:
        print(f"\n❌ Ошибка при чтении файла {filename}: {e}")
        return None


def compare_results():
    """
    Сравнивает результаты из разных источников (API и веб-скрапинг)
    """
    print("\n" + "=" * 70)
    print("🔍 СРАВНЕНИЕ МЕТОДОВ ПОЛУЧЕНИЯ ДАННЫХ")
    print("=" * 70)

    # Парсим результаты API
    df_api = parse_and_show(
        "search_results_api.csv",
        "МЕТОД 1: API (searchapi.io)"
    )

    # Парсим результаты веб-скрапинга
    df_scraping = parse_and_show(
        "search_results_scraping.csv",
        "МЕТОД 2: WEB SCRAPING (Beautiful Soup)"
    )

    # Сравнительный анализ (только если оба файла загружены)
    if df_api is not None or df_scraping is not None:
        print("\n" + "=" * 70)
        print("📈 СРАВНИТЕЛЬНЫЙ АНАЛИЗ")
        print("=" * 70)

        if df_api is not None:
            print(f"✅ API результатов: {len(df_api)}")
        else:
            print("❌ API результаты не загружены")

        if df_scraping is not None:
            print(f"✅ Web Scraping результатов: {len(df_scraping)}")
        else:
            print("❌ Web Scraping результаты не загружены")

        # Сравнение по общим заголовкам (только если оба датасета загружены)
        if df_api is not None and df_scraping is not None:
            if 'title' in df_api.columns and 'title' in df_scraping.columns:
                common_titles_api = set(df_api['title'].str.lower())
                common_titles_scraping = set(df_scraping['title'].str.lower())
                overlap = common_titles_api & common_titles_scraping
                print(f"\n🔗 Совпадающих результатов: {len(overlap)}")

        print("\n✅ Анализ завершен!")
    else:
        print("\n❌ Не удалось загрузить ни один файл с результатами!")


def main():
    """
    Главная функция с выбором режима работы
    """
    print("Выберите режим работы:")
    print("1 - Анализ API результатов")
    print("2 - Анализ веб-скрапинг результатов")
    print("3 - Сравнение обоих методов (по умолчанию)")

    choice = input("\nВведите номер (по умолчанию 3): ").strip() or "3"

    if choice == "1":
        parse_and_show("search_results_api.csv", "API результаты")
    elif choice == "2":
        parse_and_show("search_results_scraping.csv", "Web Scraping результаты")
    else:
        compare_results()


if __name__ == "__main__":
    main()

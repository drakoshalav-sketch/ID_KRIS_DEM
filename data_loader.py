import pandas as pd

def parse_search_results(csv_file="../api_example/search_results.csv"):
    # Загрузка CSV с результатами поиска
    df = pd.read_csv(csv_file)

    print(f"Всего результатов: {len(df)}")
    print("Первые 10 строк:")
    print(df[["position", "title", "link", "snippet"]].head(10))

    # Фильтрация результатов с ключевым словом "chatgpt" в названии (игнорируем регистр)
    filtered = df[df["title"].str.contains("chatgpt", case=False, na=False)]

    print(f"\nОтфильтрованные результаты с 'chatgpt' в названии (всего {len(filtered)}):")
    print(filtered[["position", "title", "link"]])

if __name__ == "__main__":
    parse_search_results()

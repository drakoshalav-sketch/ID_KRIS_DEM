import pandas as pd
import os

def parse_and_show():
    # Задаём относительный путь к csv из api_example
    path = os.path.join("..", "api_example", "search_results.csv")
    df = pd.read_csv(path, encoding="utf-8")
    print("Размер датасета:", df.shape)
    print("Заголовки:", df.columns.tolist())
    print("\nПервые 5 строк:")
    # Для красоты выводим только ключевые столбцы, если они есть
    key_cols = [c for c in ["position", "title", "link", "snippet"] if c in df.columns]
    if key_cols:
        print(df[key_cols].head())
    else:
        print(df.head())

if __name__ == "__main__":
    parse_and_show()

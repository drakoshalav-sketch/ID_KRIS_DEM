import pandas as pd

def parse_ip_data(filename="ip_geolocation.csv"):
    df = pd.read_csv(filename)
    print("Исходные данные:")
    print(df.head())

    # Пример простой фильтрации
    columns = [col for col in df.columns if any(x in col.lower() for x in ["city", "country", "latitude", "longitude"])]
    parsed = df[columns]

    print("\nВыбранные ключевые поля:")
    print(parsed)

    parsed.to_csv("parsed_ip_data.csv", index=False)
    print("Файл parsed_ip_data.csv сохранён.")

if __name__ == "__main__":
    parse_ip_data()
import pandas as pd
import requests

def load_ip_data():
    url = "https://pointp.in/"
    headers = {"Content-Type": "application/json"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    print("Ответ от Pointpin API:")
    print(data)

    # Преобразуем в DataFrame
    df = pd.json_normalize(data)
    print("\nDataFrame:")
    print(df)

    # Сохраняем
    df.to_csv("ip_geolocation.csv", index=False)
    print("Файл ip_geolocation.csv сохранён.")

if name == "main":
    load_ip_data()
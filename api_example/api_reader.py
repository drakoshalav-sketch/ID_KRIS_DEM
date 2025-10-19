import requests
import pandas as pd

def load_search_results(api_key, query="chatgpt"):
    url = "https://www.searchapi.io/api/v1/search"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json"
    }
    params = {
        "engine": "google",
        "q": query
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()

    # Извлекаем органические результаты поиска
    organic_results = data.get("organic_results", [])

    # Преобразуем в DataFrame
    df = pd.json_normalize(organic_results)
    print(df[["position", "title", "link", "snippet"]].head(10))

    # Сохраняем данные в CSV
    df.to_csv("search_results.csv", index=False)
    print("Результаты поиска сохранены в search_results.csv")

if __name__ == "__main__":
    api_key = "SXjea9sw5jhShJ9rEJ55oyfy"  # ваш ключ
    load_search_results(api_key)

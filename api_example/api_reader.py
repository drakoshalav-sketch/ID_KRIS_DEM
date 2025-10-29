import requests
import pandas as pd
from bs4 import BeautifulSoup


def load_search_results_api(api_key, query="chatgpt"):
    """
    Загрузка результатов поиска через API searchapi.io
    """
    print(f"\n🔍 Поиск через API: '{query}'")
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

    # Добавляем источник данных
    df['source'] = 'API'

    print(f"\n📊 Найдено результатов: {len(df)}")
    print("\nПервые 10 результатов:")
    print(df[["position", "title", "link", "snippet"]].head(10))

    # Сохраняем данные в CSV
    df.to_csv("search_results_api.csv", index=False, encoding='utf-8')
    print("\n✅ Результаты API сохранены в search_results_api.csv")

    return df


def load_search_results_scraping(query="chatgpt", num_results=10):
    """
    Загрузка результатов поиска через веб-скрапинг с Beautiful Soup
    """
    print(f"\n🔍 Поиск через веб-скрапинг: '{query}'")

    # Используем DuckDuckGo HTML (не требует API)
    url = f"https://html.duckduckgo.com/html/?q={query}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Парсим результаты поиска
        results = []
        result_divs = soup.find_all('div', class_='result')[:num_results]

        for idx, div in enumerate(result_divs, 1):
            # Извлекаем заголовок и ссылку
            title_tag = div.find('a', class_='result__a')
            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            link = title_tag.get('href') if title_tag else "N/A"

            # Извлекаем описание
            snippet_tag = div.find('a', class_='result__snippet')
            snippet = snippet_tag.get_text(strip=True) if snippet_tag else "N/A"

            results.append({
                'position': idx,
                'title': title,
                'link': link,
                'snippet': snippet,
                'source': 'Web Scraping'
            })

        # Преобразуем в DataFrame
        df = pd.DataFrame(results)

        print(f"\n📊 Найдено результатов: {len(df)}")
        print("\nПервые 10 результатов:")
        if not df.empty:
            print(df[["position", "title", "snippet"]].head(10))

        # Сохраняем результаты
        df.to_csv("search_results_scraping.csv", index=False, encoding='utf-8')
        print("\n✅ Результаты веб-скрапинга сохранены в search_results_scraping.csv")

        return df

    except Exception as e:
        print(f"\n❌ Ошибка при веб-скрапинге: {e}")
        return pd.DataFrame()


def compare_methods(api_key, query="chatgpt"):
    """
    Сравнение двух методов получения данных: API vs Web Scraping
    """
    print("\n" + "=" * 70)
    print("📊 СРАВНЕНИЕ МЕТОДОВ ПОЛУЧЕНИЯ ДАННЫХ")
    print("=" * 70)

    # Метод 1: API
    print("\n" + "-" * 70)
    print("МЕТОД 1: API (searchapi.io)")
    print("-" * 70)
    df_api = load_search_results_api(api_key, query)

    # Метод 2: Web Scraping
    print("\n" + "-" * 70)
    print("МЕТОД 2: WEB SCRAPING (Beautiful Soup)")
    print("-" * 70)
    df_scraping = load_search_results_scraping(query)

    # Итоговое сравнение
    print("\n" + "=" * 70)
    print("📈 ИТОГОВОЕ СРАВНЕНИЕ")
    print("=" * 70)
    print(f"API результатов: {len(df_api)}")
    print(f"Web Scraping результатов: {len(df_scraping)}")
    print("\n✅ Оба метода успешно выполнены и сохранены в отдельные CSV файлы")


if __name__ == "__main__":
    api_key = "SXjea9sw5jhShJ9rEJ55oyfy"  # ваш ключ

    # Выбор режима работы
    print("Выберите режим работы:")
    print("1 - Только API")
    print("2 - Только веб-скрапинг")
    print("3 - Сравнение обоих методов")

    choice = input("\nВведите номер (по умолчанию 3): ").strip() or "3"
    query = input("Введите поисковый запрос (по умолчанию 'chatgpt'): ").strip() or "chatgpt"

    if choice == "1":
        load_search_results_api(api_key, query)
    elif choice == "2":
        load_search_results_scraping(query)
    else:
        compare_methods(api_key, query)

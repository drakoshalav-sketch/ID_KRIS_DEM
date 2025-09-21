import pandas as pd
import os

# ID вашего файла с датасетом "Титаник"
FILE_ID = "1Tj8RQHUh7MTmE88paqDBqDJZ5Ru6mOtL"

# URL для прямого доступа к файлу с Google Drive
file_url = f"https://drive.google.com/uc?export=download&id={FILE_ID}"

try:
    # Использование pandas для чтения CSV-файла напрямую по URL
    raw_data = pd.read_csv(file_url)

    # Вывод информации о загрузке
    print("Датасет успешно загружен.")
    print(f"Размер датасета: {raw_data.shape[0]} строк, {raw_data.shape[1]} столбцов.")

    # Выводим первые 10 строк для проверки
    print("\nПервые 10 строк датасета:")
    print(raw_data.head(10))

except Exception as e:
    print(f"Ошибка при загрузке файла: {e}")
    print("Пожалуйста, убедитесь, что ваш файл общедоступен по ссылке.")
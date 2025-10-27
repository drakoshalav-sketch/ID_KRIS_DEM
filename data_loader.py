import pandas as pd

# 1. Введите свой FILE_ID ниже
FILE_ID = "17jS24dobHhStIKS0M1m9kdGf4qST3r35"
file_url = f"https://drive.google.com/uc?export=download&id={FILE_ID}"

try:
    # 2. Считываем CSV-файл (разделитель — запятая)
    raw_data = pd.read_csv(
        file_url,
        sep=",",
        quotechar='"',
        skipinitialspace=True,
        encoding="utf-8",
        on_bad_lines="skip"
    )

    print("Датасет успешно загружен.")
    print(f"Размер датасета: {raw_data.shape[0]} строк, {raw_data.shape[1]} столбцов.")
    print("\nПервые 10 строк датасета:")
    print(raw_data.head(10))

    # 3. Приведение типов под структуру вакансий
    df = raw_data.copy()

    # Текстовые или категориальные поля
    text_cols = [
        "country", "country_code", "job_board", "job_title", "job_type", "location",
        "organization", "page_url", "sector", "uniq_id"
    ]
    category_cols = ["has_expired", "job_type", "sector"]

    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    for col in category_cols:
        if col in df.columns:
            df[col] = df[col].astype("category")

    # Числовые и идентификаторы (кроме uniq_id!):
    for col in df.columns:
        if col.lower() in ["salary"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Дата
    if "date_added" in df.columns:
        df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce")

    print("\nТипы после приведения:")
    print(df.dtypes)

    # 4. Сохраняем в feather и сжатый CSV
    df.reset_index(drop=True).to_feather("dataset_converted.feather")
    df.to_csv("dataset_converted.csv.gz", index=False, compression="gzip")
    print("\nФайлы сохранены:")
    print(" - dataset_converted.feather")
    print(" - dataset_converted.csv.gz")

except Exception as e:
    print(f"Ошибка при загрузке файла: {e}")
    print("Проверьте ссылку, FILE_ID и настройки доступа на Google Диске.")

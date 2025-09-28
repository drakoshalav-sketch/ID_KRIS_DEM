import pandas as pd

FILE_ID = "1Tj8RQHUh7MTmE88paqDBqDJZ5Ru6mOtL"
file_url = f"https://drive.google.com/uc?export=download&id={FILE_ID}"

try:
    # Загружаем Titanic-csv (учёт кавычек и пробелов)
    raw_data = pd.read_csv(
        file_url,
        sep=",",
        quotechar='"',
        skipinitialspace=True,
        encoding="utf-8"
    )

    print("Датасет успешно загружен.")
    print(f"Размер датасета: {raw_data.shape[0]} строк, {raw_data.shape[1]} столбцов.")

    print("\nПервые 10 строк датасета:")
    print(raw_data.head(10))

    # === Приведение типов ===
    df = raw_data.copy()

    # Числовые с Int32 (nullable)
    for col in ["PassengerId", "Survived", "Pclass", "SibSp", "Parch"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")

    # Вещественные
    for col in ["Age", "Fare"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float32")

    # Текст/категории
    df["Name"] = df["Name"].astype("string")
    for col in ["Sex", "Ticket", "Cabin", "Embarked"]:
        df[col] = df[col].astype("string").astype("category")

    print("\nТипы после приведения:")
    print(df.dtypes)

    # === Сохранение ===
    df.reset_index(drop=True).to_feather("train_converted.feather")
    df.to_csv("train_converted.csv.gz", index=False, compression="gzip")

    print("\nФайлы сохранены:")
    print(" - train_converted.feather")
    print(" - train_converted.csv.gz")

except Exception as e:
    print(f"Ошибка при загрузке файла: {e}")
    print("Пожалуйста, убедитесь, что ваш файл общедоступен по ссылке.")

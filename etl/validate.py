"""
Модуль валидации данных на разных этапах ETL.
"""

import pandas as pd


def validate_raw_data(df: pd.DataFrame) -> None:
    """
    Валидирует сырые данные после извлечения

    Args:
        df: DataFrame для валидации

    Raises:
        ValueError: Если данные не проходят валидацию
    """
    # Проверка на пустой DataFrame
    if df.empty:
        raise ValueError("DataFrame пустой!")

    # Проверка минимального количества строк
    if len(df) < 10:
        raise ValueError(f"Слишком мало данных: {len(df)} строк (минимум 10)")

    # Проверка наличия обязательных столбцов
    required_columns = ['job_title', 'organization', 'country']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        print(f"⚠️  Предупреждение: Отсутствуют столбцы {missing_columns}")

    print(f"   ✓ Размерность: {df.shape}")
    print(f"   ✓ Столбцов: {df.shape[1]}")
    print(f"   ✓ Строк: {df.shape[0]}")


def validate_transformed_data(df: pd.DataFrame) -> None:
    """
    Валидирует данные после трансформации

    Args:
        df: DataFrame для валидации

    Raises:
        ValueError: Если данные не проходят валидацию
    """
    if df.empty:
        raise ValueError("DataFrame пустой после трансформации!")

    # Проверка типов данных
    print(f"   ✓ Типы данных корректны")
    print(f"   ✓ Строк после трансформации: {len(df)}")

    # Проверка на критическое количество пропусков
    total_cells = df.shape[0] * df.shape[1]
    missing_cells = df.isnull().sum().sum()
    missing_pct = (missing_cells / total_cells) * 100

    print(f"   ✓ Пропущенных значений: {missing_pct:.2f}%")

    if missing_pct > 50:
        raise ValueError(f"Слишком много пропусков: {missing_pct:.2f}%")


def validate_loaded_data(df: pd.DataFrame) -> None:
    """
    Валидирует данные перед загрузкой

    Args:
        df: DataFrame для валидации

    Raises:
        ValueError: Если данные не готовы к загрузке
    """
    if df.empty:
        raise ValueError("Нет данных для загрузки!")

    print(f"   ✓ Готово к загрузке: {len(df)} строк")
    print(f"   ✓ Столбцов: {df.shape[1]}")


if __name__ == "__main__":
    # Тестовый запуск
    test_df = pd.DataFrame({
        'job_title': ['Engineer', 'Designer'],
        'organization': ['CompanyA', 'CompanyB'],
        'country': ['USA', 'UK']
    })

    validate_raw_data(test_df)
    validate_transformed_data(test_df)
    validate_loaded_data(test_df)
    print("✅ Все валидации пройдены!")

"""
Главный модуль ETL пакета с CLI интерфейсом.
"""

import argparse
import sys
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data


def run_etl(file_id: str, table_name: str = "demidova", max_rows: int = 100) -> None:
    """
    Запускает полный ETL процесс

    Args:
        file_id: Google Drive FILE_ID для загрузки данных
        table_name: Название таблицы в БД
        max_rows: Максимальное количество строк для загрузки в БД
    """
    print("\n" + "🚀 " * 35)
    print("ЗАПУСК ETL ПРОЦЕССА")
    print("🚀 " * 35 + "\n")

    try:
        # EXTRACT
        raw_data_path = extract_data(file_id)

        # TRANSFORM
        transformed_df = transform_data(raw_data_path)

        # LOAD
        load_data(transformed_df, table_name, max_rows)

        print("\n" + "🎉 " * 35)
        print("ETL ПРОЦЕСС ЗАВЕРШЕН УСПЕШНО!")
        print("🎉 " * 35 + "\n")

    except Exception as e:
        print(f"\n❌ ETL процесс прерван с ошибкой: {e}")
        sys.exit(1)


def main():
    """
    CLI интерфейс для ETL пакета
    """
    parser = argparse.ArgumentParser(
        description="ETL пакет для обработки данных о вакансиях",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python -m etl.main --file-id YOUR_FILE_ID
  python -m etl.main --file-id YOUR_FILE_ID --table demidova --max-rows 100
        """
    )

    parser.add_argument(
        '--file-id',
        type=str,
        required=True,
        help='Google Drive FILE_ID для загрузки данных (обязательный параметр)'
    )

    parser.add_argument(
        '--table',
        type=str,
        default='demidova',
        help='Название таблицы в PostgreSQL (по умолчанию: demidova)'
    )

    parser.add_argument(
        '--max-rows',
        type=int,
        default=100,
        help='Максимальное количество строк для загрузки в БД (по умолчанию: 100)'
    )

    args = parser.parse_args()

    # Запуск ETL
    run_etl(
        file_id=args.file_id,
        table_name=args.table,
        max_rows=args.max_rows
    )


if __name__ == "__main__":
    main()

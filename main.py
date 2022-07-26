"""
   Скрипт запускает метод main()
   один раз в минуту делает запрос к Google Sheets документу
   PostgreSQL полученный результат заносит в БД путём поной замены таблицы
   MySQL обновляет данные, ! id заказа должен быть уникальным
   """

import csv
import settings
import threading
import os
import connectiondb
import connect_to_google


chose = str()


def insert_to_mysql(cur, sql, args, connection):
    try:
        cur.execute(sql, args)
    except Exception:
        connection.rollback()


def get_item(connection):
    with open("testRes.csv", "r", encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        conn = connectiondb.connect_to_mysql()
        cursor = conn.cursor()
        for item in reader:
            args = tuple(item)
            insert_to_mysql(cursor, sql=settings.sql_insert, args=args, connection=connection)

    conn.commit()
    cursor.close()
    conn.close()


def create_table(connection):
    # создаёт таблицу в базе данных, если таблица есть то она будет удалена и создана новая
    with connection.cursor() as cursor:
        cursor.execute(settings.create_table_mysql)


def insert_table(connection):
    #  вставляет данные в таблицу из фала .csv
    sql = "copy public.testtask from STDIN delimiter ',' csv encoding 'UTF8'"
    with connection.cursor() as cursor:
        with open("testRes.csv", "r", encoding='utf-8') as f:
            next(f)
            cursor.copy_expert(sql, f)


def main():

    try:

        while(True):
            try:
                chose = input("Выберете способ подключения к БД: \n# 1 - MySQL #   \n# 2 - PostgreSQL\n")

                if ('1' in chose) or ('2' in chose):
                    break
                else:
                    print("Выберете 1 или 2")
            except KeyboardInterrupt:
                print("[ИНФО] ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ")

        print("[ИНФО] Получаю данные из рабочего листа")
        connect_to_google.connect_to_google_sheets()

        while(True):
            task0 = threading.Timer(60.0, connect_to_google.connect_to_google_sheets)
            task0.start()

            if chose == '1':
                connection = connectiondb.connect_to_mysql()
                connection.autocommit = True
            if chose == '2':
                connection = connectiondb.connect_to_postgree()
                connection.autocommit = True

            create_table(connection)
            print("[INFO] Создали таблицу")
            # insert_table(connection)
            get_item(connection)
            print("[INFO] Обновили данные")
            task0.join()

    except Exception as e:
        if os.remove("testRes.csv"):
            print(f"[ERROR] Ошибка в функции main() testRes.csv удалён *** {e}")
        print(f"[ERROR] Ошибка в функции main() *** {e}")

    finally:
        if KeyboardInterrupt:
            print("[ИНФО] ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ")
        # if connection:
        #     connection.close()
            print("[INFO] Finally method")


if __name__ == '__main__':
    main()

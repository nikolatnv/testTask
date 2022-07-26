from psycopg2 import Error, connect
from mysql.connector import connect, Error
import settings


def connect_to_postgree():
    try:
        connection = connect(
            dbname=settings.dbname,
            user=settings.user,
            password=settings.password,
            host=settings.host)

        return connection

    except Error as e:
        print(f"[ERROR] Не удалось установить соединение с БД {e}")
        print("[ERROR] ошибка в функции *** connect_to_postgre")


def connect_to_mysql():
    try:
        connection = connect(
                host=settings.host_mysql,
                user=settings.username_mysql,
                password=settings.psw)
        return connection

    except Error as e:
        print(f"[ERROR] Не удалось установить соединение с БД {e}")
        print("[ERROR] ошибка в методе *** connect_to_mysql")

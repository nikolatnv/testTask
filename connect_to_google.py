import gspread
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import settings
import vision

def get_course():
    # актуальный курс валюты по данным ЦБ с сайта ЦБ
    url = urlopen('https://cbr.ru/')
    bs = BeautifulSoup(url, 'lxml')
    dollar = bs.find('div', text='USD').find_parent().find_all('div')[2].text
    return dollar


def connect_to_google_sheets():
    try:
        # подкючение к Google Sheets API при помощи ключа в .json
        connect_to_sheets = gspread.service_account(filename=settings.filename_serv_akk)
        open_sheets = connect_to_sheets.open("testTask")
        wks = open_sheets.worksheet("sheet1")
        all_val = wks.get_all_values()
        col = all_val.pop(0)

        # преобразование данных из таблицы в pandas DataFrame
        data = pd.DataFrame(all_val, columns=col)

        # парсинг цены в долларах
        price_in_dollar = list(data.iloc[:]['стоимость,$'])

        # парсинг цены в рублях
        rub_actual_price = str(get_course())
        s = rub_actual_price.split(' ₽')[0].replace(',', '.')
        rub_per_dollar = float(s)

        list_rub = []
        for count in price_in_dollar:
            list_rub.append(int(count) * rub_per_dollar)

        # вставка столбца в DataFrame после столбца стоимость в долларах
        data.insert(3, 'стоимость в руб.', list_rub, False)

        # запись файла в .csv
        data.to_csv('testRes.csv', index=False)
        chose = input("Вывести диграмму? y/n...\n")

        while(True):
            if 'y' in chose:
                vision.vision_price()
                break
            elif 'n' in chose:
                print("Жаль, я старался.")
                break
            else:
                print("Сделайте выбор.")

    except Exception as e:
        print(f"[ERROR] Ошибка в функции connect_to_google_sheets() *** {e}")

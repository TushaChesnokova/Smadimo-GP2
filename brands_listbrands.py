import requests
import pandas as pd
import time
import json
import logging
from bs4 import BeautifulSoup
import re
from requests.exceptions import RequestException

logging.basicConfig(
    filename='sephora_parse1.log',
    level=logging.INFO,
    format='api_parsing | %(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)
logging.info("Тестовая запись в лог")
TOKENS = [
    "8bc3bea522msh941d5f19c472aa2p1d9362jsna422162a847e",
    "71df97361cmshfa8e56c9d1d52c9p16f4cbjsnd8371aa26010",
    "06cc033d84mshb5f1fe6ebc4cc23p1e5e17jsnfba965f47afe",
    "b1f98658a4mshceede2464f383d5p1db43ajsn01003e021291",
    "ee71f8cf5amsh9606a89731714bcp16e318jsnf25c70e56aae",
    "0ccc6b07f9msh9d97bb824c2932ep1c8e14jsn405f51cc35f1"
]
CURRENT_TOKEN_INDEX = 0


def get_headers():
    """Формируем заголовки запроса с текущим токеном."""
    return {
        "x-rapidapi-key": TOKENS[CURRENT_TOKEN_INDEX],
        "x-rapidapi-host": "sephora14.p.rapidapi.com"
    }


def get_brands():
    """
    Получаем список брендов.
    Если получаем 429 — переключаем токен и повторяем.
    Если всё исчерпано — возвращаем None.
    """
    global CURRENT_TOKEN_INDEX
    url = "https://sephora14.p.rapidapi.com/brands"

    while CURRENT_TOKEN_INDEX < len(TOKENS):
        try:
            response = requests.get(url, headers=get_headers())
            if response.status_code == 200:
                logging.info("Успешно получен список брендов.")
                return response.json()

            elif response.status_code == 429:
                logging.warning(
                    f"Получен 429 при получении брендов. Токен {TOKENS[CURRENT_TOKEN_INDEX]} исчерпан. Переключаемся...")
                CURRENT_TOKEN_INDEX += 1
                if CURRENT_TOKEN_INDEX >= len(TOKENS):
                    logging.error("Все токены исчерпаны, бренды получить невозможно.")
                    return None
                continue

            else:
                logging.error(f"Неожиданный статус ответа: {response.status_code} при получении брендов.")
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при запросе списка брендов: {str(e)}")
            return None
    logging.error("Все токены исчерпаны — список брендов не получен.")
    return None


def process_products(products_data):
    """Преобразуем список продуктов в DataFrame с нужными полями."""
    if not products_data:
        return None
    df = pd.DataFrame(products_data)
    if 'currentSku' in df.columns:
        df['price'] = df['currentSku'].apply(lambda x: x.get('listPrice') if isinstance(x, dict) else None)
        df['isLimitedEdition'] = df['currentSku'].apply(lambda x: x.get('isLimitedEdition') if x else None)
        df['isLimitedTimeOffer'] = df['currentSku'].apply(lambda x: x.get('isLimitedTimeOffer') if x else None)
        df['skuType'] = df['currentSku'].apply(lambda x: x.get('skuType') if x else None)
        df['isAppExclusive'] = df['currentSku'].apply(lambda x: x.get('isAppExclusive') if x else None)
        df['isBI'] = df['currentSku'].apply(lambda x: x.get('isBI') if x else None)
        df['isBest'] = df['currentSku'].apply(lambda x: x.get('isBest') if x else None)
        df['isNatural'] = df['currentSku'].apply(lambda x: x.get('isNatural') if x else None)
        df['isNew'] = df['currentSku'].apply(lambda x: x.get('isNew') if x else None)
        df['isOnlineOnly'] = df['currentSku'].apply(lambda x: x.get('isOnlineOnly') if x else None)
        df['biExclusiveLevel'] = df['currentSku'].apply(lambda x: x.get('biExclusiveLevel') if x else None)
    return df


def get_brand_products(brand_id, page=1):
    """
    Запрос к /searchByBrand по конкретному brand_id и странице page.
    Если получаем 429 — переключаем токен и повторяем для той же позиции.
    Возвращаем кортеж (список_продуктов, total_pages).
    Если не вышло — (None, 0).
    """
    global CURRENT_TOKEN_INDEX
    url = "https://sephora14.p.rapidapi.com/searchByBrand"
    querystring = {
        "brandID": brand_id,
        "page": str(page),
        "sortBy": "NEW"
    }
    while CURRENT_TOKEN_INDEX < len(TOKENS):
        try:
            response = requests.get(url, headers=get_headers(), params=querystring)

            if response.status_code == 200:
                data = response.json()
                products = data.get('products', [])
                total_pages = data.get('totalPages', 1)
                return products, total_pages

            elif response.status_code == 429:
                logging.warning(f"429 при запросе бренда {brand_id}, стр. {page}. "
                                f"Токен {TOKENS[CURRENT_TOKEN_INDEX]} исчерпан. Переключаемся...")
                CURRENT_TOKEN_INDEX += 1
                if CURRENT_TOKEN_INDEX >= len(TOKENS):
                    logging.error("Все токены исчерпаны, дальнейшая работа невозможна.")
                    return None, 0
                continue
            elif response.status_code == 500:
                logging.error(f"Серверная ошибка 500 для бренда {brand_id}, стр. {page}. Пропускаем бренд.")
                return None, 0
            else:
                logging.error(f"Неожиданный статус {response.status_code} для бренда {brand_id}, стр. {page}.")
                return None, 0
        except requests.exceptions.RequestException as e:
            logging.error(f"Исключение при запросе бренда {brand_id}, стр. {page}: {str(e)}")
            return None, 0
    logging.error("Все токены исчерпаны, запрос невозможен.")
    return None, 0


def main():
    logging.info("Запуск скрипта Sephora.")
    print('ok1')
    brands = get_brands()
    if not brands:
        logging.error("Список брендов не получен — завершаем работу.")
        return
    print('ok2')
    df_global = pd.DataFrame()
    for brand_id in brands:
        logging.info(f"Обработка бренда: {brand_id}")
        page = 1
        products, total_pages = get_brand_products(brand_id, page)
        if products is None:
            logging.warning(f"Пропускаем бренд {brand_id}, нет данных или ошибка.")
            continue
        temp_df = process_products(products)
        if temp_df is not None:
            temp_df['brand_id'] = brand_id
            df_global = pd.concat([df_global, temp_df], ignore_index=True)
        for page_num in range(2, total_pages + 1):
            products, _ = get_brand_products(brand_id, page_num)
            if products:
                temp_df = process_products(products)
                if temp_df is not None:
                    temp_df['brand_id'] = brand_id
                    df_global = pd.concat([df_global, temp_df], ignore_index=True)
            time.sleep(1)
        if not df_global.empty:
            df_global.to_csv('sephora_products_intermediate.csv', index=False)
            logging.info(f"Промежуточно сохранено {len(df_global)} строк.")
            print('ok')
        time.sleep(2)
    if not df_global.empty:
        df_global.to_csv('sephora_products_final.csv', index=False)
        logging.info(f"Всего собрано {len(df_global)} продуктов. Итоговый файл: sephora_products_final.csv")
        print('ok4')
    else:
        logging.info("Продукты не были собраны, файл не создан.")
        print('not ok')
    logging.info("Скрипт завершён.")


if __name__ == "__main__":
    main()

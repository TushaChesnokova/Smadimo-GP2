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
    "f49cb80abfmsh00c0eb9dc816e8fp13a55ejsn0c95ca6a2dce",
    "9dd1b3dc55msh0f1e6520b7edd8ep1f73a0jsn8a1d913f81bf"
]
CURRENT_TOKEN_INDEX = 0


def get_headers():
    """Формируем заголовки запроса с текущим токеном."""
    return {
        "x-rapidapi-key": TOKENS[CURRENT_TOKEN_INDEX],
        "x-rapidapi-host": "sephora14.p.rapidapi.com"
    }


def get_product_details(product_id):
    """
    Запрашиваем детали продукта по эндпоинту /product.
    Нужно передать ?productID=<...>.
    Возвращаем JSON с детальной инфой или None.
    """
    global CURRENT_TOKEN_INDEX
    url = "https://sephora14.p.rapidapi.com/product"
    querystring = {"productID": str(product_id)}

    while CURRENT_TOKEN_INDEX < len(TOKENS):
        try:
            response = requests.get(url, headers=get_headers(), params=querystring)
            if response.status_code == 200:
                data = response.json()
                logging.info(f"Получены детали продукта {product_id}.")
                return data

            elif response.status_code == 429:
                logging.warning(f"429 при запросе деталей продукта {product_id}. Переключаемся на следующий токен...")
                CURRENT_TOKEN_INDEX += 1
                if CURRENT_TOKEN_INDEX >= len(TOKENS):
                    logging.error("Все токены исчерпаны, детали продукта получить невозможно.")
                    return None
                continue

            else:
                logging.error(f"Неожиданный статус {response.status_code} при запросе деталей продукта {product_id}.")
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при запросе деталей продукта {product_id}: {str(e)}")
            return None

    return None


def main():
    product_id = "P505624"
    details = get_product_details(product_id)
    if details is None:
        print(f"Не удалось получить детали продукта {product_id} (ошибка или пустой ответ).")
    else:
        print(f"Детали продукта {product_id}:")
        print("Название:", details.get('displayName'))
        print("Полный ответ:", details)


if __name__ == "__main__":
    main()

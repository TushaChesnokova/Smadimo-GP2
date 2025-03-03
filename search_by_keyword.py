import requests
import pandas as pd
import time
import json
import logging
from bs4 import BeautifulSoup
import re
from requests.exceptions import RequestException

logging.basicConfig(
    filename='sephora_parse2.log',
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


def search_by_keyword(keyword, page=1):
    """
    Поиск продуктов по ключевому слову через /searchByKeyword.

    Если получаем 429 — переключаем токен и повторяем запрос.
    Если все токены исчерпаны — возвращаем None.

    Возвращает полный JSON-ответ (словарь), или None в случае ошибки.
    """
    global CURRENT_TOKEN_INDEX
    url = "https://sephora14.p.rapidapi.com/searchByKeyword"
    query_params = {
        "search": keyword,
        "page": str(page),
        "sortBy": "NEW"
    }
    while CURRENT_TOKEN_INDEX < len(TOKENS):
        try:
            response = requests.get(url, headers=get_headers(), params=query_params)
            if response.status_code == 200:
                data = response.json()
                logging.info(f"Успешный поиск по ключу '{keyword}', страница {page}.")
                return data
            elif response.status_code == 429:
                logging.warning(
                    f"Получен 429 при поиске по ключу '{keyword}'. "
                    f"Токен {TOKENS[CURRENT_TOKEN_INDEX]} исчерпан. Переключаемся..."
                )
                CURRENT_TOKEN_INDEX += 1
                if CURRENT_TOKEN_INDEX >= len(TOKENS):
                    logging.error("Все токены исчерпаны. Поиск по ключу невозможен.")
                    return None
                continue
            else:
                logging.error(f"Неожиданный статус {response.status_code} при поиске по ключу '{keyword}'.")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при поиске по ключу '{keyword}': {str(e)}")
            return None
    logging.error(f"Все токены исчерпаны — не удалось выполнить поиск по ключу '{keyword}'.")
    return None

import requests
import pandas as pd
import time
import json
import logging
from bs4 import BeautifulSoup
import re
from requests.exceptions import RequestException
import logging

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


def get_categories():
    """
    Получаем список категорий с эндпоинта /categories.
    Если 429 — переключаем токен и повторяем запрос.
    Возвращаем JSON (список категорий) или None.
    """
    global CURRENT_TOKEN_INDEX
    url = "https://sephora14.p.rapidapi.com/categories"

    while CURRENT_TOKEN_INDEX < len(TOKENS):
        try:
            response = requests.get(url, headers=get_headers())
            if response.status_code == 200:
                data = response.json()
                logging.info("Успешно получен список категорий.")
                return data  # Это может быть список или словарь в зависимости от API
            elif response.status_code == 429:
                logging.warning(
                    f"429 при получении категорий. Токен {TOKENS[CURRENT_TOKEN_INDEX]} исчерпан. Переключаемся...")
                CURRENT_TOKEN_INDEX += 1
                if CURRENT_TOKEN_INDEX >= len(TOKENS):
                    logging.error("Все токены исчерпаны, категории получить невозможно.")
                    return None
                # Пробуем заново с новым токеном
                continue
            else:
                logging.error(f"Неожиданный статус {response.status_code} при запросе /categories.")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при запросе /categories: {str(e)}")
            return None
    return None


def save_categories_to_csv():
    categories_data = get_categories()
    if not categories_data:
        logging.warning("Не удалось получить данные по категориям (None или пустой результат).")
        return
    if isinstance(categories_data, list):
        df_categories = pd.DataFrame(categories_data)
    elif isinstance(categories_data, dict) and 'categories' in categories_data:
        df_categories = pd.DataFrame(categories_data['categories'])
    else:
        logging.warning("Непонятный формат ответа, попробуем привести к DataFrame напрямую.")
        df_categories = pd.DataFrame(categories_data)
    df_categories.to_csv('categories.csv', index=False)
    logging.info(f"Сохранено {len(df_categories)} записей в файл categories.csv.")


def main():
    logging.info("Запуск сохранения категорий...")
    save_categories_to_csv()
    logging.info("Готово!")

if __name__ == "__main__":
    main()

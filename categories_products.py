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


def get_category_products(categories_id, page=1):
    """
    Запрос к /searchByCategory по конкретному categoryID и странице page.
    Если получаем 429 — переключаем токен и повторяем для той же позиции.
    Возвращаем кортеж (список_продуктов, total_pages).
    Если не вышло — (None, 0).
    """
    global CURRENT_TOKEN_INDEX
    url = "https://sephora14.p.rapidapi.com/searchByCategory"
    querystring = {
        "categoriesID": categories_id,
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
                logging.warning(f"429 при запросе категории {categories_id}, стр. {page}. "
                                f"Токен {TOKENS[CURRENT_TOKEN_INDEX]} исчерпан. Переключаемся...")
                CURRENT_TOKEN_INDEX += 1
                if CURRENT_TOKEN_INDEX >= len(TOKENS):
                    logging.error("Все токены исчерпаны, дальнейшая работа невозможна.")
                    return None, 0
                continue
            elif response.status_code == 500:
                logging.error(f"Серверная ошибка 500 для категории {categories_id}, стр. {page}. Пропускаем категорию.")
                return None, 0
            else:
                logging.error(f"Неожиданный статус {response.status_code} для категории {categories_id}, стр. {page}.")
                return None, 0
        except requests.exceptions.RequestException as e:
            logging.error(f"Исключение при запросе категории {categories_id}, стр. {page}: {str(e)}")
            return None, 0
    logging.error("Все токены исчерпаны, запрос невозможен.")
    return None, 0


def process_all_categories(categories):
    """
    Перебор всех категорий и вызов get_category_products для каждой.
    Возвращает список всех продуктов.
    """
    all_products = []
    for category_label, category_id in categories:
        logging.info(f"Обрабатываем категорию: {category_label} (ID: {category_id})")
        products, total_pages = get_category_products(category_id)
        if products:
            all_products.extend(products)
            logging.info(f"Успешно получено {len(products)} продуктов для категории {category_label}.")
        else:
            logging.warning(f"Не удалось получить продукты для категории {category_label}.")
    return all_products


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    categories_df = pd.read_csv('categories.csv')
    categories_list = list(categories_df[['categoryLabel', 'categoryID']].to_records(index=False))
    all_products = process_all_categories(categories_list)
    logging.info(f"Всего получено {len(all_products)} продуктов.")

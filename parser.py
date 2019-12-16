import os
import json
import logging
import pandas as pd
from time import perf_counter
from datetime import date as dt
from .utils import detect_language

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()


def collect_category_reviews(reviews_dir, category):
    start_time = perf_counter()
    products = os.listdir(reviews_dir)
    texts, pluses, minuses, ratings, languages = [], [], [], [], []

    LOGGER.info(f'Number of products in {category}: {len(products)}')

    total_reviews = 0
    for product in products:

        reviews_path = f"{reviews_dir}/{product}"
        with open(reviews_path) as f:
            reviews = json.load(f)['data']
            total_reviews += len(reviews)
            for review in reviews:
                text, plus, minus = review['comment']['text'], review['comment']['plus'], review['comment']['minus']
                texts.append(text)
                pluses.append(plus)
                minuses.append(minus)

                language = detect_language(text or plus or minus)
                languages.append(language)

                ratings.append(review['rating'])

    LOGGER.info(f'Number of reviews in {category}: {total_reviews}')
    LOGGER.info(f'Time to collect: {perf_counter() - start_time}')

    data = {
        "text": texts,
        "plus": pluses,
        "minus": minuses,
        "language": languages,
        "rating": ratings,
    }

    df = pd.DataFrame(data=data)
    df['category'] = category
    return df


def collect_categories(base_dir):
    date = str(dt.today())
    categories = [category for category in os.listdir(base_dir) if not category.startswith('.')]
    for category in categories:
        LOGGER.info(f'Started proccessing {category}')

        start = perf_counter()
        df = collect_category_reviews(f"{base_dir}/{category}", category)

        LOGGER.info(f'Finished processing {category} in {perf_counter() - start}')

        df.to_csv(f'data/{category}_reviews_{date}.csv')


def compile_dataframes():
    files = os.listdir('data/2019-12-14')
    file = files.pop(0)
    reviews = pd.read_csv(f'data/2019-12-14/{file}',
                          usecols=['text', 'plus', 'minus', 'language', 'rating', 'category'])
    for file in files:
        df = pd.read_csv(f'data/2019-12-14/{file}', usecols=['text', 'plus', 'minus', 'language', 'rating', 'category'])
        reviews = reviews.append(df)

    reviews.to_csv('data/2019-12-14/all.csv')


if __name__ == '__main__':
    reviews_dir = f'../parser/data/reviews/{dt.today()}/'
    if not os.path.exists(reviews_dir):
        reviews_dir = '../parser/data/reviews/2019-12-13/'

    start = perf_counter()
    collect_categories(reviews_dir)
    LOGGER.info(f"Overall collection time: {perf_counter() - start}")

    start = perf_counter()
    compile_dataframes()
    LOGGER.info(f"All categories compilation time: {perf_counter() - start}")

import asyncio
import json
import logging
import os
from datetime import date as dt
from time import perf_counter

import pandas as pd

from code.utils import detect_language

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()


async def collect_category_reviews(reviews_dir, category):
    LOGGER.info(f'Started proccessing {category}')

    total_reviews = 0
    start_time = perf_counter()
    texts, pluses, minuses, ratings, languages = [], [], [], [], []

    products = os.listdir(reviews_dir)
    for product in products:

        reviews_path = f"{reviews_dir}/{product}"
        with open(reviews_path) as f:
            reviews = json.load(f)['data']
            for review in reviews:
                text, plus, minus = review['comment']['text'], review['comment']['plus'], review['comment']['minus']
                texts.append(text)
                pluses.append(plus)
                minuses.append(minus)

                language = await detect_language(text or plus or minus)
                languages.append(language)

                ratings.append(review['rating'])

            total_reviews += len(reviews)

    data = {
        "text": texts,
        "plus": pluses,
        "minus": minuses,
        "language": languages,
        "rating": ratings,
    }
    date = str(dt.today())

    df = pd.DataFrame(data=data)
    df['category'] = category
    df.to_csv(f'../data/{date}/{category}.csv')

    LOGGER.info(f'Number of products in {category}: {len(products)}')
    LOGGER.info(f'Number of reviews in {category}: {total_reviews}')
    LOGGER.info(f'Finished collection of {category} in {perf_counter() - start_time}')


async def collect_categories(base_dir):
    categories = os.listdir(base_dir)
    categories = [category for category in categories if not category.startswith('.')]

    await asyncio.gather(
        *[collect_category_reviews(f"{base_dir}/{category}", category) for category in categories]
    )


def compile_dataframe():
    data_dir = '../data'

    collection_dates = sorted(os.listdir(data_dir))
    collection_dates = [date for date in collection_dates if not date.startswith('.')]
    latest_collection = collection_dates[-1]

    categories_dir = f"{data_dir}/{latest_collection}"
    categories = os.listdir(categories_dir)

    reviews = pd.DataFrame()
    for category in categories:
        df = pd.read_csv(f'{categories_dir}/{category}',
                         usecols=['text', 'plus', 'minus', 'language', 'rating', 'category'])
        reviews = reviews.append(df, ignore_index=True)

    reviews.to_csv(f'{categories_dir}/all.csv')


async def compile_data():
    reviews_dir = os.listdir('../../parser/data/reviews')
    latest_parse_date = sorted(reviews_dir)[-1]

    LOGGER.info(f"Started categories collection")
    start = perf_counter()
    await collect_categories(latest_parse_date)
    LOGGER.info(f"Overall collection time: {perf_counter() - start}")

    LOGGER.info(f"Started categories compilation")
    start = perf_counter()
    compile_dataframe()
    LOGGER.info(f"All categories compilation time: {perf_counter() - start}")


if __name__ == '__main__':
    asyncio.run(compile_data())

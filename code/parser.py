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
    categories = [category for category in os.listdir(base_dir) if not category.startswith('.')]
    await asyncio.gather(
        *[collect_category_reviews(f"{base_dir}/{category}", category) for category in categories]
    )


def compile_dataframe():
    reviews = pd.DataFrame()

    files = os.listdir('../data/2019-12-16')
    for file in files:
        df = pd.read_csv(f'../data/2019-12-16/{file}',
                         usecols=['text', 'plus', 'minus', 'language', 'rating', 'category'])
        reviews = reviews.append(df, ignore_index=True)

    reviews.to_csv('../data/2019-12-16/all.csv')


async def compile_data():
    reviews_dir = f'../../parser/data/reviews/{dt.today()}/'
    if not os.path.exists(reviews_dir):
        reviews_dir = '../../parser/data/reviews/2019-12-13/' # TODO logger, make up backup date as const

    LOGGER.info(f"Started categories collection")
    start = perf_counter()
    await collect_categories(reviews_dir)
    LOGGER.info(f"Overall collection time: {perf_counter() - start}")

    LOGGER.info(f"Started categories compilation")
    start = perf_counter()
    compile_dataframe()
    LOGGER.info(f"All categories compilation time: {perf_counter() - start}")


if __name__ == '__main__':
    asyncio.run(compile_data())
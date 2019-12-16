import os
import json
import logging
import pandas as pd
from time import perf_counter
from datetime import date as dt

from pymystem3 import Mystem
from kaznlplib.lid.lidnb import LidNB
from kaznlplib.tokenization.tokrex import TokenizeRex

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()

mystem = Mystem()


def collect_category_reviews(REVIEWS_DIR, category):
    start_time = perf_counter()
    products = os.listdir(REVIEWS_DIR)
    texts, pluses, minuses, ratings, languages = [], [], [], [], []

    LOGGER.info(f'Number of products in {category}: {len(products)}')

    tokrex = TokenizeRex()
    cmdl = os.path.join('kaznlplib', 'lid', 'char.mdl')
    wmdl = os.path.join('kaznlplib', 'lid', 'word.mdl')
    landetector = LidNB(word_mdl=wmdl, char_mdl=cmdl)

    total_reviews = 0
    for product in products:

        reviews_path = f"{REVIEWS_DIR}/{product}"
        with open(reviews_path) as f:
            reviews = json.load(f)['data']
            total_reviews += len(reviews)
            for review in reviews:
                text, plus, minus = review['comment']['text'], review['comment']['plus'], review['comment']['minus']

                language = landetector.predict(tokrex.tokenize(text or plus or minus, lower=True)[0])
                languages.append(language)

                texts.append(text)
                pluses.append(plus)
                minuses.append(minus)
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


def walk_categories(base_dir):
    date = str(dt.today())
    categories = os.listdir(base_dir, )
    for category in categories:
        if category.startswith('.'):
            continue

        LOGGER.info(f'Started proccessing {category}')
        start_time = perf_counter()
        df = collect_category_reviews(f"{base_dir}/{category}", category)
        df.to_csv(f'data/{category}_reviews_{date}.csv')
        LOGGER.info(f'Finished processing {category} in {perf_counter() - start_time}')


def collect_dfs():
    files = os.listdir('data/2019-12-14')
    file = files.pop(0)
    reviews = pd.read_csv(f'data/2019-12-14/{file}',
                          usecols=['text', 'plus', 'minus', 'language', 'rating', 'category'])
    for file in files:
        df = pd.read_csv(f'data/2019-12-14/{file}', usecols=['text', 'plus', 'minus', 'language', 'rating', 'category'])
        reviews = reviews.append(df)

    reviews.to_csv('data/2019-12-14/all.csv')


if __name__ == '__main__':
    REVIEWS_DIR = f'../parser/data/reviews/{dt.today()}/'
    if not os.path.exists(REVIEWS_DIR):
        REVIEWS_DIR = '../parser/data/reviews/2019-12-13/'

    now = perf_counter()
    walk_categories(REVIEWS_DIR)
    LOGGER.info(f"Overall time: {perf_counter() - now}")

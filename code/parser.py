import asyncio
import logging
import os
from datetime import date as dt, timedelta
from time import perf_counter

import aiofiles
import pandas as pd
import ujson as json

from code.utils import detect_language, get_latest_date_in_dir

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()


async def compile_data():
    reviews_dir = "../data"
    latest_parse_date = get_latest_date_in_dir(reviews_dir)
    reviews_dir = f"{reviews_dir}/{latest_parse_date}"

    LOGGER.info(f"Started categories collection for {latest_parse_date}")
    start = perf_counter()
    await collect_categories(reviews_dir)
    LOGGER.info(f"Overall collection time: {perf_counter() - start}")

    LOGGER.info(f"Started categories compilation")
    start = perf_counter()
    compile_dataframe()
    LOGGER.info(f"All categories compilation time: {perf_counter() - start}")


async def collect_categories(base_dir):
    categories = os.listdir(base_dir)
    categories = [category for category in categories if not category.startswith(".")]

    categories_reviews = [
        collect_category_reviews(f"{base_dir}/{category}", category)
        for category in categories
    ]
    await asyncio.gather(*categories_reviews)


async def collect_category_reviews(reviews_dir, category):
    LOGGER.info(f"Started collection of {category}")
    start_time = perf_counter()

    texts, pluses, minuses, ratings, languages = [], [], [], [], []

    products = os.listdir(reviews_dir)
    for product in products:
        total_reviews += await collect_product_reviews(product, reviews_dir, texts, pluses, minuses, languages, ratings)

    data = {
        "text": texts,
        "plus": pluses,
        "minus": minuses,
        "language": languages,
        "rating": ratings,
    }
    date = str(dt.today() + timedelta(days=3))
    os.makedirs(f"../cleaned_data/{date}/", exist_ok=True)

    df = pd.DataFrame(data=data)
    df["category"] = category
    df.to_csv(f"../cleaned_data/{date}/{category}.csv")

    LOGGER.info(f"Number of products in {category}: {len(products)}")
    LOGGER.info(f"Finished collection of {category} in {perf_counter() - start_time}")


async def collect_product_reviews(
        product, reviews_dir, texts, pluses, minuses, languages, ratings
):
    reviews_path = f"{reviews_dir}/{product}"
    async with aiofiles.open(reviews_path) as f:
        reviews = json.loads(await f.read())["data"]

    LOGGER.info(f"Started collecting product reviews")
    reviews_start = perf_counter()

    reviews_tasks = []
    for review in reviews:
        reviews_tasks.append(
            append_review_data(review, texts, pluses, minuses, languages, ratings)
        )

    LOGGER.info(f"Collected reviews in {perf_counter() - reviews_start}")

    review_gather_start = perf_counter()
    LOGGER.info(f"Started gathering product reviews")
    await asyncio.gather(*reviews_tasks)
    LOGGER.info(
        f"Gathered product reviews in {perf_counter() - review_gather_start}"
    )


async def append_review_data(review, texts, pluses, minuses, languages, ratings):
    text, plus, minus = (
        review["comment"]["text"],
        review["comment"]["plus"],
        review["comment"]["minus"],
    )
    texts.append(text)
    pluses.append(plus)
    minuses.append(minus)

    language = detect_language(text or plus or minus)
    languages.append(language)

    ratings.append(review["rating"])


def compile_dataframe():
    data_dir = "../cleaned_data"
    latest_categories_collection = get_latest_date_in_dir(data_dir)

    LOGGER.info(f"Dataframe compilation date: {latest_categories_collection}")

    categories_dir = f"{data_dir}/{latest_categories_collection}"
    categories = os.listdir(categories_dir)

    reviews = pd.DataFrame()
    for category in categories:
        df = pd.read_csv(
            f"{categories_dir}/{category}",
            usecols=["text", "plus", "minus", "language", "rating", "category"],
        )
        reviews = reviews.append(df, ignore_index=True)

    reviews.to_csv(f"{categories_dir}/all.csv")


if __name__ == "__main__":
    asyncio.run(compile_data())

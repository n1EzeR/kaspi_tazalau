import logging
import os
import re
from time import perf_counter

import pandas as pd
from pymystem3 import Mystem

from app.constants import PUNCTUATION, STOPWORDS
from app.exceptions import DataNotCollectedException
from app.utils import get_latest_date_in_dir

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()

stem = Mystem()


def clean_data(dir, file):
    LOGGER.info(f"Started processing data from {dir}")

    start_time = perf_counter()
    data = f"{dir}/{file}"

    df = pd.read_csv(data, sep="|")

    text_columns = ["text", "plus", "minus"]
    for column in text_columns:
        df[column] = df[column].fillna("")
        df[column] = df[column].apply(clean_text)
        df[column] = df[column].apply(lemmatize_text)

    df.to_csv(f"{dir}/reviews_cleaned.csv", index=False)

    LOGGER.info(f"Finished processing data in {perf_counter() - start_time}")


def clean_text(text):
    if not text:
        return

    text = text.lower()
    text = re.sub(r"(<\s*\w+\s*>)*(<\s*/\w+\s*>)*", "", text)
    text = [
        word
        for word in text.split(" ")
        if word not in STOPWORDS and word not in PUNCTUATION
    ]

    return " ".join(text)


def lemmatize_text(text):
    if not text:
        return

    text = stem.lemmatize(text)
    text = [word for word in text if word.isalpha()]

    return " ".join(text).strip()


def process_data():
    data_dir = "../collected_data"
    latest_parse_date = get_latest_date_in_dir(data_dir)
    latest_data_dir = f"{data_dir}/{latest_parse_date}"

    if not os.path.exists(latest_data_dir):
        raise DataNotCollectedException(f"Data is not collected for {latest_data_dir}")

    clean_data(latest_data_dir, "all.csv")


if __name__ == "__main__":
    process_data()

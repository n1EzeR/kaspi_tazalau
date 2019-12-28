import logging
import os
import re
from time import perf_counter

import pandas as pd
from pymystem3 import Mystem

from code.constants import STOPWORDS, PUNCTUATION
from code.exceptions import DataNotCollectedException
from code.utils import get_latest_date_in_dir

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()

mystem = Mystem()


def clean_data(dir, file):
    LOGGER.info(f"Started processing data from {dir}")

    start_time = perf_counter()
    data = f"{dir}/{file}"

    df = pd.read_csv(data)
    df['combined_text'] = df.text + ' ' + df.plus + ' ' + df.minus

    df['combined_text'] = df['combined_text'].apply(clean_text)
    df['combined_text'] = df['combined_text'].apply(lemmatize_text)

    df.to_csv(f"{dir}/cleaned_data.csv")

    LOGGER.info(f"Finished processing data in {perf_counter() - start_time}")


def clean_text(text):
    # TODO add average cleaning time logger

    text = text.lower()
    text = re.sub(r'(<\s*\w+\s*>)*(<\s*/\w+\s*>)*', '', text)
    text = [word for word in text.split(' ') if word not in STOPWORDS and word not in PUNCTUATION]

    return " ".join(text)


def lemmatize_text(text):
    # TODO add average lemmatizing time logger

    text = mystem.lemmatize(text)
    text = [word for word in text if word != ' ']
    return " ".join(text)


def process_data():
    data_dir = '../data/'
    latest_parse_date = get_latest_date_in_dir(data_dir)
    raw_data_dir = f"{data_dir}/{latest_parse_date}"

    if not os.path.exists(raw_data_dir):
        raise DataNotCollectedException(f'Data is not collected for {raw_data_dir}')

    clean_data(raw_data_dir, 'all.csv')

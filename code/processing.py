import re

from pymystem3 import Mystem

from code.constants import STOPWORDS, PUNCTUATION

mystem = Mystem()


def clean_data(data):
    pass


def clean_text(text):
    text = text.lower()
    text = re.sub(r'(<\s*\w+\s*>)*(<\s*/\w+\s*>)*', '', text)
    text = [word for word in text.split(' ') if word not in STOPWORDS and word not in PUNCTUATION]

    return " ".join(text)


def lemmatize_text(text):
    text = mystem.lemmatize(text)
    text = [word for word in text if word != ' ']
    return " ".join(text)


def process_data():
    clean_data('../data/2019-12-14/All.csv')

import logging
import os
from code.exceptions import EmptyDirectoryException
from code.kaznlplib.lid.lidnb import LidNB
from code.kaznlplib.tokenization.tokrex import TokenizeRex

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()

tokrex = TokenizeRex()
cmdl = os.path.join("kaznlplib", "lid", "char.mdl")
wmdl = os.path.join("kaznlplib", "lid", "word.mdl")
language_detector = LidNB(word_mdl=wmdl, char_mdl=cmdl)


def detect_language(text):
    return language_detector.predict(tokrex.tokenize(text, lower=True)[0])


def get_latest_date_in_dir(dir):
    collection_dates = sorted(os.listdir(dir))
    collection_dates = [date for date in collection_dates if not date.startswith(".")]

    if not collection_dates:
        raise EmptyDirectoryException(
            f"No valid files or directories are found in {dir}"
        )

    return collection_dates[-1]

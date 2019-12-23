import os

from .kaznlplib.lid.lidnb import LidNB
from .kaznlplib.tokenization.tokrex import TokenizeRex

tokrex = TokenizeRex()
cmdl = os.path.join('kaznlplib', 'lid', 'char.mdl')
wmdl = os.path.join('kaznlplib', 'lid', 'word.mdl')
language_detector = LidNB(word_mdl=wmdl, char_mdl=cmdl)


async def detect_language(text):
    return language_detector.predict(tokrex.tokenize(text, lower=True)[0])

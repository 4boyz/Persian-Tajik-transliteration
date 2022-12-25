# Скрипт, для сопоставление всех персидских предложений со всеми таджикскими предложениями.
# Сам скрипт имеет высокую асимтотическую сложность - O(n*m), где n - количество таджикских предложений (~100k), а m - персидских (~400k).
# Поэтому сохраняются промежуточные вычисления и сопоставление распаралеллено.

from tj_pers_checker import TjPersChecker   # type: ignore
from bbc_parser import BBCParser            # type: ignore
from dump_reader import DumpReader          # type: ignore

from datetime import datetime
import pickle
import os.path
from joblib import Parallel, delayed
import pandas as pd
import numpy as np

TAJIK_CHARS = 'АБВГҒДЕЁЖЗИЙКҚЛМНОПРСТУӮФХҲЧЦЧҶШЪЭЮЯ '

def tajik_chars_filter(string):
  return ''.join(list(filter(lambda x: x in TAJIK_CHARS, list(string.upper()))))

def farsi_chars_filter(string):
  return ''.join(list(filter(lambda x: (ord(x) >= 1536 and ord(x) <= 1790) or ord(x) == ord(' '), list(string))))

PATH_TO_PERSIAN_DUMP = 'C:\\Users\\mirvo\\Downloads\\Telegram Desktop\\persian_dump\\persian_dump'
PATH_TO_TJIK_DUMP = 'C:\\Users\\mirvo\\Downloads\\Telegram Desktop\\tajik_dump\\tajik_dump'

TJ_SEN_PATH = './data/temp/tj.pckl'
TJ_SEN_PREP_PATH = './data/temp/tj-prep.pckl'
PERSIAN_SEN_PATH = './data/temp/fa.pckl'
PERSIAN_SEN_PREP_PATH = './data/temp/fa-prep.pckl'
RESULT_PATH = lambda n: f'./data/temp/result-{n}.pckl'

THREADS = 12
PARTS_COUNTS = 1000

tajik_sentences = pickle.load(open(TJ_SEN_PATH,"rb")) if os.path.isfile(TJ_SEN_PATH) else []
tj_prep = pickle.load(open(TJ_SEN_PREP_PATH,"rb")) if os.path.isfile(TJ_SEN_PREP_PATH) else []
persian_sentences = pickle.load(open(PERSIAN_SEN_PATH,"rb")) if os.path.isfile(PERSIAN_SEN_PATH) else []
pers_prep = pickle.load(open(PERSIAN_SEN_PREP_PATH,"rb")) if os.path.isfile(PERSIAN_SEN_PREP_PATH) else []

# Tajik
if not tajik_sentences:
    print(f"{datetime.now()} Tajik")
    for article in DumpReader.read(PATH_TO_TJIK_DUMP):
        tajik_sentences.extend(BBCParser.parse_text_tj(article['Content']))
    pickle.dump(tajik_sentences, open(TJ_SEN_PATH, "wb"))


# Tajik prep
if not tj_prep:
    print(f"{datetime.now()} Tajik prep")
    tajik_sentences = list(set(list(filter(lambda x: x, map(lambda x: x.strip(), map(tajik_chars_filter, tajik_sentences))))))
    tajik_sentences.sort()
    tj_df = pd.DataFrame({
        'orig' : tajik_sentences,
        'prepared' : list(map(lambda sent: TjPersChecker.str_changer(sent, 'tj'), tajik_sentences))
    })
    tj_prep = tj_df['prepared'].to_list()
    pickle.dump(tj_prep, open(TJ_SEN_PREP_PATH, "wb"))


# Persian
if not persian_sentences:
    print(f"{datetime.now()} Persian")
    for article in DumpReader.read(PATH_TO_PERSIAN_DUMP):
        persian_sentences.extend(BBCParser.parse_text_pers(article['Content']))
    pickle.dump(persian_sentences, open(PERSIAN_SEN_PATH, "wb"))


# Persian prep
if not pers_prep:
    print(f"{datetime.now()} Persian prep")
    persian_sentences = list(set(list(filter(lambda x: x, map(lambda x: x.strip(), map(farsi_chars_filter, persian_sentences))))))
    persian_sentences.sort()
    pers_df = pd.DataFrame({
        'orig' : persian_sentences,
        'prepared' : list(map(lambda sent: TjPersChecker.str_changer(sent, 'pers'), persian_sentences))
    })
    pers_prep = pers_df['prepared'].to_list()
    pickle.dump(pers_prep, open(PERSIAN_SEN_PREP_PATH, "wb"))


tj_parts = np.array_split(tj_prep, PARTS_COUNTS)
for index, tj_part in enumerate(tj_parts):
    if os.path.isfile(RESULT_PATH(index)): continue
    print(f"{datetime.now()} Parallel part {index}")
    result_part = Parallel(n_jobs=THREADS)(delayed(TjPersChecker.match_sentences_tuned)([tj_sen], pers_prep) for tj_sen in tj_part)
    pickle.dump(result_part, open(RESULT_PATH(index), "wb"))

print(f"{datetime.now()} Done")
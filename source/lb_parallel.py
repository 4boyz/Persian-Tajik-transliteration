import multiprocessing as mp
import os
from os.path import exists
import csv

# «Удлинитель» коротких ссылок для запуска в 4 потоках

from links_beautify import Beautifyer

# def start_beautifyer_process(id, path_from: str, path_to: str, offset: int = 0, count: int = -1):
#     path_to_formatted = '.'.join(path_to.split('.').insert(str(id), -1))
#     beautifyer = Beautifyer(path_from, path_to)

def start_beautifyer_process(path_from: str, path_to: str):
    # path_to_formatted = '.'.join(path_to.split('.').insert(-1, str(id)))
    beautifyer = Beautifyer(path_from, path_to)
    beautifyer.start()

# def split_csv_file(file_path, parts_count) -> list(str):
#     if not exists(file_path): return -1
    
#     rows_count = 0
#     with open(file_path, mode='r', encoding='utf8') as file:
#         rows_count = len(file.readlines())

#     return [rows_count // parts_count + rows_count % parts_count // i + 1 for i in range(parts_count)]


if __name__ == '__main__':
    
    # TWITTER_LINKS = 'data\\default_tweet_persian.csv'
    path_to_root = os.path.dirname(os.path.dirname(__file__)) + '\\'
    PROCCESS_COUNT = 4
    processes: list[mp.Process] = []
    PATHS_FROM = [
        path_to_root + 'data\\default_tweet_persian.1.csv',
        path_to_root + 'data\\default_tweet_persian.2.csv',
        path_to_root + 'data\\default_tweet_persian.3.csv',
        path_to_root + 'data\\default_tweet_persian.4.csv',
    ]
    PATH_TO = [
        path_to_root + 'data\\beauty_links_persian.1.csv',
        path_to_root + 'data\\beauty_links_persian.2.csv',
        path_to_root + 'data\\beauty_links_persian.3.csv',
        path_to_root + 'data\\beauty_links_persian.4.csv',
    ]

    mp.set_start_method('spawn')

    for i in range(PROCCESS_COUNT):
        processes.append(mp.Process(target=start_beautifyer_process, args=(PATHS_FROM[i], PATH_TO[i])))

    for p in processes:
        p.start()

    for p in processes: 
        p.join()

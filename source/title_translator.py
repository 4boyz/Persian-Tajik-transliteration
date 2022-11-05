import os
import sys
import gzip
import csv
from os.path import exists
from parse import parse
from bs4 import BeautifulSoup

import asyncio
from googletrans import Translator
import async_google_trans_new

class title_translator:
    def __get_translated_indexes(self): 
        translated_indexes = set()
        if exists(self.out_csv_path):
            with open(self.out_csv_path, encoding='utf8') as file:
                reader = csv.reader(file, delimiter=',')
                for row in reader:
                    translated_indexes.add(row[0])
        return translated_indexes

    def start_sync(self):
        self.__translator: Translator = Translator()
        self.__write_results(self.__translate_dump(self.dumps_path))

    def __translate_dump(self, dumps_path):
        translated_indexes = self.__get_translated_indexes()
        for file in os.listdir(dumps_path):
            [index, code] = parse("{}-{}.gz", file)
            if(index in translated_indexes):
                continue
            if(code != '200'):
                continue
            fullpath = os.path.join(dumps_path, file)
            yield self.__translate_file(index, fullpath)

    def __translate_file(self, index, filepath):            
        gz = gzip.open(filepath)
        soup = BeautifulSoup(gz.read(), "lxml")
        title = self.title_parser(soup)
        translator: Translator  = self.__translator
        if(title != None):
            try:
                translated_text: str = translator.translate(title.text, dest="en").text
            except Exception as e:                                
                print(f"Catch an exception during translate '{title.text}'.")     
                print(e)          
            else:
                print(translated_text)
                return [index, translated_text]
        else:
            print("Error parse file: " + filepath, file=sys.stderr)        

    def __write_results(self, results):
        with open(self.out_csv_path, encoding='utf8', mode='a', newline='') as file:
            writer = csv.writer(file, delimiter=',')
            for res in results:
                if res: writer.writerow(res)

    def __fill_task_queue(self, dumps_path):
        translated_indexes = self.__get_translated_indexes()

        for file in os.listdir(dumps_path):
            [index, code] = parse("{}-{}.gz", file)

            if(index in translated_indexes):
                continue

            if(code != '200'):
                continue

            fullpath = os.path.join(dumps_path, file)
            self.queue.put_nowait([index, fullpath])

    def __clear_task_queue(self):
        for _ in range(self.queue.qsize()):
            self.queue.get_nowait()
            self.queue.task_done()

    def __init__(self, dumps_path, title_parser, out_csv_path):
        self.title_parser = title_parser
        self.out_csv_path = out_csv_path
        self.dumps_path = dumps_path

        self.queue = asyncio.Queue()
        self.__fill_task_queue(dumps_path)

    async def __worker(self, translator1, writer):
        while True:
            [index, filepath] = await self.queue.get()
            
            gz = gzip.open(filepath)
            soup = BeautifulSoup(gz.read(), "lxml")
            title = self.title_parser(soup)
            if(title != None):
                try:
                    translator = async_google_trans_new.AsyncTranslator()
                    translated_text = await translator.translate(title.text, lang_tgt="en")
                except Exception as e:                                
                    print(f"Catch an exception during translate '{title.text}'.")     
                    print(e)          
                else:
                    writer.writerow([index, translated_text])
                    print(translated_text)
            else:
                print("Error parse file: " + filepath, file=sys.stderr)        

            self.queue.task_done()

    async def __coro(self, workers_count):
        with open(self.out_csv_path, encoding='utf8', mode='a', newline='') as file:
            writer = csv.writer(file, delimiter=',')

            tasks = []
            for i in range(workers_count):
                task = asyncio.create_task(
                    self.__worker(async_google_trans_new.AsyncTranslator(), writer))
                tasks.append(task)

            await self.queue.join()

            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def start(self, workers_count):
        asyncio.run(self.__coro(workers_count))

def tajik_title_parser(soup: BeautifulSoup):
    res = soup.find("h1", class_="story-body__h1")
    if not res: res = soup.find("h1", class_="gallery-intro__h1")
    
    return soup.find("h1", class_="story-body__h1")

def persian_title_parser(soup: BeautifulSoup):
    title = soup.find("h1", class_="bbc-1gvq3vt e1p3vdyi0")
    if not title: title = soup.find("strong", class_="ewk8wmc0 bbc-v5mdtt eglt09e1")
    if not title: title = soup.find("span", id_="promo-persianworld201408140820_u08_netanyahu_vows_campaign_cont-1")
    if not title: title = soup.find("span", class_="headline")
    if not title: title = soup.find("h1")
    return title

if __name__ == "__main__":
    
    # https://stackoverflow.com/questions/52455774/googletrans-stopped-working-with-error-nonetype-object-has-no-attribute-group

    # WORKERS_COUNT = 50
    DATA_DIRECTORY = os.path.dirname(__file__) + "\\..\\data"

    TAJIK_DUMPS_DIRECTORY = 'C:\\Users\\mirvo\\Downloads\\Telegram Desktop\\tajik_dump\\tajik_dump'
    TAJIK_TITLES_CSV = DATA_DIRECTORY + "\\tajik_titles_translated.csv"
    title_translator(dumps_path=TAJIK_DUMPS_DIRECTORY, out_csv_path=TAJIK_TITLES_CSV, title_parser=tajik_title_parser).start_sync()
    # tajik_titles = title_translator(TAJIK_DUMPS_DIRECTORY, tajik_title_parser, TAJIK_TITLES_CSV)
    # tajik_titles.start(WORKERS_COUNT)

    PERSIAN_DUMPS_DIRECTORY = 'C:\\Users\\mirvo\\Downloads\\Telegram Desktop\\persian_dump\\persian_dump'
    PERSIAN_TITLES_CSV = DATA_DIRECTORY + "\\persian_titles_translated.csv"
    title_translator(dumps_path=PERSIAN_DUMPS_DIRECTORY, out_csv_path=PERSIAN_TITLES_CSV, title_parser=persian_title_parser).start_sync()
    # persian_titles = title_translator(PERSIAN_DUMPS_DIRECTORY, persian_title_parser, PERSIAN_TITLES_CSV)
    # persian_titles.start(WORKERS_COUNT)



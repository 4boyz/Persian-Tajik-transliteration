import requests
import csv
import ast
from time import sleep

class Beautifyer:
    __from_path = None
    __to_path = None

    def __insert_link(self, id: int, link: str):
        with open(self.__to_path, encoding='utf8', mode='a', newline='') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow([id, link])

    def __get_beauty_link(self, link):
        if not link: return ''
        for i in range(10):
            try:
                return requests.get(link).url
            except:
                sleep(1)
        return ''

    def __get_last_link(self, content: str):
        return content.split(' ')[-1]

    def __get_last_writeble_index(self):
        with open(self.__to_path, mode='r', encoding='utf8') as file:
            last_line = csv.reader([file.readlines()[-1]], delimiter=',').__next__()
            if len(last_line) == 0: return -1
            return int(last_line[0])

    def start(self):
        last_id = self.__get_last_writeble_index()

        with open(self.__from_path, encoding='utf8') as file:
            reader = csv.reader(file, delimiter=',')
            reader.__next__()

            for row in reader:
                id = int(row[0]) if row[0] else -1
                first_links = ast.literal_eval(row[3])
                beauty_link = ''

                if(last_id >= id): continue

                if first_links:
                    beauty_link = self.__get_beauty_link(first_links[-1])
                else:
                    beauty_link = self.__get_beauty_link(self.__get_last_link(row[4]))

                print(f"{id}. {beauty_link}")
                self.__insert_link(id, beauty_link)

    def __init__(self, from_path, to_path):
        self.__from_path = from_path
        self.__to_path = to_path

if __name__ == "__main__":
    
    TWITTER_LINKS = 'links\\default_tweet_tajik.csv'
    BEAUTY_LINKS_CSV_PATH = 'links\\beauty_links_tajik.csv'

    beauty = Beautifyer(TWITTER_LINKS, BEAUTY_LINKS_CSV_PATH)
    beauty.start()
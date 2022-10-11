import pandas as pd
import requests
from bs4 import BeautifulSoup
import re

ARTICLES = 'links\\articles.csv'
smpl_matched_links_equile_year = pd.read_csv('links\smpl_matched_links_equile_year.csv', index_col=0)
articles = pd.DataFrame({'id': '',
                         'tj_article_title': '',
                         'tj_article_introduction': '',
                         'tj_article_text': [],
                         'persian_article_title': '',
                         'persian_article_introduction': '',
                         'persian__article_text': []
                         })

def Tj_bbc(url):
    # пример: url = 'https://www.bbc.com/tajik/news/2015/03/150315_l16_l57_kerry_assad_negotiate'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    article_title = soup.find_all('h1', class_='story-body__h1')
    article_introduction = soup.find_all('p', class_='story-body__introduction')
    article_text = soup.find_all('div', class_='story-body__inner')
    text = (re.split('</p><p*>', str(article_text)))[1:-1]
    return [article_title[0].text, article_introduction[0].text, text]

    # print(article_text[0])


def Persian_bbc(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    article_title = soup.find_all('h1', class_='bbc-1gvq3vt e1p3vdyi0')
    article_introduction = soup.find_all('p', class_='bbc-yabuuk e17g058b0')
    article_text = soup.find_all('p', class_='bbc-yabuuk e17g058b0')
    text = list((o.text for o in article_text))[1:]  # Элемент№0 это article_title. Из-за арабского языка(справа налево)
    text.reverse()
    return [article_title[0].text, article_introduction[0].text, text]


for index, row in smpl_matched_links_equile_year.iterrows():
    temp = ['','','','','','','']
    try:
        temp = ([str(row['id'])] + Tj_bbc(row['link']) + Persian_bbc(row['equals_persian_links'][2:-2]))  # Уберем [' и ']
    except Exception:
        print(Exception)
    articles.loc[len(articles)] = temp
    articles.to_csv(ARTICLES)
import pandas as ps
import csv

MATCHED_LINKS = '\\matched_links.csv'
PERSIAN_LINKS = '\\data\\beauty_links_persian.csv'
TAJIK_LINKS = '\\data\\beauty_links_tajik.scv'

persian_links = ps.read_csv(PERSIAN_LINKS)
tajik_links = ps.read_csv(TAJIK_LINKS)
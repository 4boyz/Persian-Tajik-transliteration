import snscrape.modules.twitter
import snscrape.modules.twitter as sntwitter
import pandas as pd
from pathlib import Path

filepath = Path('..*\projects\csv\out.csv')


# (from:bbcpersian) until:2015-04-01 since:2009-09-01
# (from:BBCTajikistan) until:2015-04-01 since:2009-09-01
query = "(from:bbcpersian) until:2015-04-01 since:2009-09-01"
tweets = []

for tweet in sntwitter.TwitterSearchScraper(query).get_items():
    tweets.append([tweet.date, tweet.username, tweet.outlinks, tweet.content])

df = pd.DataFrame(tweets, columns=['Date', 'User', 'Links', 'Content'])
df.to_csv(filepath)
import json
import os
import time
import random
import pandas as pd
import xlrd
import httpx
from twitter.account import Account
from twitter.scraper import Scraper
import json
from twitter.search import Search

def perpare_header(auth_token):
    try:
        url = "https://twitter.com/i/api/graphql/5yhbMCF0-WQ6M8UOAs1mAg/SearchTimeline"

        headers = {
            "authority": "twitter.com",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
            "cache-control": "no-cache",
            "cookie": f"auth_token={auth_token};ct0=",
            "pragma": "no-cache",
            "referer": "https://twitter.com/",
            "sec-ch-ua": '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
            "x-csrf-token": "",  # ct0
            "x-twitter-active-user": "yes",
            "x-twitter-auth-type": "OAuth2Session",
            "x-twitter-client-language": "zh-cn",
        }

        client = httpx.Client(headers=headers)

        res1 = client.get(url)

        ct0 = res1.cookies.get("ct0")

        client.headers.update(
            {"x-csrf-token": ct0, "cookie": f"auth_token={auth_token};ct0={ct0}"}
        )

        return client.headers
    except Exception as e:
        print(str(e))
        time.sleep(3)
        return perpare_header(random.choice(auth_token_lists))

auth_token_lists=[]

res = str(perpare_header("").get("cookie"))
ct0 = res.split(";")[1].strip("ct0=")
print(ct0)
scraper = Scraper(cookies={"ct0": ct0, "auth_token": ""})

csv_data = pd.read_csv("./data.csv")
rows = len(csv_data)
print(rows)
df = pd.DataFrame(csv_data)
tids = []
for i in range(len(df)):
    tid = str(df['tweet_id'][i])
    print(tid)
    tids.append(tid)
tweets = scraper.tweets_by_ids(tids)

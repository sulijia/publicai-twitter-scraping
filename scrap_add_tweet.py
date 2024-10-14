import base64
from datetime import datetime
import json
import os
import random
import string
import struct
import time

import httpx
import numpy
import requests
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import urllib.parse
import redis
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func, BigInteger, TIMESTAMP
from twitter.scraper import Scraper

bearer_token = os.environ.get("bearer_token")
headers = {
    'Authorization': bearer_token,
}
db_user = os.environ.get("db_user")
db_password = os.environ.get("db_password")
encoded_password = urllib.parse.quote_plus(db_password)
SQLALCHEMY_DATABASE_URL = f"postgresql://{db_user}:{encoded_password}@127.0.0.1:5432/{db_name}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    echo=False,
    pool_timeout=30, pool_recycle=3600
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
AI_count = 0
AI_MAX = 1000
web3_count = 0
web3_MAX = 1000

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    question = Column(String)
    answer = Column(String)
    count = Column(Integer)
    dataset_info = Column(Integer)
    confirmed = Column(Boolean)
    dataset_type = Column(Integer)
    sub_type = Column(Integer)
    tweet_id = Column(String)
    tweet = Column(String)
    tweet_quality = Column(Integer)
    created_at = Column(TIMESTAMP(timezone=True), default=func.now())


class TwitterDataset(Base):
    __tablename__ = "twitter_datasets"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    tweet_id = Column(String)
    tweet = Column(String)
    author_id = Column(String)
    author_name = Column(String)
    author_image = Column(String)
    org_id = Column(String)
    org_author_id = Column(String)
    tweet_type = Column(Integer)
    dataset_id = Column(Integer)
    quality = Column(Integer)
    created_at = Column(TIMESTAMP(timezone=True), default=func.now())
    tweet_at = Column(TIMESTAMP(timezone=True))

class TrainReward(Base):
    __tablename__ = "train_rewards"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    user = Column(Integer)
    train_job = Column(Integer)
    dataset = Column(Integer)
    point = Column(Integer)
    type = Column(Integer)
    created_at = Column(TIMESTAMP(timezone=True), default=func.now())


class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    identity = Column(String)
    email = Column(String)
    name = Column(String)
    avatar = Column(String)
    wallet = Column(String)
    nonce = Column(String)
    invite_code = Column(String)
    point = Column(Integer)
    twitter_id = Column(String)
    twitter_name = Column(String)
    level = Column(Integer)
    suc_count = Column(Integer)
    done_count = Column(Integer)
    created_at = Column(TIMESTAMP(timezone=True), default=func.now())

class TwitterRelationship(Base):
    __tablename__ = "twitter_relationships"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    tweet_id = Column(String)
    ref_tweet_id = Column(String)
    tweet_type = Column(Integer)
    author_id = Column(String)
    ref_author_id = Column(String)
    ref_at = Column(TIMESTAMP(timezone=True))
    created_at = Column(TIMESTAMP(timezone=True), default=func.now())


question_map = [
    "Does this tweet offer you a new understanding?",
    "Does this tweet provide you with valuable learning?",
    "Does this tweet make you want to learn more?",
    "Does this tweet provide valuable insights?",
    "Do you find the insights shared in this tweet to be valuable?",
    "Are the tweet's ideas beneficial to you?",
    "Do you trust this tweet's source?",
    "Would you consider sharing this tweet with others?",
    "Does this tweet enhance your understanding?",
    "Is this tweet's perspective unique?",
    "Has this tweet altered your perspective in any way?",
    "Does the tweet's argument sway your opinion?",
    "Does this tweet spur critical thinking?",
    "Is this tweet's argument convincing?",
    "Would you discuss this tweet with others?",
    "Does this tweet offer new facts or insights?",
    "Did this tweet reveal a new topic to you?",
    "Does this tweet inform you more?",
    "Is this tweet forward-thinking?",
    "Does this tweet impact your life or thoughts?",
]

answer_yes_map = [
    "It unveils new insights.",
    "I gain significant learning from the tweet.",
    "It ignites my curiosity to explore further.",
    "The insights are valuable.",
    "The insights are profoundly valuable.",
    "The ideas are beneficial.",
    "The source appears credible to me.",
    "I'd readily share it.",
    "It significantly enhances my understanding.",
    "It presents a unique viewpoint.",
    "It has shifted my viewpoint.",
    "The argument sways my opinion.",
    "It fosters critical analysis.",
    "The argument is compelling.",
    "Definitely worth discussing.",
    "The tweet offers new facts and insights.",
    "It opened up a new topic for me.",
    "I feel more informed after reading it.",
    "It's ahead of its time.",
    "It has a meaningful impact on me.",
]

answer_no_map = [
    "It's information I'm already familiar with.",
    "The tweet doesn't contribute to my learning.",
    "I already possess ample knowledge on this topic.",
    "The insights have minimal value.",
    "The insights lack substantial value to me.",
    "The ideas offer minimal benefit to me.",
    "I question the source's credibility.",
    "I wouldn't feel compelled to share.",
    "It adds little to my overall grasp of the topic.",
    "It echoes common perspectives.",
    "My perspective remains unchanged.",
    "The argument doesn't sway my opinion.",
    "It fails to provoke deeper thought.",
    "I find the argument unconvincing.",
    "Not something I'd bring up.",
    "It doesn't present any novel information.",
    "I was already aware of the topic.",
    "It doesn't add to my knowledge.",
    "It's not particularly forward-thinking.",
    "It's impact is negligible on my daily life.",
]


def insertDB(tweet_id, author_id, author_name, author_image, useful_text, org_author_id, org_tweet_id, tweet_type,
             ref_at, dataset_type):
    random_index = random.randint(0, 19)
    answer_obj = [answer_yes_map[random_index], answer_no_map[random_index]]
    answerList = json.dumps(answer_obj)
    tweet = {
        "author_id": author_id,
        "author_name": author_name,
        "author_image": author_image,
        "text": useful_text,
    }
    print(tweet_id, author_id, author_name, author_image, useful_text, org_author_id, org_tweet_id, tweet_type, ref_at,
          dataset_type)
    old_dataset = db.query(Dataset).filter(Dataset.tweet_id == tweet_id).first()
    if old_dataset is None:
        result = True  # AuditText(redis_conn, useful_text)
        print("AuditText:" + str(result))
        flag = False  # findTweetSimilar(useful_text)
        print("Similar:" + str(flag))
        if result and not flag:
            # redis_conn.lpush(tweet_list_key, useful_text)
            score = 5  # get_score_from_tweet(useful_text)
            if dataset_type == 3:
                dataset_info = 7
            elif dataset_type == 4:
                dataset_info = 10
            db_dataset = Dataset(question=question_map[random_index], answer=answerList,
                                 count=2,
                                 dataset_info=dataset_info, confirmed=False,
                                 dataset_type=dataset_type, sub_type=1, tweet_id=tweet_id,
                                 tweet=json.dumps(tweet, ensure_ascii=False), tweet_quality=score)
            db.add(db_dataset)
            db.commit()
            old_twitter_dataset = db.query(TwitterDataset).filter(TwitterDataset.tweet_id == tweet_id).first()
            if old_twitter_dataset is None:
                db_twitter = TwitterDataset(tweet_id=tweet_id, tweet=useful_text, author_id=author_id,
                                            author_name=author_name,
                                            author_image=author_image, org_author_id=org_author_id, org_id=org_tweet_id,
                                            tweet_type=tweet_type, dataset_id=db_dataset.id, quality=score, tweet_at=ref_at)
                db.add(db_twitter)
                db.commit()
                global AI_count
                global web3_count
                if dataset_type == 3:
                    web3_count += 1
                if dataset_type == 4:
                    AI_count += 1
        else:
            print("=======tweet audit fail:{},{}========".format(tweet_id, org_tweet_id))


def TwitterTask(uid, dataset_type):
    if dataset_type==3 and web3_count >= AI_MAX:
        return
    if dataset_type==4 and AI_count >=web3_MAX:
        return
    parse_tweet_dir(uid, dataset_type)


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

        return client.headers, auth_token
    except Exception as e:
        print(str(e))
        time.sleep(3)
        return perpare_header(random.choice(auth_token_lists))

def parse_tweet_dir(uid, dataset_type):
    for filepath, dirnames, filenames in os.walk('./data/{}'.format(uid)):
        for filename in filenames:
            file_path = os.path.join(filepath, filename)
            with open(file_path, 'r') as fcc_file:
                fcc_data = json.load(fcc_file)
                instructions = fcc_data['data']['user']['result']['timeline_v2']['timeline']['instructions']
                for instruction in instructions:
                    if instruction['type'] == 'TimelineAddEntries':
                        entries = instruction['entries']
                        for entry in entries:
                            try:
                                tweet_obj = entry['content']['itemContent']['tweet_results']['result']['legacy']
                                if 'retweeted_status_result' in tweet_obj:
                                    continue
                                tweet_id = entry['content']['itemContent']['tweet_results']['result']['rest_id']
                                author_name =entry['content']['itemContent']['tweet_results']['result']['core']['user_results']['result']['legacy']['screen_name']
                                author_image = entry['content']['itemContent']['tweet_results']['result']['core']['user_results']['result']['legacy']['profile_image_url_https']
                                useful_text = tweet_obj['full_text']
                                create_at = tweet_obj['created_at']
                                time_format = '%a %b %d %H:%M:%S %z %Y'
                                dt = datetime.strptime(create_at, time_format)
                                # timestamp = int(dt.timestamp())
                                tweet_type = 3
                                author_id = uid
                                org_tweet_id = tweet_id
                                org_author_id = uid
                                ref_at = dt
                                if author_id != "" and author_name != "" and author_image != "" and useful_text != "":
                                    insertDB(tweet_id, author_id, author_name, author_image, useful_text, org_author_id,
                                             org_tweet_id,
                                             tweet_type, ref_at, dataset_type)
                            except Exception as e:
                                print(str(e))

def scraper_tweet(uids):
    auth_token_lists=[]
    xx,auth_token = perpare_header(auth_token_lists[0])
    res = str(xx.get("cookie"))
    ct0 = res.split(";")[1].strip("ct0=")
    print(ct0)
    print(auth_token)
    scraper = Scraper(cookies={"ct0": ct0, "auth_token": auth_token})
    tweets = scraper.tweets(uids)

uid_map = {
    # "1039449274344529920": 3,  # DuneAnalytics
    # "111533746": 3,  # WuBlockchain
    #"917276219267100672": 3,  # AICoincom
    # "1462727797135216641": 3,  # lookonchain
    # "1243647961394970632": 3,  # TheBlockPro__
    # "295218901": 3,  # VitalikButerin
    # "2207129125": 3,  # Cointelegraph
    # "1333467482": 3,  # CoinDesk
    # "1321136246349910016": 3,  # CointelegraphCS
    # "963767159536209921": 3,  # TheBlock__
    # "1204315977720221696": 3,  # tokenterminal
    # "1022821051187822593": 3,  # glassnode
    # "1387497871751196672": 3,  # WatcherGuru
    # "902926941413453824": 3,
    # "1578127809792409616": 4,
    # "247853244": 4,
    # "3359503481": 3,
    # "1558634109890875392": 3,
    # "33836629": 4, #karpathy
    # "1641378826537295874": 4, #lmsysorg
    # "48008938": 4, #ylecun
    # "1067218780806307841": 4, #StanfordHAI
    # "4783690002": 4, #GoogleDeepMind
    # "64844802": 3, #a16z
    "1530530365": 3, #chainlink
    "315991624": 3, #FEhrsam
    "26377478": 3, #laurashin
    # "993530753014054912": 3, #decryptmedia
    # "928759224599040001": 3, #crypto
    # "928759224599040001": 3,  # crypto
    # "985686071244541952": 3,  # BinanceAcademy
    # "1260559704335568896": 3,  # nansen_ai
    # "534563976": 4,  # KirkDBorne
    # "2895499182": 4,  # hardmaru
    # "11518572": 4,  # chrisalbon
    # "216939636": 4,  # AndrewYNg
    # "19968025": 4,  # KirkDBorne
    # "2786431437": 4,  # mckaywrigley
    # "1604278358296055808": 4,  # llama_index
    # "1589007443853340672": 4,  # LangChainAI
    # "985243593538338816": 4,  # Teslaconomics

}

def add_tweet_to_db(uids):
    # for k, v in uid_map.items():
    #     TwitterTask(int(k), int(v))
    for uid in uids:
        TwitterTask(uid, uid_map[uid])
uids = []
for k, v in uid_map.items():
    uids.append(k)

# db = SessionLocal()
# add_tweet_to_db(uids)
# db.close()
scraper_tweet(uids)

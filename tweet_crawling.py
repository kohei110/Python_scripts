from requests_oauthlib import OAuth1Session
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
import json, datetime, time, pytz, re, sys,traceback, pymongo, config
import numpy as np
import pandas as pd
import os

CK = config.CONSUMER_KEY
CS = config.CONSUMER_SECRET
AT = config.ACCESS_TOKEN
ATS = config.ACCESS_TOKEN_SECRET

twitter = None
tweetdata = None
meta    = None

def initialize():
    global twitter, tweetdata, meta
    twitter = OAuth1Session(CK,CS,AT,ATS)
initialize()

# 検索ワードを指定して100件のTweetデータをTwitter REST APIsから取得する
def getTweetData(search_word, max_id, since_id):
    global twitter
    url = 'https://api.twitter.com/1.1/search/tweets.json'
    params = {'q': search_word,
              'count':'100',
    }
    # max_idの指定があれば設定する
    if max_id != -1:
        params['max_id'] = max_id
    # since_idの指定があれば設定する
    if since_id != -1:
        params['since_id'] = since_id

    req = twitter.get(url, params = params)   # Tweetデータの取得

    # 取得したデータの分解
    if req.status_code == 200: # 成功した場合
        timeline = json.loads(req.text)
        metadata = timeline['search_metadata']
        statuses = timeline['statuses']
        limit = req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in req.headers else 0
        reset = req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in req.headers else 0              
        return {"result":True, "metadata":metadata, "statuses":statuses, "limit":limit, "reset_time":datetime.datetime.fromtimestamp(float(reset)), "reset_time_unix":reset}
    else: # 失敗した場合
        print ("Error: %d" % req.status_code)
        return{"result":False, "status_code":req.status_code}

# 文字列を日本時間2タイムゾーンを合わせた日付型で返す
def str_to_date_jp(str_date):
    dts = datetime.datetime.strptime(str_date,'%a %b %d %H:%M:%S +0000 %Y')
    return pytz.utc.localize(dts).astimezone(pytz.timezone('Asia/Tokyo'))

# 現在時刻をUNIX Timeで返す
def now_unix_time():
    return time.mktime(datetime.datetime.now().timetuple())

#-------------繰り返しTweetデータを取得する-------------#

print("Which word do you want?")
keyword = input('>> ')


sid=-1
mid = -1 
count = 0
df_tweet_content = pd.DataFrame()
res = None
while(True):    
    try:
        count = count + 1
        res = getTweetData(keyword, max_id=mid, since_id=sid)
        if res['result']==False:
            # 失敗したら終了する
            print ("status_code", res['status_code'])
            break

        elif int(res['limit']) == 0:    # 回数制限に達したので休憩
            # 日付型の列'created_datetime'を付加する
            print ("Access limit. Wait for 15mins")
            #remove_duplicates()

            # 待ち時間の計算. リミット＋５秒後に再開する
            diff_sec = int(res['reset_time_unix']) - now_unix_time()
            if diff_sec > 0:
                time.sleep(diff_sec + 5)
                print("Restart")
        else:
            # metadata処理
            if len(res['statuses'])==0:
                sys.stdout.write("statuses is none. ")
            elif 'next_results' in res['metadata']:
                row_num = 100-1 # 100 is input number 
                for k in range(row_num):
                    dic_tweet = {'account':res['statuses'][k]['user']['screen_name'],
                     'tweet_id':res['statuses'][k]['id'],
                     'created_at':str_to_date_jp(res['statuses'][k]['created_at']),
                     'text':res['statuses'][k]['text'],
                     'retweet_status':res['statuses'][k]['retweeted']
                    }
                    df_tweet_each = pd.DataFrame([dic_tweet])
                    df_tweet_content = df_tweet_content.append(df_tweet_each)   

                next_url = res['metadata']['next_results']
                pattern = r".*max_id=([0-9]*)\&.*"
                ite = re.finditer(pattern, next_url)
                for i in ite:
                    mid = i.group(0)
                agg_rownum = df_tweet_content.shape[0]
                print(str(agg_rownum)+' rows',flush=True)
            else:
                sys.stdout.write("next is none. finished.")
                tdatetime = datetime.datetime.now()
                tstr = tdatetime.strftime('%Y-%m-%d')
                raw_csv_name = keyword+'_'+tstr+'.csv'
                df_tweet_content.to_csv(raw_csv_name,index=False,sep=',')
                break
        df_tweet_content['created_at_date'] = df_tweet_content['created_at'].apply(
            lambda x: datetime.datetime.strftime(x,'%Y-%m-%d'))
        se_agg_tweet = df_tweet_content.groupby('created_at_date')['tweet_id'].count()
        agg_csv_name = keyword+'_'+tstr+'_agg'+'.csv'
        se_agg_tweet.to_csv(agg_csv_name,sep=',')       
    except SSLError as sslerror:
        errno, sterror = sslerror.args
        print ("SSLError({0}): {1}".format(errno, strerror))
        print ("waiting 5mins")
        time.sleep(5*60)
    except ConnectionError as connectionerror:
        errno, sterror = connectionerror.args
        print ("ConnectionError({0}): {1}".format(errno, strerror))
        print ("waiting 5mins")
        time.sleep(5*60)
    except ReadTimeout as readtimeout:
        errno, sterror = readtimeout.args
        print ("ReadTimeout({0}): {1}".format(errno, strerror))
        print ("waiting 5mins")
        time.sleep(5*60)
    except:
        print ("Unexpected error:", sys.exc_info()[0])
        traceback.format_exc(sys.exc_info()[2])
        raise
    finally:
        info = sys.exc_info()

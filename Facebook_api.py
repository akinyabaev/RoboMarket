import facebook
import requests
import logging
import pandas
import sqlite3
import credentials
import json
import datetime
import time


def check_requests_count():
    try:
        q = "https://graph.facebook.com/v10.0/me?fields=id%2Cname&access_token=" + access_token

        get = requests.get(q).headers['x-app-usage']
        f = json.loads(get)
        call_count = f['call_count']
        total_cputime = f['total_cputime']
        total_time = f['total_time']
        usage = max(call_count, total_cputime, total_time)

    except KeyError as error:
        logging.error(f'Received app usage error, {error}')
        usage = None
    finally:

        logging.info(f'Current app usage = {get}')
        return usage, f


def upsert_into_table(df, table_name):
    try:
        conn = sqlite3.connect('facebook.db')
        cur = conn.cursor()
        df.to_sql(table_name, conn, if_exists='replace', index=False, index_label=False)
        conn.commit()
        cur.close()

    except sqlite3.Error as error:
        logging.error(f'Connection error, {error}')

    finally:
        conn.close()
        logging.info(f'Successful insert into {table_name} {len(df.index)} row')


def cooling_time(usage):
    try:
        if usage >= 80:
            logging.debug(f'{usage}% Rate Limit Reached. Cooling Time 5 Minutes.')
            time.sleep(300)
    except KeyError as error:
        logging.error(f'Received app usage error, {error}')


logging.basicConfig(filename='api_log.log', level=logging.INFO, filemode='w',
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

access_token = credentials.access_token
user = credentials.user

all_fields = [
    'id',
    'message',
    'created_time',
    'likes.summary(true)'
]
all_fields = ','.join(all_fields)

t = datetime.datetime.now().replace(minute=0, second=0)
date = t.strftime("%Y-%m-%d %H:%M:%S")
req_count = check_requests_count(date)

usage = check_requests_count()
cooling_time(usage[0])

if usage[0] <= 100 and usage[0] is not None:
    try:
        graph = facebook.GraphAPI(access_token)
        posts = graph.get_connections(user, "posts", fields=all_fields)
        df = pandas.json_normalize(posts['data'])
        df_posts = df.iloc[:, [0, 1, 2, 6]]
        df_posts = pandas.concat([df_posts['id'].str.split(pat='_', expand=True), df_posts], axis=1).drop(['id'],
                                                                                                          axis=1)
        df_posts.columns = ['page_id', 'id', 'message', 'created_time', 'likes_count']
        upsert_into_table(df_posts, 'posts')

        df_likes = df.iloc[:, [0, 6]]
        df_likes = pandas.concat([df_likes['id'].str.split(pat='_', expand=True), df_likes], axis=1).drop(['id'],
                                                                                                          axis=1)
        df_likes.columns = ['page_id', 'post_id', 'likes_count']
        df_likes['date'] = date
        upsert_into_table(df_likes.iloc[:, [1, 3, 2]], 'likes')

        #Проверка на наличие следующих страниц запроса и их запись
        while False:
            try:
                usage = check_requests_count()
                cooling_time(usage[0])

                posts = requests.get(posts["paging"]["next"]).json()
                df = pandas.json_normalize(posts['data'])
                df = pandas.json_normalize(posts['data'])
                df_posts = df.iloc[:, [0, 1, 2, 6]]
                df_posts = pandas.concat([df_posts['id'].str.split(pat='_', expand=True), df_posts],
                                         axis=1).drop(['id'], axis=1)
                df_posts.columns = ['page_id', 'id', 'message', 'created_time', 'likes_count']
                upsert_into_table(df_posts, 'posts')

                df_likes = df.iloc[:, [0, 6]]
                df_likes = pandas.concat([df_likes['id'].str.split(pat='_', expand=True), df_likes],
                                         axis=1).drop(['id'], axis=1)
                df_likes.columns = ['page_id', 'post_id', 'likes_count']
                df_likes['date'] = date
                upsert_into_table(df_likes.iloc[:, [1, 3, 2]], 'likes')

            except KeyError:
                logging.exception("Exception occurred", exc_info=True)
                break

    except KeyError:
        logging.exception("Exception occurred", exc_info=True)

usage = check_requests_count()
cooling_time(usage[0])

if usage[0] >= 100:
    logging.error(f'Rate Limit Reached {usage[1]}')


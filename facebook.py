import facebook
import requests
import logging
import pandas
import sqlite3
import datetime
import credentials


def check_requests_count(date):
    try:
        req_count = 0
        conn = sqlite3.connect('facebook.db')
        cur = conn.cursor()
        q = f'''SELECT COALESCE(req_count, 0) FROM api_requests
            WHERE Date = "{date}"'''
        cur.execute(q)
        req_count = cur.fetchone()[0]
        if req_count is None:
            req_count = 0
        cur.close()

    except sqlite3.Error as error:
        logging.error(f'Connection error, {error}')

    finally:
        conn.close()
        logging.info(f'Current req_count = {req_count}')
        return req_count


def update_requests_check(date, req_count):
    try:
        conn = sqlite3.connect('facebook.db')
        cur = conn.cursor()
        q = f'''INSERT INTO api_requests(Date,req_count) VALUES("{date}",{req_count})
                ON CONFLICT(Date) DO UPDATE SET req_count={req_count};'''
        cur.execute(q)
        conn.commit()
        cur.close()

    except sqlite3.Error as error:
        logging.error(f'Connection error, {error}')

    finally:
        conn.close()
        logging.info(f'Became req_count = {req_count}')


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


if req_count <= 200 and req_count is not None:
    try:
        graph = facebook.GraphAPI(access_token)
        posts = graph.get_connections(user, "posts", fields=all_fields)
        req_count += 1
        #load = json.dumps(posts, sort_keys=True, indent=4, ensure_ascii=False)
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
                posts = requests.get(posts["paging"]["next"]).json()
                df = pandas.json_normalize(posts['data'])
                req_count += 1
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

if req_count > 200:
    logging.error(f'Превыщен лимит запросов на данный час {date}')

update_requests_check(date, req_count)



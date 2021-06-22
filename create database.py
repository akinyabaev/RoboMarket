import sqlite3
import time

try:
    conn = sqlite3.connect('facebook.db')
    cur = conn.cursor()
    q = 'Drop table if exists api_requests'
    cur.execute(q)
    conn.commit()
    print("Successful connect")
    query = 'CREATE TABLE api_requests (Date TEXT, Hour INTEGER, req_count INTEGER );'
    print(query)
    cur.execute(query)
    conn.commit()
    print("Successful creating table")
    cur.close()


except sqlite3.Error as error:
    print("Ошибка при подключении к sqlite", error)

finally:
    conn.close()



# -*- coding: utf-8 -*-
"""
Created on Thu Jul 03 13:33:56 2014

@author: kohei.ito
"""

# -*- coding: utf-8 -*-

import psycopg2 
import pandas.io.sql as sqlio

# コネクション作成
conn = psycopg2.connect(
  database='***',
  user='***',
  password='***',
  host='***',
  port='***')
    
cur = conn.cursor()
sql = "select * from tablename;"
df = sqlio.read_sql(sql, conn)
print df.to_string()

conn.close()  # We've created our dataframe, so we can discard our connection now...

cur.fetchone()

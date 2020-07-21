#!/usr/bin/env python
# coding: utf-8

# Compare de-prod-etl-proj-ya5acmz0.claritin_event_lake.events_tbl with markets.ODL_execution_stage.
# 
# Using notebook due to BQ perimeter limitation on projects.

# In[287]:


import pandas as pd
import numpy as np
import pyrds
from pyrds.data import gbq_query
from matplotlib import pyplot as plt
import json

PROJECT = 'de-prod-warehouse-evdz5h3o'
CORE_PROJECT = "dsr-core-proj-rcjtxz9f"


# Read Claritin dataset

# In[278]:


sql = '''
    select timestamp,
        JSON_EXTRACT_SCALAR(data, '$.orderId') as order_id,
        JSON_EXTRACT_SCALAR(data_rows, '$.trade_id') as trade_id,
        upper(JSON_EXTRACT_SCALAR(data, '$.exchange')) as exchange,
        upper(JSON_EXTRACT_SCALAR(data, '$.orderDetails.base')) as base,
        upper(JSON_EXTRACT_SCALAR(data, '$.orderDetails.counter')) as counter,
        JSON_EXTRACT(data_rows, '$.price') as price,
        JSON_EXTRACT(data_rows, '$.amount') as amount,
        JSON_EXTRACT(data_rows, '$.proceeds') as proceeds,
        upper(JSON_EXTRACT_SCALAR(data_rows, '$.side')) as side,
        from
        de-prod-etl-proj-ya5acmz0.claritin_event_lake.events_tbl
        ,UNNEST (JSON_EXTRACT_ARRAY(data, '$.orderTrades')) as data_rows
        where schemaType='OrderTradesData' and date(timestamp) <= DATE_SUB(current_date(), INTERVAL 1 DAY) 
        order by timestamp
'''
df_claritin = gbq_query(sql, project=PROJECT)
df_claritin


# Read ODL_executions_stage table starting from the earlist timestamp in Claritin dataset

# In[283]:


earlist_claritin_ts = df_claritin.iloc[0]['timestamp']
sql = '''
    SELECT execution_time as timestamp,order_id,execution_id as trade_id,upper(exchange) as exchange,
    upper(base) as base,upper(counter) as counter,
    price,size,upper(side) as side FROM `dsr-core-proj-rcjtxz9f.markets.ODL_executions_stage`
    where execution_time >= '{}' and date(execution_time) <= DATE_SUB(current_date(), INTERVAL 1 DAY) 
    order by execution_time
'''.format(str(earlist_claritin_ts))
df_oes = gbq_query(sql, project=CORE_PROJECT)
df_oes


# Joined the 2 datasets

# In[284]:


df_claritin_indexed = df_claritin.set_index(['order_id', 'trade_id'])
df_oes_indexed = df_oes.set_index(['order_id', 'trade_id'])
df_joined = df_claritin_indexed.join(df_oes_indexed, lsuffix='_claritin', rsuffix='_oes')
df_joined


# Validate row level correctness

# In[288]:


unmatch = []
threshold = 0.05
timestamp_threshold_in_seconds = 60

exact_match = ['exchange', 'base', 'counter', 'side']
for idx, row in df_joined.iterrows():
    err_items = []
    for _metric in exact_match:
        _val_left = row["{}_claritin".format(_metric)]
        _val_right = row["{}_oes".format(_metric)]
        if pd.isnull(_val_left) or pd.isnull(_val_right):
            err_items.append("item missing")
        elif _val_left != _val_right:
            err_items.append(_metric)
            
    # compare timestamp
    _second_diff = (row['timestamp_claritin'] - row['timestamp_oes']).total_seconds()
    if _second_diff > timestamp_threshold_in_seconds:
        err_items.append("timestamp")
    
    # check amount
    amount_oes = float(row["size"])
    if pd.isnull(amount_oes) or pd.isnull(row['side_oes']):
        continue
    amount_claritin = float(row["proceeds"] if row['side_oes'].upper() == 'BUY' else row["amount"])
    if ((amount_oes - amount_claritin) / amount_claritin >= threshold):
        err_items.append("amount")
        
    if len(err_items) > 0:
        _unmatch_row = list(row.values) + [",".join(err_items)]
        unmatch.append(_unmatch_row)

df_mismatch = pd.DataFrame(unmatch, columns=list(df_joined.columns) + ["unmatched_items"])
df_mismatch


# No mismatch found. Take a look at missing values

# In[286]:


print(df_joined.isna().sum())
df_missing = df_claritin_indexed[~df_claritin_indexed.isin(df_oes_indexed)].dropna()
df_missing


# 12 records from claritin_event_lake.events_tbl misses in ODL_execution_stage

# Missing data distribution

# In[297]:


df_missing['date'] = df_missing['timestamp'].dt.date
pd.DataFrame(df_missing.groupby('date')['timestamp'].count()).reset_index().rename(columns={"timestamp":"missing_cnt"})


# In[ ]:





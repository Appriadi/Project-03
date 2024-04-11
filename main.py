import os
import connection
import sqlparse
import pandas as pd

if __name__ =='_main_':
 
  #conection data source
  config = conection.config('marketplace_prod')
  coon, engine = conection.get_conn(conf, 'datasource')
  cursor = conn.cursor()

  #conection data source
  conf_dwh = conection.config('dwh')
  conn_dwh,engine_dwh = conection.get_conn(conf_dwh, 'Data source')
  cursor_dwh = conn_dwh.cursor9()

# get query string
path_query = os.gercwd()+'/query/'
quary = sqlparse.format(
    open(path_query+'query.sql','r'),strip_comments=True
).strip()
dwh_design = sqlparse.format(
    open(path_query+'dwh_design','r'),strip_comments=True
).strip()

try:
    print('[INFO] service etl is running..')
    df = pd.read_sql(quary, engine)
    print(df)
except Exception as e:
    print('[INFO] serviceetl is failes')
    print(str(e))
#create schema dwh
cursor_dwh.execute(dwh_design)
conn_dwh.commit()

#ingrest data to dwh
df.to_sql(
    'dim_orders',
    engine_dwh,
    schema = 'apri_dwh',
    if_exists='append',
    index=False
)



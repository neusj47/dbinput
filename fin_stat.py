import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings( 'ignore' )
import numpy as np
from sqlalchemy import create_engine


# 데이터프레임 DB 입력하기
# 0. 데이터 가져오기
# 1. 대상 일자 추출하기
# 2. 일별 데이터프레임 만들기
# 3. DB에 입력하기

# 0. 데이터 가져오기
df_input = pd.read_excel('C:/Users/Check/Desktop/bzm.xlsx', sheet_name= 'Sheet3')

# 1. 대상 일자 추출하기
df_adj = df_input.iloc[7:len(df_input)]
df_adj = df_adj.rename(columns = df_adj.iloc[0])
df_adj = df_adj.drop(df_adj.index[0]).reset_index(drop=True)
tgt_date = df_adj.columns[6:len(df_adj.columns)]

# 2. 데이터프레임 만들기
columns = df_adj[['Symbol','Symbol Name']].drop_duplicates().reset_index(drop=True)
df_all = pd.DataFrame()
for s in range(0,len(tgt_date)) :
    df = pd.DataFrame()
    df = df_adj[df_adj.Symbol == columns.iloc[0].Symbol][['Item Name ',tgt_date[s]]].T
    df = df.rename(columns = df.iloc[0])
    df = df.drop(df.index[0]).reset_index(drop=True)
    df['stddate'] = datetime.strftime(tgt_date[s], "%Y-%m-%d")
    df['code'] = columns.iloc[0].Symbol
    df['company'] = columns.iloc[0]['Symbol Name']
    for i in range(1,len(columns)):
        df_temp = df_adj[df_adj.Symbol == columns.iloc[i].Symbol][['Item Name ',tgt_date[s]]].T
        df_temp = df_temp.rename(columns = df_temp.iloc[0])
        df_temp = df_temp.drop(df_temp.index[0]).reset_index(drop=True)
        df_temp['stddate'] = datetime.strftime(tgt_date[s], "%Y-%m-%d")
        df_temp['code'] = columns.iloc[i].Symbol
        df_temp['company'] = columns.iloc[i]['Symbol Name']
        df = pd.concat([df,df_temp])
    df_all = pd.concat([df,df_all])
    core_col = df_all.columns[:-3].tolist()
    key_col = df_all.columns[len(core_col):].tolist()
    df_all = df_all[key_col + core_col]

# 3. DB 생성하기

# def db_connect():
#     server = '172.200.10.116'
#     db = 'KR_STOCK'
#     user = 'root'
#     pwd = 'sjyoo1~'
#     conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';UID=' + user + ';PWD=' + pwd)
#     cursor = conn.cursor()
#     return conn, cursor

def db_connect():
    user = 'root'
    pwd = 'sjyoo1~'
    dns = 'stock_kr'
    conn = create_engine('mssql+pyodbc://' + user + ':' + pwd + '@' + dns)
    return conn
sql = """
IF NOT EXISTS (SELECT COUNT(stddate) FROM KR_STOCK.DBO.FINSTAT)
   CREATE TABLE KR_STOCK.DBO.FINSTAT (
       [stddate] DATE
      ,[code] VARCHAR(20)
      ,[company] VARCHAR(50)
      ,[총자산(억원)] DECIMAL(10,1)
      ,[총자산(평균)(억원)] DECIMAL(10,1)
      ,[유동자산(억원)] DECIMAL(10,1)
      ,[매출채권(억원)] DECIMAL(10,1)
      ,[재고자산(억원)] DECIMAL(10,1)
      ,[무형자산(억원)] DECIMAL(10,1)
      ,[산업재산권(억원)] DECIMAL(10,1)
      ,[총부채(억원)] DECIMAL(10,1)
      ,[총부채(평균)(억원)] DECIMAL(10,1)
      ,[유동부채(억원)] DECIMAL(10,1)
      ,[장기차입금(억원)] DECIMAL(10,1)
      ,[*총차입부채(억원)] DECIMAL(10,1)
      ,[총자본(억원)] DECIMAL(10,1)
      ,[총자본(평균)(억원)] DECIMAL(10,1)
      ,[자본금(억원)] DECIMAL(10,1)
      ,[자본금(평균)(억원)] DECIMAL(10,1)
      ,[*유보액(억원)] DECIMAL(10,1)
      ,[*유보액(3년평균)(억원)] DECIMAL(10,1)
	  , PRIMARY KEY (stddate, code)
	)
"""
conn = db_connect()
conn.execute(sql)

# 4. DB 입력하기
table_name = 'finstat'
conn = db_connect()

num_of_grp = 1000
grp_idx = np.linspace(0, len(df_all), num_of_grp)
grp_idx = np.ceil(grp_idx).astype(int)

for i in range(num_of_grp - 1):
    if i == (num_of_grp - 1):
        df_part = df_all.iloc[grp_idx[i]:len(df_all)]
    else:
        df_part = df_all.iloc[grp_idx[i]:grp_idx[i + 1]]
    df_part.to_sql(name=table_name, con=conn, if_exists='append', index=False, chunksize=100, method='multi')
conn.dispose()


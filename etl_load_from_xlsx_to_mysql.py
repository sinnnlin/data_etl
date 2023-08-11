from sqlalchemy import create_engine
import pandas as pd
import os

user='root'
password='123456789'
host='localhost'
port='3306'
db= 'project'
dir = r'D:\programing\git\data_etl'


class Etl_from_xlsx_to_mysql():
    def __init__(self,user,password,host,port,db,dir):
        self.user = user
        self.password = password
        self.host = host
        self.db = db
        self.port = port
        self.dir = dir
        
        
    def extract(self):
        try:
            filepath = self.dir
            for filename in os.listdir(filepath):
                if filename.endswith(".xlsx"):
                    file_wo_ext = os.path.splitext(filename)[0]
                    f = os.path.join(filepath, filename)
                    if os.path.isfile(f):
                        df = pd.read_excel(f)
                        self.load(df, file_wo_ext.lower())
        except Exception as e:
            print("Data extract error: " + str(e))
    #load data to postgres
    def load(self, df, tbl):
        try:
            rows_imported = 0
            engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
            print("opened database successfully")
            print(f'importing rows {rows_imported} to {rows_imported + len(df)}... ')
            df.to_sql(f"stg_{tbl}", engine, if_exists='replace', index=False)
            rows_imported += len(df)
            print("Data imported successful")
        except Exception as e:
            print("Data load error: " + str(e))

if __name__ == "__main__":
    df = Etl_from_xlsx_to_mysql(user,password,host,port,db,dir)
    df.extract()
    

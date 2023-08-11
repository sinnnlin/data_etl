from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

user = 'postgres'
pwd = '123456789'
server = "localhost"
db = 'project'
port = "5432"
dir = r'D:\programing\git\data_etl'




class Etl_from_xlsx():
    def __init__(self,pwd,uid,server,db,port,dir):
        self.pwd = pwd
        self.uid = uid
        self.server = server
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
                        self.load(df, file_wo_ext)
        except Exception as e:
            print("Data extract error: " + str(e))
            
    #load data to postgres
    def load(self, df, tbl):
        try:
            rows_imported = 0
            engine = create_engine(f'postgresql://{self.uid}:{self.pwd}@{self.server}:{self.port}/{self.db}')
            print("opened database successfully")
            print(f'importing rows {rows_imported} to {rows_imported + len(df)}... ')
            df.to_sql(f"stg_{tbl}", engine, if_exists='replace', index=False)
            rows_imported += len(df)
            print("Data imported successful")
        except Exception as e:
            print("Data load error: " + str(e))

if __name__ == "__main__":
    df = Etl_from_xlsx(pwd,user,server,db,port,dir)
    df.extract()
    

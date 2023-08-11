from sqlalchemy import create_engine ,text
import pandas as pd


user='root'
password='123456789'
host='localhost'
port='3306'
db= 'sql_database'
dir = r'D:\programing\git\data_etl'
table = 'branch'

class MysqlToDf():
    def __init__(self,user,password,host,port,db,dir):
        self.user = user
        self.password = password
        self.host = host
        self.db = db
        self.port = port
        self.dir = dir
    
    def extract(self):
        try:
            engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
            print("opened database successfully")
            sql_cmd = f"SELECT * FROM {table}"
            df = pd.read_sql(text(sql_cmd), engine.connect())
            print(df)
            self.load_df(df)
        except Exception as e:
            print("Data extract error: " + str(e))
            
    def load_df(self,df):
        df.to_csv('student.csv',index=False)
    
if __name__ == "__main__":
    df = MysqlToDf(user,password,host,port,db,dir)
    df.extract()
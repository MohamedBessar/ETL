import pyodbc
import sqlalchemy
from dotenv import load_dotenv
import os

load_dotenv()

class PrepareDatabase:
    def __init__(self, server="localhost", database="Y_ETL", driver = '{MYSQL}', user="root", pwd="1322000"):
        self.server = server
        self.database = database
        self.driver = driver
        self.user = user
        self.pwd = pwd

        self.connection = self.get_conn()

        self.search_database()

        self.engine = self.get_sql_alchemy_conn()


    def build_conn_str(self, database = 'master'):
        connection_string = f'DRIVER={{MYSQL}}; SERVER={self.server}; DATABASE={database}; UID={self.user};PWD={self.pwd};TrustServerCertificate=yes;'
        return connection_string
    
    def get_conn(self, database = 'sys'):
        try:
            conn = pyodbc.connect(self.build_conn_str(database=database), autocommit=True)
            self.connection = conn
            return conn
        except Exception as e:
            print(e)
            raise
    
    def search_database(self):
        sql = f"""show databases like '{self.database}'"""
        cursor = self.connection.cursor()
        cursor.execute(sql)

        returned_row_counts = len(cursor.fetchall())
        if returned_row_counts:
            print(f"The Database ({self.database}) Exists.")
        else:
            self.create_database()
            self.create_schema()
    
    def create_database(self):
        statement = f"""CREATE DATABASE {self.database}"""
        self.connection.cursor().execute(statement)
        print(f"{self.database} was created successfully!")
        conn = self.get_conn(database=self.database)
        return conn
    
    def get_sql_alchemy_conn(self):
        try:
            engine = sqlalchemy.create_engine(f"mysql+pymysql://root:1322000@localhost/Y_ETL")
            #engine = sqlalchemy.create_engine(f"mssql+pyodbc://{self.user}:{self.pwd}@{self.server}/{self.database}")
            return engine
        except Exception as e:
            print(e)
            raise

    def create_schema(self):
        schema_list = ["STATGING", "ZERO", "DWH"]
        for schema in schema_list:
            sql = f"create schema {schema}"
            try:
                self.connection.cursor().execute(sql)
                print(f"SCHEMA {schema} has been created successfully")
            except Exception as e:
                print(e)
                raise




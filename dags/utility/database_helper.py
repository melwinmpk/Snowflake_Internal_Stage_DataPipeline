from mysql.connector import connect
import snowflake.connector
import json
import pandas as pd

class database_helper:
    def __init__(self,data = 'scrapylearning'):
        database_name = data
        self.create_connection({"database_name":database_name})

    def create_connection(self, data):
        # refer to understand the connection https://www.red-gate.com/simple-talk/databases/mysql/retrieving-mysql-data-python/
        self.conn = connect(option_files = '/home/de/config/mysqldbconnectors.cnf')
        self.conn.database = data['database_name']
        self.conn.autocommit = True
        self.curr = self.conn.cursor()

    def query_exec(self,query):
        self.curr.execute(query)

    def query_exec_getresult(self,query):
        result_df = pd.read_sql(query, self.conn) # reading in Pandas Data frame
        return result_df

    def connection_close(self):
        self.conn.close()

    def __del__(self):
        self.conn.close() 


class snowflake_helper:
    def __init__(self, data):
        self.create_connection(data)

    def create_connection(self, data):
    # To understna myconnection.toml file
    # refer https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#connecting-using-the-connections-toml-file
        self.conn = snowflake.connector.connect(connection_name='myconnection',
                                                database = data.get('database_name','amazonebooks'),
                                                warehouse = data.get('warehouse','COMPUTE_WH'),
                                                schema = data.get('schema','PUBLIC')
                                                )
        self.curr = self.conn.cursor()

    def query_exec(self,query):
        self.curr.execute(query)

    def query_exec_getresult(self,query):
        result_df = pd.read_sql(query, self.conn) # reading in Pandas Data frame
        return result_df

    def connection_close(self):
        self.conn.close()

    def __del__(self):
        self.conn.close()
    

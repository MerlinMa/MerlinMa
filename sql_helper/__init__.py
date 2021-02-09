"""
sql_helper
-------------------
TODO docstring

https://docs.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-sql-server?view=sql-server-ver15
"""

import os
import json
import pandas as pd
import pyodbc

class SQLhelper:
    """ TODO docstring """

    def __init__(self, config_filename: str):
        config_filename = '../' + config_filename
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_filename)
        with open(filepath) as json_file:
            self.config = json.load(json_file)
        self.config = self.config['sql_info']

        connect_str = ';'.join([
            'DRIVER={SQL Server}',
            'SERVER=' + self.config['server'],
            'DATABASE=' + self.config['database'],
            'UID=' + self.config['username'],
            'PWD=' + self.config['password'],
        ])
        self.connection = pyodbc.connect(connect_str)
        self.cursor = self.connection.cursor()

    def execute(self, sql: str):
        """ TODO docstring """
        cursor = self.connection.cursor()
        result = cursor.execute(sql)
        self.connection.commit()
        cursor.close()
        return result


    def insert(self, table: str, att_list: list(str), values: list, cursor=None):
        """ TODO docstring """

        att_list =  "'" + "', '".join(att_list)
        values = "'" + "','".join(values)

        self.execute(f"INSERT INTO {table} ({att_list}) VALUES ({values})")


    def upload(self, table: str, data: pd.DataFrame, att_list: list(str) = None, index_name: str = 'index'):
        """ TODO docstring """
        if att_list is None:
            att_list = data.columns

        # TODO check length of att_list vs number of columns in data

        for index, row in data.iterrows():
            self.insert(table, att_list.insert(index_name, 0), row.insert(index, 0))

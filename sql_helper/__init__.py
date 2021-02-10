"""
sql_helper
-------------------
This module is used to upload data to a SQL server database during a PALS excecution
Inspiration:
https://docs.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-sql-server?view=sql-server-ver15
"""

import os
import json
from typing import List
import pandas as pd
import pyodbc

class SQLhelper:
    """
    This class allows one to connect their PALS execution to a SQL database
    The config file should have an attribute 'sql_info'
        which contains feilds for 'server', 'database', and 'schema'
    The connection is established using Windows Authentication
    Make sure that your PALS server has access to the server and database you wish to connect to
    """

    def __init__(self, config_filename: str, request_id: int, run_id: int):
        self.request_id = request_id
        self.run_id = run_id
        config_filename = '../' + config_filename
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_filename)
        with open(filepath) as json_file:
            self.config = json.load(json_file)
        self.config = self.config['sql_info']

        for driver in pyodbc.drivers():
            try:
                connect_str = ';'.join([
                    'Driver={' + driver + '}',
                    'Server=' + self.config['server'],
                    'Database=' + self.config['database'],
                    'Trusted_Connection=yes'
                ])
                self.connection = pyodbc.connect(connect_str)
            except:
                continue

        self.cursor = self.connection.cursor()


    def execute(self, query: str):
        """ Executes the given SQL query """
        print(query)
        cursor = self.connection.cursor()
        result = cursor.execute(query)
        self.connection.commit()
        cursor.close()
        return result


    def insert(self, table: str, values: List[str], att_list: List[str] = None):
        """ Executes an insert query into the given table """
        values = "', '".join(values)

        if att_list is None:
            query = f"INSERT INTO [{self.config['schema']}].[{table}] VALUES (\'{values}\')"
        else:
            att_list = "], [".join(att_list)
            query = f"INSERT INTO [{self.config['schema']}].[{table}] ([{att_list}]) VALUES (\'{values}\')"

        self.execute(query)


    def upload_tag(self, table: str, timestamps: List, tag_name: str, values):
        """ Uploads the tag data to the given table """
        for i, value in enumerate(values):
            data_list = [str(self.request_id), str(self.run_id), str(timestamps[i]), tag_name, str(value)]
            self.insert(table, data_list)


    def upload_df(self, table: str, timestamps: List, data: pd.DataFrame):
        """ TODO docstring """
        for col in data.columns:
            self.upload_tag(table, timestamps, col, data[col])

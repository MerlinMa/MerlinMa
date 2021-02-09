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

    def __init__(self, config_filename: str):
        config_filename = '../' + config_filename
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_filename)
        with open(filepath) as json_file:
            self.config = json.load(json_file)
        self.config = self.config['sql_info']

        connect_str = ';'.join([
            'Driver={SQL Server}',
            'Server=' + self.config['server'],
            'Database=' + self.config['database'],
            'Trusted_Connection=yes'
        ])
        self.connection = pyodbc.connect(connect_str)
        self.cursor = self.connection.cursor()


    def execute(self, query: str):
        """ Executes the given SQL query """
        cursor = self.connection.cursor()
        result = cursor.execute(query)
        self.connection.commit()
        cursor.close()
        return result


    def insert(self, table: str, values: List[str], att_list: List[str] = None):
        """ Executes an insert query into the given table """
        if att_list is not None:
            att_list = "], [".join(att_list)

        values = "', '".join(values)

        if att_list is None:
            query = f"INSERT INTO [{self.config['schema']}].[{table}] VALUES (\'{values}\')"
        else:
            query = f"INSERT INTO [{self.config['schema']}].[{table}] ([{att_list}]) VALUES (\'{values}\')"

        self.execute(query)


    def upload_tag(self, table: str, timestamps, tag_name: str, values):
        """ Uploads the tag data to the given table """
        for i, value in enumerate(values):
            data_list = [
                str(timestamps[i]),
                tag_name,
                str(value)]
            self.insert(table, data_list)


    # def upload(self, table: str, data: pd.DataFrame, att_list: List[str] = None, index_name: str = 'index'):
    #     """ TODO docstring """
    #     if att_list is None:
    #         att_list = list(data.columns.values)
    #     att_list.insert(0, index_name)

    #     for index, row in data.iterrows():
    #         row = list(row)
    #         row.insert(0, index)
    #         self.insert(table, att_list, row)

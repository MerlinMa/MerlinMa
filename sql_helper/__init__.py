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
        which contains feilds for 'server', 'database', and 'default_schema'
    The connection is established using Windows Authentication
    Make sure that your PALS server has access to the server and database you wish to connect to
    """

    def __init__(self, config_filename: str, main_entry_point_args: dict, verbose: bool = True):
        self.verbose = verbose
        config_filename = '../' + config_filename
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_filename)
        with open(filepath) as json_file:
            self.config = json.load(json_file)
        self.config = self.config['sql_info']
        self.default_schema = self.config['default_schema']

        # retrieve requestID and runID from main_entry_point_args
        if main_entry_point_args['Version'] >= '1.1':
            self.request_id = main_entry_point_args['PALS']['RequestKey']
            self.run_id = main_entry_point_args['PALS']['RunKey']
        else:
            self.request_id = '0'
            self.run_id = '0'

        # check that the required driver is installed
        current_driver = 'ODBC Driver 13 for SQL Server'
        if current_driver not in pyodbc.drivers():
            raise OSError(f"""\n
            {current_driver} is not installed.\n
            See here for installation: https://www.microsoft.com/en-us/download/details.aspx?id=50420\n
            The drivers available in the current environment are: {pyodbc.drivers()}""")

        # create connection to the server and database
        connect_str = ';'.join([
            'Driver={' + current_driver + '}',
            'Server=' + self.config['server'],
            'Database=' + self.config['database'],
            'Trusted_Connection=yes'
        ])
        pyodbc.pooling = False
        self.connection = pyodbc.connect(connect_str)
        self.connection.autocommit = True

    def upload_tag(self,
                   table: str,
                   tag_name: str,
                   values,
                   schema: str = None
                   ):
        """ Uploads the tag data to the given table """
        if schema is None:
            schema = self.default_schema

        query = f"INSERT INTO [{schema}].[{table}] VALUES "

        for index, value in values.items():
            data_list = [self.request_id, self.run_id, str(index), tag_name, str(value)]
            values = "', '".join(data_list)
            query += f"( \'{values}\' ), "

        query = query[:-2]
        
        self.connection.execute(query)

    def upload_df(self,
                  table: str,
                  data: pd.DataFrame,
                  schema: str = None
                  ):
        """ Uploads a pandas DataFrame to the given table """
        for col in data.columns:
            self.upload_tag(table, col, data[col], schema=schema)

    def upload_dict(self,
                        table: str,
                        data: dict(),
                        schema: str = None
                        ):
        """ Uploads the dictionary of tag data to the given table """
        if schema is None:
            schema = self.default_schema
        
        query = f"INSERT INTO [{schema}].[{table}] VALUES "

        for item in data.get('OutputData'):
            data_list = [data.get('RequestKey'), data.get('RunKey'), item[0], item[1], str(item[2])]
            values = "', '".join(data_list)
            query += f"( \'{values}\' ), "

        query = query[:-2]
        
        self.connection.execute(query)
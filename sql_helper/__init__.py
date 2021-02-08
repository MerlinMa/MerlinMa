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
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_filename)
        with open(filepath) as json_file:
            self.config = json.load(json_file)
        self.config = self.config['sql_info']

        self.connection = pyodbc.connect(
            'DRIVER={SQL Server}'
            + ';SERVER=' + self.config['server']
            + ';DATABASE=' + self.config['database']
            + ';UID=' + self.config['username']
            + ';PWD=' + self.config['password']
            )

    def upload(self, data: pd.DataFrame):
        """ TODO docstring """
        cursor = self.connection.cursor()
        for index, row in data.iterrows():
            continue
            # cursor.execute(
                # f"INSERT INTO  (DepartmentID,Name,GroupName) values(?,?,?)", row.DepartmentID, row.Name, row.GroupName)
        self.connection.commit()
        cursor.close()

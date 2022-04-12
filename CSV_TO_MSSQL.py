import pyodbc
import contextlib
import pandas as pd
import datetime

conn_str = (
    r'Driver=SQL Server;'
    r'Server=LAPTOP-9B5PTUIC;'
    r'Database=master;'
    r'Trusted_Connection=yes;'
    )

conn = pyodbc.connect(conn_str)

sql = "SELECT * FROM [master].[dbo].[timesheet_hours]"

data = pd.read_sql(sql, conn)

file_path = f'csv/data.csv'

data.to_csv(file_path)

print(data)

table_name = '[master].[dbo].[timesheet_hours]'

string = f"BULK INSERT {table_name} FROM {file_path} (WITH FORMAT = 'CSV');"
with contextlib.closing(conn):
    with contextlib.closing(conn.cursor()) as cursor:
        cursor.execute(string.format(table_name, file_path))
        conn.commit()

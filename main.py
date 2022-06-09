from dataclasses import replace
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib

def select_and_insert_to_table():

    # connect to staking server and transform the data to dataframe using read_sql_query
    con_string = pyodbc.connect("driver=ODBC Driver 17 for SQL Server;server=SBNDCDSSTGD.thaibev.com;database=SAP_DATA_Staging;uid=pysapdsusr;pwd=Pys@pdK2xT3Eu$S5;")
    df = pd.read_sql_query('SELECT KUNNR FROM KNVV_B1P',con_string)
    df.rename(columns={
        'KUNNR':'CUSTOMER_MASTER'
    },inplace=True)
    
    # insert dataframe to data mart server using to_sql
    quoted = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=SBNDCDSWHDEV.thaibev.com;DATABASE=DATA_WH_DEV;uid=pysapdsusr;pwd=Pys@pdK2xT3Eu$S5;")
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    df.to_sql('O_OUTPUT_SALES_VIEW_B1P', schema='dbo', con = engine, if_exists='replace', index=False)
    
if __name__ == '__main__':
    select_and_insert_to_table()
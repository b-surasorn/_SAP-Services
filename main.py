from dataclasses import replace
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib

def select_and_insert_to_table():

    # connect to staking server
    con_string = pyodbc.connect("driver=ODBC Driver 17 for SQL Server;server=SBNDCDSSTGD.thaibev.com;database=SAP_DATA_Staging;uid=pysapdsusr;pwd=Pys@pdK2xT3Eu$S5;")

    # transform a query data to dataframe using read_sql_query
    df = pd.read_sql('SELECT KUNNR FROM KNVV_B1P',con_string)

    # connect to staking server and transform the data to dataframe using read_sql_query
    con_string = pyodbc.connect("driver=ODBC Driver 17 for SQL Server;server=SBNDCDSSTGD.thaibev.com;database=SAP_DATA_Staging;uid=pysapdsusr;pwd=Pys@pdK2xT3Eu$S5;")
    df = pd.read_sql_query('SELECT KUNNR FROM KNVV_B1P',con_string)
    df.rename(columns={
        'KUNNR':'CUSTOMER_MASTER'
    },inplace=True)
    # print(df)

    # connect to data mart server
    constring = "mssql+pyodbc://pysapdsusr:Pys@pdK2xT3Eu$S5@SBNDCDSWHDEV.thaibev.com/DATA_WH_DEV?driver=ODBC Driver 17 for SQL Server"   
    dbEngine = create_engine(constring,fast_executemany=True, connect_args={'connect_timeout': 10}, echo=False)
    
    # check a connection of data mart server, print 'Engine invalid' if connected
    try:
        with dbEngine.connect() as con:
            con.execute("SELECT 1")
        print('engine is valid')
    except Exception as e:
        print(f'Engine invalid: {str(e)}')

    # insert dataframe to data mart server using to_sql
    df.to_sql(con=dbEngine, schema="dbo", name="O_OUTPUT_SALES_VIEW_B1P", if_exists="replace", index=False, chunksize=1000)

if __name__ == '__main__':
    select_and_insert_to_table()

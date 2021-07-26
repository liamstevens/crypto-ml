from prophet import Prophet
import pandas as pd
import postgresql

dbhost = os.environ['DBHOST']
dbport = os.environ['DBPORT']
dbuser = os.environ['DBUSER']
dbpass = os.environ['DBCREDS']


def load_data(target_table):
    dbconn = postgresql.open(f'pq://{dbuser}:{dbpass}@{dbhost}:{dbport}/postgres')
    dbconn.execute(f'DECLARE {target_table}_CUR CURSOR WITH HOLD FOR SELECT * from cryptoml.{target_table}')
    

def transform_data(dbconnection, target_table):
    #Load data
    c = dbconnection.cursor_from_id(target_table+'_CUR')
    results = c.read(quantity=1000)
    #extract data from list of tuples
    #TODO Approximate datetime objects using floor
    #TODO transform Decimal objects to float
    #TODO generate pandas df for each column
    for line in results:

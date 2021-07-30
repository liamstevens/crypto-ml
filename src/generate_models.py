from prophet import Prophet
import pandas as pd
import postgresql
import os
import datetime


dbhost = os.environ['DBHOST']
dbport = os.environ['DBPORT']
dbuser = os.environ['DBUSER']
dbpass = os.environ['DBCREDS']


def create_cursor(target_table):
    dbconn = postgresql.open(f'pq://{dbuser}:{dbpass}@{dbhost}:{dbport}/training_data')
    #dbconn.execute(f'DECLARE {target_table}_CUR CURSOR WITH HOLD FOR SELECT * from cryptoml_data.{target_table};')
    q = dbconn.prepare(f'SELECT * from cryptoml_data.{target_table};')
    c = q.declare()
    return c

def transform_data(cursor, target_table):
    #Load data
    qresults = cursor.read(quantity=1000)
    print(qresults)
    results = []
    '''
    Extract data from list of tuples. Each tuple is of format:
    (index, datetime, volume, price, total, buyer) 
    which is of type
    (int, datetime, decimal, decimal, decimal, bool)
    #TODO generate pandas df for each column'''
    for line in qresults:
        time = line[1]
        roundedtime = time - datetime.timedelta(seconds=time.second,microseconds=time.microsecond)
        results.append((roundedtime, float(line[2]), float(line[3]), float(line[4]), bool(line[5])))
    return results

if __name__ == "__main__":
    
    print(transform_data(create_cursor('btcada'),'btcada'))

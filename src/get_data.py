import scipy
import os
import time
import postgresql
from binance.client import Client as binclient

binanceID = os.environ['BINANCE_ID']
binanceKey = os.environ['BINANCE_KEY']
dbhost = os.environ['DBHOST']
dbport = os.environ['DBPORT']
dbuser = os.environ['DBUSER']
dbpass = os.environ['DBCREDS']

def create_training_database(dbconn):
    try:
        dbconn.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')
    except:
        pass
    try:
        dbconn.execute('CREATE DATABASE training_data OWNER '+dbuser+';')
    except:
        print("Unable to create database")

def create_coinpair_table(primary, secondary, dbconn):
    try:
        dbconn.execute('CREATE SCHEMA cryptoml_data;')
    except:
        pass
    try:
        dbconn.execute('CREATE TABLE cryptoml_data.'+primary+secondary+' (id serial primary key, datetime timestamp, volume numeric, price numeric, total numeric, buyer boolean);')
    except:
        pass
    #Need to batch fetches of historical data and work backwards.
    b = binclient(binanceID,binanceKey)
    trades = []
    #get the most recent 1000 historical trades
    recenttrades = b.get_historical_trades(symbol=f"{secondary}{primary}",limit=1000)
    recent = recenttrades[0]
    trades.append(recenttrades)
    recenttrades = b.get_historical_trades(symbol=f"{secondary}{primary}",limit=1000)
    #Use that to get the previous 1000 trades
    #continue to go further back by 1000 until we have 90 days of data
    #90*24*60*60=7,776,000 seconds
    while recent['time']-recenttrades[0]['time'] < 7776000:
        time.sleep(5)
        print(f"Got trades: baseID:{recent['id']-1000}")
        recenttrades = b.get_historical_trades(symbol=f"{secondary}{primary}",limit=1000,fromId=recent['id']-1000)
        #pump into database
        for trade in recenttrades:
            try:
                dbconn.execute(f"INSERT INTO cryptoml_data.{primary}{secondary} (datetime, volume, price, total, buyer) VALUES (to_timestamp({trade['time']/1000}), {trade['qty']}::numeric, {trade['price']}::numeric, {trade['quoteQty']}::numeric, {bool(trade['isBuyerMaker'])})")
            except Exception as e:
                print(f"dbexecution failed: {e}")
                continue
        recent = recenttrades[0]
        trades.append(recenttrades)
        b = binclient(binanceID,binanceKey)
    
        
def load_training_data(primary,secondary,dbconn,model):
    return None

def plot_training_data(primary,secondary,dbconn,model):
    return None

if __name__ == '__main__':
    dbconn = postgresql.open(f'pq://{dbuser}:{dbpass}@{dbhost}:{dbport}/postgres')
    create_training_database(dbconn)
    dbconn = postgresql.open(f'pq://{dbuser}:{dbpass}@{dbhost}:{dbport}/training_data')
    create_coinpair_table('BTC','ETH',dbconn)

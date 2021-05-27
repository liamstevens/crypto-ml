import scipy
import os
import postgresql
from binance.client import Client as binclient

binanceID = os.environ['BINANCE_ID']
binanceKey = os.environ['BINANCE_KEY']
dbhost = os.environ['DBHOST']
dbport = os.environ['DBPORT']
dbuser = os.environ['DBUSER']
dbcreds = os.environ['DBCREDS']

def create_training_table(dbconn):
    dbconn.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')
    dbconn.execute('CREATE DATABASE IF NOT EXISTS training_data OWNER'+dbuser+';')
    dbconn.execute('CREATE SCHEMA IF NOT EXISTS cryptoml_data;')

def create_coinpair_table(primary, secondary, dbconn):
    dbconn.execute('CREATE TABLE IF NOT EXISTS cryptoml_data.'+primary+secondary+' (id serial primary key, datetime timestamp, volume numeric, price numeric, total numeric, buyer boolean);')
    #Need to batch fetches of historical data and work backwards.
    b = binclient(binanceID,binanceKey)
    trades = []
    #get the most recent historical trade
    recent = b.get_historical_trades(symbol=f"{secondary}{primary}",limit=1)
    #Use that to get the previous 1000 trades
    recenttrades = b.get_historical_trades(symbol=f"{secondary}{primary}",limit=1000,fromId=recent.id)
    trades.append(recenttrades)
    #continue to go further back by 1000 until we have 90 days of data
    #90*24*60*60=7,776,000 seconds
    while recent.time-recenttrades[0].time < 7776000:
        recenttrades = b.get_historical_trades(symbol=f"{secondary}{primary}",limit=1000,fromId=recent.id)
        trades.append(recenttrades)
    #pump into database
    for trade in trades:
        dbconn.execute(f"INSERT INTO cryptoml_data.+{primary}+{secondary} (datetime timestamp, volume numeric, price numeric, total numeric, buyer boolean) VALUES ({trade.time}, {trade.qty}, {trade.price}, {trade.quoteQty}, {trade.isBuyerMaker})")

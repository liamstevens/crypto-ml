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
    dbconn.execute('CREATE TABLE IF NOT EXISTS cryptoml_data.'+primary+secondary+' (id serial primary key, datetime timestamp, volume numeric, maker boolean);')
    #Need to batch fetches of historical data and work backwards.
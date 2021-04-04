'''
The State Manager component of Crypto-ML. The State Manager is responsible for managing the database, as well as being
the middleman for passing messages between child processes. For more information, see the Technical Design
section of the Confluence.
'''
import postgresql
import os
dbhost = os.environ['DBHOST']
dbport = os.environ['DBPORT']
dbuser = os.environ['DBUSER']
dbcreds = os.environ['DBCREDS']
dbpass = (open(dbcreds,'r').read())


def connect_db(db='postgres'):
    dbconn = postgresql.open(f'pg://{dbuser}:{dbpass}@{dbhost}:{dbport}/{db}'
    return dbconn

def create_db(dbname='trade',dbconn):
    dbconn.execute('CREATE DATABASE '+dbname+' OWNER'+dbuser+';')
    dbconn.execute('CREATE SCHEMA cryptoml;')
    dbconn.execute('CREATE TABLE state ( guid, datetime, accountID, nodeID, num_worker, wallet_state, config);')
    dbconn.execute('CREATE TABLE transaction ( guid, creation_time, source, destination, vol_s, vol_d, complete,complete_time, stateguid );')
    dbconn.execute('CREATE TABLE exchange_creds ( guid, config_guid, accesskey, accessID );')
    dbconn.execute('CREATE TABLE node_configuration ( guid, config_guid, strategy, nodeID, start_time );')
    dbconn.execute('CREATE TABLE configuration ( guid, base_investment, state_guid, nodeconf_guid, accountID, creds_guid, last_update, realise_value, realise_target );')
    dbconn.execute('CREATE TABLE wallet_state ( guid, exchange, coin, volume, state_guid );')

def create_schema():

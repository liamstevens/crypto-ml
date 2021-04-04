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
    dbconn.execute('CREATE SCHEMA cryptoml')
    dbconn.execute('CREATE TABLE state ( guid, datetime, accountID, nodeID, numWorker, walletstate, config)')
    

def create_schema():

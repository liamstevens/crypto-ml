'''
The State Manager component of Crypto-ML. The State Manager is responsible for managing the database, as well as being
the middleman for passing messages between child processes. For more information, see the Technical Design
section of the Confluence.
'''
import postgresql
import os
from socket import AF_INET, socket, SOCK_STREAM
from threading import Thread
#Load in environment variables
dbhost = os.environ['DBHOST']
dbport = os.environ['DBPORT']
dbuser = os.environ['DBUSER']
dbcreds = os.environ['DBCREDS']
dbpass = (open(dbcreds,'r').read())

#Set up localhost to accept connections from clients
localhost = ''
port = 31415
buffersize = 4096
addr = (localhost, port)
stateserver = socket(AF_INET, SOCK_STREAM)
stateserver.bind(addr)


class StateManager:
    _threads = {}
    databaseconn = None
    '''
    Configure the State Manager to accept connections from worker nodes.
    '''
    def accept_connections(self):
        stateserver.listen(5)
        ACCEPT_THREAD = Thread(target=self.accept_new_connections)
        ACCEPT_THREAD.start()
        ACCEPT_THREAD.join()
        stateserver.close()

    '''
    Returns a database connection object for the operational database.

    @return:
        dbconn: a database connection object
    '''
    def connect_db(self):
        dbconn = postgresql.open(f'pg://{dbuser}:{dbpass}@{dbhost}:{dbport}/{db}')
        self.databaseconn = dbconn
        return dbconn
    
    '''
    Creates a database on a given dbconn object.

    @params:
        dbname: The name of the database to be created. Default: trade

    @return:
        None
    '''
    def create_db(self,dbname='trade',dbconn):
        dbconn.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')
        dbconn.execute('CREATE DATABASE '+dbname+' OWNER'+dbuser+';')
        dbconn.execute('CREATE SCHEMA cryptoml;')
        dbconn.execute('CREATE TABLE state ( guid, datetime, accountID, nodeID, num_worker, wallet_state, config);')
        dbconn.execute('CREATE TABLE transaction ( guid, exchange, creation_time, source, destination, vol_s, vol_d, complete,complete_time, stateguid );')
        dbconn.execute('CREATE TABLE exchange_creds ( guid, config_guid, accesskey, accessID );')
        dbconn.execute('CREATE TABLE node_configuration ( guid, config_guid, strategy, nodeID, start_time );')
        dbconn.execute('CREATE TABLE configuration ( guid, base_investment, state_guid, nodeconf_guid, accountID, creds_guid, last_update, realise_value, realise_target );')
        dbconn.execute('CREATE TABLE wallet_state ( guid, exchange, coin, volume, state_guid );')
    
    '''
    Handler for new connections to the State Manager. Forks off a handler thread for each connection.
    '''
    def accept_new_connections(self):
        while True:
            client, client_address = stateserver.accept()
            Thread(target=self.handle_messaging,args=(client,)).start()
    
    '''
    Message handler. Reads messages as they are posted, parses them and performs actions as required. 

    @params:
        conn: the connection object for the worker node.

    @return:
        None
    '''
    def handle_messaging(self,conn):
        message = conn.recv(buffersize)
        commandwords = {
        "create_transaction" : create_transaction,
        "complete_transaction": complete_transaction,
        "get_transaction": get_transaction,
        "update_wallet": update_wallet,
        "add_creds": add_creds,
        "update_creds": update_creds,
        "add_worker": add_worker,
        "update_worker": update_worker,
        "worker_stop": stop worker
        }
        while True:
            message = conn.recv(buffersize)
            if not message:
                break
            else:
                cmdword = message.split(',')[0]
                func = commandwords.get(cmdword, lambda: "Invalid commandword")
                func(message)

    def create_transaction(self,message):
        args = message.split(',')[1:]
        str_args = ','.join(args)
        dbconn.execute(f"INSERT INTO transaction ( guid, exchange, creation_time, source, destination, vol_s, vol_d, complete,complete_time, stateguid )
        VALUES (gen_random_uuid(),{str_args});" 

        
    


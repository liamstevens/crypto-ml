'''
The State Manager component of Crypto-ML. The State Manager is responsible for managing the database, as well as being
the middleman for passing messages between child processes. For more information, see the Technical Design
section of the Confluence.
'''
import postgresql
import os
import json
import uuid
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
    dbconn = None
    _guid = None
    def __init__(self):
        self._guid = str(uuid.uuid4())    

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
        self.dbconn = dbconn
        return dbconn
    
    '''
    Creates a database on a given dbconn object.

    @params:
        dbname: The name of the database to be created. Default: trade

    @return:
        None
    '''
    def create_db(self,dbname='trade'):
        self.dbconn.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')
        self.dbconn.execute('CREATE DATABASE '+dbname+' OWNER'+dbuser+';')
        self.dbconn.execute('CREATE SCHEMA cryptoml;')
        self.dbconn.execute('CREATE TABLE state ( guid, datetime, accountID, nodeID, num_worker, wallet_state, config);')
        self.dbconn.execute('CREATE TABLE transaction ( guid, exchange, creation_time, source, destination, vol_s, vol_d, complete,complete_time, stateguid );')
        self.dbconn.execute('CREATE TABLE exchange_creds ( guid, config_guid, accesskey, accessID );')
        self.dbconn.execute('CREATE TABLE node_configuration ( guid, config_guid, strategy, nodeID, start_time );')
        self.dbconn.execute('CREATE TABLE configuration ( guid, base_investment, state_guid, nodeconf_guid, accountID, creds_guid, last_update, realise_value, realise_target );')
        self.dbconn.execute('CREATE TABLE wallet_state ( guid, exchange, coin, volume, state_guid );')
        self.dbconn.execute('CREATE TABLE worker ( guid, start, strategy, config_guid, nodeID);')
    
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
                func(message,conn)

    def create_transaction(self,message,conn):
        args = message.split(',')[1:]
        str_args = ','.join(args)
        self.dbconn.execute(f"INSERT INTO transaction ( guid, exchange, creation_time, source, destination, vol_s, vol_d, complete,complete_time, stateguid )
        VALUES (gen_random_uuid(),{str_args},{self.guid});")
        response = f"create_transaction,success,{args[0]}"
        conn.send(response.encode('UTF-8'))
    
    def complete_transaction(self,message,conn):
        args = message.split(',')[1:]
        self.dbconn.execute(f"UPDATE transaction SET (complete,complete_time) = ({args[1]}, {args[2]})
        WHERE guid = {args[0]};")
        response = f"complete_transaction,success,{args[0]}"
        conn.send(response.encode('UTF-8'))

    def get_transaction(self,message,conn):
        args = message.split(',')[1:]
        if ':' not in message:
            response = self.dbconn.execute(f"SELECT ( guid, exchange, creation_time, source, destination, vol_s, vol_d, complete, complete_time)
            FROM transaction
            WHERE guid = {args[0]};")
        else:
            obj = json.loads(args[0])
            keys = obj.keys()
            qstr = ""
            for key in keys:
                qstr.append(key+" = "+obj[key])
                if key != keys[-1]:
                    qstr.append(" AND ")
            response = self.dbconn.execute(f"SELECT ( guid, exchange, creation_time, source, destination, vol_s, vol_d, complete, complete_time)
            FROM transaction
            WHERE {qstr};")
        conn.send(response.encode('UTF-8'))

    def update_wallet(self,message,conn):
        args = message.split(',')[1:]
        self.dbconn.execute(f"UPDATE wallet SET (coin, volume) = ({args[1]}, {args[2]}) WHERE guid = {args[0]};")
        response = f"update_wallet,success,{args[0]}"
        conn.send(response.encode('UTF-8'))

    def add_creds(self,message,conn):
        args = message.split(',')[1:]
        self.dbconn.execute(f"INSERT INTO exchange_creds (guid,config_guid,accesskey,accessID) VALUES (gen_random_uuid(),{args[2]},{args[1]},{args[0]};"

    def update_creds(self,message,conn):
        args = message.split(',')[1:]
        self.dbconn.execute(f"UPDATE exchange_creds SET (accesskey, accessID) = ({args[2]}, {args[1]}) WHERE config_guid = {args[0]};")

    def add_worker(self,message,conn):
        args = message.split(',')[1:]
        self.dbconn.execute(f"INSERT INTO ";)

    def update_worker(self,message,conn):
        args = message.split(','[1:]
        self.dbconn.execute(f"UPDATE worker

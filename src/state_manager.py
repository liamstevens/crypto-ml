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
        self.dbconn.execute('CREATE DATABASE IF NOT EXISTS '+dbname+' OWNER'+dbuser+';')
        self.dbconn.execute('CREATE SCHEMA IF NOT EXISTS cryptoml;')
        self.dbconn.execute('CREATE TABLE IF NOT EXISTS cryptoml.state ( guid uuid primary key, datetime timestamp, accountID varchar(20), nodeID int, num_worker int, wallet_state uuid, config uuid);')
        self.dbconn.execute('CREATE TABLE IF NOT EXISTS cryptoml.transaction ( guid uuid primary key, exchange varchar(20), creation_time timestamp, source varchar(8), destination varchar(8), vol_s numeric, vol_d numeric, complete boolean,complete_time timestamp, stateguid uuid);')
        self.dbconn.execute('CREATE TABLE IF NOT EXISTS cryptoml.exchange_creds ( guid uuid primary key, config_guid uuid, accesskey varchar(30), accessID varchar(30));')
        self.dbconn.execute('CREATE TABLE IF NOT EXISTS cryptoml.node_configuration ( guid uuid primary key, config_guid, strategy, nodeID, start_time );')
        self.dbconn.execute('CREATE TABLE IF NOT EXISTS cryptoml.configuration ( guid uuid primary key, base_investment numeric, state_guid uuid, nodeconf_guid uuid, accountID varchar(20), creds_guid uuid, last_update timestamp, realise_value boolean, realise_target numeric );')
        self.dbconn.execute('CREATE TABLE IF NOT EXISTS cryptoml.wallet_state ( guid uuid primary key, exchange varchar(20), coin varchar(8), volume numeric, state_guid uuid);')
        self.dbconn.execute('CREATE TABLE IF NOT EXISTS cryptoml.worker ( guid uuid primary key, start timestamp, strategy varchar(20), config_guid uuid, nodeID);')
    
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
        "worker_stop": stop_worker
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
        if self.dbconn.execute(f"INSERT INTO transaction ( guid, exchange, creation_time, source, destination, vol_s, vol_d, complete,complete_time, stateguid )
        VALUES (gen_random_uuid(),{str_args},{self.guid});"):
            response = f"create_transaction,success,{args[0]}"
            conn.send(response.encode('UTF-8'))
        
    def complete_transaction(self,message,conn):
        args = message.split(',')[1:]
        if self.dbconn.execute(f"UPDATE transaction SET (complete,complete_time) = ({args[1]}, {args[2]})
        WHERE guid = {args[0]};"):
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
        if self.dbconn.execute(f"UPDATE wallet SET (coin, volume) = ({args[1]}, {args[2]}) WHERE guid = {args[0]};"):
            response = f"update_wallet,success,{args[0]}"
            conn.send(response.encode('UTF-8'))

    def add_creds(self,message,conn):
        args = message.split(',')[1:]
        if self.dbconn.execute(f"INSERT INTO exchange_creds (guid,config_guid,accesskey,accessID) VALUES (gen_random_uuid(),{args[2]},{args[1]},{args[0]});"):
            response = f"add_creds,success,{args[3]}
            conn.send(response.encode('UTF-8'))

    def update_creds(self,message,conn):
        args = message.split(',')[1:]
        if self.dbconn.execute(f"UPDATE exchange_creds SET (accesskey, accessID) = ({args[2]}, {args[1]}) WHERE config_guid = {args[0]};"):
            response = f"update_creds,success,{args[0]}"
            conn.send(response.encode('UTF-8'))

    def add_worker(self,message,conn):
        args = message.split(',')[1:]
        if self.dbconn.execute(f"INSERT INTO worker (guid, start, strategy, config_guid, nodeID) VALUES (gen_random_uuid(), {args[0]}, {args[1]}, {args[2]}, {args[3]});"):
            response = f"add_worker,success,{args[3]}"
            conn.send(response.encode('UTF-8'))

    def update_worker(self,message,conn):
        args = message.split(','[1:]
        if self.dbconn.execute(f"UPDATE worker SET (strategy, config_guid) = ({args[0]}, {args[1]}) WHERE nodeID = {args[2]};"):
            response = f"update_worker,success,{args[2]}"
            conn.send(response.encode('UTF-8'))


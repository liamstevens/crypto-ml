import pandas as pd
import postgresql
import os
import json
import datetime
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json


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

def transform_data(cursor, target_table, q):
    #Load data
    qresults = cursor.read(quantity=q)

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
    frame = pd.DataFrame(results, columns=["Timestamp", "Volume", "Price", "Total", "Buyer-initiated"])
    print(frame)
    return frame

def get_params(model):
    res = {}
    for pname in ['k','m', 'sigma_obs']:
        res[pname] = m.params[pname][0][0]
    for pname in ['delta','beta']:
        res[pname] = m.params[pname][0]
    return res

def generate_model(data, previous=None):
    data = data.loc[:,["Timestamp","Price"]]
    data.columns = ['ds','y']
    if previous:
        m = Prophet(changepoint_prior_scale=0.01).fit(data,init=get_params(previous))
    else:
        m = Prophet(changepoint_prior_scale=0.01).fit(data)
    with open('serialized_model.json', 'w') as fout:
        json.dump(model_to_json(m), fout)
    return m

def train_on_all_data(pair):
    cursor = create_cursor(pair)
    model = None
    data = transform_data(cursor,pair,500000)
    while data:
        model = generate_model(data, previous=model)
        data = transform_data(cursor,pair,500000)
    with open('refined_model.json' 'w') as fout:
        json.dump(model_to_json(m),fout)
    return model

if __name__ == "__main__":
    
    #generate_model((transform_data(create_cursor('btcada'),'btcada',500000)))
    train_on_all_data('btcada')

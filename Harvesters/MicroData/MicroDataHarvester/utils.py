#!/usr/bin/python

import urllib
import requests
import zipfile
from datetime import datetime as dt
import numpy as np
import pandas as pd
from functools import lru_cache
import json
from time import sleep
import os, sys
import re
sys.path.append("../../../API/pyddh")
import ddh

#global ddh_host, ddh_session_key, ddh_session_value, ddh_token, ddh_protocol

def get_params(host_type, idno=None):
    if host_type == "microdata":
        config_params = {
            'protocol' : 'http',
            'host' : 'microdatalib.worldbank.org',
            'data_id' : "{}".format(idno)
            }
        return config_params
    if host_type == "microdata_pub":
        config_params = {
            'protocol' : 'http',
            'host' : 'microdata.worldbank.org',
            'data_id' : "{}".format(idno)
            }
        return config_params
    elif host_type == "ddh2":
        config_params = {
            'protocol' : 'https',
            'host' : 'ddhoutboundapiqa.asestg.worldbank.org'
            }
        return config_params
    

def config(params):
    """
    Sample params dict:
    {
    protocol : 'https'
    host : 'microdatalib.worldbank.org'
    data_id : "NPL_2006_DHS_v01_M_v01_A_IPUMS"
    }
    """
    microdatalib_protocol = params['protocol']
    microdatalib_host = params['host']
    query_string = params['data_id']
    url = url = '{}://{}/index.php/api/catalog/{}?format=json'.format(microdatalib_protocol, microdatalib_host, query_string)
    
    return url

def retry_get_microdata(url):
    response = requests.get(url)
    try:
        result = response.json()
        if not isinstance(result, dict):
            return None
        return result
    except json.JSONDecodeError:
        print(url, response.text)
        
def get_microdata(url):
    sleep(2)
    url = str(url)
    response = requests.get(url)
    try:
        result = response.json()
        if not isinstance(result, dict):
            return None
        return result
    except json.JSONDecodeError:
        retry_get_microdata(url)

#@lru_cache(maxsize=32)
def get_ou_class(token = None):

    id_, idno_, title = [], [], []
    
    ou_req = requests.get("https://microdatalib.worldbank.org/index.php/api/catalog/search?&ps=20000&format=json")
    
    assert ou_req.status_code == 200, "Invalid MicrodataLib request call."
    try:
        ou_res = ou_req.json()['result']

        for i in ou_res['rows']:
            id_.append(i['id'])
            idno_.append(i['idno'])
            title.append(i['title'])

        assert len(id_) == len(idno_) == len(title), "Lists must have same number of elements"

        df = pd.DataFrame(columns = ['id', 'idno', 'title', 'classification'])
        df['id'] = id_
        df['idno'] = idno_
        df['title'] = title
        df['classification'] = ['OFFICIAL_USE_ONLY' for i in range(len(id_))]
        df['exception'] = ['7. Member Countries/Third Party Confidence' for i in range(len(id_))]

        return df
    except JSONDecodeError:
        print(ou_req.text)

#@lru_cache(maxsize=32)
def get_pub_class(token = None):

    id_, idno_, title = [], [], []
    
    ou_req = requests.get("https://microdata.worldbank.org/index.php/api/catalog/search?&ps=20000&format=json")
    
    assert ou_req.status_code == 200, "Invalid Microdata request call."
    
    ou_res = ou_req.json()['result']
    
    for i in ou_res['rows']:
        id_.append(i['id'])
        idno_.append(i['idno'])
        title.append(i['title'])
    
    assert len(id_) == len(idno_) == len(title), "Lists must have same number of elements"
    
    df = pd.DataFrame(columns = ['id', 'idno', 'title', 'classification'])
    df['id'] = id_
    df['idno'] = idno_
    df['title'] = title
    df['classification'] = ['PUBLIC' for i in range(len(id_))]
    df['exception'] = [None for i in range(len(id_))]
    
    return df

def get_data_classfication():
    ou_df = get_ou_class()
    pub_df = get_pub_class()
    
    temp_df = ou_df[ou_df.idno.isin(pub_df.idno)]
    
    for i in temp_df.index:
        ou_df.loc[i, 'classification'] = "PUBLIC"
        ou_df.loc[i, 'exception'] = None
        
    notin_pub = pub_df[~pub_df.idno.isin(ou_df.idno)]
    
    all_df = pd.concat([ou_df, notin_pub])
    
    all_df.to_csv(os.path.join(os.getcwd(), "MDLib_data_classification.csv"))
    #return all_df
    

        
        
        
        
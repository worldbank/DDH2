#!/usr/bin/python

import urllib
import requests
import zipfile
from utils import *
from datetime import datetime as dt
from dateutil.parser import parse
import pytz
import numpy as np
import pandas as pd
import json
import os, sys
import re
sys.path.append(r"C:\Users\wb542830\OneDrive - WBG\DEC\DDH\DDH2.0\ddh2api")
import ddh2

## Function that reads in all microdata and checks it against current DDH entry
## Make an API call to check if dataset ID from MDLib exists as harvester ID on DDH
## Sample query: https://ddhoutboundapiqa.asestg.worldbank.org/DDHSearch?qname=Dataset&qterm=*&$filter=reference_system/reference_id eq 'LKA_2005_SLMS_v01_M'


### OU Root: http://microdatalib.worldbank.org
### Public Root: http://microdata.worldbank.org

global config_params


def get_mdlib_ids(response):
    """
    Returns a list of 
    """
    lis = []
    
    resp_js = response.json()
    
    for i in resp_js['result']['rows']:
        lis.append(i['idno'])
    
    #req = requests.get("https://ddhoutboundapiqa.asestg.worldbank.org/DDHSearch?qname=Dataset&qterm=*&$filter=reference_system/reference_id eq '{}'".format(idn))
    return lis  
        
@lru_cache(maxsize=32)
def get_control_vocab(token):
    #sample_parameters = {
    #   "resource": "https://ddhinboundapiqa.asestg.worldbank.org",
    #   "tenant" : "31a2fec0-266b-4c67-b56e-2796d8f59c36",
    #   "authorityHostUrl" : "https://login.microsoftonline.com",
    #   "clientId" : "b5ea6885-2e6b-46f4-9569-d04b2e2b6a75",
    #   "clientSecret" : "Pq660rD[3HjxY:jQAa:Kx-ArOLlhiB1k"
    #}
    #token = {"{}".format(token_key[0]) : "{}".format(token_value[0])}
    url = "https://ddhinboundapiuat.asestg.worldbank.org/lookup/metadata"
    #ddhs = ddh2.create_session(cache=True, params = sample_parameters)
    con_res = requests.get(url, headers = token)
    
    return con_res.json()  

def write_file(data):
    with open(r'harvested_json/harvest_{}.csv'.format(dt.now().strftime("%Y_%m_%d")), 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data)


def main():
    global limit, token
    
    ##Save a classification file before you start with harvesting
    get_data_classfication()
    
    with open("config_params") as f:
        ddh2_params = json.load(f)
    
    ddh = ddh2.create_session(cache=True, params = ddh2_params)
    token = ddh.headers
    
    control_vocab = get_control_vocab(token)
    
    limit = 10000
    mdlib_params = get_params("microdata")
    mdlib_url = "{}://{}/index.php/api/catalog/search?format=json&ps={}".format(mdlib_params['protocol'], mdlib_params['host'], limit)
    
    rr = requests.get(mdlib_url)
    
    assert (rr.status_code == 200, "API Error : {}".format(rr.text))
    
    resp = rr.json()

    ### Check if there are more observations than limit defined. If yes, change the limit and read-in all datasets

    if resp['result']['total'] > limit :
        limit = resp['result']['total']
        mdlib_url = "{}://{}/index.php/api/catalog/search?format=json&ps={}".format(mdlib_params['protocol'], mdlib_params['host'], limit)    
        new_rr = requests.get(mdlib_url)

        list_ids = get_mdlib_ids(new_rr)

    else:
        list_ids = get_mdlib_ids(rr)


    ##Loop through the IDs and check if ID_no exists
    ddh_params = get_params("ddh2uat")
    df = pd.DataFrame(columns = ['dataset_id', 'unique_id', 'indo', 'status'])
    df.to_csv('harvested_json/harvest_{}.csv'.format(dt.now().strftime("%Y_%m_%d")), index=False)
    tokens = json.dumps(token)
    timezone_nw = pytz.timezone('America/New_York')
    for ids in list_ids:
        url = "{}://{}/search?q=*&$status eq 'PUBLISHED' &$filter=lineage/source_reference eq '{}'".format(ddh_params['protocol'], ddh_params['host'], ids)
        req = requests.get(url, ddhs.headers)
        req_js = req.json()

        print(ids, len(req_js['Response']['value']))
        if len(req_js['Response']['value']) == 0: ##ID not on DDH
            #print(config(get_params("microdata", ids)))

            md_data = get_microdata(config(get_params("microdata", ids)))
            stat = harvest_mdlib(ids, md_data, tokens, ddhs, True)
            try:
                stat_js = json.loads(stat)
                write_file([stat_js['dataset_id'], stat_js['dataset_unique_id'], ids, 'new'])
            except json.JSONDecodeError:
                write_file([None, None, ids, stat])
            except TypeError as e:
                print(e)
        elif len(req_js['Response']['value']) == 1: ##ID on DDH. Check for update date
            md_data = get_microdata(config(get_params("microdata", ids)))
            if md_data['status'] == "suucess":
                md_date = md_data['dataset']['changed']
                md_date = parse(md_date).astimezone(timezone_nw).strftime('%Y-%m-%dT%H:%M:%S.%f')

                #if 'LAST_UPDATED_DATE' in [i['type'] for i in req_js['Response']['value'][0]['identification']['dates']]:
                #    res = next((sub for sub in req_js['Response']['value'][0]['identification']['dates'] if sub['type'] == 'LAST_UPDATED_DATE'), None) 
                #   #ddh_date = dt.strptime(res['date'], "%m/%d/%Y %H:%M:%S %p")
                #    ddh_date = parse(res['date']).astimezone(timezone_nw)
                #else:
                #    ddh_date = None

                ddh_date = req_js['Response']['value'][0]['last_updated_date']

                try:
                    if md_date > ddh_date:
                        stat = harvest_mdlib(ids, md_data, tokens, ddhs, False, req_js['Response']['value'][0]['dataset_id'])
                        try:
                            stat_js = json.loads(stat)
                            write_file([stat_js['dataset_id'], stat_js['dataset_unique_id'], ids, 'updated'])
                        except json.JSONDecodeError:
                            write_file([None, None, ids, stat])
                except TypeError:
                    pass
        else:
            write_file([None, None, ids, 'Unknown exception'])
        
        
        
if __name__ == "__main__":
    main()
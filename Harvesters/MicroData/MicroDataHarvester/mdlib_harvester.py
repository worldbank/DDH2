#!/usr/bin/python

import urllib
import requests
import zipfile
from utils import *
from datetime import datetime as dt
import numpy as np
import pandas as pd
import json
import os, sys
import re
import ddh2

## Function that reads in all microdata and checks it against current DDH entry
## Make an API call to check if dataset ID from MDLib exists as harvester ID on DDH
## Sample query: https://ddhoutboundapiqa.asestg.worldbank.org/DDHSearch?qname=Dataset&qterm=*&$filter=reference_system/reference_id eq 'LKA_2005_SLMS_v01_M'

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
        
    

def main():
    global limit, token
    
    with open("config_params") as f:
        ddh2_params = json.load(f)
    
    ddh = ddh2.create_session(cache=True, params = ddh2_params)
    token = ddh.headers
    
    limit = 10000
    mdlib_params = get_params("microdata")
    mdlib_url = "{}://{}/index.php/api/catalog/search?format=json&ps={}".format(mdlib_params['protocol'], mdlib_params['host'], limit)
    
    rr = requests.get(mdlib_url)
    
    if rr.status_code == 200:
        resp = rr.json()

        if resp['result']['total'] > limit :
            limit = resp['result']['total']
            mdlib_url = "{}://{}/index.php/api/catalog/search?format=json&ps={}".format(mdlib_params['protocol'], mdlib_params['host'], limit)    
            new_rr = requests.get(mdlib_url)

            list_ids = get_mdlib_ids(new_rr)

        else:
            list_ids = get_mdlib_ids(rr)
            
        
        ##Loop through the IDs and check if ID_no exists
        ddh_params = get_params("ddh2")
        timezone_nw = pytz.timezone('America/New_York')
        for ids in list_ids:
            req = requests.get("{}://{}/DDHSearch?qname=Dataset&qterm=*&$filter=reference_system/reference_id eq '{}'".format(ddh_params['protocol'], ddh_params['host'], ids))

            req_js = req.json()

            if len(req_js['Response']['value']) == 0: ##ID not on DDH
                md_data = get_microdata(config(get_params("microdata", ids)))
                harvest_mdlib(ids, md_data, token)
            elif len(req_js['Response']['value']) == 1: ##ID on DDH. Check for update date
                md_data = get_microdata(config(get_params("microdata", ids)))
                md_date = md_data['dataset']['changed']
                md_date = parse(md_date).astimezone(timezone_nw)

            if 'LAST_UPDATED_DATE' in [i['type'] for i in req_js['Response']['value'][0]['identification']['dates']]:
                res = next((sub for sub in req_js['Response']['value'][0]['identification']['dates'] if sub['type'] == 'LAST_UPDATED_DATE'), None) 
                #ddh_date = dt.strptime(res['date'], "%m/%d/%Y %H:%M:%S %p")
                ddh_date = parse(res['date']).astimezone(timezone_nw)
            else:
                ddh_date = None

            #if len(req_js['Response']['value'][0]['identification']['dates'])>0:
            #    for val in req_js['Response']['value'][0]['identification']['dates']:
            #        if val['type'] == "LAST_UPDATED_DATE":
            #            ddh_date = val['date']
            #        else:
            #            ddh_date = None
            #else:
            #    if 'LAST_UPDATED_DATE' in [i['type'] for i in req_js['Response']['value'][0]['identification']['dates']]:
            #        res = next((sub for sub in req_js['Response']['value'][0]['identification']['dates'] if sub['type'] == 'LAST_UPDATED_DATE'), None) 
            #        ddh_date = dt.strptime(res['date'], "%m/%d/%Y %H:%M:%S %p")
            #    else:
            #        ddh_date = None

            try:
                if md_date > ddh_date:
                    harvest_mdlib(ids, md_data, token)
                else:
                    #sys.exit(99)
                    #print("Condition fail 1")
                    pass
            except TypeError:
                #sys.exit(99)
                #print("Condition fail 2")
                pass
    else:
        raise ddh.APIError('get_microdata', mdlib_url, rr.text)
        
        
        
if __name__ == "__main__":
    main()
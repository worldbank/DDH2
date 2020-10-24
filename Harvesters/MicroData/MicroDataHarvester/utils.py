#!/usr/bin/python

import urllib
import requests
import zipfile
from datetime import datetime as dt
import numpy as np
import pandas as pd
import json
import os, sys
import re

#global ddh_host, ddh_session_key, ddh_session_value, ddh_token, ddh_protocol

def get_params(host_type, idno=None):
    if host_type == "microdata":
        config_params = {
            'protocol' : 'https',
            'host' : 'microdatalib.worldbank.org',
            'data_id' : "{}".format(idno)
            }
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

def get_microdata(url):
    
    url = str(url)
    response = requests.get(url)
    try:
        result = response.json()
        if type(result) is not dict:
            return None
        return result
    except:
        raise ddh.APIError('get_microdata', url, response.text)
        
        


def unpack_acks(vals):
    temp = extract_md_meta(vals)
    
    ack_lis = []
    
    for i in temp:
        ack_lis.append(i['name']+', '+i['affiliation'])
        
    return ack_lis    


def get_dates(lis, val):
    if lis[-1] == "dates":
        dtemp_ch = {"date" : extract_md_meta(['dataset', 'changed']),
                "type" : "Modified Date"}
        dtemp_cr = {"date" : extract_md_meta(['dataset', 'created']),
                   "type" : "Release Date"}
        return [dtemp_ch, dtemp_cr]
    elif lis[-1] == "end_date":
        temp = extract_md_meta(val+[0, 'end'])
        return temp
    elif lis[-1] == "start_date":
        temp = extract_md_meta(val+[0, 'start'])
        return temp
    else:
        return ''
    
    
    
def get_funding_abr(vals):
    
    for i in vals:
        if i['name'] not in funding_lis:
            funding_lis.append(i['name'])
            

            
def get_list_vals(val):
    if isinstance(val, list):
        val = val[0]
    return val['name']


def str_to_dict(ds_item, key, val):
    
    if ds_item == "data_notes":
        notes_lis.append({"{}".format(key) : "{}".format(val)})
    elif ds_item == "statistical_concept_and_methodology":
        stats_lis.append({"{}".format(key) : "{}".format(val)})
    elif ds_item == "study_type":
        study_lis.append({"{}".format(key) : "{}".format(val)})
    elif ds_item == "data_collectors":
        dcoll_lis.append({"{}".format(key) : "{}".format(val)})
    elif ds_item == "description":
        desc_lis.append({"{}".format(key) : "{}".format(val)})
    else:
        print("Key not found")
        
        
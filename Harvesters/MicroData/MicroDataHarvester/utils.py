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
        
        



        
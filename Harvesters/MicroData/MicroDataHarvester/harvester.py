#!/usr/bin/python

### this will have reading mappings, creating a json object and pushing that object to DDH2 via API

import urllib
import requests
import zipfile
from msrestazure.azure_active_directory import AADTokenCredentials
from utils import *
## Other required imports
import adal, uuid, time
from utils import *
from datetime import datetime as dt
import numpy as np
import pandas as pd
import json
import os, sys
sys.path.append("../../../API/RomaniaHub/ddh2_testing")
from ddh2 import dataset
import re
sys.path.append("../../../API/pyddh")
import ddh
ddh.load('ddh1stg.prod.acquia-sites.com')

global funding_lis, notes_dict, stats_dict, study_dict


def new_ds():
    with open(r"C:\Users\wb542830\OneDrive - WBG\DEC\DDH\DDH2.0\Testing\DDH_dataset_updated.json") as f:
        ds = json.load(f)
    return ds


def assign_poc(mdlib_poc):
    try:
        if isinstance(mdlib_poc, list):
            mdlib_poc = mdlib_poc[0]

        ds_poc = {'name': '',
          'role': '',
          'email': '',
          'type': '',
          'upi': '',
          'is_emailaddress_visibility_externally': 'false'}

        ds_poc['name'] = mdlib_poc['name']+', '+mdlib_poc['affiliation']
        ds_poc['role'] = 'OWNER'
        ds_poc['email'] = mdlib_poc['email']
        ds_poc['is_emailaddress_visibility_externally'] = "false"

        return ds_poc
    except IndexError:
        ds_poc = {'name': '',
          'role': '',
          'email': '',
          'type': '',
          'upi': '',
          'is_emailaddress_visibility_externally': 'false'}
        return ds_poc
    except KeyError:
        ds_poc = {'name': '',
          'role': '',
          'email': '',
          'type': '',
          'upi': '',
          'is_emailaddress_visibility_externally': 'false'}
        return ds_poc
    
    
def get_keywords(kdic):
    key_lis = []
    for i in kdic:
        key_lis.append({"name" : i['keyword']})
        
    return key_lis


def extract_md_meta(md_lis):
    try:
        #md_lis = map_file['json_fields'][val].split('$')
        if len(md_lis) == 2:
            temp = response[md_lis[0]][md_lis[1]]
        elif len(md_lis) == 3:
            temp = response[md_lis[0]][md_lis[1]][md_lis[2]]
        elif len(md_lis) == 4:
            temp = response[md_lis[0]][md_lis[1]][md_lis[2]][md_lis[3]]
        elif len(md_lis) == 5:
            temp = response[md_lis[0]][md_lis[1]][md_lis[2]][md_lis[3]][md_lis[4]]
        elif len(md_lis) == 6:
            temp = response[md_lis[0]][md_lis[1]][md_lis[2]][md_lis[3]][md_lis[4]][md_lis[5]]
        elif len(md_lis) == 7:
            temp = response[md_lis[0]][md_lis[1]][md_lis[2]][md_lis[3]][md_lis[4]][md_lis[5]][md_lis[6]]
        elif len(md_lis) == 8:
            temp = response[md_lis[0]][md_lis[1]][md_lis[2]][md_lis[3]][md_lis[4]][md_lis[5]][md_lis[6]][md_lis[7]]
    except KeyError:
        temp = ""
    
    return temp


def extract_ds_vals(lis, val):
    
    #cols = ["data_notes", "statistical_concept_and_methodology", "study_type"]
    
    if lis[-1] == "point_of_contact":
        temp = assign_poc(extract_md_meta(val))
        return [temp]
    #elif lis[-1] in cols:
    elif lis[-1] == "data_collectors":
        str_to_dict(lis[-1], val[-1],  extract_md_meta(val)[0]['name'])
        #temp = ds[lis[0]][lis[1]][lis[2]] + '\n ' +': '.join(["{}".format(val[-1]), extract_md_meta(val)[0]['name']])
        temp = dcoll_lis
    elif lis[-1] == "data_notes":
        str_to_dict(lis[-1], val[-1],  extract_md_meta(val))
        temp = notes_lis
    elif lis[-1] == "statistical_concept_and_methodology":
        str_to_dict(lis[-1], val[-1],  extract_md_meta(val))
        temp = stats_lis
    elif lis[-1] == "study_type":
        str_to_dict(lis[-1], val[-1],  extract_md_meta(val))
        temp = study_lis
    #else:
    #    temp = ds[lis[0]][lis[1]][lis[2]] + '\n ' +': '.join(["{}".format(val[-1]), extract_md_meta(val)])
    elif lis[-1] == "description" and lis[-2] == 'lineage':
        str_to_dict(lis[-1], val[-1],  extract_md_meta(val))
        temp = study_lis
        #temp = ds[lis[0]][lis[1]][lis[2]] + ': '.join(["{}".format(val[-1]), extract_md_meta(val)])
    elif lis[-1] == "funding_name_abbreviation_role":
        get_funding_abr(extract_md_meta(val))
        temp = funding_lis
    elif lis[-1] in ['end_date', 'start_date', 'dates']:
        temp = get_dates(lis, val)
    elif lis[-1] == 'keywords':
        temp = get_keywords(extract_md_meta(val))
    elif lis[-1] in ['coverage']:
        temp = extract_md_meta(val)[0]['name']
    elif lis[-1] == "useConstraints":
        temp = extract_md_meta(val)[0]['txt']
    elif lis[-1] == "other_acknowledgements":
        temp = unpack_acks(val)
    elif lis[-1] == "classification":
        temp = "OFFICIAL_USE_ONLY"
    elif lis[-1] == 'granularity':
        temp = extract_md_meta(val).split("\n")
    else:
        temp = extract_md_meta(val)
        
    return temp


def harvest_mdlib():
    global response
    response = get_microdata(config(get_params(ids)))
    
    funding_lis, notes_lis, stats_lis, study_lis, dcoll_lis, desc_lis = [], [], [], [], [], []
    ds = new_ds()
    for i in map_file.index:
        try:
            lis = map_file['ddh2_fields'][i].split('>')
            val = map_file['json_fields'][i].split('$')
            #if 'point_of_contact' in lis:
            #    print(assign_poc(extract_md_meta(i)))
            #else:
            try:
                if len(lis) == 2:
                    ds[lis[0]][lis[1]] = extract_ds_vals(lis, val)
                elif len(lis) == 3:
                    ds[lis[0]][lis[1]][lis[2]] = extract_ds_vals(lis, val)
                elif len(lis) == 4 :
                    ds[lis[0]][lis[1]][lis[2]][lis[3]] = extract_ds_vals(lis, val)
                
            except KeyError:
                pass
        except AttributeError:
            if map_file['ddh2_fields'][i].split('>')[-1] == "classification":
                lis = map_file['ddh2_fields'][i].split('>')
                ds[lis[0]][lis[1]][lis[2]][lis[3]] = extract_ds_vals(lis, val)
            elif map_file['ddh2_fields'][i].split('>')[-1] == "source_type":
                ds[lis[0]][lis[1]][lis[2]] = "Microdata Library"
            else:    
                pass
        except TypeError as e:
            print(i, '::', e)
            
    return ds
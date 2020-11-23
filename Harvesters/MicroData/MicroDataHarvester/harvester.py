#!/usr/bin/python

### this will read mappings, create a json object and push that object to DDH2 via API
## TODO: 
##      Create a function to compare two JSON objects and update the differences
##      Add static information to JSON object before pushing it
##      Add a function to add languages and ISO codes/coverage from country names
##      Check how current harvester addresses data classification  -->> DONE
##      Use controlled vocab endpoint wherever possible to avoid errors
##      Add a function to add resource to published dataset (Rsource will be a link to dataset page on MDLib site) with a constant resource name

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
#from ddh2 import dataset
import re
sys.path.append("../../../API/pyddh")
import ddh
ddh.load('ddh1stg.prod.acquia-sites.com')

global funding_lis, notes_dict, stats_dict, study_dict


def new_ds():
    with open(r"C:\Users\wb542830\OneDrive - WBG\DEC\DDH\DDH2.0\Testing\DDH_dataset_updated.json") as f:
        ds = json.load(f)
    return ds


def clean_empty(d):
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [v for v in (clean_empty(v) for v in d) if v]
    return {k: v for k, v in ((k, clean_empty(v)) for k, v in d.items()) if v}


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
        return temp
    elif lis[-1] == "data_notes":
        str_to_dict(lis[-1], val[-1],  extract_md_meta(val))
        temp = notes_lis
        return temp
    elif lis[-1] == "statistical_concept_and_methodology":
        str_to_dict(lis[-1], val[-1],  extract_md_meta(val))
        temp = stats_lis
        return temp
    elif lis[-1] == "study_type":
        str_to_dict(lis[-1], val[-1],  extract_md_meta(val))
        temp = study_lis
        return temp
    #else:
    #    temp = ds[lis[0]][lis[1]][lis[2]] + '\n ' +': '.join(["{}".format(val[-1]), extract_md_meta(val)])
    elif lis[-1] == "description" and lis[-2] == 'lineage':
        str_to_dict(lis[-1], val[-1],  extract_md_meta(val))
        temp = study_lis
        return temp
        #temp = ds[lis[0]][lis[1]][lis[2]] + ': '.join(["{}".format(val[-1]), extract_md_meta(val)])
    elif lis[-1] == "funding_name_abbreviation_role":
        get_funding_abr(extract_md_meta(val))
        temp = funding_lis
        return temp
    elif lis[-1] in ['end_date', 'start_date', 'dates']:
        temp = get_dates(lis, val)
        return temp
    elif lis[-1] == 'keywords':
        temp = get_keywords(extract_md_meta(val))
        return temp
    elif lis[-1] in ['coverage']:
        temp = extract_md_meta(val)[0]['name']
        return temp
    elif lis[-1] == "useConstraints":
        try:
            temp = extract_md_meta(val)[0]['txt']
            return temp
        except (ValueError, IndexError) as e:
            pass
    elif lis[-1] == "other_acknowledgements":
        temp = unpack_acks(val)
        return temp
    #elif lis[-1] == "classification":
    #    temp = "OFFICIAL_USE_ONLY"
    #    return temp
    elif lis[-1] == 'granularity':
        temp = extract_md_meta(val).split("\n")
        return temp
    else:
        temp = extract_md_meta(val)
        return temp

def add_to_ddh(ds, token):
    ddh_params = get_params("ddh2")
    req = requests.post("{}://{}/dataset/create".format(ddh_params['protocol'], ddh_params['host']),
                   json = ds, headers = token)
    
    if req.status_code == 417:
        print("Error: {}".format(req.text))
    elif req.status_code == 200:
        print("Dataset Added! {}".format(req.text))

def _add_to_ddh(ids, ds):
    with open("{}_MDLib.json".format(ids), 'w') as f:
        json.dump(ds, f, indent = 6) 

def harvest_mdlib(ids, res, token):
    global response, funding_lis, notes_lis, stats_lis, study_lis, dcoll_lis, desc_lis
    response = res
    
    map_file = pd.read_excel(r"C:\Users\wb542830\OneDrive - WBG\DEC\DDH\DDH2.0\Harvesters\MicroData\MDLib_DDH2_mapping.xlsx", sheet_name=1)
    
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
                cl = pd.read_csv(os.path.join(os.getcwd(), "MDLib_data_classification.csv"))
                ds[lis[0]][lis[1]][lis[2]][lis[3]] = cl[cl.idno == ids]['classification'].iloc[0]
            elif map_file['ddh2_fields'][i].split('>')[-1] == "source_type":
                ds[lis[0]][lis[1]][lis[2]] = "Microdata Library"
            else:    
                pass
        except TypeError as e:
            print(i, '::', e)
    
    ds = clean_empty(ds)
    with open("{}_MDLib.json".format(ids), 'w') as f:
        json.dump(ds, f, indent = 6)
    #if ds['title']:
    #    _add_to_ddh(ids, ds)
    #if ds:
    #    #add_to_ddh(ds, token)
    
    #else:
    #    print("Failed to add dataset!")
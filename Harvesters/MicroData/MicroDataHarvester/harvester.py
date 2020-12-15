#!/usr/bin/python

### this will read mappings, create a json object and push that object to DDH2 via API
## TODO: 
##      Add static information to JSON object before pushing it -->> DONE
##      Add a function to add languages and ISO codes/coverage from country names -->> DONE
##      Check how current harvester addresses data classification  -->> DONE
##      Use controlled vocab endpoint wherever possible to avoid errors -->> DONE
##      Add a function to add resource to published dataset (Resource will be a link to dataset page on MDLib site) with a constant resource name -->> DONE
##      Granularity is a controlled vocab field -->> DONE
##      Coverage under geograpgical coverage is a list -->> DONE
##      Some dates in MDLib is YYYY-MM, convert them to YYYY-MM-DD -->> DONE
##      Add country name to datasets -->> DONE
##      License classification ==> "data_na" > "Data not available", "public" > "Creative Commons Attribution 4.0", "remote" > "License specified externally", "direct" > "Research Data License" , "licensed" > "Reearch Data License", "open" >  "Creative Commons Attribution 4.0"

##      Official Use datasets have "public" license on MDLib. Fix it
##      Function to update dataset it it already exists but last modified date is different



import urllib
import requests
import zipfile
import ddh2
from dateutil.parser import parse, ParserError
from msrestazure.azure_active_directory import AADTokenCredentials
from utils import *
from functools import lru_cache
import pytz
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
#sys.path.append("../../../API/pyddh")
#import ddh
#ddh.load('ddh1stg.prod.acquia-sites.com')

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
        ds_poc['role'] = {"code" : "OWNER"}
        ds_poc['email'] = mdlib_poc['email']
        ds_poc['is_emailaddress_visibility_externally'] = "false"

        return ds_poc
    except IndexError:
        ds_poc = {'name': '',
          'role': '',
          'email': 'microdata@worldbank.org',
          'type': '',
          'upi': '',
          'is_emailaddress_visibility_externally': 'false'}
        return ds_poc
    except KeyError:
        ds_poc = {'name': '',
          'role': '',
          'email': 'microdata@worldbank.org',
          'type': '',
          'upi': '',
          'is_emailaddress_visibility_externally': 'false'}
        return ds_poc
    except TypeError:
        return 'NULL'
    

def unpack_acks(vals):
    temp = extract_md_meta(vals)
    
    ack_lis = []
    
    for i in temp:
        ack_lis.append(i['name']+', '+i['affiliation'])
        
    return ack_lis    

def simplyfy_dates(temp):
    try:
        dd = dt.strptime(temp, "%Y").strftime('%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            dd = dt.strptime(temp, "%Y-%m").astimezone(timezone_nw).strftime('%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            try:
                dd = parse(temp).astimezone(timezone_nw).strftime('%Y-%m-%dT%H:%M:%S.%f')
            except ParserError:
                dd = ""
        except OSError:
            dd = parse(temp).strftime('%Y-%m-%dT%H:%M:%S.%f')
    except OSError:
        dd = parse(temp).strftime('%Y-%m-%dT%H:%M:%S.%f')
        
    return dd

def get_dates(lis, val):
    timezone_nw = pytz.timezone('America/New_York')
    if lis[-1] == "dates":
        ch_date = parse(extract_md_meta(['dataset', 'changed'])).replace(day=1).astimezone(timezone_nw).strftime('%Y-%m-%dT%H:%M:%S.%f')
        dtemp_ch = {"date" : ch_date,
                "type" : "MODIFIED_DATE"}
        cr_date = parse(extract_md_meta(['dataset', 'created'])).replace(day=1).astimezone(timezone_nw).strftime('%Y-%m-%dT%H:%M:%S.%f')
        dtemp_cr = {"date" : cr_date,
                   "type" : "RELEASE_DATE"}
        return [dtemp_ch, dtemp_cr]
    elif lis[-1] == "end_date":
        temp = extract_md_meta(val+[0, 'end'])
        dd = simplyfy_dates(temp)
        return dd
    elif lis[-1] == "start_date":
        temp = extract_md_meta(val+[0, 'start'])
        dd = simplyfy_dates(temp)
        return dd
    else:
        return ''
    
def get_license_info(classi, key):
    if classi == "PUBLIC":
        master_dic = {
            "data_na" : "Data not available", 
            "public" : "Creative Commons Attribution 4.0", 
            "remote" : "License specified externally", 
            "direct" : "Research Data License" , 
            "licensed" : "Research Data License", 
            "open" :  "Creative Commons Attribution 4.0"
        }

        return master_dic[key]
    elif classi == "OFFICIAL_USE_ONLY":
        master_dic = {
            "data_na" : "Data not available", 
            "public" : "Research Data License", 
            "remote" : "License specified externally", 
            "direct" : "Research Data License" , 
            "licensed" : "Reearch Data License", 
            "open" :  "Research Data License"
        }

        return master_dic[key]
    
def get_funding_abr(vals):
    
    for i in vals:
        if i['name'] not in funding_lis:
            funding_lis.append(i['name'])
    
    return

@lru_cache(maxsize=32)
def get_control_vocab(tokens):
    #sample_parameters = {
    #   "resource": "https://ddhinboundapiqa.asestg.worldbank.org",
    #   "tenant" : "31a2fec0-266b-4c67-b56e-2796d8f59c36",
    #   "authorityHostUrl" : "https://login.microsoftonline.com",
    #   "clientId" : "b5ea6885-2e6b-46f4-9569-d04b2e2b6a75",
    #   "clientSecret" : "Pq660rD[3HjxY:jQAa:Kx-ArOLlhiB1k"
    #}
    token = json.loads(tokens)
    url = "https://ddhinboundapiuat.asestg.worldbank.org/lookup/metadata"
    #ddhs = ddh2.create_session(cache=True, params = sample_parameters)
    con_res = requests.get(url, headers = token)
    
    return con_res.json()


def simplify_granularity(vals, tokens):
    temp = extract_md_meta(vals)
    temp_ = temp.upper()
    con_res = get_control_vocab(tokens)
    gran_lis = []
    for i in con_res['granularities']:
        gran_lis.append(i['code'])
    
    temp_lis = [i for i in gran_lis if i in temp_]
    
    if 'NA' in temp_lis and len(temp_lis)==1:
        return('NA')
    elif 'NA' in temp_lis and len(temp_lis)>1:
        temp_lis.remove('NA')
        return ', '.join(temp_lis)
    else:
        return ', '.join(temp_lis)

    
def get_geo_coverage(vals, tokens):
    cov_lis = []
    string = extract_md_meta(vals)[0]['abbreviation']
    rr = requests.get("http://api.worldbank.org/v2/country/{}?format=json".format(string))
    try:
        if rr.status_code == 200:
            cov_lis.append({"code" : rr.json()[1][0]['iso2Code']})  
    except IndexError:
        con_res = get_control_vocab(tokens)
        try:
            for i in con_res['countries']:
                if string in i['name']:
                    cov_lis.append({"code" : i['code']})
        except KeyError:
            pass
        except TypeError:
            print(string)
    return cov_lis
    

def get_country_name(vals):
    country = ''
    string = extract_md_meta(vals)[0]['abbreviation']
    rr = requests.get("http://api.worldbank.org/v2/country/{}?format=jsonhttp://api.worldbank.org/v2/country/{}?format=json".format(string))
    if rr.status_code == 200:
        country = rr.json()[1][0]['name']  
    return country

def get_list_vals(val):
    if isinstance(val, list):
        val = val[0]
    return val['name']


def str_to_dict_(ds_item, key, val):
    
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
    except TypeError:
        temp = ""
    
    return temp


def extract_ds_vals(ds, lis, val, tokens):

    md_meta = extract_md_meta(val)
    
    if md_meta != "":
        if lis[-1] == "point_of_contact":
            temp = assign_poc(extract_md_meta(val))
            if temp == 'NULL':
                val = ['dataset','metadata','doc_desc','producers']
                temp = assign_poc(extract_md_meta(val))
            return [temp]
        elif (lis[-1] == "statistical_concept_and_methodology") and (val[-1] == "data_collectors"):
            temp = "{}: <br> {}".format(ds[lis[0]][lis[1]][lis[2]], ': '.join([val[-1].upper(), str(extract_md_meta(val)[0]['name'])]))
            return temp
        elif lis[-1] == "data_notes":
            temp = "{}: <br> {}".format(ds[lis[0]][lis[1]][lis[2]], ': '.join([val[-1].upper(), str(extract_md_meta(val))]))
            temp = notes_lis
            return temp
        elif lis[-1] == "statistical_concept_and_methodology":
            temp = "{}: <br> {}".format(ds[lis[0]][lis[1]][lis[2]], ': '.join([val[-1].upper(), str(extract_md_meta(val))]))
            return temp
        elif lis[-1] == "study_type":
            temp = temp = "{}: <br> {}".format(ds[lis[0]][lis[1]][lis[2]], ': '.join([val[-1].upper(), str(extract_md_meta(val))]))
            return temp
        elif lis[-1] == "description" and lis[-2] == 'lineage':
            temp = "{}: <br> {}".format(ds[lis[0]][lis[1]][lis[2]], ': '.join([val[-1].upper(), str(extract_md_meta(val))]))
            return temp       
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
            temp = get_geo_coverage(val, tokens)
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
        elif lis[-1] == 'granularity':
            temp = simplify_granularity(val, tokens)
            return temp
        else:
            temp = extract_md_meta(val)
            return temp
    else:
        return ""



    
def get_static_info(ds):
    ###curation harvest source
    ds['Dataset']['lineage']['curation_harvest_source'] = "MICRODATA"
    
    ### language supported
    ds['Dataset']['identification']['language_supported'] = ["EN"]
    
    ### Topic not specified
    ds['Dataset']['identification']['topics'] = [{'name': 'Topic not specified'}]
    
    ### Tag for MDLib
    ds['Dataset']['keywords'] = [{"name" : "microdata"}]
    
    return ds
    
    
# https://ddhinboundapiuat.asestg.worldbank.org/dataset/create     
def add_to_ddh(ids, ds, tokens, ddh_sess):
    ddh_params = get_params("ddh2")
    token = json.loads(tokens)
    req = ddh_sess.post("dataset/create", ds)
    #req = requests.post("https://ddhinboundapiuat.asestg.worldbank.org/dataset/create",
    #               json = ds, headers = token)
    
    if req.status_code == 417:
        print("Error: {}".format(req.text))
    elif req.status_code == 200:
        print("Dataset Created! {}".format(req.text))
        try:
            stat = json.loads(req.text)
            res = get_resource_json(stat['dataset_id'], ids, tokens, ddh_sess)
            resp = add_resource(res, stat['dataset_id'], ddh_sess)
            
            if resp == "Success":
                return req.text
        except json.JSONDecodeError:
            print(ids, req.text)
            return req.text
    else:
        print(ids,'else', req.text)

    
def update_existing(ddh_sess):
    return
 
    
    
def harvest_mdlib(ids, res, tokens, ddh_sess, add_new):
    global response, funding_lis, notes_lis, stats_lis, study_lis, dcoll_lis, desc_lis, ds, timezone_nw
    response = res
    map_file = pd.read_excel(r"C:\Users\wb542830\OneDrive - WBG\DEC\DDH\DDH2.0\Harvesters\MicroData\MDLib_DDH2_mapping.xlsx", sheet_name=1)
    timezone_nw = pytz.timezone('America/New_York')
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
                    ds[lis[0]][lis[1]] = extract_ds_vals(ds, lis, val, tokens)
                elif len(lis) == 3:
                    ds[lis[0]][lis[1]][lis[2]] = extract_ds_vals(ds, lis, val, tokens)
                elif len(lis) == 4 :
                    ds[lis[0]][lis[1]][lis[2]][lis[3]] = extract_ds_vals(ds, lis, val, tokens)
                
            except KeyError:
                pass
        except AttributeError:
            if map_file['ddh2_fields'][i].split('>')[-1] == "classification":
                if os.path.exists(os.path.join(os.getcwd(), "MDLib_data_classification.csv")):
                    cl = pd.read_csv(os.path.join(os.getcwd(), "MDLib_data_classification.csv"))
                else:
                    get_data_classfication()
                    cl = pd.read_csv(os.path.join(os.getcwd(), "MDLib_data_classification.csv"))
                
                ds[lis[0]][lis[1]][lis[2]][lis[3]] = cl[cl.idno == ids]['classification'].iloc[0]
            #elif map_file['ddh2_fields'][i].split('>')[-1] == "source_type":
            #    ds[lis[0]][lis[1]][lis[2]] = "Microdata Library"
            else:    
                pass
        #except TypeError as e:
        #    print(i, '::', e)
    country = extract_md_meta(['dataset','metadata', 'study_desc', 'study_info', 'nation'])[0]['name']
    ds['Dataset']['identification']['title'] = "{} - {}".format(country, ds['Dataset']['identification']['title'])
    
    ds['Dataset']['constraints']['license']['license_id'] = get_license_info(ds['Dataset']['constraints']['security']['classification'], extract_md_meta(['dataset', 'data_access_type']))
    
    ds = get_static_info(ds)
    ds = clean_empty(ds)
    
    #add_to_ddh(ids, ds, tokens)
    
    with open("harvested_json/{}_MDLib.json".format(ids), 'w') as f:
        json.dump(ds, f, indent = 6)
        
    if add_new:
        stat = add_to_ddh(ids, ds, tokens, ddh_sess)
        return stat
        #try:
        #    stat_id = json.loads(stat)
        #except json.JSONDecodeError:
        #    print(ids, stat)
    else:
        stat = update_existing(ddh_sess)
        return stat


    
    
def get_resource_json(dataset_id, idno, tokens, ddh_sess):
    file = pd.read_csv(os.path.join(os.getcwd(), "MDLib_data_classification.csv"))
    temp = file[file.idno == idno].iloc[0]
    #sample_parameters = {
    #   "resource": "https://ddhinboundapiqa.asestg.worldbank.org",
    #   "tenant" : "31a2fec0-266b-4c67-b56e-2796d8f59c36",
    #   "authorityHostUrl" : "https://login.microsoftonline.com",
    #   "clientId" : "b5ea6885-2e6b-46f4-9569-d04b2e2b6a75",
    #   "clientSecret" : "Pq660rD[3HjxY:jQAa:Kx-ArOLlhiB1k"
    #}
    #ddhs = ddh2.create_session(cache=True, params = sample_parameters)
    
    view_res = ddh_sess.post("dataset/view", {'data': { 'dataset_id' : "{}".format(dataset_id)}, 'showDatasetResources':'true'}).json()
    
    #if view_res['constraints']['security']['classification'].upper() == "PUBLIC":
    #    ##should be source_reference ideally
    #    res_url = "https://microdata.worldbank.org/index.php/catalog/{}".format(view_res['lineage']['source_reference'])
    #else:
    res_url = "https://microdatalib.worldbank.org/index.php/catalog/{}".format(format(view_res['lineage']['source_reference']))
    
    #res_view_req = ddhs.post("resource/view", {'data': { 'resource_id' : "{}".format(view_res['resources'][0]['resource_id'])}}).json()
 #          "dataset": {
 #           "name": "{}".format(view_res['name'])
 #         },   

    resource = {"Resource" : {
          "dataset_id": "{}".format(dataset_id),
          "name": "Related materials (Questionnaires, reports, tables,\n  technical documents, and data files)",
          "unique_id": "{}".format(view_res['dataset_unique_id']),
          "last_updated": "{}".format(view_res['last_updated_date']),
          "identification": {
            "name": "Related materials (Questionnaires, reports, tables,\n  technical documents, and data files)",
            "type": "Landing page"
            #"status": "Active"
          },
          "constraints": {
            "security": {
              "data_classification_of_file": {
                "code": "{}".format(view_res['constraints']['security']['classification'].upper())
              },
                "exception" : "{}".format(view_res['constraints']['security']['exception'])
            },
          },
          "distribution": {
            "format": "html",
            "website_url": "{}".format(res_url),
          },
          "lineage": {
            "source_reference": "{}".format(res_url)
          }
        }
    }
    
    return resource

def add_resource(res_js, dataset_id, ddh_sess):
    
    resp = ddh_sess.post('resource/create', res_js)
    
    req_pub = ddh_sess.post("workflow/updatestatus", {"dataset_id" : "{}".format(dataset_id),
                                                       "dataset_status" : "PUBLISHED",
                                               "publish_label" : "resource add1"})
    if req_pub.text.startswith('Ent'):
        print("Resource added and updated!")
        return "Success"
    else:
        print("Error creating resource: {}".format(req_pub.text))
        return
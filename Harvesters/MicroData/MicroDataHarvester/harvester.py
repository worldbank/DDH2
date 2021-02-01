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
import ast
import csv
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
sys.path.append(r"C:\Users\wb542830\OneDrive - WBG\DEC\DDH\DDH2.0\ddh2api")
import ddh2
import re
#sys.path.append("../../../API/pyddh")
#import ddh
#ddh.load('ddh1stg.prod.acquia-sites.com')

global funding_lis, notes_dict, stats_dict, study_dict


def new_ds():
    with open(r"DDH_dataset_updated.json") as f:
        ds = json.load(f)
    return ds


def clean_empty(d):
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [v for v in (clean_empty(v) for v in d) if v]
    return {k: v for k, v in ((k, clean_empty(v)) for k, v in d.items()) if v}

def get_blank_poc():
    ds_poc = {'name': '',
              'role': '',
              'email': '',
              'type': '',
              'upi': '',
              'is_emailaddress_visibility_externally': 'false'}
    return ds_poc

def assign_poc(val):
    
    if val[-1] == "contact":
        temp = extract_md_meta(val)
        for te in temp:
            ds_poc = get_blank_poc()
            ds_poc['name'] = te['name']+', '+te['affiliation']
            ds_poc['role'] = {"code" : "POINT_OF_CONTACT"}
            try:
                ds_poc['email'] = te['email']
            except KeyError:
                pass
            ds_poc['is_emailaddress_visibility_externally'] = False
            poc_lis.append(ds_poc)
            
    elif val[-1] == "authoring_entity":
        temp = extract_md_meta(val)
        for te in temp:
            ds_poc = get_blank_poc()
            ds_poc['name'] = te['name']+', '+te['affiliation']
            ds_poc['role'] = {"code" : "PRINCIPAL_INVESTIGATOR"}
            try:
                ds_poc['email'] = te['email']
            except KeyError:
                pass
            ds_poc['is_emailaddress_visibility_externally'] = False
            poc_lis.append(ds_poc)
    
    elif val[-1] == "producers":
        temp = extract_md_meta(val)
        for te in temp:
            ds_poc = get_blank_poc()
            ds_poc['name'] = te['name']+', '+te['affiliation']
            ds_poc['role'] = {"code" : "OWNER"}
            try:
                ds_poc['email'] = te['email']
            except KeyError:
                pass
            ds_poc['is_emailaddress_visibility_externally'] = False
            poc_lis.append(ds_poc)
            
    return
        
def waste_stuff():
    #### Things from PoC
    try:
        if isinstance(mdlib_poc, list):
            #mdlib_poc = mdlib_poc[0]
            for temp in mdlib_poc:
                ds_poc = {'name': '',
                          'role': '',
                          'email': '',
                          'type': '',
                          'upi': '',
                          'is_emailaddress_visibility_externally': 'false'}
                ds_poc['name'] = temp['name']+', '+temp['affiliation']
                ds_poc['role'] = {"code" : "POINT_OF_CONTACT"}
                try:
                    ds_poc['email'] = temp['email']
                except KeyError:
                    pass
                ds_poc['is_emailaddress_visibility_externally'] = False
                

        ds_poc = {'name': '',
          'role': '',
          'email': '',
          'type': '',
          'upi': '',
          'is_emailaddress_visibility_externally': 'false'}

        ds_poc['name'] = mdlib_poc['name']+', '+mdlib_poc['affiliation']
        ds_poc['role'] = {"code" : "POINT_OF_CONTACT"}
        ds_poc['email'] = mdlib_poc['email']
        ds_poc['is_emailaddress_visibility_externally'] = False

        return ds_poc
    except IndexError:
        ds_poc = {'name': '',
          'role': '',
          'email': 'microdata@worldbank.org',
          'type': '',
          'upi': '',
          'is_emailaddress_visibility_externally': False}
        return ds_poc
    except KeyError:
        ds_poc = {'name': '',
          'role': '',
          'email': 'microdata@worldbank.org',
          'type': '',
          'upi': '',
          'is_emailaddress_visibility_externally': False}
        return ds_poc
    except TypeError:
        return 'NULL'
    

def unpack_acks(vals):
    temp = extract_md_meta(vals)
    
    ack_lis = []
    
    for i in temp:
        ack_lis.append(i['name']+', '+i['affiliation'])
        
    return '; '.join(ack_lis)    

def simplyfy_dates(temp):
    try:
        dd = dt.strptime(temp, "%Y").strftime('%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            dd = dt.strptime(temp, "%Y-%m").strftime('%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            try:
                dd = parse(temp).strftime('%Y-%m-%dT%H:%M:%S.%f')
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
        ch_date = parse(extract_md_meta(['dataset', 'changed'])).astimezone(timezone_nw).strftime('%Y-%m-%dT%H:%M:%S.%f')
        dtemp_ch = {"date" : ch_date,
                "type" : "LAST_UPDATED_DATE"}
        cr_date = parse(extract_md_meta(['dataset', 'created'])).astimezone(timezone_nw).strftime('%Y-%m-%dT%H:%M:%S.%f')
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

    
def get_data_license_exception(classi):
    if classi == "PUBLIC":
        return ""
    elif classi == "OFFICIAL_USE_ONLY":
        return "7. Member Countries/Third Party Confidence"

def get_headings(word):
    word = word.lower()
    
    new_dic = {
        "cleaning_operations" : "Cleaning Operations",
        "sampling_error_estimates" : "Sampling Error Estimates",
        "data_kind" : "Data Notes",
        "coll_mode" : "Mode of Data Collection",
        "data_collectors" : "Data Collectors",
        "method_notes" : "Notes on Data Collection",
        "data_appraisal" : "Other Forms of Data Appraisal",
        "notes" : "Data Processing Notes",
        "research_instrument" : "Research Instrument",
        "response_rate" : "Response Rate",
        "sampling_procedure" : "Sampling Procedure",
        "unit_type" : "Unit of Analysis",
        "universe" : "Coverage Universe",
        "series_name" : "Series Name",
        "series_info" : "Series Info",
        "data_kind" : "Data Kind",
        "coll_situation" : "Data Notes"
        
    }
    
    return new_dic[word]
    
def get_license_info(classi, key):
    if classi == "PUBLIC":
        master_dic = {
            "data_na" : "Data not available", 
            "public" : "Research Data License", 
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
            "licensed" : "Research Data License", 
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
    token = json.loads(tokens)
    url = "https://ddhinboundapiuat.asestg.worldbank.org/lookup/metadata"
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
    abs_lis = extract_md_meta(vals)
    for coun in abs_lis:
        string = coun['abbreviation']
        rr = requests.get("http://api.worldbank.org/v2/country/{}?format=json".format(string))
        try:
            if rr.status_code == 200:
                if rr.json()[1][0]['iso2Code'] == "A9":
                    cov_lis.append({"code" : "3A"})
                else:
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
    #for i in extract_md_meta(vals): [0]['abbreviation']
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


def clean_breaks(vals):
    
    if vals.startswith(" <br>"):
        return vals[9:]
    elif vals.startswith("<br>"):
        return vals[8:]
    else:
        return vals


def extract_ds_vals(ds, lis, val, tokens):

    md_meta = extract_md_meta(val)
    
    if md_meta != "":
        if lis[-1] == "point_of_contact":
            #temp = assign_poc(val)
            assign_poc(val)
            return
        elif (lis[-1] == "statistical_concept_and_methodology") and (val[-1] == "data_collectors"):
            temp = "{} <br><br> {}".format(temp_list[-1], ': '.join([get_headings(val[-1]), str(extract_md_meta(val)[0]['name'])]))
            return clean_breaks(temp)
        elif lis[-1] == "statistical_concept_and_methodology":
            temp = "{} <br><br> {}".format(temp_list[-1], ': '.join([get_headings(val[-1]), str(extract_md_meta(val))]))
            return clean_breaks(temp)
        elif lis[-1] == "data_notes":
            temp = "{} <br><br> {}".format(ds[lis[0]][lis[1]][lis[2]], ': '.join([get_headings(val[-1]), str(extract_md_meta(val))]))
            return clean_breaks(temp)
        elif lis[-1] == "study_type":
            temp = "{} <br><br> {}".format(ds[lis[0]][lis[1]][lis[2]], ': '.join([get_headings(val[-1]), str(extract_md_meta(val))]))
            return clean_breaks(temp)
        elif lis[-1] == "description" and lis[-2] == 'lineage':
            temp = "{} <br><br> {}".format(ds[lis[0]][lis[1]][lis[2]], ': '.join([get_headings(val[-1]), str(extract_md_meta(val))]))
            return clean_breaks(temp)       
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
        elif lis[-1] == "accessConstraints":
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

    
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '$')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '$')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out



def recursive_split(inp, search):
    # check whether it's a dict, list, tuple, or scalar
    if isinstance(inp, dict):
        items = inp.items()
    elif isinstance(inp, (list, tuple)):
        items = enumerate(inp)
    else:
        # just a value, split and return
        if inp == None:
            return ''
        else:
            return inp

    # now call ourself for every value and replace in the input
    for key, value in items:
        inp[key] = recursive_split(value, search)
    return inp

def post_process_list():
    
    ##poc
    for plen in range(len(wrk_cp['identification']['point_of_contact'])):
        if 'name' in wrk_cp['identification']['point_of_contact'][plen]['role'].keys():
            del wrk_cp['identification']['point_of_contact'][plen]['role']['name']  
    
    ## lang supported
    for ls in range(len(wrk_cp['identification']['language_supported'])):
        if 'name' in wrk_cp['identification']['language_supported'][ls].keys():
            del wrk_cp['identification']['language_supported'][ls]['name']
            del wrk_cp['identification']['language_supported'][ls]['language_id']
            
    ## Coverage
    for clen in range(len(wrk_cp['geographical_extent']['coverage'])):
        if 'name' in wrk_cp['geographical_extent']['coverage'][clen].keys():
            del wrk_cp['geographical_extent']['coverage'][clen]['name']
            
    return
            
            
def manage_list_items():
    #['collection_id', 'language_supported', 'keyowrds', 'temporal_coverage', 'funding_name_abbreviation_role', 'point_of_contact',
    #        'curation_related_links_and_publications', 'coverage', 'dates']
    
    #### Loop over new PoCs and append the ones not found
    to_add_poc = []
    master_poc = ["{}; {}".format(poc['name'], poc['role']['code']) for poc in wrk_cp['identification']['point_of_contact']]
    for i in ds['Dataset']['identification']['point_of_contact']:
        if "{}; {}".format(i['name'], i['role']['code']) not in master_poc:
            if i not in to_add_poc:
                to_add_poc.append(i)

    for k in to_add_poc:
        wrk_cp['identification']['point_of_contact'].append(k)
    
    #### check if any PoC is removed from source
    master_poc = ["{}; {}".format(poc['name'], poc['role']['code']) for poc in ds['Dataset']['identification']['point_of_contact']]
    for ilen in range(len(wrk_cp['identification']['point_of_contact'])):
        itemp = wrk_cp['identification']['point_of_contact'][ilen]
        if "{}; {}".format(itemp['name'], itemp['role']['code']) not in master_poc:
            wrk_cp['identification']['point_of_contact'][ilen]['is_delete'] = True
            
        
    #### language supported
    #to_add_ls = []
    master_ls = [i['code'] for i in wrk_cp['identification']['language_supported']]
    for ls in ds['Dataset']['identification']['language_supported']:
        if ls['code'] not in master_ls:
            wrk_cp['identification']['language_supported'].append({'code' : ls['code']})
    
    #### Keywords
    master_tags = [i['name'] for i in wrk_cp['keywords']]
    for ls in ds['Dataset']['keywords']:
        if ls['name'] not in master_tags:
            wrk_cp['keywords'].append({'name' : ls['name']})
            
    #### funding_name_abbreviation_role
    for i in funding_lis:
        if i not in wrk_cp['lineage']['funding_name_abbreviation_role']:
            wrk_cp['lineage']['funding_name_abbreviation_role'].append(i)
    
    #### coverage
    master_cov = [i['code'] for i in wrk_cp['geographical_extent']['coverage']]
    for ls in ds['Dataset']['geographical_extent']['coverage']:
        if ls['code'] not in master_cov:
            wrk_cp['geographical_extent']['coverage'].append({'code' : ls['code']})
            
    #### update dates
    for i in range(len(ds['Dataset']['identification']['dates'])):
        for j in range(len(wrk_cp['identification']['dates'])):
            if ds['Dataset']['identification']['dates'][i]['type'] == wrk_cp['identification']['dates'][j]['type']:
                if ds['Dataset']['identification']['dates'][i]['date'] != wrk_cp['identification']['dates'][j]['date']:
                    wrk_cp['identification']['dates'][j]['date'] = ds['Dataset']['identification']['dates'][i]['date']
    
    
    return
  
    
def update_object(lis):
    liss = lis.split('$')
    vals = lis.split('$')
    liss = [int(ele) if ele.isdigit() else ele for ele in liss]
    vals = [int(ele) if ele.isdigit() else ele for ele in vals]
    
    try:
        if len(liss) == 2:
            wrk_cp[liss[0]][liss[1]] = ds['Dataset'][vals[0]][vals[1]]
        elif len(liss) == 3:
            wrk_cp[liss[0]][liss[1]][liss[2]] = ds['Dataset'][vals[0]][vals[1]][vals[2]]
        elif len(liss) == 4:
            wrk_cp[liss[0]][liss[1]][liss[2]][liss[3]] = ds['Dataset'][vals[0]][vals[1]][vals[2]][vals[3]]
        elif len(liss) == 5:
            wrk_cp[liss[0]][liss[1]][liss[2]][liss[3]][liss[4]] = ds['Dataset'][vals[0]][vals[1]][vals[2]][vals[3]][vals[4]]
        else:
            assert("Index Overflow")
    except IndexError:
        if len(liss) == 2:
            wrk_cp[liss[0]].append({liss[1] : ds['Dataset'][vals[0]][vals[1]]}) 
        elif len(liss) == 3:
            wrk_cp[liss[0]][liss[1]].append({liss[2] : ds['Dataset'][vals[0]][vals[1]][vals[2]]}) 
        elif len(liss) == 4:
            wrk_cp[liss[0]][liss[1]].append({liss[3] : ds['Dataset'][vals[0]][vals[1]][vals[2]][vals[3]]}) 
        elif len(liss) == 5:
            wrk_cp[liss[0]][liss[1]][liss[2]].append({liss[4] : ds['Dataset'][vals[0]][vals[1]][vals[2]][vals[3]][vals[4]]}) 
        else:
            assert("Index Overflow")
    except KeyError:
        if len(liss) == 3:
            wrk_cp[liss[0]][liss[1]] = {liss[2] : ds['Dataset'][vals[0]][vals[1]][vals[2]]}
        elif len(liss) == 4:
            wrk_cp[liss[0]][liss[1]][liss[2]] = {liss[3] : ds['Dataset'][vals[0]][vals[1]][vals[2]][vals[3]]}
        elif len(liss) == 5:
            wrk_cp[liss[0]][liss[1]][liss[2]][liss[3]] = {liss[4] : ds['Dataset'][vals[0]][vals[1]][vals[2]][vals[3]][vals[4]]}    
        else:
            assert("Index not found!")
    return
    
def get_static_info(ds):
    ###curation harvest source
    #ds['Dataset']['lineage']['curation_harvest_source'] = "MICRODATA"
    ds['Dataset']['lineage']['harvest_system'] = "MICRODATA"
    
    ### language supported
    ds['Dataset']['identification']['language_supported'] = [{"code" : "EN"}]
    
    ### Tag for MDLib
    ds['Dataset']['keywords'] = [{"name" : "microdata"}]
    
    return ds
    
def write_error(data):
    with open(r'harvested_json/Error_File_{}.csv'.format(dt.now().strftime("%Y_%m_%d")), 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data)

def add_to_ddh(ids, ds, tokens, ddh_sess):
    ddh_sess = ddh2.resume_session()
    ddh_sess.check_tokens()
    ddh_params = get_params("ddh2")
    token = json.loads(tokens)
    req = ddh_sess.post("dataset/create", ds)
    
    if req.status_code == 417:
        print("Error: {}".format(req.text), "ID : {}".format(ids))
        write_error([ids, req.text])
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
            write_error([ids, req.text])
            return req.text
    else:
        print("Error {} creating dataset {} with message {}".format(req.status_code, ids, req.test))

def set_archive_id(idno):
    
    temp = pd.read_excel("MDLib_archive_id.xlsx")
    
    try:
        nid = int(temp[temp.idno == idno]['nid'].iloc[0])
    except:
        nid = None
        
    return nid
    
def update_existing(did, ds, ddhs_sess):
    wrk_cp = ddhs_sess.post("dataset/view", {"data" : {"dataset_id" : "{}".format(did)}}).json()
    
    temp1 = flatten_json(recursive_split(wrk_cp, None))
    temp2 = flatten_json(ds['Dataset'])
    
    nocheck_list = ['collection_id', 'language_supported', 'keywords', 'temporal_coverage', 'funding_name_abbreviation_role', 'point_of_contact',
            'curation_related_links_and_publications', 'coverage', 'dates', 'dataset_id', 'dataset_unique_id', 'archive_nid', 'status']
    
    for i in temp2.keys():
        if any([True for k in i.split('$') if k in nocheck_list]):
            pass
        else:
            try:
                if temp1[i] != temp2[i]:
                    update_object(i)
                else:
                    pass
            except KeyError:
                update_object(i)

    manage_list_items()
    post_process_list()
    wrk_cp = clean_empty(wrk_cp)

    ddh_up = ddhs_sess.post("dataset/update", {'Dataset': wrk_cp}).text
    try:
        stat = json.loads(ddh_up.text)
        return req.text
    except json.JSONDecodeError:
        print(ids, req.text)
        write_error([ids, ddh_up.text])
        return req.text
 
    
    
def harvest_mdlib(ids, res, tokens, ddh_sess, add_new, did = None):
    ddh_sess.check_tokens()
    global response, ds, timezone_nw, temp_list, funding_lis, poc_lis
    #notes_lis, stats_lis, study_lis, dcoll_lis, desc_lis,
    response = res
    map_file = pd.read_excel(r"C:\Users\wb542830\OneDrive - WBG\DEC\DDH\DDH2.0\Harvesters\MicroData\MDLib_DDH2_mapping.xlsx", sheet_name=1)
    timezone_nw = pytz.timezone('America/New_York')
    funding_lis = []
    #funding_lis, notes_lis, stats_lis, study_lis, dcoll_lis, desc_lis = [], [], [], [], [], []
    temp_list, poc_lis = [""], []
    
    ds = new_ds()
    for i in map_file.index:

        lis = map_file['ddh2_fields'][i].split('>')
        val = map_file['json_fields'][i].split('$')

        try:
            if len(lis) == 2:
                try:
                    ds[lis[0]][lis[1]] = extract_ds_vals(ds, lis, val, tokens).replace("\n", "<br>")
                except AttributeError:
                    ds[lis[0]][lis[1]] = extract_ds_vals(ds, lis, val, tokens)
                
            elif len(lis) == 3:
                try:
                    ds[lis[0]][lis[1]][lis[2]] = extract_ds_vals(ds, lis, val, tokens).replace("\n", "<br>")
                except AttributeError:
                    ds[lis[0]][lis[1]][lis[2]] = extract_ds_vals(ds, lis, val, tokens)
                if (lis[-1] == "statistical_concept_and_methodology") and (ds[lis[0]][lis[1]][lis[2]] != ""):
                    temp_list.append(ds[lis[0]][lis[1]][lis[2]])

            elif len(lis) == 4 :
                try:
                    ds[lis[0]][lis[1]][lis[2]][lis[3]] = extract_ds_vals(ds, lis, val, tokens).replace("\n", "<br>")
                except AttributeError:
                    ds[lis[0]][lis[1]][lis[2]][lis[3]] = extract_ds_vals(ds, lis, val, tokens)
        except KeyError:
            print(i,'::',lis)
            
            
    if os.path.exists(os.path.join(os.getcwd(), "MDLib_data_classification.csv")):
        cl = pd.read_csv(os.path.join(os.getcwd(), "MDLib_data_classification.csv"))
    else:
        get_data_classfication()
        cl = pd.read_csv(os.path.join(os.getcwd(), "MDLib_data_classification.csv"))

    ds['Dataset']['constraints']['security']['classification'] = cl[cl.idno == ids]['classification'].iloc[0]

    if len(extract_md_meta(['dataset','metadata', 'study_desc', 'study_info', 'nation'])) > 1:
        ds['Dataset']['identification']['title'] = "{} - {}".format("", ds['Dataset']['identification']['title'].replace("'", " "))
    else:
        country = extract_md_meta(['dataset','metadata', 'study_desc', 'study_info', 'nation'])[0]['name']
        ds['Dataset']['identification']['title'] = "{} - {}".format(country, ds['Dataset']['identification']['title'].replace("'", " "))

    ds['Dataset']['identification']['point_of_contact'] = [ast.literal_eval(el1) for el1 in set([str(el2) for el2 in poc_lis])]
    
    ds['Dataset']['constraints']['license']['license_id'] = get_license_info(ds['Dataset']['constraints']['security']['classification'], extract_md_meta(['dataset', 'data_access_type']))

    ds['Dataset']['constraints']['security']['exception'] = get_data_license_exception(ds['Dataset']['constraints']['security']['classification'])

    ds['Dataset']['lineage']['statistical_concept_and_methodology'] = temp_list[-1].replace("\n", "<br>")
    
    ds['Dataset']['last_updated_date'] = ch_date = parse(extract_md_meta(['dataset', 'changed'])).astimezone(timezone_nw).strftime('%Y-%m-%dT%H:%M:%S.%f')
    
    ds['Dataset']['archive_nid'] = set_archive_id(ids)

    #print("Getting static info!!")
    ds = get_static_info(ds)
    ds = clean_empty(ds)

    #add_to_ddh(ids, ds, tokens)

    with open("harvested_json/{}_MDLib.json".format(ids), 'w') as f:
        json.dump(ds, f, indent = 6)
    #print("TYPE: {}".format(temp_list))
    #return
    if add_new:
        stat = add_to_ddh(ids, ds, tokens, ddh_sess)
        return stat
    else:
        stat = update_existing(did, ds, ddh_sess)
        return stat


    
    
def get_resource_json(dataset_id, idno, tokens, ddh_sess):
    ddh_sess = ddh2.resume_session()
    ddh_sess.check_tokens()
    file = pd.read_csv(os.path.join(os.getcwd(), "MDLib_data_classification.csv"))
    temp = file[file.idno == idno].iloc[0]
    
    view_res = ddh_sess.post("dataset/view", {'data': { 'dataset_id' : "{}".format(dataset_id)}, 'showDatasetResources':'true'}).json()

    res_url = "https://microdatalib.worldbank.org/index.php/catalog/{}".format(format(view_res['lineage']['harvest_system_reference'])) 

    resource = {"Resource" : {
          "dataset_id": "{}".format(dataset_id),
          "name": "Related materials (Questionnaires, reports, tables,\n  technical documents, and data files)",
          "unique_id": "{}".format(view_res['dataset_unique_id']),
          "last_updated": "{}".format(view_res['last_updated_date']),
          "identification": {
            "name": "Related materials (Questionnaires, reports, tables,\n  technical documents, and data files)",
            "type": "Landing page"
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
    ddh_sess = ddh2.resume_session()
    ddh_sess.check_tokens()
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
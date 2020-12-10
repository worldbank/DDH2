import ddh2
import pandas as pd
import requests
import json
import dateutil
import datetime
import deepdiff


class EEXException(Exception):
    def __init__(self, response):
        self.status_code = response.status_code
        self.text = response.text

    def __repr__(self):
        return 'EEXException [{}]: {}'.format(self.status_code, self.text)

    def __str__(self):
        return self.text

def map_from_eex(eex_field, eex_value):
    with open('./eex_dataset_mapping.json') as f:
        df = pd.DataFrame(json.load(f))
    value = df.loc[(df['eex_field'] == eex_field) & (df['eex_value'] == eex_value), 'ddh_value']
    if not value.empty:
        value = value.item()
    return value

def create_new_resource(s, ddh_id, eex_r):
    res = ddh2.DDH2Resource(s)
    with open('.//eex_resource_template.json') as f:
        r = json.load(f)
    r['dataset_id'] = ddh_id['dataset_id']
    r['identification']['name'] = eex_r['name']
    r['identification']['description'] = eex_r['description']
    r['dates'].append({"date": eex_r['last_modified'], "type": "LAST_UPDATED_DATE"})
    r['distribution']['format'] = eex_r['format']
    r['distribution']['url'] = eex_r['url']
    r['distribution']['distribution_format'] = eex_r['format']
    r['distribution']['distribution_size'] = eex_r['size']
    r_json = res.create(r)

    return

def create_dataset_json(id, eex):
    ds = None
    with open('./eex_dataset_template.json') as f:
        ds = json.load(f)
    ds['name'] = eex['title']
    ds['identification']['title'] = eex['title']
    ds['identification']['description'] = eex["notes"]
    poc = {"name": eex['author'], "role": "EXTERNAL_CONTACT",
           "email": eex['author_email'], "is_emailaddress_visibility_externally": True
           }
    #    poc = {"name": eex['author'], "role": {"code": "EXTERNAL_CONTACT", "name": "External Contact"},
    #           "email": eex['author_email'], "is_emailaddress_visibility_externally": True
    #    }
    ds['identification']['point_of_contact'].append(poc)
    ds['identification']['dates'].append({"date": eex['metadata_created'], "type": "RELEASE_DATE"})
    ds['identification']['dates'].append({"date": eex['metadata_modified'], "type": "LAST_UPDATED_DATE"})
    ds['last_Updated_Date'] = (datetime.datetime.strptime(eex['metadata_modified'], '%Y-%m-%dT%H:%M:%S.%f')).strftime('%Y-%m-%dT%H:%M:%S')
    ds['lineage']['curation_harvest_system'] = id
    ds['constraints']['license']['license_id'] = map_from_eex('license_title', eex['license_title'])
    ds['constraints']['license']['license_reference'] = map_from_eex('license_title', eex['license_title'])
    #    for c in eex['country_code']:
    #        ds['geographical_extent']['coverage'].append({'code': c})
    #    ds['geographical_extent']['coverage'] = {'code': eex['country_code']}
    if len(eex['end_date']) > 0:
        if not ('temporal_extent' in ds):
            ds['temporal_extent'] = {}
        dt = dateutil.parser.parse(eex['end_date'], default=datetime.datetime(1900, 12, 31, 12, 0, 0))
        ds['temporal_extent']['end_date'] = dt.strftime('%d/%m/%Y %I:%M:%S %p')
    if len(eex['start_date']) > 0:
        if not ('temporal_extent' in ds):
            ds['temporal_extent'] = {}
        dt = dateutil.parser.parse(eex['start_date'], default=datetime.datetime(1900, 1, 1, 12, 0, 0))
        ds['temporal_extent']['start_date'] = dt.strftime('%d/%m/%Y %I:%M:%S %p')
    return ds

def create_new_dataset(r, s):
    eex = get_eex_dataset(r['nid_x'])
    ddh = ddh2.DDH2Dataset(s)
    ds = create_dataset_json(r['nid_x'], eex)
    new_id = ddh.create(ds)
    # create resources
    for i in eex['resources']:
        rid = create_new_resource(s, new_id, i)

    # publish dataset
    pl = {"dataset_id": new_id['dataset_id'], "dataset_status": "PUBLISHED",
          "publish_label": "Dataset created and published by EEX Harvester."}
    response = s.post('workflow/updatestatus', pl)

    return new_id['dataset_id']

def update_dataset(r, s):
    eex = get_eex_dataset(r['nid_x'])
    new_ds = create_dataset_json(r['nid_x'], eex)
    ddh = ddh2.DDH2Dataset(s)
    ds = ddh.get(r['nid_y'])
    new_ds['dataset_id'] = r['nid_y']
    new_ds['dataset_unique_id'] = ds['dataset_unique_id']
    # update lists

    # compare new fields with old
    ddiff = deepdiff.DeepDiff(ds, new_ds, ignore_order=True)
    print(ddiff)

    return

def connect_eex(endpoint, query=None, root="https://energydata.info", token=""):
    url = root + endpoint
    headers = {'X-API-KEY': token, 'charset': 'utf-8'}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise EEXException(response)
    return response

def get_eex_dataset(id):
    response = connect_eex(endpoint="/api/3/action/package_show?id={}".format(id))
    if response.status_code != 200:
        raise EEXException(response)
    return (response.json())['result']

def get_eex_datasets():
    response = connect_eex(endpoint="/api/3/action/package_search?fq=eex_user_origin:True%20organization:world-bank-grou&rows=1000")
    js = response.json()
    df = pd.DataFrame(js['result']['results'])
    print(df)
    eex = pd.DataFrame()
    eex['nid'] = df['id']
    eex['metadata_created'] = df['metadata_created']
    eex['metadata_modified'] = df['metadata_modified']

    return eex

def filter_eex_datasets(r, s):
    if r['dataset_id']:
        details = ddh2.DDH2Dataset(s).get(r['dataset_id'])
        if details['lineage']['curation_harvest_source']:
            print("harvest source: {}".format(details['lineage']['curation_harvest_source']))
            return True, details['lineage']['curation_harvest_source'], \
                   details['lineage']['curation_harvest_system'], details['last_Updated_Date']
        elif details['lineage']['curation_harvest_system']:
            return True, details['lineage']['curation_harvest_source'], \
                   details['lineage']['curation_harvest_system'], details['last_Updated_Date']
    return False, None, None, None


def get_ddh_records_status(s):
    ds = ddh2.DDH2Dataset(s)
    datasets = pd.DataFrame(ds.list())
    datasets = datasets.loc[datasets['status'] == 'PUBLISHED']
    datasets[['eex', 'harvest_source', 'harvest_id', 'last_modified']] = datasets.apply(filter_eex_datasets, args=(s,), axis=1, result_type='expand')
    eex_datasets = datasets.loc[datasets['eex'] == True]
    results = pd.DataFrame()
    results['nid'] = eex_datasets['dataset_id']
    results['harvest_src'] = eex_datasets['harvest_source']
    results['harvest_sys_id'] = eex_datasets['harvest_id']
    results['modified_date'] = eex_datasets['last_modified']
#    eex_ds = datasets.loc[datasets['']]

    return results


def main():
#    session = ddh2.create_session(cache=True)
    session = ddh2.resume_session()
    if not session:
        session = ddh2.create_session(cache=True)
### work offline
###    eex_df = get_eex_datasets()
###    ddh_df = get_ddh_records_status(session)


    # combine and set status on each dataset - new (not in DDH), current (up to date in DDH)
    # updated (needs to be updated in DDH) or deleted (need to remove from DDH)
###    full_df = pd.merge(eex_df, ddh_df, how="left", left_on="nid", right_on="harvest_sys_id")
###    full_df.loc[full_df['nid_y'].isna(), 'status'] = 'new'
###    full_df.loc[full_df['nid_x'].isna(), 'status'] = 'deleted'
###    full_df.loc[pd.to_datetime(full_df['metadata_modified']) <
###                pd.to_datetime(full_df['modified_date']), 'status'] = 'updated'
###    full_df.loc[pd.to_datetime(full_df['metadata_modified']) >=
###                pd.to_datetime(full_df['modified_date']), 'status'] = 'current'

### load data from file
    full_df = pd.read_csv('./full_df.csv')
    full_df.loc[[full_df['status'] == 'updated'], 'nid_y'] = (full_df.loc[full_df['status'] == 'updated']).apply(update_dataset, args=(session,), axis=1)
    full_df.loc[[full_df['status'] == 'new'], 'nid_y'] = (full_df.loc[full_df['status'] == 'new']).apply(create_new_dataset, args=(session,), axis=1)
    return

if __name__ == '__main__':
    main()
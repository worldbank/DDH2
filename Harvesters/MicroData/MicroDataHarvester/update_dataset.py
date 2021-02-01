def update_dataset(r, s):
    eex = get_eex_dataset(r['nid_x'])
    new_ds = create_dataset_json(r['nid_x'], eex)
    ddh = ddh2.DDH2Dataset(s)
    ds = ddh.get(r['nid_y'], resources=True)
    new_ds['dataset_id'] = r['nid_y']
    new_ds['unique_id'] = ds['dataset_unique_id']
    new_ds['identification']['id'] = ds['identification']['id']
    new_ds['lineage']['id'] = ds['lineage']['id']
    new_ds['constraints']['id'] = ds['constraints']['id']
    if 'temporal_extent' in new_ds:
        new_ds['temporal_extent']['id'] = ds['temporal_extent']['id']
    if 'reference_system' in new_ds:
        new_ds['reference_system']['id'] = ds['reference_system']['id']
    if 'geographical_extent' in new_ds:
        new_ds['geographical_extent']['id'] = ds['geographical_extent']['id']
    # update lists
    # point of contacts
    for poc in ds['identification']['point_of_contact']:
        poc.pop('role')
        poc['is_delete'] = True
        new_ds['identification']['point_of_contact'].append(poc)
    # topics - check if already exists
    for t_new in new_ds['identification']['topics']:
        if ds['identification']['topics']:
            for t_orig in ds['identification']['topics']:
                if t_new['name'] == t_orig['name']:
                    t_new['id'] = t_orig['id']
                    break
    # languages
    for l_new in new_ds['identification']['language_supported']:
        if ds['identification']['language_supported']:
            for l_orig in ds['identification']['language_supported']:
                if l_new['code'] == l_orig['code']:
                    l_new['id'] = l_orig['id']
                    break
    # keywords
    for k_new in new_ds['keywords']:
        if ds['keywords']:
            for k_orig in ds['keywords']:
                if k_new['name'] == k_orig['name']:
                    k_new['id'] = k_orig['id']
                    break
    # dates
    for d_new in new_ds['identification']['dates']:
        if ds['identification']['dates']:
            for d_orig in ds['identification']['dates']:
                if (d_new['type'] == d_orig['type']) & (d_new['date'] == d_orig['date']):
                    d_new['id'] = d_orig['id']
                    break
    # countries
    if 'coverage' in new_ds['geographical_extent']:
        for c_new in new_ds['geographical_extent']['coverage']:
            if ds['geographical_extent']['coverage']:
                for c_orig in ds['geographical_extent']['coverage']:
                    if c_new['code'] == c_orig['code']:
                        c_new['id'] = c_orig['id']
                        break

    ddh.update(new_ds)
    # update resources
    for i in eex['resources']:
        rj = create_resource_json(i)
        for j in ds['resources']:
            # find matching resource
            if j['name'] == i['name']:
                # get resource metadata details
                res = ddh2.DDH2Resource(s)
                resource = res.get(j['resource_id'])
#                rj['lineage'] = {'harvest_system': 'ENERGY', 'source_reference': i['id']}
                if 'lineage' in resource:
                    rj['lineage']['id'] = resource['lineage']['id']
                rj['resource_id'] = resource['resource_id']
                rj['resource_unique_id'] = resource['resource_unique_id']
                if 'dates' in resource:
                    for d_new in rj['dates']:
                        for d_orig in resource['dates']:
                            if (d_new['type'] == d_orig['type']) & (d_new['date'] == d_orig['date']):
                                d_new['id'] = d_orig['id']
                                break
                if 'distribution' in resource:
                    rj['distribution']['id'] = resource['distribution']['id']
                res.update(rj)
                break

    #publish dataset
    pl = {"dataset_id": new_ds['dataset_id'], "dataset_status": "PUBLISHED", "publish_label": "published via API"}
    response = ddh.session.post('workflow/updatestatus', pl)
    # compare new fields with old
    ddiff = deepdiff.DeepDiff(ds, new_ds, ignore_order=True)
    print(ddiff)

    return new_ds['dataset_id']

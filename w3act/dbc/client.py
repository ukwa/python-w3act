import dateutil.parser
import datetime
import argparse
import psycopg2
import logging
from urllib.parse import urlparse
import shutil
import json
import csv
import os
import re

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )

def get_csv(csv_dir, params):
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    csv_dir = os.path.abspath(csv_dir)
    if not os.path.exists(csv_dir):
        os.mkdir(csv_dir)

    cur.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")
    for table in cur.fetchall():
        print("Downloading table %s" % table)
        csv_file = os.path.join(csv_dir,'%s.csv' % table)
        with open(csv_file, 'wb') as f:
            cur.copy_expert("COPY %s TO STDOUT WITH CSV HEADER" % table, f)

    cur.close()
    conn.close()


def csv_to_zip(csv_dir):
    # Bundle as a ZIP:
    parent_dir = os.path.abspath(os.path.join(csv_dir, os.pardir))
    return shutil.make_archive(csv_dir, 'zip', parent_dir, os.path.basename(csv_dir))


def check_npld_status(target):
    # Manual Flags:
    if target['professional_judgement'] \
            or target['uk_postal_address'] \
            or target['via_correspondence']:
        return True
    # Automatic flags (computed by W3ACT at this stage):
    if target['is_top_level_domain'] \
            or target['is_uk_hosting'] \
            or target['is_uk_registration']:
        # TODO Re-check the above
        return True
    return False


def check_oa_status(target):
    lics = target.get('licenses', [])
    if len(lics) > 0:
        return True
    return False


def attach_child_terms(col, bypid):
    children = col.get('children', [])
    cid = col['id']
    for child in bypid.get(cid, []):
        attach_child_terms(child, bypid)
        children.append(child)
    col['children'] = children


def extract_taxonomy(tax, tax_name):
    topc = {}
    bypid = {}
    for tid in tax:
        if tax[tid]['ttype'] == tax_name:
            if not tax[tid]['parent_id']:
                topc[tid] = tax[tid]
            else:
                pid = int(tax[tid]['parent_id'])
                children = bypid.get(pid,[])
                children.append(tax[tid])
                bypid[pid] = children

    for tid in topc:
        col = topc[tid]
        attach_child_terms(col, bypid)

    return topc


def load_csv(csv_dir="./test/w3act-csv"):
    logger.info("Loading W3ACT data...")
    logger.info("Loading targets...")

    if not os.path.exists(csv_dir):
        raise ValueError("ERROR! CSV folder does not exist: %s" % csv_dir)

    if not os.listdir(csv_dir):
        raise ValueError("ERROR! CSV folder is empty: %s" % csv_dir)        

    targets = {}
    with open(os.path.join(csv_dir,'target.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['id'] != 'id': # Skip header row
                # Turn booleans into proper booleans:
                tfs = ["active", "hidden", "ignore_robots_txt", "is_in_scope_ip", "is_in_scope_ip_without_license",
                       "is_top_level_domain", "is_uk_hosting", "is_uk_registration", "key_site", "no_ld_criteria_met",
                       "professional_judgement", "special_dispensation", "uk_postal_address", "via_correspondence"]
                for tf in tfs:
                    if row[tf] == 't':
                        row[tf] = True
                    else:
                        row[tf] = False
                # turn id into int
                row['id'] = int(row['id'])
                # And store
                targets[row['id']] = row

    # JOIN to get URLs:
    logger.info("Loading URLs...")
    with open(os.path.join(csv_dir,'field_url.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['id'] != 'id':
                tid = int(row['target_id'])
                if tid not in targets:
                    logger.warning(f"So such Target {tid} - no match for URL row: {row}")
                    continue
                urls = targets[tid].get('urls', [])
                if row['position'] != '':
                    urls.insert(int(row['position']),row['url'])
                else:
                    urls.append(row['url'])
                targets[tid]['urls'] = urls

    # Grab the taxonomies
    logger.info("Loading taxonomies...")
    tax = {}
    with open(os.path.join(csv_dir,'taxonomy.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['id'] != 'id':
                # Turn booleans into proper booleans:
                tfs = ["publish"] # ie. ["publish", optional_boolean2, ..., optional_booleanN]
                for tf in tfs:
                    if row[tf] == 't':
                        row[tf] = True
                    else:
                        row[tf] = False
                # turn id into int
                row['id'] = int(row['id'])
                tax[row['id']] = row

    # Grab the collection-target associations...
    logger.info("Loading collection_target associations...")
    tid_cid = {}
    with open(os.path.join(csv_dir,'collection_target.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['target_id'] != 'target_id':
                tid = int(row['target_id'])
                cid = int(row['collection_id'])
                # Collections by Target
                cids = tid_cid.get(tid, set())
                cids.add(cid)
                tid_cid[tid] = cids
                # Targets by Collection
                tids = tax[cid].get('target_ids', [])
                tids.append(tid)
                tax[cid]['target_ids'] = tids

    # Grab the subjects
    logger.info("Loading subject_target associations...")
    tid_sid = {}
    with open(os.path.join(csv_dir,'subject_target.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['target_id'] != 'target_id':
                tid = int(row['target_id'])
                sid = int(row['subject_id'])
                # Subjects by Target
                sids = tid_sid.get(tid, set())
                sids.add(sid)
                tid_sid[tid] = sids
                # Targets by Subject
                tids = tax[sid].get('target_ids', [])
                tids.append(tid)
                tax[sid]['target_ids'] = tids

    # Also get the high-level 'collection areas'...
    logger.info("Loading collection areas...")
    caid_cid = {}
    collection_areas = {}
    with open(os.path.join(csv_dir,'taxonomy_parents_all.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['taxonomy_id'] != 'taxonomy_id':
                caid = int(row['taxonomy_id'])
                cid = int(row['parent_id'])
                # Collections  by Collection Area
                cids = caid_cid.get(caid, [])
                cids.append(cid)
                caid_cid[caid] = cids
                if caid not in collection_areas:
                    collection_areas[caid] = {
                        'id': caid,
                        'name': tax[caid]['name'],
                        'description': tax[caid]['description'],
                        'collections': caid_cid[caid]
                    }
                # Add collection area to collection:
                caids = tax[cid].get('collection_area_ids', [])
                caids.append(caid)
                tax[cid]['collection_area_ids'] = caids

    # Watched Target setup
    logger.info("Loading watched_target associations...")
    with open(os.path.join(csv_dir,'watched_target.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['id'] != 'id':
                tid = int(row['id_target'])
                targets[tid]['watched'] = True
                targets[tid]['document_url_scheme'] = row['document_url_scheme']

    # Licenses license_target table to Taxonomy table (yay American spelling!)
    logger.info("Loading licenses...")
    with open(os.path.join(csv_dir,'license_target.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            tid = int(row['target_id'])
            licid = int(row['license_id'])
            # License Names:
            tlic = targets[tid].get('licenses', [])
            tlic.append(tax[licid]['name'])
            targets[tid]['licenses'] = tlic
            # Also keep IDs:
            tlic = targets[tid].get('license_ids', [])
            tlic.append(licid)
            targets[tid]['license_ids'] = tlic

    # Grab the authors/curators:
    logger.info("Loading creators...")
    authors = {}
    with open(os.path.join(csv_dir,'creator.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['id'] != 'id':
                # Pop some unnecessary fields:
                for f in ['password', 'url', 'edit_url', 'affiliation']:
                    row.pop(f)
                # turn id into int
                row['id'] = int(row['id'])
                # Store
                authors[row['id']] = row

    # Load the organisations:
    logger.info("Loading organisations...")
    orgs = {}
    with open(os.path.join(csv_dir,'organisation.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['id'] != 'id':
                # Pop some unnecessary fields:
                for f in ['author_id', 'url', 'edit_url', 'affiliation']:
                    row.pop(f)
                # turn id into int
                row['id'] = int(row['id'])
                # Store
                orgs[row['id']] = row

    # JOIN to get
    #
    # CrawlPermissions crawl_permission table
    # Subjects subject_target Taxonomy table
    # Tags tag_target Taxonomy table
    # Flags flag_target Taxonomy table
    # LookupEntries (unused?) from lookup_entry table
    #
    # DocumentOwner document_owner_id from creator table
    # WatchedTarget from watched_target table
    #
    # To be NPLD need to check the fields, can be inherited.
    #
    # To be OA need to must have (or inherit) a license

    # Extract the Collections heirarchy:
    collections = extract_taxonomy(tax,'collections')

    # And the subjects:
    subjects = extract_taxonomy(tax,'subject')

    # And the licences:
    licenses = extract_taxonomy(tax,'licenses')

    # Post-processs the targets
    oa_urls = set()
    npld_urls = set()
    for tid in targets:
        # Collections:
        targets[tid]['collection_ids'] = list(tid_cid.get(tid,[]))
        # Subjects:
        targets[tid]['subject_ids'] = list(tid_sid.get(tid,[]))
        # QA Issues qaissue_id  Taxonomy
        qaid = targets[tid]['qaissue_id']
        targets[tid]['qaissue_score'] = 0
        if qaid:
            qaid = int(qaid)
            targets[tid]['qaissue'] = tax[qaid]['name']
            if qaid == 233:
                targets[tid]['qaissue_score'] = 1 # QA Issues
            elif qaid == 909:
                targets[tid]['qaissue_score'] = 2 # QA Issues (but OK to publish)
            elif qaid == 190:
                targets[tid]['qaissue_score'] = 3 # No QA Issues
        # NPLD status:
        targets[tid]['isNPLD'] = check_npld_status(targets[tid])
        if targets[tid]['isNPLD']:
            for url in targets[tid].get('urls',[]):
                npld_urls.add(url)
        # OA status:
        targets[tid]['isOA'] = check_oa_status(targets[tid])
        if targets[tid]['isOA']:
            for url in targets[tid].get('urls',[]):
                oa_urls.add(url)

    # Second pass to add inherited statuses:
    # FIXME Both should be inherited from all higher-level Targets. This version only inherits from hosts.
    for tid in targets:
        for url in targets[tid].get('urls',[]):
            parsed_uri = urlparse(url)
            base = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
            if base in oa_urls and not targets[tid]['isOA']:
                targets[tid]['isOA'] = True
                targets[tid]['inheritsOA'] = True
            if base in npld_urls and not targets[tid]['isNPLD']:
                targets[tid]['isNPLD'] = True
                targets[tid]['inheritsNPLD'] = True

    # Perform some additional validation.
    invalid_tids = set()
    bare_twit = re.compile("https?:\/\/[\w.]*twitter\.com\/[a-zA-Z0-9_?=]{0,15}$")
    for tid in targets:
        for url in targets[tid].get('urls',[]):
            if bare_twit.match(url):
                logger.error("This target (%s) has a bare Twitter URL as a seed! %s" % ( tid, url))
                targets[tid]['invalid_reason'] = "Bare Twitter URLs are not allowed."
                invalid_tids.add(tid)

    # Now drop the bad ones:
    invalid_targets = []
    for tid in invalid_tids:
        logger.debug("Dropping invalid target %s" % tid )
        target = targets[tid]
        invalid_targets.append(target)
        del targets[tid]

    # Assemble the results into a single dict():
    all = {
        'targets': targets,
        'invalid_targets': invalid_targets,
        'curators' : authors,
        'organisations': orgs,
        'collections': collections,
        'collection_areas': collection_areas,
        'subjects': subjects,
        'licenses': licenses
    }

    return all


def filtered_targets(targets, frequency=None, terms='npld', include_hidden=True, omit_uk_tlds=False, include_expired=True):
        # aggregate
        filtered = []
        for t in targets.values():
            # Only emit un-hidden Targets here:
            if not include_hidden and t['hidden']:
                continue
            # Filter out based on frequencies:
            if frequency:
                # If 'all', filter out NEVERCRAWL (whereas 'None' doesn't filter at all).
                if frequency == 'all':
                    if t['crawl_frequency'].lower() == 'nevercrawl':
                        continue
                # Otherwise, only emit matching frequency:
                elif t['crawl_frequency'].lower() != frequency.lower():
                    continue
            # Filter down by crawl terms:
            if terms == 'npld':
                if not t.get('isNPLD', None):
                    continue
            elif terms == 'oa':
                if not t.get('isOA', None):
                    continue
            elif terms == 'bypm':
                # Skip if not OA:
                if not t.get('isOA', None):
                    continue
                # Skip if NPLD:
                if t.get('isNPLD', None):
                    continue
                # i.e. this one has an OA license but is not NPLD, so is BYPM:
            elif terms == 'all':
                # Let everything through:
                pass
            elif terms != None:
                logging.error("Unrecognised terms filter '%s'! Only None, 'npld', 'oa' and 'all' are implemented!" % terms)
                continue
            # Don't bother outputting items that are trivially in scope:
            # (Note that as this is target-level, this only matches if all seeds are .uk)
            if omit_uk_tlds and t['is_top_level_domain']:
                continue
            # Don't bother outputting expired items:
            if not include_expired and t['crawl_end_date']:
                end_date = dateutil.parser.parse(t['crawl_end_date'])
                if end_date < datetime.datetime.now():
                    date_delta = end_date - datetime.datetime.now()
                    logger.info("Skipping target %i '%s' with crawl end date in the past (%s)" %(t['id'], t['title'], date_delta))
                    continue
            # Othewise, emit:
            filtered.append(t)
        # And return
        return filtered


def filtered_collections(collections, include_unpublished=False):

    if not include_unpublished:
        new_collections = {}
        for cid in collections:
            new_collection = clear_unpublished_collections(collections[cid])
            if new_collection != None:
                new_collections[cid] = new_collection
        collections = new_collections

    return collections


def to_crawl_feed_format(target):
    cf = {
        "id": target['id'],
        "title": target['title'],
        "seeds": target.get('urls', []),
        "depth": target['depth'],
        "scope": target['scope'],
        "ignoreRobotsTxt": target['ignore_robots_txt'],
        "schedules": [
            {
                "startDate": target['crawl_start_date'],
                "endDate": target['crawl_end_date'],
                "frequency": target['crawl_frequency']
            }
        ],
        "watched": target.get('watched', False),
        "documentUrlScheme": target.get('document_url_scheme', None),
        "loginPageUrl": target['login_page_url'],
        "logoutUrl": target['logout_url'],
        "secretId": target['secret_id']
    }
    return cf


def remove_curator_info(targets, curators):

    curator_names = []
    for curator in curators:
        if curator['name'] in "admin,readonly": 
            continue
        curator_names.append(curator['name'])

    for target in targets:
        target.pop('author_id', None)
        for key in target:
            if isinstance(target[key], str): # I originally checked against a specific list of fields but this is more robust and not much slower
                if key == 'title': continue # occasional hit but shouldn't matter
                for curator_name in curator_names:
                    if curator_name in target[key]:
                        target[key] = target[key].replace(curator_name, 'the curator')


# walk the collections (a tree of dictionaries and lists), replacing lists of target ids with target data
def replace_target_ids_with_data(obj): 
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "target_ids":
                denorm_targets = []
                for target_id in obj[k]:
                    try:
                        denorm_targets.append(target_lookup[target_id]) # target_lookup is a global variable
                    except KeyError: # not in valid target set
                        for invalid_target in invalid_target_lookup: # global again
                            if invalid_target['id'] == target_id:
                                denorm_targets.append(invalid_target) 
                                break
                        else: # invalid target not found either
                            logger.warning("Could not find target %i in target_lookup" % target_id)
                            denorm_targets.append({'id': str(target_id), 'warning': '<Target Not In Result Set>'})
                obj[k] = denorm_targets
            elif isinstance(v, (dict, list)):
                replace_target_ids_with_data(v)

    elif isinstance(obj, list):
        for item in obj:
            replace_target_ids_with_data(item)


def rename_key(iterable, before_key, after_key):
    if isinstance(iterable, dict):
        for key in list(iterable.keys()):
            if key == before_key:
                iterable[after_key] = iterable[before_key]
                del iterable[key]
        for obj in iterable:
            rename_key(iterable[obj], before_key, after_key)
    elif type(iterable) is list:
        for obj in iterable:
            rename_key(obj,before_key,after_key)

def clear_unpublished_collections(collection):
    logger.info(f"Looking to clear unpublished collections in collection: {collection['name']}...")
    # If this collection should not be published, signal that it should be dropped:
    if collection['publish'] == False:
        return None
    else:
        # Accept the collection, and process the child collections:
        new_children = []
        for child in collection['children']:
            new_child = clear_unpublished_collections(child)
            if new_child != None:
                new_children.append(new_child)
        collection['children'] = new_children
        return collection

def csv_to_api_json(target_data, invalid_target_data, collection_data, curators, output_dir='/tmp/test'):

    global target_lookup, invalid_target_lookup  
    target_lookup = target_data   
    invalid_target_lookup = invalid_target_data   

    # remove private curatorial data from targets 
    # (targets and invalid_targets are slightly different data structures)
    remove_curator_info(target_lookup.values(), curators.values())
    remove_curator_info(invalid_target_lookup, curators.values())

    logger.info("Replacing lists of target_ids with lists of targets...")
    # replace ids with data via a nested (recursive) update
    replace_target_ids_with_data(collection_data)
    # now rename the keys themselves, similar method
    rename_key(collection_data, "target_ids", "targets")

    # save the collections with newly expanded target data into one file per collection
    output_dir = output_dir + '/collection/'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for k,v in collection_data.items():
        logger.info(f"Writing json file for collection {k}")
        with open(output_dir  + str(k) + '.json', 'w') as f:
            json.dump(v, f, indent=4, sort_keys=True)


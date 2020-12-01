# -*- coding: utf-8 -*-
import json
import logging
import datetime
import pytz

logger = logging.getLogger(__name__)

def convert_to_full_iso(db_datetime):
    dbdt = datetime.datetime.strptime(db_datetime, '%Y-%m-%d %H:%M:%S')
    dbdt = dbdt.replace(tzinfo=pytz.utc)
    return dbdt.isoformat(timespec='milliseconds')

def generate_annotations(targets_by_id, collections_by_id, subjects_by_id):
    # Both Collections and Subjects are stored as trees, but to lookup subjects we need to flatten the tree:
    subjects_by_id_flat = {}
    for subject in _flatten_tree(subjects_by_id.values()):
        sid = subject['id']
        subjects_by_id_flat[sid] = subject

    # Assemble the annotations, keyed on scope + url:
    annotations = {
        "collections": {
            "subdomains": {
            },
            "resource": {
            },
            "root": {
            },
            "plus1": {
            }
        },
        "collectionDateRanges": {
        }
    }

    for collection_id in collections_by_id:
        collection = collections_by_id[collection_id]
        _add_annotations(annotations, collection, targets_by_id, subjects_by_id_flat)

    return annotations

def _flatten_tree(collection):
    for item in collection:
        yield item
        # And the children:
        if 'children' in item:
            for item in _flatten_tree(item['children']):
                yield item

def _add_annotations(annotations, collection, targets_by_id, subjects_by_id, prefix=""):
    # assemble full collection name:
    collection_name = "%s%s" % (prefix, collection['name'])
    # deal with all targets:
    for tid in collection.get('target_ids',[]):
        if tid not in targets_by_id:
            logger.error("Target %i not found in targets list!" % tid)
            continue
        target = targets_by_id[tid]
        scope = target['scope']
        if scope is None or scope == '':
            logger.error("Scope not set for %s - %s!" % (tid, target['urls']) )
            continue
        for url in target.get('urls',[]):
            ann = annotations['collections'][scope].get(url, {'collection': collection_name, 'collections': [], 'subject': []})
            if collection_name not in ann['collections']:
                ann['collections'].append(collection_name)
            # And subjects:
            for sid in target['subject_ids']:
                if sid in subjects_by_id:
                    subject_name = subjects_by_id[sid]['name']
                    #logger.debug("Subject %s referenced in target %i FOUND: %s" % (sid, tid,subject_name))
                    if subject_name not in ann['subject']:
                        ann['subject'].append(subject_name)
                else:
                    logger.warn("Subject %s referenced in target %i does not appear to exist!?" % (sid, tid))
            # and patch back in:
            annotations['collections'][scope][url] = ann

    # And add date ranges:
    # n.b. format from DB/CSV: 2020-03-13 13:16:22.445
    annotations['collectionDateRanges'][collection_name] = {}
    if collection['start_date']:
        annotations['collectionDateRanges'][collection_name]['start'] = convert_to_full_iso(collection['start_date'])
    else:
        annotations['collectionDateRanges'][collection_name]['start'] = None
    if collection['end_date']:
        annotations['collectionDateRanges'][collection_name]['end'] = convert_to_full_iso(collection['end_date'])
    else:
        annotations['collectionDateRanges'][collection_name]['end'] = None

    # And process child collections:
    for child_collection in collection['children']:
        _add_annotations(annotations, child_collection, targets_by_id, subjects_by_id, prefix="%s|" % collection_name)

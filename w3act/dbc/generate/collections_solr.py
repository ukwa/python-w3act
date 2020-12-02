# -*- coding: utf-8 -*-
import json
import pysolr
import logging

logger = logging.getLogger(__name__)

def add_collection(s, targets_by_id, col, parent_id):
    if col['publish']:
        logger.info("Publishing collection '%s'..." % col['name'])

        # add a document to the Solr index
        s.add([
            {
                "id": col["id"],
                "type": "collection",
                "name": col["name"],
                "description": col["description"],
                "parentId": parent_id,
                "collectionAreaId": col.get('collection_area_id', None)
            }
        ], commit=False)

        # Look up all Targets within this Collection and add them.
        targets_sent = 0
        for tid in col.get('target_ids',[]):
            # Get the Target:
            target = targets_by_id.get(tid, None)
            if not target:
                logger.error("Warning! Could not find target %i" % tid)
                continue

            # Skip items with no URLs:
            if len(target.get('urls',[])) == 0:
                continue

            # Determine license status:
            licenses = []
            if target.get('isOA', False):
                licenses = target.get("license_ids",[])
                # Use a special value to indicate an inherited license:
                if len(licenses) == 0:
                    licenses = ['1000']

            # add a document to the Solr index
            s.add([{
                "id": "cid:%i-tid:%i" % (col['id'], target['id']),
                "type": "target",
                "parentId": col['id'],
                "title": target["title"],
                "description": target["description"],
                "url": target["urls"][0],
                "additionalUrl": target["urls"][1:],
                "language": target["language"],
                "startDate": target["crawl_start_date"],
                "endDate": target["crawl_end_date"],
                "licenses": licenses
            }], commit=False)

            # Count successes:
            targets_sent += 1
        
        # Log targets
        logger.info("Added %i targets of %i in the collection." % (targets_sent, len(targets_by_id)))

        # Add child collections
        for cc in col["children"]:
            add_collection(s, targets_by_id, cc, col['id'])
    else:
        logger.warn("Skipping unpublished collection '%s'." % col['name'])

    return

def populate_collections_solr(solr_endpoint, targets, collections, subjects):

    # (re)build look-up table for Target IDs
    targets_by_id = {}
    target_count = len(targets)
    for target in targets:
        tid = target['id']
        targets_by_id[tid] = target
        target_count += 1
    logger.info("Found %i targets..." % target_count)

    s = pysolr.Solr(solr_endpoint, timeout=30)

    # First, we delete everything (!)
    s.delete(q="*:*", commit=False)

    # Update the collections:
    for col in collections.values():
        add_collection(s, targets_by_id, col, None)

    # Now commit all changes:
    s.commit()
    logger.info("Changes committed.")


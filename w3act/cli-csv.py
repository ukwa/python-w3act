#!/usr/bin/env python
# encoding: utf-8
#
# Utils for managing CSV data from W3ACT

import argparse
import json
import csv
import os


def load_csv(csv_dir="./test/w3act-csv"):
    targets = {}
    with open(os.path.join(csv_dir,'target.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['id'] != 'id':
                targets[int(row['id'])] = row

    # JOIN to get URLs:
    with open(os.path.join(csv_dir,'field_url.csv'), 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['id'] != 'id':
                tid = int(row['target_id'])
                urls = targets[tid].get('urls', [])
                if row['position'] != '':
                    urls.insert(int(row['position']),row['url'])
                else:
                    urls.append(row['url'])
                targets[tid]['urls'] = urls

    # JOIN to get
    #
    # QA Issues qaissue_id  Taxonomy
    # CrawlPermissions crawl_permission table
    # Licenses license_target table to Taxonomy table
    # Subjects subject_target Taxonomy table
    # Collections collection_target Taxonomy table
    # Tags tag_target Taxonomy table
    # Flags flag_target Taxonomy table
    # LookupEntries (unused?) from lookup_entry table
    # AuthorUser author_id from creator table
    # Nominating organisation organsation_id from organisation table
    #
    # DocumentOwner document_owner_id from creator table
    # WatchedTarget from watched_target table
    #
    # To be NPLD need to check the fields, can be inherited.
    #
    # To be OA need to must have (or inherit) a license


    for tid in targets:
        for url in targets[tid].get('urls',[]):
            print("%s %s" % ( targets[tid]['crawl_frequency'], url))
    #print(json.dumps(targets[8938], indent=2))


def main():
    parser = argparse.ArgumentParser('Manipulate W3ACT CSV')
    load_csv()


if __name__ == "__main__":
    main()
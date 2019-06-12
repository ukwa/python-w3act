#!/usr/bin/env python
# encoding: utf-8
#
# Utils for managing CSV data from W3ACT

import argparse
import psycopg2
import shutil
import json
import csv
import os


def get_csv(csv_dir='../w3act-db-csv/'):
    params = {
        'password': os.environ.get("W3ACT_PSQL_PASSWORD"),
        'database': os.environ.get("W3ACT_PSQL_DATABASE", "w3act"),
        'user': os.environ.get("W3ACT_PSQL_USER", "w3act"),
        'host': os.environ.get("W3ACT_PSQL_HOST", "192.168.45.60"),
        'port': os.environ.get("W3ACT_PSQL_PORT", 5434)
    }
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    csv_dir = os.path.abspath(csv_dir)

    cur.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")
    for table in cur.fetchall():
        print("Downloading table %s" % table)
        csv_file = os.path.join(csv_dir,'%s.csv' % table)
        with open(csv_file, 'wb') as f:
            cur.copy_expert("COPY %s TO STDOUT WITH CSV HEADER" % table, f)

    cur.close()
    conn.close()

    # and bundle:
    parent_dir = os.path.abspath(os.path.join(csv_dir, os.pardir))
    shutil.make_archive(csv_dir, 'zip', parent_dir, os.path.basename(csv_dir))


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
    get_csv()
    #load_csv()


if __name__ == "__main__":
    main()

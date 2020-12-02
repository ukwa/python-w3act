#!/usr/bin/env python
# encoding: utf-8
#
# Utils for managing CSV data from W3ACT

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
from w3act.dbc.client import get_csv, load_csv, filtered_targets, csv_to_zip, to_crawl_feed_format
from w3act.dbc.generate.acls import generate_oa_allow_list
from w3act.dbc.generate.annotations import generate_annotations
from w3act.dbc.generate.collections_solr import populate_collections_solr
from w3act.dbc.generate.site import GenerateSitePages

# Set up overall logging config:
logging.basicConfig(level=logging.WARNING, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

# Set up logger for this module:
logger = logging.getLogger(__name__)

def write_json(filename, all):
    with open(filename,"w") as f:
        json.dump(all, f, indent=2)

def main():
    parser = argparse.ArgumentParser('Export and manipulate W3ACT CSV')
    parser.add_argument('-v', '--verbose',  action='count', default=0, help='Logging level; add more -v for more logging.')
    parser.add_argument('-d', '--csv-dir', dest='csv_dir', help="Folder to cache CSV data in.", default="w3act-db-csv")

    parser.add_argument('-f', '--frequency', dest="frequency", type=str,
                        default='all', choices=[ None, 'nevercrawl', 'daily', 'weekly',
                                                'monthly', 'quarterly', 'sixmonthly',
                                                'annual', 'domaincrawl', 'all'],
                        help="Filter targets by crawl frequency (n.b. 'all' means all-but-nevercrawl) [default: %(default)s]")

    # Which terms, e.g. NPLD, by-permission, or both, or no terms that permit crawling:
    parser.add_argument('-t', '--terms', dest='terms', type=str, default='npld',
                        choices=[ 'npld', 'bypm', 'no-terms', 'all'],
                        help="Filter by the terms under which we may crawl. " 
                             "NPLD or by-permission, no terms at all, or all records (no filtering). [default: %(default)s]")

    # Whether to include items with UK TLDs in the results. Useful for separating seeds from scope for domain crawls.
    parser.add_argument('--omit-uk-tlds', dest='omit_uk_tlds', action='store_true', default=False,
                        help='Omit URLs that are already in scope, because they have a UK TLD. [default: %(default)s]')

    # Whether to include 'hidden' items:
    parser.add_argument('--include-hidden', dest='include_hidden', action='store_true', default=False,
                        help='Include targets marked as "hidden" in W3ACT. [default: %(default)s]')

    # Whether to prevent old targets from being included (i.e. ones with crawl end dates in the past)
    parser.add_argument('--include-expired', dest='include_expired', action='store_true', default=False,
                        help='Include targets even if the crawl end date has past. [default: %(default)s]')

#  npld_only=True, frequency=None,
    # omit_hidden=True,
    # omit_uk_tlds=False

    # Set up for sub-commands:
    subparsers = parser.add_subparsers(help='Action to perform', dest='action')

    # Get CSV
    get_parser = subparsers.add_parser("get-csv", help="Download data from W3ACT PostgreSQL and store as CSV.")
    get_parser.add_argument('-H', '--db-host', dest='db_host',
                    type=str, default="localhost",
                    help="Hostname of W3ACT PostgreSQL database [default: %(default)s]" )
    get_parser.add_argument('-P', '--db-port', dest='db_port',
                    type=int, default=5432,
                    help="Port number of W3ACT PostgreSQL database [default: %(default)s]" )
    get_parser.add_argument('-u', '--db-user', dest='db_user',
                    type=str, default="w3act",
                    help="Database user to login with [default: %(default)s]" )
    get_parser.add_argument('-p', '--db-pw', dest='db_pw',
                    type=str, default=None,
                    help="Database user password [default: %(default)s]" )
    get_parser.add_argument('-D', '--db-name', dest='db_name',
                    type=str, default="w3act",
                    help="Name of the W3ACT PostgreSQL database [default: %(default)s]" )

    # Turn to JSON
    to_json_parser = subparsers.add_parser("csv-to-json", help="Load CSV and store as JSON.")

    # Create
    urllist_parser = subparsers.add_parser("list-urls", help="List URLs from Targets in the W3ACT CSV data.")

    # Generate crawl feed
    crawlfeed_parser = subparsers.add_parser("crawl-feed", help="Generate crawl-feed format files from W3ACT CSV data.")

    # Generate access lists
    acl_parser = subparsers.add_parser("gen-acl", help="Generate aclj from W3ACT CSV data.")
    acl_parser.add_argument('--format', choices=['pywb','surts'], help="The file format to write: 'pywb' for the pywb aclj format, or 'surts' for a sorted list of SURT prefixes.", default='pywb')
    acl_parser.add_argument('output_file', type=str, help="File to write output path to.")

    # Generate annotations for full-text search indexing:
    ann_parser = subparsers.add_parser("gen-annotations", help="Generate search annotations from W3ACT CSV data.")
    ann_parser.add_argument('output_file', type=str, help="File to write output path to.")

    # Generate static site version
    sitegen_parser = subparsers.add_parser("gen-site", help="Generate Hugo static site source files from W3ACT CSV data.")
    sitegen_parser.add_argument('output_dir', type=str, help="Directory to output to.")

    # Update a collections Solr instance
    colsol_parser = subparsers.add_parser("update-collections-solr", help="Update ukwa-ui-collections-solr instance with these targets and collections.")
    colsol_parser.add_argument('solr_url', type=str, help="The Solr URL for the ukwa-ui-collections-solr index to populat, e.g. http://host:8983/solr/collection")

    # Parse up:
    args = parser.parse_args()

    # Set up verbose logging:
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)    
        # PySolr tends to be too chatty at 'INFO':
        logging.getLogger('pysolr').setLevel(logging.WARNING)
    elif args.verbose > 1:
        logging.getLogger().setLevel(logging.DEBUG)

    # Clean up:
    args.csv_dir = args.csv_dir.rstrip('/')

    # Handle:
    if args.action == "get-csv":
        # Setup connection params
        params = {
            'password': os.environ.get("W3ACT_PSQL_PASSWORD", None),
            'database': args.db_name,
            'user': args.db_user,
            'host': args.db_host,
            'port': args.db_port
        }
        # make command-line pw override any env var:
        if args.db_pw:
            params['password'] = args.db_pw
        # And pull down the data tables as CSV:
        get_csv(csv_dir=args.csv_dir, params=params)
    else:
        # FIXME Fail if CSV folder is empty/non-existent

        # Load in for processing:
        all = load_csv(csv_dir=args.csv_dir)

        # Actions to perform:
        if args.action  == "list-urls":
            targets = filtered_targets(all['targets'],
                                       frequency=args.frequency,
                                       terms=args.terms,
                                       omit_uk_tlds=args.omit_uk_tlds,
                                       include_hidden=args.include_hidden,
                                       include_expired=args.include_expired
                                       )

            for target in targets:
                # So print!
                for url in target.get('urls', []):
                    print("%s" % url )
        elif args.action == "csv-to-json":
            write_json("%s.json" % args.csv_dir, all)
        elif args.action == "csv-to-zip":
            csv_to_zip(args.csv_dir)
        elif args.action == "gen-acl":
            # Generate Open Access targets subset:
            oa_targets = filtered_targets(all['targets'], frequency='all', terms='oa', include_expired=True, include_hidden=False)
            # Generate the OA list:
            acls = generate_oa_allow_list(oa_targets, fmt=args.format)
            with open(args.output_file, 'w') as f:
                for line in acls:
                    f.write("%s\n" % line)
        elif args.action == "gen-annotations":
            # Pass on unfiltered targets etc.
            annotations = generate_annotations(all['targets'], all['collections'], all['subjects'])
            with open(args.output_file, 'w') as f_out:
                f_out.write('{}'.format(json.dumps(annotations, indent=4)))

        elif args.action == "update-collections-solr":
            # Generate 'all but hidden' targets subset:
            public_targets = filtered_targets(all['targets'], frequency='all', terms='all', include_expired=True, include_hidden=False)
            # Send to Solr:
            populate_collections_solr(args.solr_url, public_targets, all['collections'], all['subjects'])

        elif args.action == "gen-site":
            sg = GenerateSitePages(all, args.output_dir)
            sg.generate()

        elif args.action == "crawl-feed":
            targets = filtered_targets(all['targets'],
                                       frequency=args.frequency,
                                       terms=args.terms,
                                       omit_uk_tlds=args.omit_uk_tlds,
                                       include_hidden=args.include_hidden,
                                       include_expired=args.include_expired
                                       )
            feed = []
            for target in targets:
                feed.append(to_crawl_feed_format(target))
            write_json("%s.crawl-feed.json" % args.csv_dir, feed)

        else:
            print("No action specified! Use -h flag to see available actions.")


if __name__ == "__main__":
    main()

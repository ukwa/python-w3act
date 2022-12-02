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
import sys
import os
import re
from w3act.dbc.client import get_csv, load_csv, filtered_targets, filtered_collections, csv_to_zip, to_crawl_feed_format, csv_to_api_json
from w3act.dbc.generate.acls import generate_acl
from w3act.dbc.generate.annotations import generate_annotations
from w3act.dbc.generate.collections_solr import populate_collections_solr
from w3act.dbc.generate.site import GenerateSitePages

# Set up overall logging config:
logging.basicConfig(level=logging.WARNING, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

# Set up logger for this module:
logger = logging.getLogger(__name__)

def write_json(filename, all, format='json', include_w3act_type=True):
    if format == 'json':
        with OutputFileOrStdout(filename) as f:
            json.dump(all, f, indent=2)
    elif format == 'jsonl':
        with OutputFileOrStdout(filename) as f:
            for w3act_type in all:
                if w3act_type != "invalid_targets":
                    logger.debug(f"Looking at w3act_type={w3act_type}")
                    for item_key in all[w3act_type]:
                        item = all[w3act_type][item_key]
                        if include_w3act_type:
                            item['w3act_type'] = w3act_type
                        json.dump(item,f)
                        f.write('\n')
                else:
                    for item in all[w3act_type]:
                        if include_w3act_type:
                            item['w3act_type'] = w3act_type
                        json.dump(item,f)
                        f.write('\n')
    else:
        raise Exception(f"Unknown format {format}!")

def write_sqlite(filename, all):
    if filename == '-':
        raise Exception("Can't write SQLite to output stream.")
    import pandas as pd
    import os.path
    from sqlalchemy import create_engine
    # Set up DB:
    file_path = os.path.abspath(filename)
    logger.info(f"Writing to {file_path}...")
    engine = create_engine(f"sqlite:///{file_path}", echo=False)
    # Process entries:
    for w3act_type in all:
        items = []
        if w3act_type != "invalid_targets":
            for item_key in all[w3act_type]:
                item = all[w3act_type][item_key]
                items.append(item)
        else:
            items = all[w3act_type]
        # Load into pandas
        df = pd.DataFrame(items)
        df.to_sql(w3act_type, con=engine, if_exists='replace')

class OutputFileOrStdout():
    def __init__(self, output_file):
        self.output_file = output_file
    def __enter__(self):
        if self.output_file == '-':
            self.writer = sys.stdout
        else:
            self.writer = open(self.output_file, 'w')
        return self.writer
    def __exit__(self, type, value, traceback):
        if self.writer is not sys.stdout:
            self.writer.close()        

def main():
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('-v', '--verbose',  action='count', default=0, help='Logging level; add more -v for more logging.')
    common_parser.add_argument('-d', '--csv-dir', dest='csv_dir', help="Folder to cache CSV data in.", default="w3act-db-csv")

    target_filter_parser = argparse.ArgumentParser(add_help=False)
    target_filter_parser.add_argument('-f', '--frequency', dest="frequency", type=str,
                        default='all', choices=[ None, 'nevercrawl', 'daily', 'weekly',
                                                'monthly', 'quarterly', 'sixmonthly',
                                                'annual', 'domaincrawl', 'all'],
                        help="Filter targets by crawl frequency (n.b. 'all' means all-but-nevercrawl) [default: %(default)s]")

    # Which terms, e.g. NPLD, by-permission, or both, or no terms that permit crawling:
    target_filter_parser.add_argument('-t', '--terms', dest='terms', type=str, default='npld',
                        choices=[ 'npld', 'bypm', 'no-terms', 'all'],
                        help="Filter by the terms under which we may crawl. " 
                             "NPLD or by-permission, no terms at all, or all records (no filtering). [default: %(default)s]")

    # Whether to include items with UK TLDs in the results. Useful for separating seeds from scope for domain crawls.
    target_filter_parser.add_argument('--omit-uk-tlds', dest='omit_uk_tlds', action='store_true', default=False,
                        help='Omit URLs that are already in scope, because they have a UK TLD. [default: %(default)s]')

    # Whether to include 'hidden' items:
    target_filter_parser.add_argument('--include-hidden', dest='include_hidden', action='store_true', default=False,
                        help='Include targets marked as "hidden" in W3ACT. [default: %(default)s]')

    # Whether to prevent old targets from being included (i.e. ones with crawl end dates in the past)
    target_filter_parser.add_argument('--include-expired', dest='include_expired', action='store_true', default=False,
                        help='Include targets even if the crawl end date has past. [default: %(default)s]')
  
    # Whether to include collections that should not be published (default: no)
    collection_filter_parser = argparse.ArgumentParser(add_help=False)
    collection_filter_parser.add_argument('--include-unpublished-collections', dest='include_unpublished', action='store_true', default=False,
                        help='Include collections that are marked "not for publishing". [default: %(default)s]')

#  npld_only=True, frequency=None,
    # omit_hidden=True,
    # omit_uk_tlds=False

    # Set up the main parser
    parser = argparse.ArgumentParser('w3act')

    # Set up for sub-commands:
    subparsers = parser.add_subparsers(help='Action to perform', dest='action')

    # Get CSV
    get_parser = subparsers.add_parser("get-csv", 
        help="Download data from W3ACT PostgreSQL and store as CSV.",
        parents=[common_parser])
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
    to_json_parser = subparsers.add_parser("csv-to-json", 
        help="Load CSV and store as JSON.",
        parents=[common_parser, collection_filter_parser])

    to_jsonl_parser = subparsers.add_parser("csv-to-jsonl", 
        help="Load CSV and store as JSON Lines.",
        parents=[common_parser, collection_filter_parser])

    to_sqlite_parser = subparsers.add_parser("csv-to-sqlite", 
        help="Load CSV and store as a SQLite database. !!! WARNING: This is a work-in-progress and is currently broken!!!",
        parents=[common_parser, collection_filter_parser])

    to_api_json_parser = subparsers.add_parser("csv-to-api-json", 
        help="Load CSV and store collections as separate JSON files.",
        parents=[common_parser, collection_filter_parser])
    to_api_json_parser.add_argument('-o', '--api-output-dir', dest='api_output_dir', help="Output directory for files retrieved from API", default="api_json")

    # Create
    urllist_parser = subparsers.add_parser("list-urls", 
        help="List URLs from Targets in the W3ACT CSV data.",
        parents=[common_parser, target_filter_parser])
    urllist_parser.add_argument('-F', '--format', choices=['pywb','surts','urls'], help="The file format to write: 'pywb' for the pywb aclj format, 'surts' for a sorted list of SURT prefixes, or 'urls' for plain URLs.", default='urls')
    urllist_parser.add_argument('output_file', type=str, help="File to write output path to.")

    # Generate crawl feed
    crawlfeed_parser = subparsers.add_parser("crawl-feed",
        help="Generate crawl-feed format files from W3ACT CSV data.",
        parents=[common_parser, target_filter_parser])
    crawlfeed_parser.add_argument('-F', '--format', 
        choices=['json','jsonl'], 
        help="The file format to write: 'json' for one large json file, 'jsonl' for JSONLines.", 
        default='json')
    crawlfeed_parser.add_argument('output_file', type=str, help="File to write output to.")

    # Generate access lists
    acl_parser = subparsers.add_parser("gen-oa-acl", 
        help="Generate open access surts/aclj from W3ACT CSV data.",
        parents=[common_parser])
    acl_parser.add_argument('-F', '--format', choices=['pywb','surts'], help="The file format to write: 'pywb' for the pywb aclj format, or 'surts' for a sorted list of SURT prefixes.", default='pywb')
    acl_parser.add_argument('output_file', type=str, help="File to write output path to.")

    # Generate annotations for full-text search indexing:
    ann_parser = subparsers.add_parser("gen-annotations", 
        help="Generate search annotations from W3ACT CSV data.",
        parents=[common_parser, collection_filter_parser])
    ann_parser.add_argument('output_file', type=str, help="File to write output path to.")

    # Generate static site version
    sitegen_parser = subparsers.add_parser("gen-site", 
        help="Generate Hugo static site source files from W3ACT CSV data.",
        parents=[common_parser, collection_filter_parser])
    sitegen_parser.add_argument('output_dir', type=str, help="Directory to output to.")

    # Update a collections Solr instance
    colsol_parser = subparsers.add_parser("update-collections-solr", 
        help="Update ukwa-ui-collections-solr instance with these targets and collections.",
        parents=[common_parser, collection_filter_parser])
    colsol_parser.add_argument('solr_url', type=str, help="The Solr URL for the ukwa-ui-collections-solr index to populate, e.g. http://host:8983/solr/collection")

    # Parse up:
    args = parser.parse_args()

    # Set up verbose logging:
    if hasattr(args, 'verbose'):
        if args.verbose == 1:
            logging.getLogger().setLevel(logging.INFO)    
            # PySolr tends to be too chatty at 'INFO':
            logging.getLogger('pysolr').setLevel(logging.WARNING)
        elif args.verbose > 1:
            logging.getLogger().setLevel(logging.DEBUG)

    # Clean up:
    if hasattr(args, 'csv_dir'):
        args.csv_dir = args.csv_dir.rstrip('/')    
        
    if hasattr(args, 'api_output_dir'):
        args.api_output_dir = args.api_output_dir.rstrip('/')

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
        # Fail if args.action is empty
        if not args.action:
            print("ERROR! No action specified! Use -h for help.")
            return

        # Load in for processing:
        try:
            all = load_csv(csv_dir=args.csv_dir)            
        except ValueError as err:
            print(err)
            return

        # Filter if needed:
        if args.action in [
            "gen-annotations", 
            "update-collections-solr",
            "gen-site",
            "csv-to-json",
            "csv-to-jsonl",
            "csv-to-sqlite",
            "csv-to-api-json"
            ]:
            matching_collections = filtered_collections(all['collections'], args.include_unpublished)
            # some actions don't handle the collections separately, so replace here in advance
            if not args.include_unpublished: # else the replacement is redundant; all originally includes everything after load_csv
                all['collections'] = matching_collections 

        if args.action in ['list-urls', 'crawl-feed']:
            matching_targets = filtered_targets(all['targets'],
                                       frequency=args.frequency,
                                       terms=args.terms,
                                       omit_uk_tlds=args.omit_uk_tlds,
                                       include_hidden=args.include_hidden,
                                       include_expired=args.include_expired
                                       )

        # Actions to perform:
        if args.action  == "list-urls":
            results = generate_acl(matching_targets, False, fmt=args.format)
            with OutputFileOrStdout(args.output_file) as f:
                for line in results:
                    f.write("%s\n" % line)

        elif args.action == "crawl-feed":
            feed = {}
            feed['targets'] = {}
            for target in matching_targets:
                tid = target['id']
                feed['targets'][tid] = to_crawl_feed_format(target)
            write_json(args.output_file, feed, args.format, include_w3act_type=False)

        elif args.action == "gen-oa-acl":
            # Generate Open Access targets subset:
            oa_targets = filtered_targets(all['targets'], frequency='all', terms='oa', include_expired=True, include_hidden=False)
            # Generate the OA list:
            acls = generate_acl(oa_targets, True, fmt=args.format)
            with OutputFileOrStdout(args.output_file) as f:
                for line in acls:
                    f.write("%s\n" % line)

        elif args.action == "gen-annotations":
            # Pass on unfiltered targets etc.
            annotations = generate_annotations(
                all['targets'], 
                matching_collections, 
                all['subjects']
                )
            with OutputFileOrStdout(args.output_file) as f_out:
                f_out.write('{}'.format(json.dumps(annotations, indent=4)))

        elif args.action == "update-collections-solr":
            # Generate 'all but hidden' targets subset:
            public_targets = filtered_targets(all['targets'], frequency='all', terms='all', include_expired=True, include_hidden=False)
            # Send to Solr:
            populate_collections_solr(
                args.solr_url, 
                public_targets, 
                matching_collections, 
                all['subjects']
            )

        elif args.action == "gen-site":
            sg = GenerateSitePages(all, args.output_dir)
            sg.generate()

        elif args.action == "csv-to-json":
            write_json("%s.json" % args.csv_dir, all)

        elif args.action == "csv-to-jsonl":
            write_json("%s.jsonl" % args.csv_dir, all, format='jsonl')

        elif args.action == "csv-to-sqlite":
            write_sqlite("%s.sqlite" % args.csv_dir, all)

        elif args.action == "csv-to-zip":
            csv_to_zip(args.csv_dir)

        elif args.action == "csv-to-api-json":
            csv_to_api_json(
                all['targets'], 
                all['invalid_targets'], 
                matching_collections, 
                args.api_output_dir
                )
        else:
            print("No known action specified! Use -h flag to see available actions.")


if __name__ == "__main__":
    main()

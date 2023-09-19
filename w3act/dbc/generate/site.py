# -*- coding: utf-8 -*-
import re
import os
import json
import yaml
import logging
import base64
import hashlib
import datetime
import tldextract
import unicodedata
from jinja2 import Environment, PackageLoader
from urllib.parse import urlparse
from base64 import urlsafe_b64encode


# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )

# Helper to turn timestamp etc. into full PWID:
# (copied from ukwa-api)
def gen_pwid(wb14_timestamp, url, archive_id='webarchive.org.uk', scope='page', encodeBase64=True):
    # Format the PWID string:
    yy1,yy2,MM,dd,hh,mm,ss = re.findall('..', wb14_timestamp)
    iso_ts = f"{yy1}{yy2}-{MM}-{dd}T{hh}:{hh}:{ss}Z"
    pwid = f"urn:pwid:{archive_id}:{iso_ts}:page:{url}"
    
    # Encode as appropriate:
    if encodeBase64:
        pwid_enc = urlsafe_b64encode(pwid.encode('utf-8')).decode('utf-8')
        return pwid_enc
    else:
        return pwid


def slugify(value):
    """
    Converts to lowercase, removes non-word characters (alphanumerics and
    underscores) and converts spaces to hyphens. Also strips leading and
    trailing whitespace.
    """
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    return re.sub('[-\s]+', '-', value)


class GenerateSitePages():

    record_count = 0
    blocked_record_count = 0
    missing_record_count = 0
    embargoed_record_count = 0

    target_count = 0
    collection_count = 0
    collection_published_count = 0
    subject_count = 0


    def __init__(self, source, output_dir):
        self.source = source
        self.output_dir = output_dir

    def get_collections_by_id(self, collections, collections_by_id):
        for col in collections:
            collections_by_id[int(col['id'])] = col
            if col['publish']:
                self.collection_published_count += 1
            self.get_collections_by_id(col.get('children', []), collections_by_id)

    def filter_down(self, targets, collections):
        '''
        This is used to filter down the whole collection to a much smaller subset, for rapid testing purposes.
        :return:
        '''
        new_collections = []
        new_collection_ids = []
        for col in collections:
            #if len(new_collections) < 5 or col['id'] == 329: # Add in known collection with sub-collections
                #if col['publish']:
                    new_collections.append(col)
                    new_collection_ids.append(col['id'])

        new_targets = []
        for target in targets:
            for col_id in target['collection_ids']:
                if col_id in new_collection_ids:
                    new_targets.append(target)

        return new_targets, new_collections

    def generate(self):
        # Get the data:
        targets = self.source['targets'].values()
        self.target_count = len(targets)
        collections = self.source['collections'].values()
        self.collection_count = len(collections)
        #subjects = self.source['subjects'].values()
        #self.subject_count = len(subjects)

        # Filter down, for testing:
        #FIXME targets, collections = self.filter_down(targets,collections)

        # Index collections by ID:
        collections_by_id = {}
        self.get_collections_by_id(collections, collections_by_id)

        # Index targets by ID:
        targets_by_id = {}
        for target in targets:
            targets_by_id[int(target['id'])] = target

        # Setup template environment:
        env = Environment(loader=PackageLoader('w3act.dbc.generate', 'site_templates'))

        # Targets
        # FIXME this should build up an 'id' to 'page-source-path' mapping, and link to collections:
        self.generate_targets(env, targets, collections_by_id)

        # Collections
        # FIXME this should output targets using 'page-source-path' rather than ID:
        self.generate_collections("%s/content/collection" % self.output_dir, env, collections, targets_by_id)

    def generate_collections(self, base_path, env, collections, targets_by_id):
        template = env.get_template('site-target-template.md')
        # Emit this level:
        for col in collections:
            # Skip unpublished collections:
            if col['publish'] != True:
                logger.warning("The Collection '%s' not to be published! (publish = %s)" % (col['name'], col['publish']) )
                # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
                continue
            # And write:
            rec = {
                'id': col['id'],
                'title': col['name'],
            }
            description = ""
            if 'description' in col and col['description'] != None:
                description = col['description'].replace('\r\n', '\n')

            # Use the ID as the URL:
            rec['url'] = f"ukwa/collection/{col['id']}"

            # Store under a slugified file path:
            file_path = "%s/%s" % (base_path, slugify(col['name']))
            # Recurse to generate child collections:
            if 'children' in col:
                self.generate_collections(file_path, env, col['children'], targets_by_id)

            # Collect the Targets
            target_ids = []
            rec['targets'] = []
            rec['stats'] = {}
            rec['stats']['num_targets'] = 0
            rec['stats']['num_oa_targets'] = 0
            for tid in col.get('target_ids', []):
                target = targets_by_id.get(tid, None)
                # FIXME blocking etc.
                if target:
                    if target.get('hidden', False): #target.get('inheritsOA', False) or target.get('inheritsNPLD', False):
                        continue
                    target_ids.append(tid)
                    # Also store the path:
                    rec['targets'].append(self.get_target_file_path(target))
                    rec['stats']['num_targets'] += 1
                    if target.get('isOA', False):
                        rec['stats']['num_oa_targets'] += 1
            # And remove the plain TID list:
            #col.pop('target_ids', None)
            # Store string rather than integer references:
            rec['target_ids'] = target_ids

            # and write:
            col_md = "%s/_index.en.md" % file_path
            directory = os.path.dirname(col_md)
            if not os.path.exists(directory):
                #logger.info("Making directory: %s" % directory)
                os.makedirs(directory)
            with open(col_md, 'w') as f:
                logger.info("Writing: %s" % col_md)
                for part in template.generate({ "record": col, "yaml": yaml.dump(dict(rec), default_flow_style=False), "description": description }):
                    f.write(part)

    def get_target_start_date_force(self, target):
        start_date = target.get('crawl_start_date')
        if not start_date:
            start_date = "2006-01-01 12:00:00"
        return start_date

    def get_target_file_path(self, target):
        start_date = self.get_target_start_date_force(target)
        return "%s/%s-%s" % (start_date[:4], start_date[:10], slugify(target['title'][:32]))

    def generate_targets(self, env, targets, collections_by_id):
        # Setup specific template:
        template = env.get_template('site-target-template.md')

        # Export targets
        for target in targets:
            # Skip blocked items:
            if target['crawl_frequency'] == 'NEVERCRAWL':
                logger.warning("The Target '%s' is blocked (NEVERCRAWL)." % target['title'])
                self.blocked_record_count += 1
                # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
                continue
            # Skip items that have no crawl permission?
            # hasOpenAccessLicense == False, and inScopeForLegalDeposit == False ?
            # Skip items with no URLs:
            if len(target.get('urls',[])) == 0:
                logger.warning("The Target '%s' has no URLs!" % target['title'] )
                # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
                continue
            # Skip hidden targets:
            if target['hidden']:
                logger.warning("The Target '%s' is hidden!" % target['title'] )
                # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
                continue
            ## Skip non-top-level targets:
            #if target.get('inheritsNPLD', False):
            #    logger.warning("The Target '%s' inherits NPLD status!" % target['title'] )
            #    # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
            #    continue
            #if target.get('inheritsOA', False):
            #    logger.warning("The Target '%s' inherits OA status!" % target['title'] )
            #    # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
            #    continue
            # Get the ID, WCT ID preferred:
            tid = target['id']
            if target.get('wct_id', None):
                tid = target['wct_id']
            # Get the url, use the first:
            url = target['urls'][0]

            ## Check it's a host-level record:
            #url_path = urlparse(url).path
            #if url_path != '/':
            #    logger.info("The Target '%s' has a path!" % target['title'] )
            #    # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
            #    continue

            # Extract the domain:
            parsed_url = tldextract.extract(url)
            publisher = parsed_url.registered_domain
            # Lookup in CDX:
            #wayback_date_str = CdxIndex().get_first_capture_date(url) # Get date in '20130401120000' form.
            #if wayback_date_str is None:
            #    logger.warning("The URL '%s' is not yet available, inScopeForLegalDeposit = %s" % (url, target['inScopeForLegalDeposit']))
            #    self.missing_record_count += 1
            #    continue
            start_date_str = target['crawl_start_date']
            if not start_date_str:
                # FIXME This is a Big Problem
                logger.warning(f"No start date on Target {tid}!")
                continue
            start_date =  datetime.datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
            start_date_iso = start_date.isoformat()
            wayback_date_str = start_date.strftime('%Y%m%d%H%M%S')
            url_b64 = base64.b64encode(hashlib.md5(url.encode('utf-8')).digest())
            record_id = "%s/%s" % (wayback_date_str, str(url_b64, "utf-8") )

            # And format the end date
            end_date_iso = None
            if target.get('crawl_end_date', None):
                end_date_iso = datetime.datetime.strptime(target['crawl_end_date'], '%Y-%m-%d %H:%M:%S').isoformat()

            # Honour embargo
            #ago = datetime.datetime.now() - wayback_date
            #if ago.days <= 7:
            #    self.embargoed_record_count += 1
            #    continue

            # Strip out Windows newlines
            if 'description' in target and target['description'] != None:
                target['description'] = target['description'].replace('\r\n', '\n')

            # Otherwise, build the record:
            rec = {
                'url': f"ukwa/target/{tid}",
                'id': target['id'], # Hugo needs strings as identifiers, and we may too later.
                'pwid': gen_pwid(wayback_date_str, url, encodeBase64=False),
                'pwid_b64': gen_pwid(wayback_date_str, url, encodeBase64=True),
                'wct_id': target.get('wct_id', None),
                'record_id': record_id,
                'date': start_date_iso,
                'wayback_date': wayback_date_str,
                'target_url': url,
                'title': target['title'],
                'publisher': publisher,
                'start_date': start_date_iso,
                'end_date': end_date_iso,
                'open_access': target['isOA'],
                'npld': target['isNPLD'],
                'scope': target['scope'],
                'nominating_organisation': target.get('nominating_organisation', {}).get('title',None),
                'subjects': [],
                'qaissue_score': target.get('qaissue_score', None),
                'qaissue': target.get('qaissue', None),
                'originating_organisation': target.get('originating_organisation', None),
                'curator_id': target.get('author_id', None),
                'organisation_id': target.get('organisation_id', None),
                'crawl_frequency': target.get('crawl_frequency', None),
                'license_status': target.get('license_status', None),
                'live_site_status': target.get('live_site_status', None),
                'licenses': target.get('licenses', []),
            }

            # For subjects
            #for sub_id in target['subjects']:
                #pass
                #pass
                #col = subjects.get(int(target['collectionIds'][0]), {})
                #if 'name' in col:
                #    rec['collections'].append({
                #        'id': col['id'],
                #        'name': col['name']
                #    })

            # And the organisation:
            if 'nominating_organisation' in target and target['nominating_organisation'] != None:
                rec['organisation'] = {
                    'id': target['nominating_organisation']['id'],
                    'name': target['nominating_organisation']['title'],
                    'abbreviation': target['nominating_organisation']['abbreviation']
                }

            # And write:
            file_path = self.get_target_file_path(target)
            target['file_path'] = file_path
            target_md = "%s/content/target/%s/index.en.md" % (self.output_dir,file_path)
            directory = os.path.dirname(target_md)
            if not os.path.exists(directory):
                #logger.info("Making directory: %s" % directory)
                os.makedirs(directory)
            with open(target_md, 'w') as f:
                logger.info("Writing: %s" % target_md)
                for part in template.generate({ "record": rec, "yaml": yaml.dump(rec, default_flow_style=False), "description": target['description'] }):
                    f.write(part)




if __name__ == '__main__':
    pass

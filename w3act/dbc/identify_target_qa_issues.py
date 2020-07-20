#!/usr/bin/env python
# coding: utf-8

"""
Identifies Targets that:
1) Potentially infringe the Regulations (scoping in non-UK websites as "legal deposit") and,
2) Indicate unnecessary crawling (too deep, too frequent) which has implications for the crawlers and for storage, and which has a cost associated with it.

Params
-d lookback days (default 7)
-m mail recipient(s); if multiple, enclose in quotes (default needs manually coding if in public repo)

Usage examples:
[python3 identify_target_qa_issues.py...]

identify_target_qa_issues.py    >>> default email address & look back days (7)
identify_target_qa_issues.py -d 30   >>> default email address, look back 30 days
identify_target_qa_issues.py -m person@x.com   >>> single recipient
identify_target_qa_issues.py -d 30 -m "person1@x.com, person2@y.com"   >>> multiple recipients, look back 30 days

Env Vars:
CURATOR_EMAIL_LIST: Default email list; overridden by command line params, no email sent if neither are supplied.
W3ACT_JSON_DIR:     Default data source dir; cwd if not supplied. NB. Parent of the actual dir which is timestamp-related
                    at time of writing, see code for details.

"""


import os
import sys
import pandas as pd
import json
import datetime
import smtplib
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import argparse
import re
import logging
import tldextract

w3act_target_url_prefix = 'https://www.webarchive.org.uk/act/targets/'

# maybe not for public repos
default_email_list = os.getenv('CURATOR_EMAIL_LIST')
data_dir = os.getenv('W3ACT_JSON_DIR', '.')
#

output_file = './target_issues.csv'
log_file = "./target_issues.log"
log_level = 'INFO'

if log_level == 'INFO':
    logging.basicConfig(filename=log_file, format='%(message)s', level=log_level)
else:
    logging.basicConfig(filename=log_file, format='%(asctime)s [%(levelname)s] %(message)s', level=log_level)

logging.info("Started " + sys.argv[0]) # log program start with name


def invalid_URL(urls):
    # true returned if:
    # primary url doesn't end in a slash or an extension
    try:
        primary_seed = urls[0]
        _, *_, last = primary_seed.split('/')  # last entry in url

        if last == '':  # ends /
            return False  # ends / = ok
        if '.' in last:
            return False  # extension = ok
    except Exception:
        return True  # corrupt url - flag it

    # no extension, no trailing /
    return True

def social_media_URL(urls):
    # true returned if:
    # primary url is a social/multimedia site
    sites_to_flag = ["facebook", "vimeo", "instagram", "youtube"] # specified by curator admin

    try:
        primary_seed = urls[0]
        extracted_domain = tldextract.extract(primary_seed)
        return (extracted_domain.domain in sites_to_flag)
    except Exception:
        return True  # corrupt url - flag it


def shallow_crawl(urls, frequency):
    # true returned if:
    # primary url looks like an insufficently deep target for frequent crawl
    try:
        primary_seed = urls[0]
        # curator admin specified indicators, probably a single page
        if (len(primary_seed) > 80) or (primary_seed.count('/') > 8) and (frequency in ['DAILY', 'WEEKLY', 'MONTHLY']):
            return True
    except Exception:
        return True  # corrupt url - flag it

    return False


def multiple_domains(urls):
    # true returned if:
    # the list of urls contains more than one domain
    # nb. subdomains don't count: url1=news.xyz.com, url2=sport.xyz.com, url3=xyz.com = 1 domain (xyz.com)

    if len(urls) == 1: return False # single url, no checks needed

    try:
        primary_seed = urls[0]
        primary_domain = tldextract.extract(primary_seed).domain

        for url in urls[1:]:
            if tldextract.extract(url).domain != primary_domain: return True

    except Exception:
        return True  # corrupt urls - flag it

    return False # all domains match


# initialise from args
parser = argparse.ArgumentParser('Report Targets That May Have QA Issues.')
parser.add_argument('-d', '--days', dest='lookback_days', type=int, default=7, required=False,
                    help="Days to look back [default: %(default)s]")
parser.add_argument('-m', '--mailto', dest='email_list', type=str, default=default_email_list, required=False,
                    help='List of email recipients. [default: %(default)s]')
parser.add_argument('-f', '--full', dest='full_report', action='store_true', default=False, required=False,
                    help='Full Report. [default: %(default)s]')
args = parser.parse_args()

email_to = args.email_list
lookback_days = args.lookback_days

# original report was just aimed at checking legality and crawl limits were ok; full adds various qa checks
full_report = args.full_report

# Load the targets from the daily json dump
today = datetime.date.today()
data_file = data_dir + "/" + str(today)[0:7] + '/' + str(today) + '-w3act-csv-all.json'

try:
    with open(data_file) as f:
        all = json.load(f)
except IOError:
    logging.error("Unable to access today's w3act data file: " + data_file)
    sys.exit(0)

targets = all['targets']

df = pd.DataFrame(targets)
df = df.transpose()

# Targets looking back n days
df_scope = df[(pd.to_datetime(df.created_at) > (pd.to_datetime('today') - pd.DateOffset(days=lookback_days + 1)))]

# Frequent Crawl issues - targets assigned a daily/weekly schedule with no end date
df_issue_frequent = df_scope[df_scope.crawl_frequency.isin(['DAILY', 'WEEKLY']) & (df_scope.crawl_end_date == '')].copy()
df_issue_frequent["issue_reason"] = "No End Date"
df_issue_frequent["issue_info"] = df_scope.crawl_frequency

# Uncapped targets
df_issue_capped = df_scope[~df_scope.depth.isin(['CAPPED', 'CAPPED_LARGE'])].copy()
df_issue_capped["issue_reason"] = "Uncapped"
df_issue_capped["issue_info"] = df_scope.depth

# Manual Scoping
df_issue_judgement = df_scope[df_scope.professional_judgement].copy()
df_issue_judgement["issue_reason"] = "Professional Judgement"
df_issue_judgement["issue_info"] = df_scope.professional_judgement_exp

# Regular or deep crawling of subdomain
df_issue_subdomain = df_scope[(df_scope.scope == 'subdomains') &
                              (df_scope.crawl_frequency.isin(['DAILY', 'WEEKLY', 'MONTHLY']) |
                                   df_scope.depth.isin(['CAPPED_LARGE', 'DEEP']))].copy()
df_issue_subdomain["issue_reason"] = "Subdomain Crawl Scope"
df_issue_subdomain["issue_info"] = df_scope.crawl_frequency + "/" + df_scope.depth

# Temp df used because I couldn't get apply() to work on the original
df_temp = df_scope.copy()

# Create a column that flags invalid URLS
df_temp['invalid'] = df_temp['urls'].apply(invalid_URL)

# A trailing slash is not included at the end of the starting seed (unless it ends in a tld or file name extension)
df_issue_urls = df_temp[df_temp['invalid']].copy()
df_issue_urls["issue_reason"] = "URL Should End /"
df_issue_urls["issue_info"] = df_temp.urls

if full_report:
    df_issue_postal_address = df_scope[df_scope.uk_postal_address].copy()
    df_issue_postal_address["issue_reason"] = "UK Postal Address"
    df_issue_postal_address["issue_info"] = df_scope.uk_postal_address_url

    df_issue_correspondence = df_scope[df_scope.via_correspondence].copy()
    df_issue_correspondence["issue_reason"] = "Via Correspondenxe"
    df_issue_correspondence["issue_info"] = df_scope.value # source: w3act TargetController.java

    # No description
    df_issue_description = df_scope[df_scope.description == ''].copy()
    df_issue_description["issue_reason"] = "No Description"
    df_issue_description["issue_info"] = "Title: " + df_scope.title

    # License not initiated
    df_issue_license = df_scope[df_scope.license_status.isin(['NOT_INITIATED', ''])].copy()
    df_issue_license["issue_reason"] = "License"
    df_issue_license["issue_info"] = df_scope.license_status

    # social media
    df_temp = df_scope.copy()
    df_temp['invalid'] = df_temp['urls'].apply(social_media_URL)

    df_issue_social_media = df_temp[df_temp['invalid']].copy()
    df_issue_social_media["issue_reason"] = "Social Media"
    df_issue_social_media["issue_info"] = df_temp.urls

    # shallow crawl - long url, probably won't need frequent crawl
    df_temp = df_scope.copy()
    df_temp['invalid'] = df_temp.apply(lambda x: shallow_crawl(x.urls, x.crawl_frequency), axis=1)  #lambda cos multi column

    df_issue_shallow_crawl = df_temp[df_temp['invalid']].copy()
    df_issue_shallow_crawl["issue_reason"] = "Shallow Frequent Crawl"
    df_issue_shallow_crawl["issue_info"] = df_temp.urls # + "/" + df_temp.crawl_frequency

    # multiple domains
    df_temp = df_scope.copy()
    df_temp['invalid'] = df_temp['urls'].apply(multiple_domains)

    df_issue_multiple_domains = df_temp[df_temp['invalid']].copy()
    df_issue_multiple_domains["issue_reason"] = "Multiple Domains"
    df_issue_multiple_domains["issue_info"] = df_temp.urls

    # Large number of seeds
    df_issue_seed_count = df_scope[(df_scope.urls.str.len() > 4)].copy()
    df_issue_seed_count["issue_reason"] = "Large Seed Count"
    df_issue_seed_count["issue_info"] = df_scope.urls
# end full_report

# Bring the separate issues together
if full_report:
    df_target_issues = pd.concat([df_issue_frequent,
                                  df_issue_capped,
                                  df_issue_urls,
                                  df_issue_subdomain,
                                  df_issue_judgement,
                                  df_issue_social_media,
                                  df_issue_shallow_crawl,
                                  df_issue_multiple_domains,
                                  df_issue_seed_count,
                                  df_issue_description,
                                  df_issue_license,
                                  df_issue_correspondence,
                                  df_issue_postal_address
                                  ])
else:
    df_target_issues = pd.concat([df_issue_frequent,
                                  df_issue_capped,
                                  df_issue_urls,
                                  df_issue_subdomain,
                                  df_issue_judgement
                                  ])


# Get curator info...
curators=pd.DataFrame(all['curators']).transpose()
df_target_issues = df_target_issues.join(curators[['name', 'email']], on='author_id', how='inner')

# ...and organisation
organisations=pd.DataFrame(all['organisations']).transpose()
df_target_issues = df_target_issues.join(organisations[['title']], on='organisation_id', rsuffix='_organisation', lsuffix='_target', how='inner')


# Add a link to the problem record
df_target_issues['W3ACT URL'] = w3act_target_url_prefix + df_target_issues.id.astype(str)

# Get rid of the columns we ardf_issue_uk_scope = df_scope[df_scope.professional_judgement].copy()
# df_issue_uk_scope["issue_reason"] = "Professional Judgement"
# df_issue_uk_scope["issue_info"] = df_scope.professional_judgement_expen't reporting on
df_target_issues = df_target_issues[['title_organisation', 'name', 'email', 'title_target','issue_reason', 'issue_info', 'depth', 'crawl_end_date', 'W3ACT URL']]

# Rename for presentation
df_target_issues.columns = ['Organisation', 'User', 'Email', 'Title', 'Issue', 'Info', 'Depth', 'Crawl End Date', "Target URL"]

# The index column has derived from target id, so we've dropped that from the output above
df_target_issues.index.name = 'Target ID'

# Output ready to email
df_target_issues.to_csv(output_file)

if email_to is not None:
    # Initialise the email
    msg = MIMEMultipart()
    msg['From'] = 'TargetIssues@bl.uk'
    msg['Subject'] = 'Targets Flagged as Having QA Issues'

    # Attach the csv
    try:
        with open(output_file, 'r', encoding="utf8") as f:
            attachment = MIMEText(f.read())
            attachment.add_header('Content-Disposition', 'attachment', filename="issues.csv")
            msg.attach(attachment)
    except IOError:
        logging.error("Unable to attach csv file: " + output_file)
        sys.exit(0)

    s = None
    try:
        s = smtplib.SMTP('juno.bl.uk')
    except socket.error as e:
        logging.error("Unable to connect to email server")

    if s is not None:
        # I couldn't find a workable single call with multiple recipients and attachments, so loop instead
        for contact in email_to.split(','):
            email = contact.strip()
            if not re.match(r"[^@]+@[^@]+\.[^@]+", email): # very basic validation on email address
                logging.error("Invalid email address: " + email)
                break
            msg['To'] = email
            s.send_message(msg)
            del msg['to'] # required; setting msg['To'] above adds a new email header - we want to overwrite it

        s.quit()
else:
    logging.info("No email address supplied; output to local file only.")

logging.info("Output file: " + output_file)
logging.info("Finished " + sys.argv[0])



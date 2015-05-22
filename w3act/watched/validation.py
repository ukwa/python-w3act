"""
Specific validation steps for Watched Targets.
"""

import sys
import json
import w3act
import logging
import requests
from lxml import html
from urlparse import urlparse, urljoin
from w3act.job import get_relevant_fields

logger = logging.getLogger("w3act")
logger.setLevel(logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
requests.packages.urllib3.disable_warnings()


class Validator(object):
    def __init__(self):
        self.watched_targets = None

    def get_watched_targets(self):
        if self.watched_targets is None:
            w = w3act.ACT()
            all = w.get_ld_export("all")
            self.watched_targets = get_relevant_fields([t for t in all if "watched" in t.keys() and t["watched"]])

    def validate(self, target, doc):
        key = urlparse(doc["landing_page_url"]).netloc.replace(".", "_")
        try:
            validator = getattr(self, key)
            return validator(target, doc)
        except AttributeError:
            return True

    def www_gov_uk(self, target, doc, check_siblings=True):
        """Dept. names appear in a <aside class="meta"/> tag."""
        try:
            r = requests.get(doc["landing_page_url"])
            h = html.fromstring(r.content)
            texts = [t for t in h.xpath("//aside[contains(@class, 'meta')]//a/text()")]
            if len([t for t in texts if t in target["title"]]) > 0:
                return True
            else:
                if check_siblings:
                    """Include docs. if they don't belong to other gov.uk Watched Targets."""
                    if self.watched_targets is None:
                        self.get_watched_targets()
                    host = urlparse(doc["landing_page_url"]).netloc
                    siblings = [t for t in self.watched_targets for u in t["seeds"] if urlparse(u).netloc == host]
                    for s in siblings:
                        if self.www_gov_uk(s, doc, check_siblings=False):
                            return False
                    return True
                else:
                    return False
        except:
            logger.error("www_gov_uk: %s" % sys.exc_info()[0])
            return False


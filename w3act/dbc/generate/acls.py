import re
import json
import shutil
import logging
import datetime
import surt

logger = logging.getLogger(__name__)

RE_NONCHARS = re.compile(r"""
[^	# search for any characters that aren't those below
\w
:
/
\.
\-
=
?
&
~
%
+
@
,
;
]
""", re.VERBOSE)
RE_SCHEME = re.compile('https?://')

CDN_SURTS = [
    'http://(com,wp,s0',
    'http://(com,wp,s1',
    'http://(com,wp,s2',
    'http://(com,wordpress,files,',
    'http://(com,twimg,',
    'http://(com,blogspot,bp,',
    'http://(com,blogblog,img1',
    'http://(com,blogblog,img2',
    'http://(com,squarespace,static)',
    'http://(net,typekit,use)',
    'http://(com,blogger,www)/img/',
    'http://(com,blogger,www)/static/',
    'http://(com,blogger,www)/dyn-css/',
    'http://(net,cloudfront,',
    'http://(com,googleusercontent,',
    'http://(com,googleapis,ajax',
    # Various media hosts used by BBC pages:
    'http://(uk,co,bbcimg,',
    'http://(uk,co,bbci,',
    # Allow Twitter Service Worker and API.
    'http://(com,twitter)/sw.js',
    'http://(com,twitter,api)'
]


def generate_surt(url):
    surtVal = surt.surt(url)

    #### WA: ensure SURT has scheme of original URL ------------
    # line_scheme = RE_SCHEME.match(line)           # would allow http and https (and any others)
    line_scheme = 'http://'  # for wayback, all schemes need to be only http
    surt_scheme = RE_SCHEME.match(surtVal)

    if line_scheme and not surt_scheme:
        if re.match(r'\(', surtVal):
            # surtVal = line_scheme.group(0) + surtVal
            surtVal = line_scheme + surtVal
            logger.debug("Added scheme [%s] to surt [%s]" % (line_scheme, surtVal))
        else:
            # surtVal = line_scheme.group(0) + '(' + surtVal
            surtVal = line_scheme + '(' + surtVal
            # logger.debug("Added scheme [%s] and ( to surt [%s]" % (line_scheme, surtVal))

    # If it ends with )/, open it up to subdomains by ending with a , instead:
    surtVal = re.sub(r'\)/$', ',', surtVal)

    return surtVal


def generate_acl(targets, include_cdns, fmt="pywb"):
    # collate surts
    all_urls = set()
    all_surts = set()
    all_surts_and_urls = list()

    if include_cdns == True:
        # Start with allowing all from know CDNs:
        for cdn_surt in CDN_SURTS:
            all_surts.add(cdn_surt)
            all_surts_and_urls.append({
                'surt': cdn_surt,
                'url': cdn_surt
            })
        logger.info("%s surts for CDNs added" % len(CDN_SURTS))

    # Add SURTs from ACT:
    for target in targets:
        for seed in target.get('urls',[]):
            # Check
            if RE_NONCHARS.search(seed):
                logger.warn("Questionable characters found in URL [%s] in target %i" % (seed, target['id']))
                continue
            if seed == "http://../":
                logger.warn("Nonsense URL [%s] in target %i" % (seed, target['id']))
                continue

            # Generate SURT
            act_surt = generate_surt(seed)
            if act_surt is not None:
                all_surts.add(act_surt)
                all_surts_and_urls.append({
                    'surt': act_surt,
                    'url': seed
                })
            else:
                logger.warning("Got no SURT from %s" % seed)

            # Record as URL
            all_urls.add(seed)

    # And write out the SURTs:
    if fmt == "urls":
        return sorted(all_urls)
    elif fmt == "surts":
        return sorted(all_surts)
    elif fmt == "pywb":
        # Return as a pywb acl list:
        pywb_rules = set()
        for item in all_surts_and_urls:
            rule = {
                'access': 'allow',
                'url': item['url']
            }
            surt = item['surt']
            surt = surt.replace('http://(', '', 1)
            surt = surt.rstrip(',') # Strip any trailing comma
            pywb_rules.add("%s - %s" % (surt, json.dumps(rule)))
        return sorted(pywb_rules, reverse=True)
    else:
        raise Exception("Unknown access list format '%s'!" % fmt)


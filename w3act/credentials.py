import re
import sys
import json
import w3act
import logging
import heritrix
import requests
from lxml import html
from slugify import slugify
from urlparse import urlparse, urljoin

logger = logging.getLogger("w3act.credentials")
logging.getLogger("requests").setLevel(logging.WARNING)
requests.packages.urllib3.disable_warnings()

def scrape_login_page(url, username, password):
    """Scrape a page for hidden fields pre-login."""
    r = requests.get(url)
    h = html.fromstring(r.content)
    path = urlparse(url).path
    forms = h.xpath("//form[contains(@action, %s)]" % path)
    if len(forms) > 1:
        logger.warning("Multiple forms found!")
        sys.exit(1)
    form = forms[0]
    fields = []
    for f in form.xpath(".//input"):
        if any(val in f.attrib["name"].lower() for val in ["user", "email"]):
            fields.append({"name": f.attrib["name"].replace("$", "\\$"), "value": username})
        elif any(val in f.attrib["name"].lower() for val in ["pass"]):
            fields.append({"name": f.attrib["name"].replace("$", "\\$"), "value": password})
        else:
            if "value" in f.keys():
                fields.append({"name": f.attrib["name"].replace("$", "\\$"), "value": f.attrib["value"]})
    form_action = urljoin(url, form.attrib["action"])
    return (form_action, fields)

def get_logout_regex(logout_url):
    """Generates the regular expression to avoid the logout page."""
    parsed = urlparse(logout_url)
    logout_regex = "https?://(www[0-9]?\\.)?%s%s.*" % (
        re.escape(re.sub("^www[0-9]?\.", "", parsed.netloc)),
        re.escape(parsed.path)
    )
    return logout_regex.replace("\\", "\\\\")

def get_credential_script(info):
    """Generates a Heritrix script to add new credentials."""
    domain = urlparse(info["watchedTarget"]["loginPageUrl"]).netloc
    id = slugify(domain)
    script = """hfc = new org.archive.modules.credential.HtmlFormCredential()
    hfc.setDomain("%s")
    hfc.setLoginUri("%s")
    fi = hfc.getFormItems()""" % (domain, info["watchedTarget"]["loginPageUrl"])
    for field in fields:
        script += """\nfi.put("%s", "%s")""" % (field["name"], field["value"])
    script += """\nappCtx.getBean("credentialStore").getCredentials().put("%s", hfc)""" % (id)
    return script

def get_seeds_script(seeds):
    """Generates script to add a list of seeds."""
    script = ""
    for seed in seeds:
        script += """\nappCtx.getBean("seeds").seedLine("%s")""" % (seed)
    return script

def get_logout_exclusion_script(logout_regex):
    return """appCtx.getBean("listRegexFilterOut").regexList.add(java.util.regex.Pattern.compile("%s"))""" % (logout_regex)

def handle_credentials(info, job, api):
    w = w3act.ACT()
    if "watched" in info.keys() and info["watched"]:
        if info["watchedTarget"]["secretId"] is not None:
            secret = w.get_secret(info["watchedTarget"]["secretId"])
            form_action, fields = scrape_login_page(info["watchedTarget"]["loginPageUrl"], secret["username"], secret["password"])
            logout_regex = get_logout_regex(info["watchedTarget"]["logoutUrl"])
            logger.info("Excluding: %s" % logout_regex)
            api.execute(script=get_logout_exclusion_script(logout_regex), job=job, engine="groovy")
            logger.info("Adding credentials...")
            api.execute(script=get_credential_script(info), job=job, engine="groovy")
            logger.info("Adding login page as a seeds...")
            api.execute(script=get_seeds_script(set([info["watchedTarget"]["loginPageUrl"], secret["url"]])), job=job, engine="groovy")

if __name__ == "__main__":
    """For testing purposes."""
    info = json.loads("""{
        "watched": true,
        "seeds": [
            "http://www.hypodiab.com"
        ],
        "watchedTarget": {
            "waybackTimestamp": 20150508100336,
            "loginPageUrl": "http://www.hypodiab.com/login.aspx",
            "documentUrlScheme": "www.hypodiab.com/",
            "logoutUrl": "http://www.hypodiab.com/logout.aspx",
            "id": 48,
            "secretId": 1247
        },
        "id": 19827,
        "title": "Diabetic Hypoglycemia"
    }""")
    api = heritrix.API(host="https://opera.bl.uk:8443/engine", user="admin", passwd="bl_uk", verbose=False, verify=False)
    job = "paywall-test-20150422134313"
    handle_credentials(info, job, api)

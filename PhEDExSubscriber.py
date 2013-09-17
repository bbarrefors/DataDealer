#!/usr/bin/python

import os
import re
import sys
import urllib
import urllib2
import httplib
try:
    import json
except ImportError:
    import simplejson as json
import xml.dom.minidom

PHEDEX_BASE = "https://cmsweb.cern.ch/phedex/datasvc/"
#PHEDEX_INSTANCE = "prod"
PHEDEX_INSTANCE = "dev"

SITE = "T2_US_Nebraska"
#DATASET = "/Pyquen_WToMuNu_TuneZ2_5023GeV_pythia6/HiWinter13-pa_STARTHI53_V25-v2/GEN-SIM-RECO"
DATASET = "/BTau/GowdyTest10-Run2010Av3/RAW"
#GROUP = 'local'
GROUP = 'Jupiter'
COMMENTS = 'BjornBarrefors'

class HTTPSGridAuthHandler(urllib2.HTTPSHandler):

    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key = self.getProxy()
        self.cert = self.key

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getProxy(self):
        proxy = os.environ.get("X509_USER_PROXY")
        if not proxy:
            proxy = "/tmp/x509up_u%d" % os.geteuid()
        return proxy

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

def dictIteration(data, xml):
    for k, v in data.iteritems():
        k = k.replace("_", "-")
        if type(v) is list:
            xml = xml + ">"
            for v1 in v:
                xml = xml + "<" + k
                xml = dictIteration(v1, xml)
                if (k == "file"):
                    xml = xml + "/>"
                else:
                    xml = xml + "</" + k + ">"
        else:
            if k == "lfn":
                k = "name"
            elif k == "size":
                k = "bytes"
            xml = xml + " " + k + "=" + '"%s"' % v
    return xml

def getData(dataset):
    url = urllib.basejoin(PHEDEX_BASE, "json/%s/data" % PHEDEX_INSTANCE) + "?" + urllib.urlencode({"dataset": dataset})
    print "Querying url %s for data information" % url
    try:
        data_response = urllib2.urlopen(url)
    except:
        print "Failed phedex call"
        sys.exit()
    
    json_data = json.load(data_response)
    json_data = json_data.get('phedex')
    xml = '<data version="2">'
    for k, v in json_data.iteritems():
        if k == "dbs":
            xml = xml + "<" + k
            xml = dictIteration(v[0], xml)
            xml = xml + "</" + k + ">"
    xml_data = xml + "</data>"
    return xml_data

def subscribe(site, dataset):
    data = getData(dataset)
    level = 'dataset'
    priority = 'low'
    move = 'n'
    static = 'n'
    custodial = 'n'
    request_only = 'n'
    values = { 'node' : site, 'data' : data, 'level' : level,
               'priority' : priority, 'move' : move, 'static' : static,
               'custodial' : custodial, 'request_only' : request_only,
               'group': GROUP, 'comments' : COMMENTS }
    data = urllib.urlencode(values)
    subscription_url = urllib.basejoin(PHEDEX_BASE, "xml/%s/subscribe" % PHEDEX_INSTANCE)
    print "Querying %s for subscription with data:\n%s" % (subscription_url, data)

    #opener = urllib2.build_opener(HTTPSGridAuthHandler())
    #request = urllib2.Request("https://cmsweb.cern.ch/auth/trouble/")
    opener = urllib2.build_opener(HTTPSGridAuthHandler())
    request = urllib2.Request(subscription_url, data)
    try:
        sub_response = opener.open(request)
    except urllib2.HTTPError, he:
        print he.read()
        raise

    sub_status = sub_response.read()
    print sub_status

    return 0

def delete(site, dataset):
    data = getData(dataset)
    level = 'dataset'
    rm_subs = 'y'
    values = { 'node' : site, 'data' : data, 'level' : level,
               'rm_subscriptions' : rm_subs, 'comments' : COMMENTS }
    data = urllib.urlencode(values)
    delete_url = urllib.basejoin(PHEDEX_BASE, "xml/%s/delete" % PHEDEX_INSTANCE)
    print "Querying %s for deletion with data:\n%s" % (delete_url, data)

    #opener = urllib2.build_opener(HTTPSGridAuthHandler())
    #request = urllib2.Request("https://cmsweb.cern.ch/auth/trouble/")
    opener = urllib2.build_opener(HTTPSGridAuthHandler())
    request = urllib2.Request(delete_url, data)
    try:
        sub_response = opener.open(request)
    except urllib2.HTTPError, he:
        print he.read()
        raise

    sub_status = sub_response.read()
    print sub_status

    return 0

def main():
    #subscribe(SITE, DATASET)
    delete(SITE, DATASET)
    
if __name__ == '__main__':
    sys.exit(main())

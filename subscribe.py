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
GROUP = 'local'

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

def subscribe(site, dataset):
    # node = UNL
    # data = some small set not at UNL
    # 
    url = urllib.basejoin(PHEDEX_BASE, "json/%s/data" % PHEDEX_INSTANCE) + "?" + urllib.urlencode({"dataset": dataset})
    print "Querying url %s for data information" % url
    try:
        data_response = urllib2.urlopen(url)
    except:
        print "Failed phedex call"
        sys.exit()
        #data_response = urllib2.urlopen(phedex_call)
    
    json_data = json.load(data_response)
    print json_data
    #dom = xml.dom.minidom.parseString(xml_data)
    #dbs_dom = dom.getElementsByTagName("dbs")[0] # TODO: error checking
    #doc = xml.dom.minidom.getDOMImplementation().createDocument(None, "data", None)
    #result = doc.createElement("data")
    #result.setAttribute("version", "2")
    #result.appendChild(dbs_dom)
    #xml_data = result.toxml()
    #print "Corresponding data:\n%s" % xml_data
    level = 'dataset'
    priority = 'low'
    move = 'n'
    static = 'n'
    custodial = 'n'
    request_only = 'n'
    #values = { 'node' : site, 'data' : xml_data, 'level' : level,
    #           'priority' : priority, 'move' : move, 'static' : static,
    #           'custodial' : custodial, 'request_only' : request_only,
    #           'group': GROUP }
    #data = urllib.urlencode(values)
    #subscription_url = urllib.basejoin(PHEDEX_BASE, "xml/%s/subscribe" % PHEDEX_INSTANCE)
    #print "Querying %s for subscription with data:\n%s" % (subscription_url, data)

    #opener = urllib2.build_opener(HTTPSGridAuthHandler())
    #request = urllib2.Request(subscription_url, data)
    #try:
    #    sub_response = opener.open(request)
    #except urllib2.HTTPError, he:
    #    print he.read()
    #    raise

    #sub_status = sub_response.read()
    #print sub_status

    return 0

def main():
    subscribe(SITE, DATASET)

if __name__ == '__main__':
    sys.exit(main())

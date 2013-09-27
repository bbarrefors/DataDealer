#!/usr/bin/env python26
"""
_PhEDExAPI_

Make subscriptions and deletions of datasets using PhEDEx API.

Created by Bjorn Barrefors & Brian Bockelman on 15/9/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
################################################################################
#                                                                              #
#                             P h E D E x   A P I                              #
#                                                                              #
################################################################################

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

from PhEDExLogger import log, error

PHEDEX_BASE = "https://cmsweb.cern.ch/phedex/datasvc/"
#PHEDEX_INSTANCE = "prod"
PHEDEX_INSTANCE = "dev"
DATA_TYPE = "json"
#DATA_TYPE = "xml"

SITE = "T2_US_Nebraska"
DATASET = "/BTau/GowdyTest10-Run2010Av3/RAW"
GROUP = 'Jupiter'
COMMENTS = 'BjornBarrefors'

################################################################################
#                                                                              #
#                H T T P S   G R I D   A U T H   H A N D L E R                 #
#                                                                              #
################################################################################

class HTTPSGridAuthHandler(urllib2.HTTPSHandler):
    """
    _HTTPSGridAuthHandler_
    
    Set up certificate and proxy to get acces to PhEDEx API subscription calls.
    """
    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key = self.getProxy()
        self.cert = self.key

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getProxy(self):
        proxy = os.environ.get("X509_USER_PROXY")
        if not proxy:
            proxy = "/tmp/x509up_u%d" % (os.geteuid(),)
        return proxy

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

################################################################################
#                                                                              #
#                                 P A R S E                                    #
#                                                                              #
################################################################################

def parse(data, xml):
    """
    _parse_
    
    Take data output from PhEDEx and parse it into  xml syntax corresponding to 
    subscribe and delete calls.
    """
    for k, v in data.iteritems():
        k = k.replace("_", "-")
        if type(v) is list:
            xml = "%s>" % (xml,)
            for v1 in v:
                xml = "%s<%s" % (xml, k)
                xml = dictIteration(v1, xml)
                if (k == "file"):
                    xml = "%s/>" % (xml,)
                else:
                    xml = "%s</%s>" % (xml, k)
        else:
            if k == "lfn":
                k = "name"
            elif k == "size":
                k = "bytes"
            xml = '%s %s="%s"' % (xml, k, v)
    return xml

################################################################################
#                                                                              #
#                                 Q U E R Y                                    #
#                                                                              #
################################################################################

def query(tag, data):
    """
    _query_
    
    Make a PhEDEx data call.
    Return data as json.
    """
    name = "APIQuery"
    encoded = urllib.urlencode({'%s' % (tag,) : data})
    url = urllib.basejoin(PHEDEX_BASE, "%s/%s/data?%s" % (DATA_TYPE, PHEDEX_INSTANCE, encoded))
    try:
        data_response = urllib2.urlopen(url)
    except:
        error(name, "Failed phedex call on dataset %s" % (dataset,))
        return 1
    json_data = json.load(data_response)
    return json_data

################################################################################
#                                                                              #
#                                  D A T A                                     #
#                                                                              #
################################################################################

def data(dataset):
    """
    _data_

    Return data information as xml structure complying with PhEDEx
    subscribe and delete call.
    """
    name = "APIdata"
    json_data = query("dataset", dataset) 
    json_data = json_data.get('phedex')
    if (not json_data):
        error(name, "No data for dataset %s" % (dataset,))
    xml = '<data version="2">'
    for k, v in json_data.iteritems():
        if k == "dbs":
            xml = "%s<%s" % (xml, k)
            xml = dictIteration(v[0], xml)
            xml = "%s</%s>" % (xml, k)
    xml_data = "%s</data>" % (xml,)
    return xml_data

################################################################################
#                                                                              #
#                                  C A L L                                     #
#                                                                              #
################################################################################

def call(url, data):
    """
    _call_

    Make http post call to PhEDEx API.
    """
    name = "APICall"
    opener = urllib2.build_opener(HTTPSGridAuthHandler())
    request = urllib2.Request(url, data)
    try:
        sub_response = opener.open(request)
    except urllib2.HTTPError, he:
        error(name, he.read())
        return 1

    sub_status = sub_response.read()
    log(name, sub_status)
    return 0

################################################################################
#                                                                              #
#                             S U B S C R I B E                                #
#                                                                              #
################################################################################

def subscribe(site, dataset):
    """
    _subscribe_

    Set up subscription call to PhEDEx API.
    """
    name = "APISubscribe"
    log(name, "Subscribing %s to %s" % (dataset, site))
    data = data(dataset)
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
    if call(subscription_url, data):
        return 1
    return 0

################################################################################
#                                                                              #
#                                D E L E T E                                   #
#                                                                              #
################################################################################

def delete(site, dataset):
    """
    _delete_

    Set up delete call to PhEDEx API.
    """
    name = "APIDelete"
    log(name, "Deleting %s from %s" % (dataset, site))
    data = getData(dataset)
    level = 'dataset'
    rm_subs = 'y'
    values = { 'node' : site, 'data' : data, 'level' : level,
               'rm_subscriptions' : rm_subs, 'comments' : COMMENTS }
    data = urllib.urlencode(values)
    delete_url = urllib.basejoin(PHEDEX_BASE, "xml/%s/delete" % PHEDEX_INSTANCE)
    if call(delete_url, data):
        return 1
    return 0

################################################################################
#                                                                              #
#                        D A T A S E T   S I Z E                               #
#                                                                              #
################################################################################

def datasetSize(dataset):
    """
    _datasetSize_

    Get total size of dataset in GB.
    """
    name = "APIdatasetSize"
    json_data = query("dataset", dataset) 
    if ( not json_data = json_data.get('phedex')):
        error(name, "No data for dataset %s" % (dataset,))
        return 1
    data = json_data.get('dbs')[0].get('dataset')[0].get('block')
    size = float(0)
    for block in data:
        size += block.get('bytes')

    size = size / 10**9
    log(name, "Total size of dataset %s is %dGB" % (dataset, size))
    return int(size)

def main():
    #subscribe(SITE, DATASET)
    delete(SITE, DATASET)
    
if __name__ == '__main__':
    sys.exit(main())

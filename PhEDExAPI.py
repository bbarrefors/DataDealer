#!/usr/bin/env python
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
import time
import datetime
try:
    import json
except ImportError:
    import simplejson as json

from CMSDATALogger import log, error

PHEDEX_BASE = "https://cmsweb.cern.ch/phedex/datasvc/"
PHEDEX_INSTANCE = "prod"
#PHEDEX_INSTANCE = "dev"
DATA_TYPE = "json"
#DATA_TYPE = "xml"
SITE = "T2_US_Nebraska"
DATASET = "/BTau/GowdyTest10-Run2010Av3/RAW"
GROUP = 'local'
#GROUP = 'Jupiter'
COMMENTS = 'BjornBarrefors'

################################################################################
#                                                                              #
#                H T T P S   G R I D   A U T H   H A N D L E R                 #
#                                                                              #
################################################################################

class HTTPSGridAuthHandler(urllib2.HTTPSHandler):
    """
    _HTTPSGridAuthHandler_
    
    Set up certificate and proxy to get acces to PhEDEx API subscription and 
    delete calls.
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
#                           P h E D E x   C A L L                              #
#                                                                              #
################################################################################

def PhEDExCall(url, values):
    """
    _PhEDExCall_

    Make http post call to PhEDEx API.
    Function only gauranttees that something is returned.
    The caller need to check the response for correctness.

    Returns a check variable, if 0 no error was encountered.
    """
    name = "PhEDExAPICall"
    data = urllib.urlencode(values)
    opener = urllib2.build_opener(HTTPSGridAuthHandler())
    request = urllib2.Request(url, data)
    try:
        response = opener.open(request)
    except urllib2.HTTPError, he:
        return 1, str(he.read())
    except urllib2.URLError, e:
        return 1, "A URLError was received"
    return 0, response

################################################################################
#                                                                              #
#                                  D A T A                                     #
#                                                                              #
################################################################################

def data(dataset="", block="", file_name="", level="block", create_since="", format="json", instance="prod"):
    """
    _findDataset_

    Build PhEDEx data call.
    At least one of the arguments, dataset, block, file have to be passed.
    """
    if ((not dataset) and (not block) and (not file_name)):
        return 1, "Not enough parameters passed"
    values = { 'dataset' : dataset, 'block' : block, 'file' : file_name, 
               'level' : level, 'create_since' : create_since }
    data_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/data" % (format, instance))
    check, response = PhEDExCall(data_url, values)
    if check:
        # An error occurred
        return 1, response
    if format == "json":
        data = json.load(response)
        if not data:
            return 1, "No json data available"
    else:
        data = response
    test = data.get('db')
    return 0, data

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
    #log(name, "Subscribing %s to %s" % (dataset, site))
    sub_data = xmlData(dataset)
    if not sub_data:
        error(name, "Subscribe did not succeed")
        return 1
    level = 'dataset'
    priority = 'low'
    move = 'n'
    static = 'n'
    custodial = 'n'
    request_only = 'n'
    values = { 'node' : site, 'data' : sub_data, 'level' : level,
               'priority' : priority, 'move' : move, 'static' : static,
               'custodial' : custodial, 'request_only' : request_only,
               'group': GROUP, 'comments' : COMMENTS }
    subscription_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/subscribe" % (DATA_TYPE, PHEDEX_INSTANCE,))
    response = PhEDExCall(subscription_url, values)
    if response:
        #log(name, "Subscribe response %s" % (str(response),))
        return 0
    else:
        error(name, "Subscribe did not succeed")
        return 1

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
    #log(name, "Deleting %s from %s" % (dataset, site))
    del_data = xmlData(dataset)
    if not del_data:
        error(name, "Delete did not succeed")
        return 1
    level = 'dataset'
    rm_subs = 'y'
    values = { 'node' : site, 'data' : del_data, 'level' : level,
               'rm_subscriptions' : rm_subs, 'comments' : COMMENTS }
    delete_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/delete" % (DATA_TYPE, PHEDEX_INSTANCE))
    response = PhEDExCall(delete_url, values)
    if response:
        #log(name, "Delete response %s" % (str(response),))
        return 0
    else:
        error(name, "Delete did not succeed")
        return 1

################################################################################
#                                                                              #
#                                 E X I S T S                                  #
#                                                                              #
################################################################################

def exists(site, dataset):
    """
    _exists_

    Set up blockreplicas call to PhEDEx API.
    """
    name = "APIExists"
    #log(name, "Check if %s exists on %s" % (dataset, site))
    data = dataset
    node = site
    complete = 'y'
    show_dataset = 'y'
    values = { 'node' : site, 'dataset' : data, 'complete' : complete,
               'show_dataset' : show_dataset }
    subscription_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/blockreplicas" % (DATA_TYPE, PHEDEX_INSTANCE))
    response = PhEDExCall(subscription_url, values)
    if response:
        exists = response.get('dataset')
        if exists:
            return 1
        return 0
    else:
        return 0

################################################################################
#                                                                              #
#                         S U B S C R I P T I O N S                            #
#                                                                              #
################################################################################

def subscriptions(site, days):
    """
    _subscriptions_

    Return all subscriptions made to UNL in the last week by group Jupiter.
    """
    name = "APISubscriptions"
    # Created since a week ago?
    past = datetime.datetime.now() - datetime.timedelta(days = days)
    create_since = time.mktime(past.utctimetuple())
    values = { 'node' : site, 'create_since' : create_since, 'group' : GROUP }
    subscriptions_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/subscriptions" % (DATA_TYPE, PHEDEX_INSTANCE))
    response = PhEDExCall(subscriptions_url, values)
    if not response:
        error(name, "Subscriptions did not succeed")
        return 1
    # TODO : Do stuff with data
    datasets = []
    data = response.get('dataset')
    if not data:
        return datasets
    for dataset in data:
        datasets.append(dataset.get('name'))
    return datasets

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
    values = { 'dataset' : dataset }
    size_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/data" % (DATA_TYPE, PHEDEX_INSTANCE))
    response = PhEDExCall(size_url, values) 
    if not response:
        return 0
    dbs = response.get('dbs')
    if (not dbs):
        error(name, "No data for dataset %s" % (dataset,))
        return 0
    data = dbs[0].get('dataset')[0].get('block')
    size = float(0)
    for block in data:
        size += block.get('bytes')

    size = size / 10**9
    #log(name, "Total size of dataset %s is %dGB" % (dataset, size))
    return int(size)

################################################################################
#                                                                              #
#                              R E P L I C A S                                 #
#                                                                              #
################################################################################

def replicas(dataset):
    """
    _replicas_

    Set up blockreplicas call to PhEDEx API.
    """
    name = "APIExists"
    data = dataset
    complete = 'y'
    show_dataset = 'n'
    values = { 'dataset' : data, 'complete' : complete,
               'show_dataset' : show_dataset }
    subscription_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/blockreplicas" % (DATA_TYPE, PHEDEX_INSTANCE))
    response = PhEDExCall(subscription_url, values)
    sites = []
    if response:
        block = response.get('block')
        replicas = block[0].get('replica')
        for replica in replicas:
            site = replica.get('node')
            sites.append(site)
    return sites

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
                xml = parse(v1, xml)
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
#                             X M L   D A T A                                  #
#                                                                              #
################################################################################

def xmlData(dataset):
    """
    _xmlData_

    Return data information as xml structure complying with PhEDEx
    subscribe and delete call.
    """
    name = "APIXMLData"
    values = { 'dataset' : dataset }
    data_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/data" % (DATA_TYPE, PHEDEX_INSTANCE))
    response = PhEDExCall(data_url, values)
    if not response:
        error(name, "No data for dataset %s" % (dataset,))
        return 0
    xml = '<data version="2">'
    for k, v in response.iteritems():
        if k == "dbs":
            xml = "%s<%s" % (xml, k)
            xml = parse(v[0], xml)
            xml = "%s</%s>" % (xml, k)
    xml_data = "%s</data>" % (xml,)
    return xml_data

if __name__ == '__main__':
    """
    __main__

    For testing purpose only.
    """
    check, response = data(dataset=DATASET)
    sys.exit(0)

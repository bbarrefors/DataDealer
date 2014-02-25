#!/usr/bin/python -B

"""
_PhEDExAPI_

Created by Bjorn Barrefors & Brian Bockelman on 15/9/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
__author__       = 'Bjorn Barrefors'
__organization__ = 'Holland Computing Center - University of Nebraska-Lincoln'
__email__        = 'bbarrefo@cse.unl.edu'

import sys
import os
import re
import urllib
import urllib2
import httplib
import time
import datetime
try:
    import json
except ImportError:
    import simplejson as json

from CMSDATALogger import CMSDATALogger


################################################################################
#                                                                              #
#                             P h E D E x   A P I                              #
#                                                                              #
################################################################################

class PhEDExAPI:
    """
    _PhEDExAPI_

    Interface to submit queries to the PhEDEx API
    For specifications of calls see https://cmsweb.cern.ch/phedex/datasvc/doc
    
    Class variables:
    PHEDEX_BASE -- Base URL to the PhEDEx web API
    logger      -- Used to print log and error messages to log file
    """
    # Useful variables
    # PHEDEX_BASE = "https://cmsweb.cern.ch/phedex/datasvc/"
    # PHEDEX_INSTANCE = "prod"
    # PHEDEX_INSTANCE = "dev"
    # DATA_TYPE = "json"
    # DATA_TYPE = "xml"
    # SITE = "T2_US_Nebraska"
    # DATASET = "/BTau/GowdyTest10-Run2010Av3/RAW"
    # GROUP = 'local'
    # GROUP = 'Jupiter'
    # COMMENTS = 'BjornBarrefors'
    def __init__(self):
        """
        __init__
        
        Set up class constants
        """
        self.logger      = CMSDATALogger()
        self.PHEDEX_BASE = "https://cmsweb.cern.ch/phedex/datasvc/"


    ################################################################################
    #                                                                              #
    #                           P h E D E x   C A L L                              #
    #                                                                              #
    ################################################################################
    
    def phedexCall(self, url, values):
        """
        _phedexCall_
        
        Make http post call to PhEDEx API.
        
        Function only gaurantees that something is returned,
        the caller need to check the response for correctness.
        
        Keyword arguments:
        url    -- URL to make API call
        values -- Arguments to pass to the call
        
        Return values:
        1 -- Status, 0 = everything went well, 1 = something went wrong
        2 -- IF status == 0 : HTTP response ELSE : Error message
        """
        name = "phedexCall"
        data = urllib.urlencode(values)
        opener = urllib2.build_opener(HTTPSGridAuthHandler())
        request = urllib2.Request(url, data)
        try:
            response = opener.open(request)
        except urllib2.HTTPError, e:
            self.logger.error(name, e.read())
            return 1, "Error"
        except urllib2.URLError, e:
            self.logger.error(name, e.args)
            return 1, "Error"
        return 0, response


    ################################################################################
    #                                                                              #
    #                                  D A T A                                     #
    #                                                                              #
    ################################################################################
    
    def data(self, dataset='', block='', file_name='', level='block', create_since='', format='json', instance='prod'):
        """
        _data_
        
        PhEDEx data call
        
        At least one of the arguments dataset, block, file have to be passed
        
        No checking is made for xml data
        
        Even if JSON data is returned no gaurantees are made for the structure
        of it
        
        Keyword arguments:
        dataset      -- Name of dataset to look up
        block        -- Only return data for this block
        file_name    -- Data for file file_name returned
        level        -- Which granularity of dataset information to show
        create_since -- Files/blocks/datasets created since this date/time
        format       -- Which format to return data as, XML or JSON
        instance     -- Which instance of PhEDEx to query, dev or prod

        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- json structure if json format, xml structure if xml format
        """
        name = "data"
        if not (dataset or block or file_name):
            self.logger.error(name, "Need to pass at least one of dataset/block/file_name")
            return 1, "Error"
        
        values = { 'dataset' : dataset, 'block' : block, 'file' : file_name,  
                   'level' : level, 'create_since' : create_since }
        
        data_url = urllib.basejoin(self.PHEDEX_BASE, "%s/%s/data" % (format, instance))
        check, response = self.phedexCall(data_url, values)
        if check:
            # An error occurred
            self.logger.error(name, "Data call failed")
            return 1, "Error"
        if format == "json":
            try:
                data = json.load(response)
            except ValueError, e:
                # This usually means that PhEDEx didn't like the URL
                self.logger.error(name, "In call to url %s : %s" % (data_url, str(e)))
                return 1, "Error"
            if not data:
                self.logger.error(name, "No json data available")
                return 1, "Error"
        else:
            data = response.read()
        return 0, data


    ################################################################################
    #                                                                              #
    #                                 P A R S E                                    #
    #                                                                              #
    ################################################################################
    
    def parse(self, data, xml):
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
                    xml = self.parse(v1, xml)
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
    
    def xmlData(self, dataset='', instance='prod'):
        """
        _xmlData_
        
        Return data information as xml structure complying with PhEDEx
        subscribe and delete call.
        """
        check, response = self.data(dataset=dataset, instance=instance)
        if check:
            return 1, "Error"
        data = response.get('phedex')
        xml = '<data version="2">'
        for k, v in data.iteritems():
            if k == "dbs":
                xml = "%s<%s" % (xml, k)
                xml = self.parse(v[0], xml)
                xml = "%s</%s>" % (xml, k)
        xml_data = "%s</data>" % (xml,)
        return 0, xml_data
    
    
    ################################################################################
    #                                                                              #
    #                             S U B S C R I B E                                #
    #                                                                              #
    ################################################################################

    def subscribe(self, node='', data='', level='dataset', priority='low', move='n', static='n', custodial='n', group='local', time_start='', request_only='n', no_mail='n', comments='', format='json', instance='prod'):
        """
        _subscribe_
        
        Set up subscription call to PhEDEx API.
        """
        name = "subscribe"
        if not (node and data):
            self.logger.error(name, "Need to pass both node and data")
            return 1, "Error"
        
        values = { 'node' : node, 'data' : data, 'level' : level,
                   'priority' : priority, 'move' : move, 'static' : static,
                   'custodial' : custodial, 'group' : group, 
                   'time_start' : time_start, 'request_only' : request_only,
                   'no_mail' : no_mail, 'comments' : comments }
        
        subscription_url = urllib.basejoin(self.PHEDEX_BASE, "%s/%s/subscribe" % (format, instance))
        check, response = self.phedexCall(subscription_url, values)
        if check:
            # An error occurred
            self.logger.error(name, "Subscription call failed")
            return 1, "Error"
        return 0, response
        

    ################################################################################
    #                                                                              #
    #                                D E L E T E                                   #
    #                                                                              #
    ################################################################################

    def delete(self, node='', data='', level='dataset', rm_subscriptions='y', comments='', format='json', instance='prod'):
        """
        _subscribe_
        
        Set up subscription call to PhEDEx API.
        """
        name = "delete"
        if not (node and data):
            self.logger.error(name, "Need to pass both node and data")
            return 1, "Error"

        values = { 'node' : node, 'data' : data, 'level' : level,
                   'rm_subscriptions' : rm_subscriptions, 'comments' : comments }
        
        delete_url = urllib.basejoin(self.PHEDEX_BASE, "%s/%s/delete" % (format, instance))
        check, response = self.phedexCall(delete_url, values)
        if check:
            # An error occurred
            self.logger.error(name, "Delete call failed")
            return 1, "Error"
        return 0, response


    ################################################################################
    #                                                                              #
    #                   B L O C K   R E P L I C A   S U M M A R Y                  #
    #                                                                              #
    ################################################################################
    
#    def blockReplicaSummary(block="", dataset="", node="", update_since="", create_since="", complete="", dist_complete="", subscribed="", custodial="", format="json", instance="prod"):
#        """
#        _blockReplicaSummary_
#        
#        PhEDEx blockReplicaSummary call
#        
#        At least one of the arguments dataset, block, file have to be passed.
#        No checking is made for xml data.
#        Even if JSON data is returned no gaurantees are made for the structure
#        of it.
#        
#        TODO: See data
#        """
#        if ((not dataset) and (not block) and (not file_name)):
#            return 1, "Not enough parameters passed"
#        
#        values = { 'block' : block, 'dataset' : dataset, 'node' : node, 
#                   'update_since' : update_since, 'create_since' : create_since 
#                   'complete' : complete, 'dist_complete' : dist_complete, 
#                   'subscribed' : subscribed, 'custodial' : custodial }
#        
#        data_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/blockreplicasummary" % (format, instance))
#        check, response = PhEDExCall(data_url, values)
#        if check:
#            # An error occurred
#            return 1, response
#        if format == "json":
#            data = json.load(response)
#            if not data:
#                return 1, "No json data available"
#        else:
#            data = response
#        return 0, data


################################################################################
#                                                                              #
#                        D A T A S E T   S I Z E                               #
#                                                                              #
################################################################################

#def datasetSize(dataset):
#    """
#    _datasetSize_
#
#    Get total size of dataset in GB.
#    """
#    name = "APIdatasetSize"
#    values = { 'dataset' : dataset }
#    size_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/data" % (DATA_TYPE, PHEDEX_INSTANCE))
#    response = PhEDExCall(size_url, values) 
#    if not response:
#        return 0
#    dbs = response.get('dbs')
#    if (not dbs):
#        error(name, "No data for dataset %s" % (dataset,))
#        return 0
#    data = dbs[0].get('dataset')[0].get('block')
#    size = float(0)
#    for block in data:
#        size += block.get('bytes')

#    size = size / 10**9
#    #log(name, "Total size of dataset %s is %dGB" % (dataset, size))
#    return int(size)


################################################################################
#                                                                              #
#                              R E P L I C A S                                 #
#                                                                              #
################################################################################

#def replicas(dataset):
#    """
#    _replicas_
#
#    Set up blockreplicas call to PhEDEx API.
#    """
#    name = "APIExists"
#    data = dataset
#    complete = 'y'
#    show_dataset = 'n'
#    values = { 'dataset' : data, 'complete' : complete,
#               'show_dataset' : show_dataset }
#    subscription_url = urllib.basejoin(PHEDEX_BASE, "%s/%s/blockreplicas" % (DATA_TYPE, PHEDEX_INSTANCE))
#    response = PhEDExCall(subscription_url, values)
#    sites = []
#    if response:
#        block = response.get('block')
#        replicas = block[0].get('replica')
#        for replica in replicas:
#            site = replica.get('node')
#            sites.append(site)
#    return sites


################################################################################
#                                                                              #
#                H T T P S   G R I D   A U T H   H A N D L E R                 #
#                                                                              #
################################################################################

class HTTPSGridAuthHandler(urllib2.HTTPSHandler):
    """
    _HTTPSGridAuthHandler_
    
    Get  proxy to acces PhEDEx API

    Needed for subscribe and delete calls

    Class variables:
    key  -- User key to CERN with access to PhEDEx
    cert -- User certificate connected to key
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
#                                  M A I N                                     #
#                                                                              #
################################################################################

if __name__ == '__main__':
    """
    __main__
    
    For testing purpose only
    """
    phedex_api = PhEDExAPI()
    check, data = phedex_api.xmlData(dataset='/BTau/GowdyTest10-Run2010Av3/RAW', instance='dev')
    if check:
        sys.exit(1)
    #check, response = phedex_api.subscribe(node='T2_US_Nebraska', data=data, group='Jupiter', comments='This is just a test by Bjorn Barrefors, he will deal with this request.', instance='dev')
    check, response = phedex_api.delete(node='T2_US_Nebraska', data=data, group='Jupiter', comments='This is just a test by Bjorn Barrefors, he will deal with this request.', instance='dev')
    if check:
        print response
        sys.exit(1)
    print response.read()
    sys.exit(0)

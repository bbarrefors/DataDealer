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

from DynDTALogger import DynDTALogger


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
    # GROUP = 'AnalysisOps'
    # GROUP = 'Jupiter'
    # COMMENTS = 'BjornBarrefors'
    def __init__(self):
        """
        __init__

        Set up class constants
        """
        self.logger      = DynDTALogger()
        self.PHEDEX_BASE = "https://cmsweb.cern.ch/phedex/datasvc/"


    ############################################################################
    #                                                                          #
    #                           P h E D E x   C A L L                          #
    #                                                                          #
    ############################################################################

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
            self.logger.error(name, "URL: %s" % (str(url),))
            self.logger.error(name, "VALUES: %s" % (str(values),))
            return 1, "Error"
        except urllib2.URLError, e:
            self.logger.error(name, e.args)
            self.logger.error(name, "URL: %s" % (str(url),))
            self.logger.error(name, "VALUES: %s" % (str(values),))
            return 1, "Error"
        return 0, response


    ############################################################################
    #                                                                          #
    #                                  D A T A                                 #
    #                                                                          #
    ############################################################################

    def data(self, dataset='', block='', file_name='', level='block',
             create_since='', format='json', instance='prod'):
        """
        _data_

        PhEDEx data call

        At least one of the arguments dataset, block, file have to be passed

        No checking is made for xml data

        Even if JSON data is returned no gaurantees are made for the structure
        of it

        Keyword arguments:
        dataset      -- Name of dataset to look up
        block        -- Name of block to look up
        file         -- Name of file to look up
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


    ############################################################################
    #                                                                          #
    #                                 P A R S E                                #
    #                                                                          #
    ############################################################################

    def parse(self, data, xml):
        """
        _parse_

        Trasverse a json structure and parse it into xml data structure
        which complies with the PhEDEx delete/subscribe xml data structure

        Keyword arguments:
        data -- The remaining json data
        xml  -- The current xml data

        Return values:
        xml -- The converted data now represented as an xml structure
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
                if (k == "name" or k == "is-open" or k == "is-transient" or k == "bytes" or k== "checksum"):
                    xml = '%s %s="%s"' % (xml, k, v)
        return xml


    ############################################################################
    #                                                                          #
    #                             X M L   D A T A                              #
    #                                                                          #
    ############################################################################

    def xmlData(self, datasets=[], instance='prod'):
        """
        _xmlData_

        Get json data from PhEDEx for all datasets and convert it to a xml
        structure complient with the PhEDEx delete/subscribe call.

        Keyword arguments:
        datasets -- List of dataset names
        instance -- The instance on which the datasets resides, prod/dev

        Return values:
        error -- 1 if an error occurred, 0 if everything went as expected
        xml   -- The converted data now represented as an xml structure
        """
        name = "xmlData"
        if not datasets:
            self.logger.error(name, "Need to pass at least one dataset")
            return 1, "Error"
        xml = '<data version="2">'
        xml = '%s<%s name="https://cmsweb.cern.ch/dbs/%s/global/DBSReader">' % (xml, 'dbs', instance)
        for dataset in datasets:
            check = 1
            check, response = self.data(dataset=dataset, level='file', instance=instance)
            if check:
                return 1, "Error"
            data = response.get('phedex').get('dbs')
            if not data:
                return 1, "Error"
            xml = "%s<%s" % (xml, 'dataset')
            data = data[0].get('dataset')
            xml = self.parse(data[0], xml)
            xml = "%s</%s>" % (xml, 'dataset')
        xml = "%s</%s>" % (xml, 'dbs')
        xml_data = "%s</data>" % (xml,)
        return 0, xml_data


    ############################################################################
    #                                                                          #
    #                             S U B S C R I B E                            #
    #                                                                          #
    ############################################################################

    def subscribe(self, node='', data='', level='dataset', priority='low',
                  move='n', static='n', custodial='n', group='AnalysisOps',
                  time_start='', request_only='y', no_mail='n', comments='',
                  format='json', instance='prod'):
        """
        _subscribe_

        PhEDEx subscribe call

        Both node and data have to be passed

        If the PhEDExCall fails an error will be returned.

        Keyword arguments:
        node         -- Node on which the datasets should be subscribed to
        data         -- XML data structure for datasets to subscribe
        level        -- Which granularity of dataset information to show
        priority     -- Standard is low
        move         -- If the set should be moved or replicated
        static       -- Standard is no
        custodial    -- Make this new copy custodial
        group        -- The responsible group
        time_start   -- When to start the subscription
        request_only -- Decides if a decision is needed or not
        no_mail      -- Send email to the data managers etc
        comments     -- Any comments
        format       -- Which format to return data as, XML or JSON
        instance     -- Which instance of PhEDEx to query, dev or prod

        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- Error message or the return message from PhEDEx
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
            # @TODO : Print out better logging, url + values
            return 1, "Error"
        json_data = json.load(response)
        request_id = json_data.get('phedex').get('request_created')[0].get('id')
        return 0, request_id


    ############################################################################
    #                                                                          #
    #                                D E L E T E                               #
    #                                                                          #
    ############################################################################

    def delete(self, node='', data='', level='dataset', rm_subscriptions='y',
               comments='', format='json', instance='prod'):
        """
        _subscribe_

        PhEDEx delete call

        Both node and data have to be passed

        If the PhEDExCall fails an error will be returned.

        Keyword arguments:
        node             -- Node on which the datasets resides
        data             -- XML data structure for datasets to subscribe
        level            -- Which granularity of dataset information to show
        rm_subscriptions -- Delete any pending subscriptions
        comments         -- Any comments
        format           -- Which format to return data as, XML or JSON
        instance         -- Which instance of PhEDEx to query, dev or prod

        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- Error message or the return message from PhEDEx
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
            # @TODO : Print out better logging, url + values
            return 1, "Error"
        json_data = json.load(response)
        request_id = json_data.get('phedex').get('request_created')[0].get('id')
        return 0, request_id


    ############################################################################
    #                                                                          #
    #                       U P D A T E   R E Q U E S T                        #
    #                                                                          #
    ############################################################################

    def updateRequest(self, decision='', request='', node='',
                      comments='', format='json', instance='prod'):
        """
        _subscribe_

        PhEDEx updateRequest call

        Approve or dissapprove an existing PhEDEx request.

        If the PhEDExCall fails an error will be returned.

        Keyword arguments:
        decision -- Approve or disapprove
        request  -- ID of request to update
        node     -- Destination node names
        comments -- Any comments
        format   -- Which format to return data as, XML or JSON
        instance -- Which instance of PhEDEx to query, dev or prod

        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- Error message or the return message from PhEDEx
        """
        name = "delete"

        values = { 'decision' : decision, 'request' : request, 'node' : node,
                   'comments' : comments }

        update_url = urllib.basejoin(self.PHEDEX_BASE, "%s/%s/updaterequest" % (format, instance))
        check, response = self.phedexCall(update_url, values)
        if check:
            # An error occurred
            self.logger.error(name, "UpdateRequest call failed")
            # @TODO : Print out better logging, url + values
            return 1, "Error"
        return 0, response


    ############################################################################
    #                                                                          #
    #                         B L O C K   R E P L I C A S                      #
    #                                                                          #
    ############################################################################

    def blockReplicas(self, block="", dataset="", node="", se="",
                      update_since="", create_since="", complete="",
                      dist_complete="", subscribed="", custodial="", group="",
                      show_dataset="", format="json", instance="prod"):
        """
        _blockReplicas_

        PhEDEx blockReplicas call

        Both node and data have to be passed

        If the PhEDExCall fails an error will be returned.

        Keyword arguments:
        block         -- Block name
        dataset       -- Dataset name
        node          -- Node name
        se            -- Storage element name
        update_since  -- Last updated
        create_since  -- Created on
        complete      -- Show only complete sets
        dist_complete -- Return only replicas which has at least one complete replica
        subscribed    -- Include sets subscribed
        custodial     -- Return custodial datasets
        group         -- The responsible group data managers etc
        format        -- Which format to return data as, XML or JSON
        instance      -- Which instance of PhEDEx to query, dev or prod

        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- Error message or the return message from PhEDEx
        """
        values = { 'block' : block, 'dataset' : dataset, 'node' : node,
                   'se' : se, 'update_since' : update_since,
                   'create_since' : create_since, 'complete' : complete,
                   'dist_complete' : dist_complete, 'subscribed' : subscribed,
                   'custodial' : custodial, 'group' : group,
                   'show_dataset' : show_dataset }

        data_url = urllib.basejoin(self.PHEDEX_BASE, "%s/%s/blockreplicas" % (format, instance))
        check, response = self.phedexCall(data_url, values)
        if check:
            # An error occurred
            return 1, response
        if format == "json":
            data = json.load(response)
            if not data:
                return 1, "No json data available"
        else:
            data = response
        return 0, data


    ############################################################################
    #                                                                          #
    #                           D E L E T I O N S                              #
    #                                                                          #
    ############################################################################
    def deletions(self, node="", se="", block="", dataset="",
                  id="", request="", request_since="", complete="",
                  complete_since="", format="json", instance="prod"):
        """
        _deletions_

        PhEDEx deletions call

        If the PhEDExCall fails an error will be returned.

        Keyword arguments:
        node           -- Node name
        se             -- Storage element name
        block          -- Block name
        dataset        -- Dataset name
        id             -- Block ID
        request        -- Request ID
        request_since  -- Request since this date
        complete       -- Show only complete sets
        complete_since -- Complete was done since this date
        format        -- Which format to return data as, XML or JSON
        instance      -- Which instance of PhEDEx to query, dev or prod

        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- Error message or the return message from PhEDEx
        """
        values = { 'node' : node, 'se' : se, 'block' : block,
                   'dataset' : dataset, 'id' : id, 'request' : request,
                   'request_since' : request_since, 'complete' : complete,
                   'complete_since' : complete_since }

        data_url = urllib.basejoin(self.PHEDEX_BASE, "%s/%s/deletions" % (format, instance))
        check, response = self.phedexCall(data_url, values)
        if check:
            # An error occurred
            return 1, response
        if format == "json":
            data = json.load(response)
            if not data:
                return 1, "No json data available"
        else:
            data = response
        return 0, data

    ############################################################################
    #                                                                          #
    #                      D E L E T E   R E Q U E S T S                       #
    #                                                                          #
    ############################################################################
    def deleteRequests(self, request="", node="", create_since="", limit="",
                  approval="", requested_by="", format="json", instance="prod"):
        """
        deleteRequests

        PhEDEx deleteRequests call


        If the PhEDExCall fails an error will be returned.

        Keyword arguments:
        request      -- Request ID
        node         -- Site
        create_since -- Requests created after this date
        limit        -- Maximum number of records returned
        approval     -- Approval state
        requested_by -- Request ID
        format       -- Which format to return data as, XML or JSON
        instance     -- Which instance of PhEDEx to query, dev or prod

        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- Error message or the return message from PhEDEx
        """
        values = { 'request' : request, 'node' : node,
                   'create_since' : create_since, 'limit' : limit,
                   'approval' : approval, 'requested_by' : requested_by }

        data_url = urllib.basejoin(self.PHEDEX_BASE, "%s/%s/deleteRequests" % (format, instance))
        check, response = self.phedexCall(data_url, values)
        if check:
            # An error occurred
            return 1, response
        if format == "json":
            data = json.load(response)
            if not data:
                return 1, "No json data available"
        else:
            data = response
        return 0, data

    ############################################################################
    #                                                                          #
    #                    T R A N S F E R   R E Q U E S T S                     #
    #                                                                          #
    ############################################################################
    def transferRequests(self, request="", node="", group="", create_since="", limit="",
                         approval="", requested_by="", format="json", instance="prod"):
        """
        transferRequests

        PhEDEx transferRequests call


        If the PhEDExCall fails an error will be returned.

        Keyword arguments:
        request      -- Request ID
        node         -- Site
        create_since -- Requests created after this date
        limit        -- Maximum number of records returned
        approval     -- Approval state
        requested_by -- Request ID
        format       -- Which format to return data as, XML or JSON
        instance     -- Which instance of PhEDEx to query, dev or prod

        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- Error message or the return message from PhEDEx
        """
        values = { 'request' : request, 'node' : node, 'group' : group,
                   'create_since' : create_since, 'limit' : limit,
                   'approval' : approval, 'requested_by' : requested_by }

        data_url = urllib.basejoin(self.PHEDEX_BASE, "%s/%s/transferRequests" % (format, instance))
        check, response = self.phedexCall(data_url, values)
        if check:
            # An error occurred
            return 1, response
        if format == "json":
            data = json.load(response)
            if not data:
                return 1, "No json data available"
        else:
            data = response
        return 0, data


    ############################################################################
    #                                                                          #
    #                         R E Q U E S T   L I S T                          #
    #                                                                          #
    ############################################################################

    def requestList(self, request='', type_='', approval='', decision='', group='',
                    requested_by='', node='', create_since='', create_until='',
                    decide_since='', decide_until='', dataset='', block='',
                    decided_by='', format='json', instance='prod'):
        """

        """
        name = "delete"

        values = { 'request' : request, 'type' : type_, 'approval' : approval,
                   'decision' : decision, 'group' : group, 'requested_by' : requested_by,
                   'node' : node, 'create_since' : create_since, 'create_until' : create_until,
                   'decide_since' : decide_since, 'decide_until' : decide_until, 'dataset' : dataset,
                   'block' : block, 'decided_by' : decided_by}

        request_url = urllib.basejoin(self.PHEDEX_BASE, "%s/%s/requestList" % (format, instance))
        check, response = self.phedexCall(request_url, values)
        if check:
            # An error occurred
            self.logger.error(name, "UpdateRequest call failed")
            # @TODO : Print out better logging, url + values
            return 1, "Error"
        if format == "json":
            data = json.load(response)
            if not data:
                return 1, "No json data available"
        else:
            data = response
        return 0, data

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
    check, data = phedex_api.requestList(type_='xfer', approval='mixed', group='AnalysisOps', create_since='2013-11-01', create_until='2014-06-01', requested_by='Bjorn Peter Barrefors', instance='prod')
    
    if check:
        print "Error"
    else:
        print data
        requests = data.get('phedex').get('request')
        for request in requests:
            rid = request.get('id')
            print rid
    sys.exit(0)

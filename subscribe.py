#!/usr/bin/python

import re
import urllib
import urllib2
import json
import sys

if __name__ == '__main__':
    # node = UNL
    # data = some small set not at UNL
    # 
    dataset = '/Pyquen_WToMuNu_TuneZ2_5023GeV_pythia6/HiWinter13-pa_STARTHI53_V25-v2/GEN-SIM-RECO'
    phedex_call = "http://cmsweb.cern.ch/phedex/datasvc/xml/prod/data?dataset=" + dataset
    try:
        data_response = urllib2.urlopen(phedex_call)
    except:
        print "Failed phedex call"
        sys.exit()
        #data_response = urllib2.urlopen(phedex_call)
    
    node = 'T2_US_Nebraska'
    xml_data = data_response.read()
    level = 'dataset'
    priority = 'low'
    move = 'n'
    static = 'n'
    custodial = 'n'
    request_only = 'n'
    values = { 'node=' : node, 'data' : xml_data, 'level' : level,
               'priority' : priority, 'move' : move, 'static' : static,
               'custodial' : custodial, 'request_only' : request_only }
    data = urllib.urlencode(values)
    url = 'http://cmsweb.cern.ch/phedex/datasvc/xml/prod/subscription'
    request = urllib2.Request(url, data)
#    try:
    sub_response = urllib2.urlopen(request)
    #except:
    #    print "Failed subscription call"
    #    sys.exit()

    sub_status = sub_response.read()
    print sub_status

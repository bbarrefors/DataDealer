#!/usr/bin/python

import re
import urllib2
import json

if __name__ == '__main__':
    # node = UNL
    # data = some small set not at UNL
    # 
    node = "T2_US_UNL"
    dataset = "/Pyquen_WToMuNu_TuneZ2_5023GeV_pythia6/HiWinter13-pa_STARTHI53_V25-v2/GEN-SIM-RECO"
    phedex_call = "http://cmsweb.cern.ch/phedex/datasvc/xml/prod/data?dataset=" + dataset
    try:
        data = urllib2.urlopen(phedex_call)
    except:
        # retry?
    subscription = "http://cmsweb.cern.ch/phedex/datasvc/xml/prod/subscription?node=" + node + "data=" + data
    response = urllib2.urlopen(subscription)
    print response

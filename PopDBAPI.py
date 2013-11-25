#!/usr/bin/env python
"""
_PopDBAPI_

Make requests to the CMS Data Popularity API.

Created by Bjorn Barrefors on 4/11/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
################################################################################
#                                                                              #
#                             P o p   D B   A P I                              #
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
from subprocess import call, Popen, PIPE

from CMSDATALogger import log, error

POP_DB_BASE = "https://cms-popularity.cern.ch/popdb/popularity/"
SITE = "T2_US_Nebraska"
CERT = "/grid_home/cmsphedex/gridcert/myCert.pem"
KEY = "/grid_home/cmsphedex/gridcert/myCert.key"
COOKIE = "/grid_home/cmsphedex/gridcert/ssocookie.txt"

def renewSSOCookie():
    call(["cern-get-sso-cookie", "--cert", CERT, "--key", KEY, "-u", POP_DB_BASE, "-o", COOKIE])

def PopDBCall(url, values):
    """
    _PopDBCall_

    cURL PopDB API call.
    """
    name = "PopDBAPICall"
    data = urllib.urlencode(values)
    request = urllib2.Request(url, data)
    full_url = request.get_full_url() + request.get_data()
    p1 = Popen(["curl", "-k", "-L", "--cookie", COOKIE, "--cookie-jar", COOKIE, full_url], stdout=PIPE)
    response = p1.communicate()[0]
    try:
        json_data = json.loads(response)
    except ValueError:
        #error(name, response)
        print "ERROR"
        return 0
    return json_data

def DSStatInTimeWindow(start, stop, site):
    tstart = start
    tstop = stop
    sitename = site
    values = { 'tstart' : tstart, 'tstop' : tstop,
               'sitename' : sitename }
    dsstat_url = urllib.basejoin(POP_DB_BASE, "%s/?&" % ("DSStatInTimeWindow",))
    response = PopDBCall(dsstat_url, values)
    nacc = 0
    cpuh = 0
    if response:
        data = response.get('DATA')
        for dset in data:
            nacc += int(dset.get('NACC'))
            cpuh += int(dset.get('TOTCPU'))
    return nacc, cpuh

def getDSdata(start, stop, orderby):
    tstart = start
    tstop = stop
    aggr = "year"
    values = { 'tstart' : tstart, 'tstop' : tstop,
               'aggr' : aggr, 'orderby' : orderby }
    dsstat_url = urllib.basejoin(POP_DB_BASE, "%s/?&" % ("getDSdata",))
    response = PopDBCall(dsstat_url, values)
    nacc = 0
    cpuh = 0
    sets = []
    if response:
        data = response.get('data')
        for dset in data:
            sets.append(dset.get('name'))
    return sets

if __name__ == '__main__':
    """
    __main__

    For testing purpose only.
    """
    renewSSOCookie()
    sets = getDSdata("2013-11-23", "2013-11-25", "totcpu")
    print sets
    sys.exit(1)

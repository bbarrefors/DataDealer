#!/usr/bin/python -B

"""
_PopDBAPI_

Created by Bjorn Barrefors on 4/11/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
__author__       = 'Bjorn Barrefors'
__organization__ = 'Holland Computing Center - University of Nebraska-Lincoln'
__email__        = 'bbarrefo@cse.unl.edu'

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


################################################################################
#                                                                              #
#                             P o p   D B   A P I                              #
#                                                                              #
################################################################################

class PopDBAPI():
    """
    _PopDBAPI_

    Interface to submit queries to the Popularity DB API

    Class variables:
    POP_DB_BASE -- Base URL to the Popularity web API
    logger      -- Used to print log and error messages to log file
    CERT        -- Path to .pem file
    KEY         -- Path to .key file
    COOKIE      -- Path to sso cookie file
    """
    # Useful variables
    # POP_DB_BASE = "https://cms-popularity.cern.ch/popdb/popularity/"
    # SITE = "T2_US_Nebraska"
    # CERT = "/grid_home/cmsphedex/gridcert/myCert.pem"
    # KEY = "/grid_home/cmsphedex/gridcert/myCert.key"
    # COOKIE = "/grid_home/cmsphedex/gridcert/ssocookie.txt"
    def __init__(self):
        """
        __init__
        
        Set up class constants
        """
        self.logger      = CMSDATALogger()
        self.PHEDEX_BASE = "https://cms-popularity.cern.ch/popdb/popularity/"
        self.CERT        = "/grid_home/cmsphedex/gridcert/myCert.pem"
        self.KEY         = "/grid_home/cmsphedex/gridcert/myCert.key"
        self.COOKIE      = "/grid_home/cmsphedex/gridcert/ssocookie.txt"
        

    def renewSSOCookie(self):
        """
        _renewSSOCookie_
        
        Renew the SSO Cookie used for accessing popularity db
        """
        call(["cern-get-sso-cookie", "--cert", self.CERT, "--key", self.KEY, "-u", self.POP_DB_BASE, "-o", self.COOKIE])

    def PopDBCall(self, url, values):
        """
        _PopDBCall_
        
        cURL PopDB API call.
        """
        name = "PopDBAPICall"
        data = urllib.urlencode(values)
        request = urllib2.Request(url, data)
        full_url = request.get_full_url() + request.get_data()
        p1 = Popen(["curl", "-k", "-L", "--cookie", COOKIE, "--cookie-jar", COOKIE, full_url], stdout=PIPE)
        try:
            response = p1.communicate()[0]
        except ValueError:
            return 1, "Error"
        return 0, reponse


    def DSStatInTimeWindow(self, start, stop, site):
        tstart = start
        tstop = stop
        sitename = site
        values = { 'tstart' : tstart, 'tstop' : tstop,
                   'sitename' : sitename }
        dsstat_url = urllib.basejoin(POP_DB_BASE, "%s/?&" % ("DSStatInTimeWindow",))
        check, response = PopDBCall(dsstat_url, values)
        
        nacc = 0
        cpuh = 0
        if response:
            data = response.get('DATA')
            for dset in data:
                nacc += int(dset.get('NACC'))
                cpuh += int(dset.get('TOTCPU'))
        return nacc, cpuh

    def getDSdata(self, start, stop, orderby, n):
        tstart = start
        tstop = stop
        aggr = "year"
        values = { 'tstart' : tstart, 'tstop' : tstop, 'n' : n,
                   'aggr' : aggr, 'orderby' : orderby }
        dsstat_url = urllib.basejoin(POP_DB_BASE, "%s/?&" % ("getDSdata",))
        response = PopDBCall(dsstat_url, values)
        nacc = 0
        cpuh = 0
        sets = []
        if response:
            data = response.get('data')
            print data
            for dset in data:
                sets.append(dset.get('name'))
        return sets

if __name__ == '__main__':
    """
    __main__

    For testing purpose only.
    """
    #renewSSOCookie()
    today = datetime.date.today()
    tstart = today - datetime.timedelta(days=7)
    tstop = today
    sets = getDSdata(tstart, tstop, "totcpu", "5")
    for s in sets:
        access, totcpu = DSStatInTimeWindow(tstart, tstop, "summary")
        print ("%s have %d accesses and %d CPU hours from %s to %s" % (s, int(access), int(totcpu), str(tstart), str(tstop)))
        for i in range(7):
            tstart = tstart - datetime.timedelta(days=1)
            tstop = tstart
            access, totcpu = DSStatInTimeWindow(tstart, tstop, "summary")
            print ("%s have %d accesses and %d CPU hours during %s" % (s, int(access), int(totcpu), str(tstart)))
    print sets
    sys.exit(1)

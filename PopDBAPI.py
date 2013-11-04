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

from CMSDATALogger import log, error

POP_DB_BASE = "https://cms-popularity.cern.ch/popdb/popularity/"
SITE = "T2_US_Nebraska"

if __name__ == '__main__':
    """
    __main__

    For testing purpose only.
    """
    sys.exit(1)

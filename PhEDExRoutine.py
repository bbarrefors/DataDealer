#!/usr/bin/env python26

"""
_PhEDExRoutine_

Keep database up to date and analyze it to make subscribtion and deletion
decisions.

Created by Bjorn Barrefors on 23/9/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
################################################################################
#                                                                              #
#                        P h E D E x   R O U T I N E                           #
#                                                                              #
################################################################################

import sys
import os

from PhEDExDatabase import delete, setAccess, ignore
from PhEDExAPI import datasetSize
from PhEDExLogger import log, error

################################################################################
#                                                                              #
#                            S I T E   S P A C E                               #
#                                                                              #
################################################################################

def siteSpace():
    """
    _siteSpace_

    Find the available space of current site, UNL. Available space means
    space that can be utilized without filling storage over 90%.
    No site is passed at the moment as this is only implemented at UNL.
    For the future we would like to make this generic for any site.
    Return space in GB.
    """
    name = "RoutineSiteSpace"
    info = os.statvfs("/mnt/hadoop")
    total = (info.f_blocks * info.f_bsize) / (1024**3)
    free = (info.f_bfree * info.f_bsize) / (1024**3)
    minimum_free = total*(0.1)
    available_space = free - minimum_free
    log(name, "Total of %dGB available for dataset transfers on phedex" % (available_space,))
    print available_space
    return int(available_space)

################################################################################
#                                                                              #
#                             J A N I T O R                                    #
#                                                                              #
################################################################################

def janitor():
    """
    _janitor_

    Delete entries in database that are expired.
    Update SetAccess based on deletions.
    TODO : Delete sets which was subscribed more than a set time period ago.
    """
    name = "RoutineJanitor"
    log(name, "Updating database")
    delete()
    #deleteSubscriptions()

################################################################################
#                                                                              #
#                              A N A L Y Z E                                   #
#                                                                              #
################################################################################

def analyze():
    """
    _analyze_

    Find candidate datasets to subscribe.
    Need to make sure the budget is not exceeded.
    TODO : Check if set is already at site
    TODO : Check budget
    TODO : Free up more space
    """
    name = "RoutineAnalyze"
    log(name, "Analyze database for possible subscriptions")
    space = siteSpace()
    count = setAccess()
    for dataset in count:
        if (not ignore(dataset)):
            size = datasetSize(dataset)
            if (size):
                while (size > space):
                    # Add check for budgeting
                    subscribe("T2_US_Nebraska", dataset)
                #else:
                    # Can we delete some sets previously subscribed to free up space
                    # Look up oldest subscription
                    
    return 0

if __name__ == '__main__':
    """
    __main__

    For testing purpose only.
    """
    sys.exit(siteSpace())

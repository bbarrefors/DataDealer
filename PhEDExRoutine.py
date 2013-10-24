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

from PhEDExDatabase import clean, setAccess, setBudget, ignore, access, BUDGET
from PhEDExAPI import datasetSize, subscribe, delete, exists, subscriptions
from PhEDExLogger import log, error

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
    """
    name = "RoutineJanitor"
    #log(name, "Updating database")
    clean()
    return 0

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
#                              A N A L Y Z E                                   #
#                                                                              #
################################################################################

def analyze():
    """
    _analyze_

    Find candidate datasets to subscribe.
    Need to make sure the budget is not exceeded.
    """
    name = "RoutineAnalyze"
    #log(name, "Analyze database for possible subscriptions")
    space = siteSpace()
    subscribedSets = subscriptions("T2_US_Nebraska", 6*4*7)
    budget = 0
    for dset in subscribedSets:
        budget += datasetSize(dset)
    count = setAccess()
    for datas in count:
        dataset = datas[0]
        #log(name, "Possible subscription of %s" % (dataset,))
        if (ignore(dataset)):
            #log(name, "Dataset %s is in ignore" % (dataset,))
            continue
        if (exists("T2_US_Nebraska", dataset)):
            #log(name, "Dataset %s is already at %s" % (dataset, "T2_US_Nebraska"))
            continue
        size = datasetSize(dataset)
        #log(name, "Dataset %s size is %dGB" % (dataset, size))
        if (not size):
            continue
        if (size < space and size < BUDGET - budget):
            subscribe("T2_US_Nebraska", dataset)
            continue
        #log(name, "Trying to free up space")
        subSets = subscriptions("T2_US_Nebraska", 3)
        datasets = subscribedSets - subSets
        for del_dataset in datasets:
            if (size < space and size < BUDGET - budget):
                break
            if (not delete("T2_US_Nebraska", del_dataset)):
                new_size = datasetSize(del_dataset)
                budget -= new_size
                space += new_size
        else:
            continue
        if (not subscribe("T2_US_Nebraska", dataset)):
            budget += size
            space -= size
    return 0

################################################################################
#                                                                              #
#                              S U M M A R Y                                   #
#                                                                              #
################################################################################

def summary():
    """
    _summary_

    How much data does CMS DATA own at site?
    How much data have CMS DATA transferred to site in the last 24h?
    """
    name = "RoutineSummary"
    subscribedSets = subscriptions("T2_US_Nebraska", 1)
    transferredData = 0
    for dset in subscribedSets:
        transferredData += datasetSize(dset)
    subscribedSets = subscriptions("T2_US_Nebraska", 6*4*7)
    ownedData = 0
    for dset in subscribedSets:
        ownedData += datasetSize(dset)
    log(name, "CMS DATA owns a total of %dGB of data at site" % (ownedData,))
    log(name, "CMS DATA have subscribed a total of %dGB of data to site in the last 24h" % (transferredData,))
    subscribedSets = subscriptions("T2_US_Nebraska", 7)
    log(name, "Number of accesses during the last 3 days for the sets subscribed in the last week")
    for set in subscribedSets:
        accesses = access(set)
        log(name, "%s - %d" % (set, accesses))

if __name__ == '__main__':
    """
    __main__

    For testing purpose only.
    """
    sys.exit(janitor())

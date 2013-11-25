#!/usr/bin/env python
"""
_CMSDATARoutine_

Keep database up to date and analyze it to make subscribtion and deletion
decisions.

Created by Bjorn Barrefors on 23/9/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
################################################################################
#                                                                              #
#                       C M S D A T A   R O U T I N E                          #
#                                                                              #
################################################################################

import sys
import os
import datetime

from CMSDATADatabase import clean, setAccess, setBudget, ignore, access, BUDGET
from PhEDExAPI import datasetSize, subscribe, delete, exists, subscriptions, replicas
from CMSDATALogger import log, error
from PopDBAPI import renewSSOCookie, DSStatInTimeWindow

################################################################################
#                                                                              #
#                             J A N I T O R                                    #
#                                                                              #
################################################################################

def janitor():
    """
    _janitor_

    Delete entries in database that have expired.
    Update SetAccess based on deletions.
    """
    name = "RoutineJanitor"
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
    space = siteSpace()
    subscribedSets = subscriptions("T2_US_Nebraska", 6*4*7)
    subSets = set(subscribedSets)
    budget = 0
    for dset in subSets:
        budget += datasetSize(dset)
    count = setAccess()
    for datas in count:
        dataset = datas[0]
        if (ignore(dataset)):
            continue
        if (exists("T2_US_Nebraska", dataset)):
            continue
        size = datasetSize(dataset)
        if (not size):
            continue
        if (size < space and size < BUDGET - budget):
            #if (not subscribe("T2_US_Nebraska", dataset)):
            log(name, "Data set %s just subscribed have replicas at the following sites." % (dataset, ))
            sites = replicas(dset)
            today = datetime.date.today()
            tstart = today - datetime.timedelta(days=1)
            tstop = tstart
            for site in sites:
                accesses, cpu_hours = DSStatInTimeWindow(tstart, tstop, site)
                if accesses:
                    log(name, "%s have %d accesses and %d CPU hours during %s" % (site, int(accesses), int(cpu_hours), str(tstart)))
            continue
        #log(name, "Trying to free up space")
        subSets = subscriptions("T2_US_Nebraska", 3)
        datasets = set(subscribedSets) - set(subSets)
        for del_dataset in datasets:
            if (size < space and size < BUDGET - budget):
                break
            #if (not delete("T2_US_Nebraska", del_dataset)):
            log(name, "Delete data set %s to free up space." % (del_dataset,))
            new_size = datasetSize(del_dataset)
            budget -= new_size
            space += new_size
        else:
            continue
        #if (not subscribe("T2_US_Nebraska", dataset)):
        log(name, "Data set %s just subscribed have replicas at the following sites." % (dataset, ))
        sites = replicas(dset)
        today = datetime.date.today()
        tstart = today - datetime.timedelta(days=1)
        tstop = tstart
        for site in sites:
            accesses, cpu_hours = DSStatInTimeWindow(tstart, tstop, site)
            if accesses:
                log(name, "%s have %d accesses and %d CPU hours during %s" % (site, int(accesses), int(cpu_hours), str(tstart)))
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
    renewSSOCookie()
    subscribedSets = subscriptions("T2_US_Nebraska", 1)
    subSets = set(subscribedSets)
    transferredData = 0
    for dset in subSets:
        transferredData += datasetSize(dset)
    subscribedSets = subscriptions("T2_US_Nebraska", 6*4*7)
    subSets = set(subscribedSets)
    ownedData = 0
    today = datetime.date.today()
    tstart = today - datetime.timedelta(days=1)
    tstop = tstart
    for dset in subSets:
        ownedData += datasetSize(dset)
        log(name, "Data set %s have replicas at the following sites." % (dset, ))
        sites = replicas(dset)
        for site in sites:
            # Get the total number of accesss and CPU hours at site
            accesses, cpu_hours = DSStatInTimeWindow(tstart, tstop, site)
            if accesses:
                log(name, "%s have %d accesses and %d CPU hours during %s" % (site, int(accesses), int(cpu_hours), str(tstart)))
    log(name, "CMS DATA owns a total of %dGB of data at site" % (ownedData,))
    log(name, "CMS DATA have subscribed a total of %dGB of data to site in the last 24h" % (transferredData,))
    subscribedSets = subscriptions("T2_US_Nebraska", 7)
    subSets = set(subscribedSets)
    log(name, "Number of accesses during the last 3 days for the sets subscribed in the last week")
    for subSet in subSets:
        accesses = access(subSet)
        log(name, "%s - %d" % (subSet, int(accesses)))

if __name__ == '__main__':
    """
    __main__

    For testing purpose only.
    """
    sys.exit(janitor())

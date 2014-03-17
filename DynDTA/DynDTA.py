#!/usr/bin/python -B

"""
_DynDTA_

Created by Bjorn Barrefors & Brian Bockelman on 14/3/2014
for Dynamic Data Transfer Agent

Holland Computing Center - University of Nebraska-Lincoln
"""
__author__       = 'Bjorn Barrefors'
__organization__ = 'Holland Computing Center - University of Nebraska-Lincoln'
__email__        = 'bbarrefo@cse.unl.edu'

import sys
import time
import datetime
import math

from DynDTALogger import DynDTALogger
from PhEDExAPI import PhEDExAPI
from PopDBAPI import PopDBAPI


    ############################################################################
    #                                                                          #
    #                        D A T A S E T   S I Z E                           #
    #                                                                          #
    ############################################################################

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


    ############################################################################
    #                                                                          #
    #                              R E P L I C A S                             #
    #                                                                          #
    ############################################################################

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
#                                  A G E N T                                   #
#                                                                              #
################################################################################

class DynDTA:
    """
    _DynDTA_

    Run a daily agent which ranks sets based on popularity. Selection process is
    done in a weighted random selection based on the ranking.

    Class variables:
    pop_db_api -- Used to make all popularity db calls
    phedex_api -- Used to make all phedex calls
    """
    def __init__(self):
        """
        __init__

        Set up class constants
        """
        self.pop_db_api = PopDBAPI()
        self.phedex_api = PhEDExAPI()
        self.time_window = 3


    ############################################################################
    #                                                                          #
    #                                A G E N T                                 #
    #                                                                          #
    ############################################################################

    def agent(self):
        """
        _agent_

        The daily agent routine.
        """
        while(True):
            # Renew SSO Cookie for Popularity DB calls
            self.pop_db_api.renewSSOCookie()
            # Restart daily budget in TB
            budget = 30.0
            # Find candidates. Top 200 accessed sets
            check, candidates = self.candidates()
            if check:
                continue
            # @TODO : Get ganking data. n_access | n_replicas | size_TB
            for dataset in candidates:
                n_access_t = nAccess(dataset, self.time_window)
                n_access_2t = nAccess(dataset, self.time_window*2)
                n_replicas = nReplicas(dataset)
                size_TB = size(dataset)
                rank = (math.log10(n_access_t)*max(2*n_access_t - n_access_2t, 1))/(size_TB*(n_replicas**2)
                dataset[1] = rank
            # @TODO : Do weighted random
            # @TODO : Keep track of daily budget
            # @TODO : Subscribe set
            # @TODO : Subscribe on block level
            time.sleep(86400)


    ############################################################################
    #                                                                          #
    #                            C A N D I D A T E S                           #
    #                                                                          #
    ############################################################################

    def candidates(self, n='200'):
        tstop = datetime.date.today()
        tstart = tstop - datetime.timedelta(days=self.time_window)
        check, data = self.pop_db_api.getDSdata(tstart=tstart, tstop=tstop, aggr='week', n=n, orderby='naccess')
        if check:
            return check, data
        datasets = []
        for dataset in data:
            datasets.append([dataset.get('name'), 1])
        return check, datasets


    ############################################################################
    #                                                                          #
    #                              N A C C E S S                               #
    #                                                                          #
    ############################################################################

    def nAccess(self, dataset='', time_frame=''):
        tstop = datetime.date.today()
        tstart = tstop - datetime.timedelta(days=time_frame)
        check, data = self.pop_db_api.getDSdata(tstart=tstart, tstop=tstop, aggr='week', n=n, orderby='naccess')
        if check:
            return check, data
        datasets = []
        for dataset in data:
            datasets.append([dataset.get('name'), 1])
        return check, datasets



################################################################################
#                                                                              #
#                                  M A I N                                     #
#                                                                              #
################################################################################

if __name__ == '__main__':
    """
    __main__

    This is where is all starts
    """
    agent = DynDTA()
    check, response = agent.candidates()
    if check:
        sys.exit(1)
    else:
        print response
    sys.exit(0)

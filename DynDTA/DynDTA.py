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
import iteritems

from DynDTALogger import DynDTALogger
from PhEDExAPI import PhEDExAPI
from PopDBAPI import PopDBAPI


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
        sites = ["T2_US_Nebraska", "T2_US_MIT", "T2_DE_RWTH"]
        site = 0
        while(True):
            # Renew SSO Cookie for Popularity DB calls
            self.pop_db_api.renewSSOCookie()
            # Restart daily budget in TB
            budget = 30.0
            # Find candidates. Top 200 accessed sets
            check, candidates = self.candidates()
            if check:
                continue
            # Get ganking data. n_access | n_replicas | size_TB
            tstop = datetime.date.today()
            tstart = tstop - datetime.timedelta(days=(2*self.time_window))
            check, t2_data = self.pop_db_api.getDSStatInTimeWindow(tstart=tstart, tstop=tstop)
            access = {}
            for dataset in data:
                if dataset.get('COLLNAME') in candidates:
                    accesses[dataset.get('COLLNAME')] = dataset.get('NACC')
            datasets = []
            n_access_t = 1
            n_access_2t = 1
            n_replicas = 1
            size_TB = 1
            for dataset, access in candidates.iteritems():
                n_access_t = access
                n_access_2t = accesses[dataset]
                n_replicas = nReplicas(dataset)
                size_TB = size(dataset)
                rank = (math.log10(n_access_t)*max(2*n_access_t - n_access_2t, 1))/(size_TB*(n_replicas**2)
                datasets.append((dataset, rank))
            # Do weighted random selection
            subscriptions = []
            while budget > 0:
                dataset = weightedChoice(datasets)
                print size_TB
                if size_TB > budget:
                    break
                subscriptions.append(dataset)
                # Keep track of daily budget
                budget -= size_TB
            # Subscribe sets
            print subscriptions
            #data = self.phedex_api.xmlData(datasets=subscriptions)
            #check, response = self.phedex_api.subscribe(node=sites[site], data=data, comments='Dynamic data transfer --JUST A TEST--')
            # @TODO : Subscribe on block level
            # Rotate through sites
            site = (site + 1) % 3
            time.sleep(86400)


    ############################################################################
    #                                                                          #
    #                            C A N D I D A T E S                           #
    #                                                                          #
    ############################################################################

    def candidates(self):
        tstop = datetime.date.today()
        tstart = tstop - datetime.timedelta(days=self.time_window)
        check, data = self.pop_db_api.getDSStatInTimeWindow(tstart=tstart, tstop=tstop)
        if check:
            return check, data
        datasets = {}
        i = 0
        for dataset in data:
            if i == 300:
                break
            datasets[dataset.get('COLLNAME')] = datasets.get('NACC')
            i += 1
        return check, datasets


    ############################################################################
    #                                                                          #
    #                            N   R E P L I C A S                           #
    #                                                                          #
    ############################################################################

    def nReplicas(dataset):
        """
        _nReplicas_

        Set up blockreplicas call to PhEDEx API.
        """
        check, response = self.phedex_api.blockReplicas(dataset=dataset, node="T2_US_Nebraska")
        if check:
            return 100
        block = response.get('block')
        replicas = block[0].get('replica')
        n_replicas = len(replicas)
        return n_replicas

    ############################################################################
    #                                                                          #
    #                        D A T A S E T   S I Z E                           #
    #                                                                          #
    ############################################################################

    def size(dataset):
        """
    _datasetSize_

    Get total size of dataset in TB.
    """
    check, response = self.phedex_api.data(dataset=dataset)
    if not response:
        return 1000
    data = reponse.get('dataset')[0].get('block')
    size = float(0)
    for block in data:
        size += block.get('bytes')

    size = size / 10**12
    return size


    ############################################################################
    #                                                                          #
    #                        D A T A S E T   S I Z E                           #
    #                                                                          #
    ############################################################################

    def weightedChoice(choices):
        """
        _weightedChoice_

        Return a weighted randomly selected dataset
        """
        total = sum(w for c, w in choices)
        r = random.uniform(0, total)
        upto = 0
        for c, w in choices:
            if upto + w > r:
                return c
            upto += w


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

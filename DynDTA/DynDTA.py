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
import random

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
        self.logger = DynDTALogger()
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
            if check:
                continue
            accesses = {}
            for dataset in t2_data:
                if dataset.get('COLLNAME') in candidates:
                    accesses[dataset.get('COLLNAME')] = dataset.get('NACC')
            datasets = []
            n_access_t = 1
            n_access_2t = 1
            n_replicas = 1
            size_TB = 1
            for dataset, access in candidates.iteritems():
                n_access_t = access
                try:
                    n_access_2t = accesses[dataset]
                except KeyError, e:
                    n_access_2t = n_access_t
                n_replicas = self.nReplicas(dataset)
                size_TB = self.size(dataset)
                rank = (math.log10(n_access_t)*max(2*n_access_t - n_access_2t, 1))/(size_TB*(n_replicas**2))
                datasets.append((dataset, rank))
            # Do weighted random selection
            subscriptions = [[], [], []]
            dataset_block = ''
            budget = 30
            selected_sets = []
            surrent_site = site
            while budget > 0:
                dataset = self.weightedChoice(datasets)
                # Check if set was already selected
                if dataset in selected_sets:
                    continue
                selected_sets.append(dataset)
                size_TB = self.size(dataset)
                if size_TB == 1000:
                    continue
                # Check if set already exists at site(s)
                i = 0
                current_site = site
                while i < 3:
                    if not (self.replicas(dataset, sites[current_site])):
                        break
                    i += 1
                    current_site = (current_site + 1) % 3
                else:
                    continue
                if (size_TB > budget):
                    dataset_block = dataset
                    break
                subscriptions[current_site].append(dataset)
                # Keep track of daily budget
                self.logger.log("Agent", "A set of size %s selected" % (size_TB,))
                budget -= size_TB
            # Get blocks to subscribe
            subscriptions = blockSubscription(dataset_block, budget, subscriptions, current_site)
            # Subscribe sets
            i = 0
            for sets in subscriptions:
                if not sets:
                    i += 1
                    continue
                check, data = self.phedex_api.xmlData(datasets=sets)
                if check:
                    i += 1
                    continue
                check, response = self.phedex_api.subscribe(node=sites[i], data=data, request_only='y', comments='Dynamic Data Transfer Agent')
                if check:
                    continue
                #self.logger.log("Agent", "The following subscription was made: " + str(response.read()))
                i += 1
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
        datasets = dict()
        i = 0
        for dataset in data:
            if i == 50:
                break
            if dataset['COLLNAME'] == 'unknown':
                continue
            elif (dataset['COLLNAME'].find("/USER") != -1):
                continue
            datasets[dataset['COLLNAME']] = dataset['NACC']
            i += 1
        return 0, datasets


    ############################################################################
    #                                                                          #
    #                            N   R E P L I C A S                           #
    #                                                                          #
    ############################################################################

    def nReplicas(self, dataset):
        """
        _nReplicas_

        Set up blockreplicas call to PhEDEx API.
        """
        # Don't even bother querying phedex if it is a user dataset
        if (dataset.find("/USER") != -1):
            return 100
        check, response = self.phedex_api.blockReplicas(dataset=dataset)
        if check:
            return 100
        data = response.get('phedex')
        block = data.get('block')
        try:
            replicas = block[0].get('replica')
        except IndexError, e:
            return 100
        n_replicas = len(replicas)
        return n_replicas


    ############################################################################
    #                                                                          #
    #                        D A T A S E T   S I Z E                           #
    #                                                                          #
    ############################################################################

    def size(self, dataset):
        """
        _datasetSize_

        Get total size of dataset in TB.
        """
        # Don't even bother querying phedex if it is a user dataset
        if (dataset.find("/USER") != -1):
            return 1000
        check, response = self.phedex_api.data(dataset=dataset)
        if check:
            return 1000
        try:
            data = response.get('phedex').get('dbs')[0]
            data = data.get('dataset')[0].get('block')
        except IndexError, e:
            return 1000
        size = float(0)
        for block in data:
            size += block.get('bytes')
        size = size / 10**12
        return size


    ############################################################################
    #                                                                          #
    #                      W E I G H T E D   C H O I C E                       #
    #                                                                          #
    ############################################################################

    def weightedChoice(self, choices):
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

    ############################################################################
    #                                                                          #
    #                            R E P L I C A S                              #
    #                                                                          #
    ############################################################################

    def replicas(self, dataset, node):
        """
        _replicas_

        Return the sites at which dataset have replicas.
        """
        # Don't even bother querying phedex if it is a user dataset
        if (dataset.find("/USER") != -1):
            return True
        check, response = self.phedex_api.blockReplicas(dataset=dataset, node=node)
        if check:
            return True
        data = response.get('phedex')
        block = data.get('block')
        try:
            replicas = block[0].get('replica')
        except IndexError, e:
            return False
        return True


    ############################################################################
    #                                                                          #
    #                    B L O C K   S U B S C R I P T I O N                   #
    #                                                                          #
    ############################################################################

    def blockSubscription(self, dataset_block, budget, subscriptions, current_site):
        """
        _datasetSize_

        Get total size of dataset in TB.
        """
        # Don't even bother querying phedex if it is a user dataset
        if (dataset.find("/USER") != -1):
            return subscriptions
        check, response = self.phedex_api.data(dataset=dataset)
        if check:
            return subscriptions
        try:
            data = response.get('phedex').get('dbs')[0]
            data = data.get('dataset')[0].get('block')
        except IndexError, e:
            return subscriptions
        size = float(0)
        for block in data:
            size = block.get('bytes')
            size = size / 10**12
            if size > budget:
                break
            block_name = block.get('name')
            subscriptions.append(block_name)
            budget -= size
        return subscriptions


################################################################################
#                                                                              #
#                                  M A I N                                     #
#                                                                              #
################################################################################

if __name__ == '__main__':
    """
    __main__

    This is where it all starts
    """
    agent = DynDTA()
    agent.agent()
    sys.exit(0)

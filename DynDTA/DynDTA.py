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

from operator import itemgetter

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
        # Renew SSO Cookie for Popularity DB calls
        self.pop_db_api.renewSSOCookie()
        # Rank sites based on current available space
        sites = ["T2_US_Nebraska", "T2_US_MIT", "T2_DE_RWTH"]
        site_rank = siteRanking(sites)
        # Restart daily budget in TB
        budget = 30.0
        # Find candidates. Top 200 accessed sets
        check, candidates = self.candidates()
        if check:
            return 1
        # Get ranking data. n_access | n_replicas | size_TB
        tstop = datetime.date.today()
        tstart = tstop - datetime.timedelta(days=(2*self.time_window))
        check, t2_data = self.pop_db_api.getDSStatInTimeWindow(tstart=tstart, tstop=tstop)
        if check:
            return 1
        accesses = {}
        for dataset in t2_data:
            if dataset.get('COLLNAME') in candidates:
                accesses[dataset.get('COLLNAME')] = dataset.get('NACC')
        datasets = []
        n_access_t = 1
        n_access_2t = 1
        n_replicas = 1
        size_TB = 1
        #printing = []
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
            #printing.append((rank, n_access_t, n_replicas, dataset))
        # Do weighted random selection
        subscriptions = dict()
        for sit in sites:
            subscriptions[sit] = []
        datasets = sorted(datasets, key=itemgetter(1))
        datasets.reverse()
        #printing = sorted(printing, key=itemgetter(0))
        #printing.reverse()
        #print("%s \t %s \t %s \t %s" % ("Rank", "Acc", "Replicas", "Dataset"))
        #y = 0
        #for sets in printing:
        #    if (sets[3] == "/QCD_Pt_170_250_EMEnriched_TuneZ2star_8TeV_pythia6/Summer12_DR53X-PU_S10_START53_V7A-v1/AODSIM") or (sets[3] == "/TT_CT10_TuneZ2star_8TeV-powheg-tauola/Summer12_DR53X-PU_S10_START53_V7A-v2/AODSIM") or (sets[3] == "/MuOniaParked/Run2012C-22Jan2013-v1/AOD"):
        #        print("%.3f \t %d \t %d \t %s" % (sets[0], int(sets[1]), int(sets[2]), sets[3]))
        #    y += 1
        #return 0
        dataset_block = ''
        budget = 30
        selected_sets = []
        block_site = ''
        while budget > 0:
            dataset = self.weightedChoice(datasets)
            # Check if set was already selected
            if dataset in selected_sets:
                continue
            selected_sets.append(dataset)
            size_TB = self.size(dataset)
            if size_TB == 1000:
                continue
            # Check if set was deleted from any of the sites in the last 2 weeks
            if deleted(dataset, sites):
                continue
            # Select site
            # First remove sites which already have dataset
            site_remove = unavailableSites(dataset, site_rank)
            available_sites = site_rank - site_remove
            if not available_sites:
                continue
            selected_site = self.weightedChoice(available_sites)
            if (size_TB > budget):
                dataset_block = dataset
                block_site = selected_site
                break
            subscriptions[selected_site].append(dataset)
            # Keep track of daily budget
            #self.logger.log("Agent", "A set of size %s selected" % (size_TB,))
            budget -= size_TB
        # Get blocks to subscribe
        subscriptions = self.blockSubscription(dataset_block, budget, subscriptions, block_site)
        # Subscribe sets
        i = 0
        for sit, sets in subscriptions:
            if not sets:
                continue
            check, data = self.phedex_api.xmlData(datasets=sets)
            if check:
                continue
            check, response = self.phedex_api.subscribe(node=sit, data=data, request_only='y', comments='Dynamic Data Transfer Agent')
        return 0


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
            if i == 200:
                break
            if dataset['COLLNAME'] == 'unknown':
                continue
            elif (dataset['COLLNAME'].find("/USER") != -1):
                continue
            elif (dataset['COLLNAME'].find("/AOD") == -1):
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

    def blockSubscription(self, dataset_block, budget, subscriptions, selected_site):
        """
        _blockSubscription_

        Add blocks to subscriptions.
        """
        # Don't even bother querying phedex if it is a user dataset
        if (dataset_block.find("/USER") != -1):
            return subscriptions
        check, response = self.phedex_api.data(dataset=dataset_block)
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
            subscriptions[selected_site].append(block_name)
            budget -= size
        return subscriptions


    ############################################################################
    #                                                                          #
    #                              D E L E T E D                               #
    #                                                                          #
    ############################################################################

    def deleted(self, dataset, site):
        """
        _deleted_

        Check if dataset was deleted from any of the sites by AnalysisOps in the
        last 2 weeks.
        """
        check, response = self.phedex_api.deletions(node=site, dataset=dataset, request_since='last_30days')
        if check:
            return False
        try:
            data = response.get('phedex').get('dataset')[0]
        except IndexError, e:
            return False
        return True



    ############################################################################
    #                                                                          #
    #                        S I T E   R A N K I N G                           #
    #                                                                          #
    ############################################################################

    def siteRanking(self, sites):
        """
        _deleted_

        Check if dataset was deleted from any of the sites by AnalysisOps in the
        last 2 weeks.
        """
        site_rank = []
        for site in sites:
            check, response = self.phedex_api.blockReplicas(node=site, group="AnalysisOps")
            if check:
                site_rank.append((site, 0))
            blocks = response.get('phedex').get('block')
            used_space = float(0)
            for block in blocks:
                bytes = block.get('bytes')
                used_space += bytes
            used_space = used_space / 10**12
            site_rank.append((site, 250 - used_space))
        return site_rank





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
    sites = ["T2_US_Nebraska", "T2_US_MIT", "T2_DE_RWTH"]
    ranking = agent.siteRanking(sites)
    sys.exit(0)

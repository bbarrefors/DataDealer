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

    def agent(self, test):
        """
        _agent_

        The daily agent routine.
        """
        # Renew SSO Cookie for Popularity DB calls
        self.pop_db_api.renewSSOCookie()
        # Rank sites based on current available space
        sites = ["T2_US_Nebraska", "T2_US_MIT", "T2_DE_RWTH", "T2_ES_CIEMAT", "T2_US_Wisconsin", "T2_US_Florida", "T2_US_Caltech"]
        site_rank, max_budget = self.siteRanking(sites)
        # Restart daily budget in TB
        budget = min(30.0, max_budget)
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
        datasets = dict()
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
            datasets[dataset] = rank
        # Do weighted random selection
        sorted_ranking = sorted(datasets.iteritems(), key=itemgetter(1))
        for rank in sorted_ranking:
            self.logger.log("Ranking", str(rank[1]) + "\t" + str(rank[0]))
            if rank[1] < 100:
                del datasets[rank[0]]
        subscriptions = dict()
        for site in sites:
            subscriptions[site] = []
        dataset_block = ''
        block_site = ''
        while ((budget > 0) and (datasets)):
            dataset = self.weightedChoice(datasets)
            # Check if set was already selected
            del datasets[dataset]
            size_TB = self.size(dataset)
            if size_TB == 1000:
                continue
            # Check if set was deleted from any of the sites in the last 2 weeks
            if self.deleted(dataset, sites):
                continue
            # Select site
            # First remove sites which already have dataset
            available_sites = dict()
            site_remove = self.unavailableSites(dataset, site_rank)
            for k, v in site_rank.iteritems():
                if k in site_remove:
                    continue
                available_sites[k] = v
            if not available_sites:
                continue
            selected_site = self.weightedChoice(available_sites)
            if (size_TB > budget):
                subscriptions[selected_site].append(dataset)
                break
            subscriptions[selected_site].append(dataset)
            # Update the ranking
            site_rank[selected_site] = site_rank[selected_site] - size_TB
            # Keep track of daily budget
            budget -= size_TB
        # Get blocks to subscribe
        # Subscribe sets
        i = 0
        for site, sets in subscriptions.iteritems():
            if not sets:
                continue
            check, data = self.phedex_api.xmlData(datasets=sets)
            if check:
                continue
            for dataset in sets:
                if not test:
                    self.logger.log("Subscription", str(site) + " : " + str(dataset))
            if not test:
                check, response = self.phedex_api.subscribe(node=site, data=data, request_only='y', comments='Dynamic Data Transfer Agent')
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

        Return a weighted randomly selected object.
        Expects a dictionary.
        """
        total = sum(w for c, w in choices.iteritems())
        r = random.uniform(0, total)
        upto = 0
        for c, w in choices.iteritems():
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

    def deleted(self, dataset, sites):
        """
        _deleted_

        Check if dataset was deleted from any of the sites by AnalysisOps in the
        last 2 weeks.
        """
        for site in sites:
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
        _siteRanking_

        Rank the sites based on available storage
        """
        site_quotas = {"T2_US_Nebraska" : 450, "T2_US_MIT" : 250, "T2_DE_RWTH" : 200, "T2_ES_CIEMAT" : 200, "T2_US_Wisconsin" : 250, "T2_US_Florida" : 275, "T2_US_Caltech" : 250}
        site_rank = dict()
        max_budget = 0
        for site in sites:
            check, response = self.phedex_api.blockReplicas(node=site, group="AnalysisOps")
            if check:
                site_rank[site] = 0
            blocks = response.get('phedex').get('block')
            used_space = float(0)
            for block in blocks:
                replica = block.get('replica')
                if replica[0].get('subscribed') == 'y':
                  bytes = block.get('bytes')
                else:
                  bytes = replica[0].get('bytes')
                used_space += bytes
            used_space = used_space / 10**12
            #site_quota = siteQuota(site)
            site_quota = site_quotas[site]
            rank = (0.95*site_quota) - used_space
            if (rank >= 30):
              site_rank[site] = rank
              max_budget += rank
        return site_rank, max_budget


    ############################################################################
    #                                                                          #
    #                   U N A V A I L A B L E   S I T E S                      #
    #                                                                          #
    ############################################################################

    def unavailableSites(self, dataset, site_rank):
        """
        _unavailableSites_

        Find all of our sites which already has the dataset
        """
        unavailable_sites = dict()
        for site, rank in site_rank.iteritems():
            check, response = self.phedex_api.blockReplicas(dataset=dataset, node=site)
            if check:
                unavailable_sites[site] = rank
                continue
            data = response.get('phedex')
            block = data.get('block')
            try:
                replicas = block[0].get('replica')
            except IndexError, e:
                continue
            unavailable_sites[site] = rank
        return unavailable_sites


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
    test = 0
    if len(sys.argv) == 2:
        test = int(sys.argv[1])
    agent = DynDTA()
    sys.exit(agent.agent(test=test))

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

from CMSDATALogger import CMSDATALogger
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

def agent():
    """
    _agent_


    """


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

    sys.exit(agent())

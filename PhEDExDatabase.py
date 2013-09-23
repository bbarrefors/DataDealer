#!/usr/bin/env python26
"""
_PhEDEXDatabase_

Manage the database keeping track of which datasets have been accessed and when.

Created by Bjorn Barrefors on 22/9/2013

Holland Computing Center - University of Nebraska-Lincoln
"""
################################################################################
#                                                                              #
#                     P h E D E x   D A T A B A S E                            #
#                                                                              #
################################################################################

import os.path
import datetime
import sqlite3 as lite
from PhEDExLogger import log, error

#DB_PATH = '/home/bockelman/barrefors/'
DB_PATH = '/home/barrefors/scripts/python/'
DB_FILE = 'phedex.db'

################################################################################
#                                                                              #
#                                S E T U P                                     #
#                                                                              #
################################################################################

def setup():
    """
    _setup_

    Set up sqlite3 database and create tables if not already exist.
    Add dummy datasets to Ignore table.
    """
    name = "DatabaseSetup"
    if not os.path.exists(DB_PATH):
        error(name, "Database path " + DB_PATH + " does not exist")
        return 1
    connection = lite.connect(DB_PATH + DB_FILE)
    with connection:
        cur = connection.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS FileSet (File TEXT, Dataset TEXT, Expiration TIMESTAMP)')
        cur.execute('CREATE TABLE IF NOT EXISTS SetAccess (Dataset TEXT, Count INTEGER)')
        cur.execute('CREATE TABLE IF NOT EXISTS Unknown (File TEXT UNIQUE, Dataset TEXT, Expiration TIMESTAMP)')
        cur.execute('CREATE TABLE IF NOT EXISTS Ignore (Dataset TEXT UNIQUE)')
        dataset = "/GenericTTbar/SAM-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO"
        cur.execute('INSERT OR IGNORE INTO Ignore VALUES(?)', [dataset])
        dataset = "/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO"
        cur.execute('INSERT OR IGNORE INTO Ignore VALUES(?)', [dataset])

    connection.close()
    log(name, "Database initialized")
    return 0

#!/usr/bin/env python
"""
_PhEDEXDatabase_

Manage the database keeping track of which datasets have been accessed and when.

Created by Bjorn Barrefors on 22/9/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
################################################################################
#                                                                              #
#                     P h E D E x   D A T A B A S E                            #
#                                                                              #
################################################################################

import sys
import os.path
import datetime
import urllib
import sqlite3 as lite

from PhEDExLogger import log, error
from PhEDExAPI import findDataset

SET_ACCESS = 500
TIME_FRAME = 72
BUDGET = 100000
DB_PATH = '/grid_home/cmsphedex/'
#DB_PATH = '/home/bockelman/barrefors/cmsdata/'
#DB_PATH = '/home/barrefors/cmsdata/'
DB_FILE = 'cmsdata.db'

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
        error(name, "Database path %s does not exist" % DB_PATH)
        return 1
    connection = lite.connect(DB_PATH + DB_FILE)
    with connection:
        cur = connection.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS Access (Dataset TEXT, Expiration TIMESTAMP)')
        cur.execute('CREATE TABLE IF NOT EXISTS FileSet (File TEXT, Dataset TEXT)')
        cur.execute('CREATE TABLE IF NOT EXISTS SetAccess (Dataset TEXT, Count INTEGER)')
        cur.execute('CREATE TABLE IF NOT EXISTS Ignore (Dataset TEXT UNIQUE)')
        dataset = "/GenericTTbar/SAM-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO"
        cur.execute('INSERT OR IGNORE INTO Ignore VALUES(?)', [dataset])
        dataset = "/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO"
        cur.execute('INSERT OR IGNORE INTO Ignore VALUES(?)', [dataset])
        dataset = "UNKNOWN"
        cur.execute('INSERT OR IGNORE INTO Ignore VALUES(?)', [dataset])

    connection.close()
    #log(name, "Database initialized")
    return 0

################################################################################
#                                                                              #
#                                I N S E R T                                   #
#                                                                              #
################################################################################

def insert(dir, file_name):
    """
    _insert_
    
    Insert values to table FileSet and update SetAccess.
    If dataset can't be found add to Unknown.
    Check cache first.
    """
    name = "DatabaseInsert"
    if not os.path.exists(DB_PATH):
        error(name, "Database path %s does not exist" % DB_PATH)
        return 1
    connection = lite.connect(DB_PATH + DB_FILE)
    with connection:
        cur = connection.cursor()
        # Check if file is already in cache
        # Technically we only check directory since this will decrease the size of cache
        expiration = datetime.datetime.now() + datetime.timedelta(hours=TIME_FRAME)
        cur.execute("SELECT EXISTS(SELECT * FROM FileSet WHERE File=?)", [dir])
        test = cur.fetchone()[0]
        if int(test) == int(1):
            cur.execute('SELECT Dataset FROM FileSet WHERE File=?', [dir])
            dataset = cur.fetchone()[0]
        else:
            dataset = findDataset(file_name)
            cur.execute('INSERT INTO FileSet VALUES(?,?)', (dir, dataset))
        cur.execute('INSERT INTO Access VALUES(?,?)', (dataset, expiration))
        cur.execute("SELECT EXISTS(SELECT * FROM SetAccess WHERE Dataset=?)", [dataset])
        test = cur.fetchone()[0]
        if int(test) == int(1):
            cur.execute('UPDATE SetAccess SET Count=Count+1 WHERE Dataset=?', [dataset])
        else:
            cur.execute('INSERT INTO SetAccess VALUES(?,?)', (dataset, 1))
    connection.close()
    return 0

################################################################################
#                                                                              #
#                                D E L E T E                                   #
#                                                                              #
################################################################################

def clean():
    """
    _clean_
    
    Delete expired entries from the database.
    Update SetAccess.
    """
    name = "DatabaseClean"
    if not os.path.exists(DB_PATH):
        error(name, "Database path %s does not exist" % DB_PATH)
        return 1
    connection = lite.connect(DB_PATH + DB_FILE)
    with connection:
        cur = connection.cursor()
        cur.execute('SELECT Dataset FROM SetAccess')
        datasets = []
        while True:
            ds = cur.fetchone()
            if ds == None:
                break
            datasets.append(ds)
        for ds in datasets:
            del_count = 0;
            dataset = ds[0]
            cur.execute('DELETE FROM Access WHERE Expiration<? AND Dataset=?', (datetime.datetime.now(),dataset))
            del_count = cur.rowcount
            cur.execute('UPDATE SetAccess SET Count=Count-? WHERE Dataset=?',(del_count, dataset))
            
        minCount = 1
        cur.execute('DELETE FROM SetAccess WHERE Count<?', [minCount])
    connection.close()
    return 0

################################################################################
#                                                                              #
#                           S E T   A C C E S S                                #
#                                                                              #
################################################################################

def setAccess():
    """
    _setAccess_
    
    Get all sets accessed more than SET_ACCESS times during the TIME_FRAME.
    Sort from most to least accesses.
    """
    name = "DatabaseSetAccess"
    if not os.path.exists(DB_PATH):
        error(name, "Database path %s does not exist" % DB_PATH)
        return 1
    connection = lite.connect(DB_PATH + DB_FILE)
    with connection:
        cur = connection.cursor()
        cur.execute('SELECT Dataset FROM SetAccess WHERE Count>=? ORDER BY Count', [SET_ACCESS])
        datasets = []
        while True:
            ds = cur.fetchone()
            if ds == None:
                break
            datasets.append(ds)
        datasets.reverse()
    connection.close()
    return datasets

################################################################################
#                                                                              #
#                           S E T   A C C E S S                                #
#                                                                              #
################################################################################

def access(dataset):
    """
    _access_
    
    Get the number of accesses for the given set.
    """
    name = "DatabaseAccess"
    if not os.path.exists(DB_PATH):
        error(name, "Database path %s does not exist" % DB_PATH)
        return 1
    connection = lite.connect(DB_PATH + DB_FILE)
    with connection:
        cur = connection.cursor()
        cur.execute('SELECT Count FROM SetAccess WHERE Dataset=?', [dataset])
        ac = cur.fetchone()
        if ac == None:
            accesses = 0
        else:
            accesses = ac[0]
    connection.close()
    return int(accesses)

################################################################################
#                                                                              #
#                                I G N O R E                                   #
#                                                                              #
################################################################################

def ignore(dataset):
    """
    _ignore_
    
    Is the dataset on the ignore list?
    Return bool
    """
    name = "DatabaseIgnore"
    if not os.path.exists(DB_PATH):
        error(name, "Database path %s does not exist" % DB_PATH)
        return 1
    connection = lite.connect(DB_PATH + DB_FILE)
    with connection:
        cur = connection.cursor()
        cur.execute("SELECT EXISTS(SELECT * FROM Ignore WHERE Dataset=?)", [dataset])
        test = cur.fetchone()[0]
    connection.close()
    if int(test) == int(1):
        print "Set is in ignore"
        return True
    else:
        print "Set is not in ignore"
        return False

################################################################################
#                                                                              #
#                        S E T   S E T   A C C E S S                           #
#                                                                              #
################################################################################

def setSetAccess(set_access):
    """
    _setAccess_
    
    Set the SET_ACCESS variable.
    """
    name = "SetAccess"
    global SET_ACCESS
    SET_ACCESS = set_access

################################################################################
#                                                                              #
#                        S E T   T I M E   F R A M E                           #
#                                                                              #
################################################################################

def setTimeFrame(time_frame):
    """
    _timeFrame_
    
    Set the TIME_FRAME variable.
    """
    name = "TimeFrame"
    global TIME_FRAME
    TIME_FRAME = time_frame

################################################################################
#                                                                              #
#                           S E T    B U D G E T                               #
#                                                                              #
################################################################################

def setBudget(budget):
    """
    _budget_
    
    Set the BUDGET variable.
    """
    name = "Budget"
    global BUDGET
    BUDGET = budget

if __name__ == '__main__':
    """
    __main__

    For testing purpose only.
    """
    #sys.exit(setup())
    sys.exit(insert("/store/data/GowdyTest10/BTau/RAW/Run2010Av3/000/142/132/204BD8FD-2D09-E011-A254-00304879BAB2.root"))

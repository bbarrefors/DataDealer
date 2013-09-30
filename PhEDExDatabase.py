#!/usr/bin/env python26
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

import os.path
import datetime
import sqlite3 as lite

from PhEDExLogger import log, error
from PhEDExUDPListener import TIME_FRAME, SET_ACCESS
from PhEDExAPI import query

DB_PATH = '/home/bockelman/barrefors/'
#DB_PATH = '/home/barrefors/scripts/python/'
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
        error(name, "Database path %s does not exist" % DB_PATH)
        return 1
    connection = lite.connect(DB_PATH + DB_FILE)
    with connection:
        cur = connection.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS Access (Dataset TEXT, Expiration TIMESTAMP)')
        cur.execute('CREATE TABLE IF NOT EXISTS FileSet (File TEXT, Dataset TEXT, Expiration TIMESTAMP)')
        cur.execute('CREATE TABLE IF NOT EXISTS SetAccess (Dataset TEXT, Count INTEGER)')
        cur.execute('CREATE TABLE IF NOT EXISTS Ignore (Dataset TEXT UNIQUE)')
        dataset = "/GenericTTbar/SAM-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO"
        cur.execute('INSERT OR IGNORE INTO Ignore VALUES(?)', [dataset])
        dataset = "/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO"
        cur.execute('INSERT OR IGNORE INTO Ignore VALUES(?)', [dataset])
        dataset = "UNKNOWN"
        cur.execute('INSERT OR IGNORE INTO Ignore VALUES(?)', [dataset])

    connection.close()
    log(name, "Database initialized")
    return 0

################################################################################
#                                                                              #
#                                I N S E R T                                   #
#                                                                              #
################################################################################

def insert(file_name):
    """
    _insert_

    Insert values to table FileSet and update SetAccess.
    If dataset can't be found add to Unknown
    """
    name = "DatabaseInsert"
    if not os.path.exists(DB_PATH):
        error(name, "Database path %s does not exist" % DB_PATH)
        return 1
    connection = lite.connect(DB_PATH + DB_FILE)
    with connection:
        cur = connection.cursor()
        # Check if file is already in cache
        expiration = datetime.datetime.now() + datetime.timedelta(hours=TIME_FRAME)
        cur.execute("SELECT EXISTS(SELECT * FROM FileSet WHERE File=?)", [file_name])
        test = cur.fetchone()[0]
        if int(test) == int(1):
            cur.execute('SELECT Dataset FROM FileSet WHERE File=?', [file_name])
            cur.execute('UPDATE FileSet SET Expiration=? WHERE File=?', (file_name, expiration))
            dataset = cur.fetchone()[0]
        else:
            dataset = query("file", file_name)
            if json_data.get('phedex').get('dbs'):
                dataset = json_data.get('phedex').get('dbs')[0].get('dataset')[0].get('name')
            else:
                dataset = "UNKNOWN"
            cur.execute('INSERT INTO FileSet VALUES(?,?,?)', (file_name, dataset, expiration))
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

def delete():
    """
    _delete_
    
    Delete expired entries from the database.
    Update SetAccess.
    """
    name = "DatabaseDelete"
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
            
        cur.execute('DELETE FROM FileSet WHERE Expiration<?', [datetime.datetime.now()])
        minCount = 1
        cur.execute('DELETE FROM SetCount WHERE Count<?', [minCount])
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
        return True
    else:
        return False

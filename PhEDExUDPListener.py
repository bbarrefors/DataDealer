#!/usr/bin/env python26
"""
_PhEDExUDPListener_

Listen for UDP packets.
Packets contain information of files that have been accessed.
Store information in a local database and analyzed to
make decisions on when to subscribe a dataset.

Created by Bjorn Barrefors on 11/9/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
################################################################################
#                                                                              #
#                   P h E D E x   U D P   L I S T E N E R                      #
#                                                                              #
################################################################################

import sys
import socket
import ast
import re
import urllib2
import json
import time
import datetime
import traceback
from multiprocessing import Manager, Process, Pool

from PhEDExLogger import log, error, LOG_PATH, LOG_FILE
from PhEDExDatabase import setup, insert
from PhEDEXRoutine import janitor, analyzer
from PhEDExAPI import subscribe, delete

SET_ACCESS = 200
TIME_FRAME = 72

def subscribe(dataset, size, l):
    """
    _subscribe_
    
    
    """
    ID = "Subscribe"
    l.acquire()
    fs = open(LOG_PATH, 'a')
    fs.write(str(datetime.datetime.now()) + " " + str(ID) + ": Subscribe dataset " + str(dataset) + "\n")
    fs.close()
    l.release()
    con = lite.connect(SQLITE_PATH)
    with con:
        cur = con.cursor()
        cur.execute('INSERT OR IGNORE INTO DontMove VALUES(?)', [dataset])
        timestamp = datetime.datetime.now()
        delta = datetime.timedelta(hours=BUDGET_TIME_FRAME)
        expiration = timestamp + delta
        cur.execute('INSERT INTO Budget VALUES(?,?,?)', (dataset, int(size), expiration))
    con.close()
    return 1

def datasetSize(dataset, l):
    """
    _datasetSize_
    
    Accumulate all block sizes and calculate total size of dataset in GB.
    In case of error in PhEDEx call return 0.
    """
    ID = "DatasetSize"
    phedex_call = "http://cmsweb.cern.ch/phedex/datasvc/json/prod/data?dataset=" + str(dataset)
    try:
        response = urllib2.urlopen(phedex_call)
    except:
        l.acquire()
        fs = open(LOG_PATH, 'a')
        fs.write(str(datetime.datetime.now()) + " " + str(ID) + ": Couldn't get dataset " + str(dataset) + " size\n")
        fs.close()
        l.release()
        return 0
    json_data = json.load(response)
    data = json_data.get('phedex').get('dbs')[0].get('dataset')[0].get('block')
    size_dataset = float(0)
    for block in data:
        size_dataset += block.get('bytes')

    size_dataset = size_dataset / 10**9
    l.acquire()
    fs = open(LOG_PATH, 'a')
    fs.write(str(datetime.datetime.now()) + " " + str(ID) + ": Dataset " + str(dataset) + " size is " + str(size_dataset) + "GB\n")
    fs.close()
    l.release()
    return int(size_dataset)

def availableSpace(l):
    """
    _availableSpace_

    Return available space on phedex at UNL.
    Need to have at least 10% free at all times so return the space 
    available in GB to use without reaching 90% capacity.
    """
    ID = "PhedexCheck"
    info = os.statvfs("/mnt/hadoop")
    total = (info.f_blocks * info.f_bsize) / (1024**3)
    free = (info.f_bfree * info.f_bsize) / (1024**3)
    minimum_free = total*(0.1)
    available_space = free - minimum_free
    l.acquire()
    fs = open(LOG_PATH, 'a')
    fs.write(str(datetime.datetime.now()) + " " + str(ID) + ": Phedex available space is " + str(available_space) + "GB\n")
    fs.close()
    l.release()
    return int(available_space)

def spaceCheck(dataset, l):
    """
    _spaceCheck_

    Check if dataset can be moved to datacenter 
    without going over the space limit.
    Return 0 fail and size of dataset if possible.
    """
    ID = "SpaceCheck"
    size_dataset = datasetSize(dataset, l)
    if (size_dataset == 0):
        fs.close()
        return 0
    else:
        available_space = availableSpace(l)
        if (available_space >= size_dataset):
            fs.close()
            return int(size_dataset)
        else:
            fs.close()
            return 0
    return 0

def subscriptionDecision(l):
    """
    _subscriptionDecision_

    Suggest subscription if a set have been accesses more than SET_ACCESS 
    and moving the set will not fill the new node more than 90%.
    """
    ID = "Decision"
    con = lite.connect(SQLITE_PATH)
    with con:
        cur = con.cursor()
        cur.execute('SELECT * FROM SetCount WHERE Count>=?', [SET_ACCESS])
        while True:
            row = cur.fetchone()
            if row == None:
                break
            dataset = row[0]
            setAccess = row[1]
            l.acquire()
            fs = open(LOG_PATH, 'a')
            fs.write(str(datetime.datetime.now()) + " " + str(ID) + ": Dataset " + str(dataset) + " have " + str(setAccess) + " set accesses\n")
            fs.close()
            l.release()
            cur.execute('SELECT * FROM DontMove WHERE Dataset=?', [dataset])
            row = cur.fetchone()
            if row:
                break

            budget = 0
            cur.execute('SELECT * FROM Budget')
            while True:
                row = cur.fetchone()
                if row == None:
                    break
                budget += row[1]
            l.acquire()
            fs = open(LOG_PATH, 'a')
            fs.write(str(datetime.datetime.now()) + " " + str(ID) + ": Total budget used " + str(budget) + "GB\n")
            fs.close()
            l.release()
            dataset_size = spaceCheck(str(dataset), l)
            if (budget + dataset_size > TOTAL_BUDGET):
                break
            if (not (dataset_size == 0)):
                # TODO : Check if subscription succeeded
                subscribe(str(dataset), int(dataset_size), l)
    con.close()
    return 1

def update(l):
    """
    _update_

    Delete entries where the expiration timestamp is older than current time.
    Update SetCount to reflect database after deletions.
    Delete sets from SetCount if count is 0 or less.
    """
    ID = "Update"
    con = lite.connect(SQLITE_PATH)
    with con:
        cur = con.cursor()
        cur.execute('SELECT Dataset FROM SetCount')
        while True:
            dataSet = cur.fetchone()
            if dataSet == None:
                break
            del_count = 0;
            cur.execute('DELETE FROM AccessTimestamp WHERE Expiration<? AND Dataset=?', (datetime.datetime.now(),dataSet[0]))
            del_count = cur.rowcount
            cur.execute('UPDATE SetCount SET Count=Count-? WHERE Dataset=?',(del_count, dataSet[0]))
            
        cur.execute('DELETE FROM FileToSet WHERE Expiration<?', [datetime.datetime.now()])
        minCount = 1
        cur.execute('DELETE FROM SetCount WHERE Count<?', [minCount])
        cur.execute('DELETE FROM UnknownSet WHERE Expiration<?', [datetime.datetime.now()])
        #cur.execute('DELETE FROM Budget WHERE Expiration<?', [datetime.datetime.now()])
    con.close()
    l.acquire()
    fs = open(LOG_PATH, 'a')
    fs.write(str(datetime.datetime.now()) + " " + str(ID) + ": Done updating database\n")
    fs.close()
    l.release()
    return 1

################################################################################
#                                                                              #
#                                                       #
#                                                                              #
################################################################################

def routine():
    """
    _routine_
    
    Run the janitor and analyzer once every hour. 
    The janitor is in charge of cleaning out expired 
    entries in the database and the analyzer suggests subscriptions.
    """
    # Run every hour
    while True:
        time.sleep(3600)
        # Update database, delete entries older than 12h
        janitor()
        # Check if should make subscriptions
        subscriptionDecision()
    return 1

################################################################################
#                                                                              #
#                           D A T A   H A N D L E R                            #
#                                                                              #
################################################################################

def dataHandler(d):
    """
    _dataHandler_

    Analyze dictionary extracted from UDP packet
    to insert dataset accesses in database.
    Dataset may not exist, record this as unknown.
    """
    lfn = str(d['file_lfn'])
    insert(lfn)

################################################################################
#                                                                              #
#                                P A R S E                                     #
#                                                                              #
################################################################################

def parse(data):
    """
    _parse_
    
    Extract data from UDP packet and insert into dictionary.
    """
    d = {}
    for line in data.split('\n'):
        if '=' in line:
            k, v = line.strip().split('=',1)
            if v:
                d[k] = v
    return d

################################################################################
#                                                                              #
#                                 W O R K                                      #
#                                                                              #
################################################################################

def work(q):
    """
    _work_
    
    Distribute data handling of UDP packets to worker processes.
    """
    while True:
        data = q.get()
        dataHandler(parse(data))

################################################################################
#                                                                              #
#                               C O N F I G                                    #
#                                                                              #
################################################################################

def config():
"""
_config_

Parse input file listener.conf for values.
If file not found, use default values.
"""
    name = "Config"
    if os.path.isFile('listener.conf'):
        config_f = open('listener.conf', 'r')
    else:
        log(name, "Config file listener.conf does not exist, will use default values")
        return 1
    for line in config_f:
        if re.match("set_access", line):
            value = re.split(" = ", line)
            SET_ACCESS = int(value[1].rstrip())
        elif re.match("time _frame", line):
            value = re.split(" = ", line)
            TIME_FRAME = int(value[1].rstrip())
    config_f.close()
    return 0

################################################################################
#                                                                              #
#                                 M A I N                                      #
#                                                                              #
################################################################################

def main():
    """
    __main__

    Spawn worker processes.
    Listem for UDP packets and send to parser and then distribute to workers.
    """
    # Initialize
    name = "Main"
    config():

    if setup():
        return 1

    try:
        # Spawn worker processes that will parse data and insert into database
        pool = Pool(processes=4)
        manager = Manager()
        queue = manager.Queue()
        
        # Spawn process o clean out database and make reports every 1h
        process = Process(target=janitor, args=())
        process.start()
        workers = pool.apply_async(work, (queue,))
        
        # UDP packets containing information about file access
        UDPSock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        listen_addr = ("0.0.0.0", 9345)
        UDPSock.bind(listen_addr)
        buf = 64*1024
        # Listen for UDP packets
        while True:
            data,addr = UDPSock.recvfrom(buf)
            queue.put(data)
    except:
        # Print out raised exception to log file
        if os.path.exists(LOG_PATH):
            log_file = open(LOG_PATH + LOG_FILE, 'a')
        else:
            return 1
        traceback.print_exc(file=log_file)
        log_file.close()
        
    finally:
        #Close everything if program is interupted
        UDPSock.close()
        pool.close()
        pool.join()
        process.join()
        return 1
    
if __name__ == '__main__':
    sys.exit(main())

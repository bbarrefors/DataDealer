#!/usr/bin/python26 -B
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
import os
import time
import re
import socket
import traceback
from multiprocessing import Manager, Process, Pool

from PhEDExLogger import log, error, LOG_PATH, LOG_FILE
from PhEDExDatabase import setup, insert, setTimeFrame, setSetAccess, setBudget
from PhEDExRoutine import janitor, analyze

CONFIG_FILE = 'cmsdata.config'

################################################################################
#                                                                              #
#                                 R O U T I N E                                #
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
        analyze()
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
    
    if os.path.isfile(CONFIG_FILE):
        config_f = open(CONFIG_FILE, 'r')
    else:
        error(name, "Config file %s does not exist, will use default values" % (CONFIG_FILE,))
        return 1
    for line in config_f:
        if re.match("set_access", line):
            value = re.split(" = ", line)
            set_access = int(value[1].rstrip())
            setSetAccess(set_access)
        elif re.match("time _frame", line):
            value = re.split(" = ", line)
            time_frame = int(value[1].rstrip())
            setTimeFrame(time_frame)
        elif re.match("budget", line):
            value = re.split(" = ", line)
            budget = int(value[1].rstrip())
            setBudget(budget)
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
    Listen for UDP packets and send to parser and then distribute to workers.
    """
    # Initialize
    name = "Main"
    config()

    if setup():
        return 1

    try:
        # Spawn worker processes that will parse data and insert into database
        pool = Pool(processes=4)
        manager = Manager()
        queue = manager.Queue()
        
        # Spawn process o clean out database and make reports every 1h
        process = Process(target=routine, args=())
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
        process.join()
    
if __name__ == '__main__':
    """
    __main__

    This is where it all starts.
    """
    sys.exit(main())

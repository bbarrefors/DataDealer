#!/usr/bin/python -B

"""
_CMSDATAListener_

Created by Bjorn Barrefors on 11/9/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
__author__       = 'Bjorn Barrefors'
__organization__ = 'Holland Computing Center - University of Nebraska-Lincoln'
__email__        = 'bbarrefo@cse.unl.edu'

import sys
import os
import time
import re
import socket

from multiprocessing import Manager, Process, Pool
from operator        import itemgetter
from email.mime.text import MIMEText
from subprocess      import Popen, PIPE

from CMSDATALogger   import CMSDATALogger
from CMSDATADatabase import CMSDATADatabase
from PhEDExAPI       import PhEDExAPI


################################################################################
#                                                                              #
#                       C M S D A T A   L I S T E N E R                        #
#                                                                              #
################################################################################

class CMSDATAListener():
    """
    _CMSDATAListener_

    Listen for UDP packets.
    Packets contain information of files that have been accessed.
    Store information in a local database, analyze to
    make decisions on when to subscribe or delete a dataset.
    
    Class variables:
    logger   -- Used to print log and error messages to log file
    phedex   -- Query PhEDEx API
    """
    def __init__(self):
        """
        __init__

        Initialize database, logger, phedex objects
        """
        self.name      = "CMSDATAListener"
        self.logger    = CMSDATALogger()
        self.phedex    = PhEDExAPI()
        self.sender    = "bbarrefo@cse.unl.edu"
        self.receivers = ["bbarrefo@cse.unl.edu", "bbockelm@cse.unl.edu"]

    ################################################################################
    #                                                                              #
    #                                 R O U T I N E                                #
    #                                                                              #
    ################################################################################

    def routine(self):
        """
        _routine_
        
        Run the janitor and analyzer once every hour. 
        The janitor is in charge of cleaning out expired 
        entries in the database and the analyzer suggests subscriptions.
        """
        database = CMSDATADatabase()
        # Run once a day
        while True:
            time.sleep(86400)
            # Clear entries
            database.cleanAccess()
            datasets = database.datasets()
            set_count = []
            for dataset in datasets:
                count = database.accessCount(dataset)
                set_count.append((count, dataset))
            # Sort set_count and print out the top N sets w accesses
            set_count = sorted(set_count, key=itemgetter(0))
            set_count.reverse()
            text = ""
            i = 1
            for dataset in set_count:
                if i > 100:
                    break
                text = text + str(i) + ". " + str(dataset[1]) + "\t" + str(dataset[0]) + "\n"
                i += 1
            msg = MIMEText(text)
            msg['Subject'] = "Dataset report from CMSDATA"
            msg['From'] = self.sender
            msg['To'] = self.receivers
            p = Popen(["/usr/sbin/sendmail", "-toi"], stdin=PIPE)
            p.communicate(msg.as_string())
            database.cleanCache()
            #time.sleep(60)
        return 1

    ################################################################################
    #                                                                              #
    #                           D A T A   H A N D L E R                            #
    #                                                                              #
    ################################################################################
    
    def dataHandler(self, d, database):
        """
        _dataHandler_
        
        Analyze dictionary extracted from UDP packet
        to insert dataset accesses in database.
        Dataset may not exist, record this as unknown.
        """
        lfn = str(d['file_lfn'])
        directory = lfn.rsplit('/',2)[0]
        # Check if dir is in cache
        check, dataset = database.lookup(directory)
        if check:
            # If not call PhEDExAPI
            check, data = self.phedex.data(file_name=lfn, level='file')
            if check:
                return 1
            data = data.get('phedex').get('dbs')
            if not data:
                return 1
            dataset = data[0].get('dataset')[0].get('name')
            database.insertDirectory(directory, dataset)
        # update access (insertDataset)
        database.insertDataset(dataset)

    ################################################################################
    #                                                                              #
    #                                P A R S E                                     #
    #                                                                              #
    ################################################################################
    
    def parse(self, data):
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
    global listener
    database = CMSDATADatabase()

    while True:
        data = q.get()
        listener.dataHandler(listener.parse(data), database)
        

################################################################################
#                                                                              #
#                                 L I S T E N                                  #
#                                                                              #
################################################################################

def listen():
    """
    _listen_
    
    Spawn worker processes.
    Listen for UDP packets and send to parser and then distribute to workers.
    """
    global listener

    # Spawn worker processes that will parse data and insert into database
    pool = Pool(processes=4)
    manager = Manager()
    queue = manager.Queue()
    
    # Spawn process o clean out database and make reports every 1h
    process = Process(target=listener.routine, args=())
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
    #finally:
        #Close everything if program is interupted
        #UDPSock.close()
        #pool.close()
        #process.join()
    return 1

################################################################################
#                                                                              #
#                                 M A I N                                      #
#                                                                              #
################################################################################

listener = CMSDATAListener()

if __name__ == '__main__':
    """
    __main__

    This is where it all starts.
    """
    sys.exit(listen())
    

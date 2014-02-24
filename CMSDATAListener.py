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
import datetime

from multiprocessing import Manager, Process, Pool
from operator        import itemgetter
from email.mime.text import MIMEText
from subprocess      import Popen, PIPE

from CMSDATALogger   import CMSDATALogger
from CMSDATADatabase import CMSDATADatabase
from PhEDExAPI       import PhEDExAPI
from PopDBAPI        import PopDBAPI


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
    name      -- ID used when logging
    logger    -- Used to print log and error messages to log file
    phedex    -- Query PhEDEx API
    sender    -- Email address of sender for daily reports
    receivers -- Recipient email addresses for daily reports
    """
    def __init__(self):
        """
        __init__

        Initialize class variables
        """
        self.name       = "CMSDATAListener"
        self.logger     = CMSDATALogger()
        self.phedex     = PhEDExAPI()
        self.popdb      = PopDBAPI()
        self.sender     = "bbarrefo@cse.unl.edu"
        #self.receivers  = "bbarrefo@cse.unl.edu,bbockelm@cse.unl.edu"
        self.receivers  = "bbarrefo@cse.unl.edu"
        self.graph_path = "/home/bockelman/barrefors/data/"
        self.graph_file = "cmsdata.dat"


    ################################################################################
    #                                                                              #
    #                                 R O U T I N E                                #
    #                                                                              #
    ################################################################################

    def routine(self):
        """
        _routine_
        
        Ran once a day to identify the 100 most popular datasets
        and clean out old entries in the database
        """
        database = CMSDATADatabase()
        # Run once a day
        graph_data = dict()
        while True:
            #time.sleep(86400)
            self.popdb.renewSSOCookie()
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
            # Get date
            today = str(datetime.datetime.now().strftime("%Y%m%d"))
            text = "The 100 most accessed datasets in the last 24h\n\n"
            i = 1
            for dataset in set_count:
                if i <= 100:
                    text = text + str(i) + ". " + str(dataset[1]) + "\t" + str(dataset[0]) + "\n"
                # Get previous entry is exist
                try:
                    old_data = graph_data[str(dataset[1])]
                except KeyError:
                    new_data = [(today, dataset[0])]
                else:
                    new_data = old_data.append((today, dataset[0]))
                graph_data[dataset[1]] = new_data
                i += 1
            msg = MIMEText(text)
            msg['Subject'] = "Dataset report from CMSDATA"
            msg['From'] = self.sender
            msg['To'] = self.receivers
            p = Popen(["/usr/sbin/sendmail", "-toi"], stdin=PIPE)
            p.communicate(msg.as_string())
            try:
                if not os.path.isdir(self.graph_path):
                    os.makedirs(self.graph_path)
                graph_fd = open(self.graph_path + self.graph_file, 'w')
            except IOError, e:
                # Couldn't open file
                self.logger.error(self.name, "Couldn\'t access graph file. Reason: %s" % (e,))
                sys.exit(1)
            except OSError, e:
                # Couldn't create path to log file
                self.logger.error(self.name, "Couldn\'t create graph file. Reason: %s" % (e,))
                sys.exit(1)
            for dataset, data in graph_data.iteritems():
                graph_fd.write(dataset + "\n")
                for dates in data:
                    graph_fd.write(dates[0] + "\t" + dates[1] + "\n")
                graph_fd.write("\n")
            graph_fd.close()
            database.cleanCache()
            time.sleep(86400)
        return 1

    ################################################################################
    #                                                                              #
    #                           D A T A   H A N D L E R                            #
    #                                                                              #
    ################################################################################
    
    def dataHandler(self, d, database):
        """
        _dataHandler_
        
        Look up dataset of accessed file, first look in database cache, if not
        found query PhEDEx

        Dataset might not exist in PhEDEx
        """
        lfn = str(d['file_lfn'])
        # Check for invalid sets
        if ((lfn.find("/store", 0, 6) == -1) or (lfn.find("/store/user", 0, 11) == 0) or (lfn.count("/") < 3)):
            return 1
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
    

#!/usr/bin/env python

"""
_CMSDATALogger_

Created by Bjorn Barrefors on 22/9/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
__author__ =  'Bjorn Barrefors'
__version__ = '0.3'

import sys
import os
import datetime


################################################################################
#                                                                              #
#                       C M S D A T A   L O G G E R                            #
#                                                                              #
################################################################################

class CMSDATALogger:
    """
    _CMSDATALogger_
    
    Print log and error messages from other modules to log file
    Esceptions need to be caught by the caller, see the __main__
    function for implementation of error handling

    Class variables:
    log_file -- File descriptor for log file
    """
    def __init__(self, log_path='/home/barrefors/cmsdata/test/', file_name='cmsdata.log'):
        """
        Open log file filedescriptor

        Keyword arguments:
        log_path  -- Path to log file
        file_name -- Name of log file
        """
        # Alternative log paths:
        # /home/barrefors/cmsdata/test/
        # /grid_home/cmsphedex/
        # /home/bockelman/barrefors/cmsdata/
        # /root/test/cmsdata/

        if not os.path.isdir(log_path):
            os.makedirs(log_path)
        self.log_file = open(log_path + file_name, 'a')


    ################################################################################
    #                                                                              #
    #                                L O G                                         #
    #                                                                              #
    ################################################################################

    def log(self, name, msg):
        """
        _log_

        Function is fairly straight forward, name is the name of 
        module printing log message.
        """
        self.log_file.write("LOG: %s %s: %s\n" % (str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), str(name), str(msg)))


    ################################################################################
    #                                                                              #
    #                                E R R O R                                     #
    #                                                                              #
    ################################################################################

    def error(self, name, msg):
        """
        _error_

        Identical to log but prints out error messages to the same file.
        """
        self.log_file.write("ERROR: %s %s: %s\n" % (str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), str(name), str(msg)))


################################################################################
#                                                                              #
#                                  M A I N                                     #
#                                                                              #
################################################################################

if __name__ == '__main__':
    """
    __main__
    
    For testing purpose only.
    """
    try:
        my_logger = CMSDATALogger()
        my_logger.error("Bjorn", "Test message 2")
    except IOError, e:
        # Couldn't open file
        print "Couldn\'t access log file. Reason: %s" % (e,)
        sys.exit(1)
    except OSError, e:
        # Couldn't create path to log file
        print "Couldn\'t create log file. Reason: %s" % (e,)
        sys.exit(1)
    sys.exit(0)

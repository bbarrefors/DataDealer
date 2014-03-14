#!/usr/bin/python -B

"""
_DynDTALogger_

Created by Bjorn Barrefors on 22/9/2013
for DynDTA (Dynamic DataTransfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
__author__       = 'Bjorn Barrefors'
__organization__ = 'Holland Computing Center - University of Nebraska-Lincoln'
__email__        = 'bbarrefo@cse.unl.edu'

import sys
import os
import datetime


################################################################################
#                                                                              #
#                         D Y N D T A   L O G G E R                            #
#                                                                              #
################################################################################

class DynDTALogger:
    """
    _DynDTALogger_

    Print log and error messages from other modules to log file

    Exceptions need to be caught by the caller, see the __main__
    function for implementation of error handling

    Class variables:
    log_fd -- File descriptor for log file
    """
    def __init__(self, log_path='/home/bockelman/barrefors/logs/', log_file='dyndta.log'):
        """
        __init__

        Open log file filedescriptor

        Keyword arguments:
        log_path -- Path to log file
        log_file -- Name of log file
        """
        # Alternative log paths:
        # /home/barrefors/cmsdata/logs/
        # /home/bockelman/barrefors/logs/

        try:
            if not os.path.isdir(log_path):
                os.makedirs(log_path)
            self.log_fd = open(log_path + log_file, 'a')
        except IOError, e:
            # Couldn't open file
            print "Couldn\'t access log file. Reason: %s" % (e,)
            sys.exit(1)
        except OSError, e:
            # Couldn't create path to log file
            print "Couldn\'t create log file. Reason: %s" % (e,)
            sys.exit(1)


    ############################################################################
    #                                                                          #
    #                                L O G                                     #
    #                                                                          #
    ############################################################################

    def log(self, name, msg):
        """
        _log_

        Function is fairly straight forward, name is the name of
        module printing log message.

        Keyword arguments:
        name -- Module printing log message
        msg  -- Message to be printed to log file
        """
        self.log_fd.write("LOG: %s %s: %s\n" % (str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), str(name), str(msg)))


    ############################################################################
    #                                                                          #
    #                                E R R O R                                 #
    #                                                                          #
    ############################################################################

    def error(self, name, msg):
        """
        _error_

        Identical to log but prints out error messages to the same file.

        Keyword arguments:
        name -- Module printing error message
        msg  -- Message to be printed to log file
        """
        self.log_fd.write("ERROR: %s %s: %s\n" % (str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), str(name), str(msg)))
        # Return a status if write failed?


################################################################################
#                                                                              #
#                                  M A I N                                     #
#                                                                              #
################################################################################

if __name__ == '__main__':
    """
    __main__

    For testing purpose only
    """
    my_logger = DynDTALogger()
    my_logger.error("Bjorn", "Error test message")
    my_logger.log("Bjorn", "Log test message")
    sys.exit(0)

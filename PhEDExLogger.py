#!/usr/bin/env python26

"""
_PhEDExLogger_

Print log and error messages from other modules to log file.

Created by Bjorn Barrefors on 22/9/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
################################################################################
#                                                                              #
#                         P h E D E x   L O G G E R                            #
#                                                                              #
################################################################################

import os.path
import datetime
import fcntl

#LOG_PATH = '/home/bockelman/barrefors/'
LOG_PATH = '/home/barrefors/scripts/python/'
LOG_FILE = 'datad.log'

################################################################################
#                                                                              #
#                                L O G                                         #
#                                                                              #
################################################################################

def log(name, msg):
    """
    _log_

    Function is fairly straight forward, pass the ID of module
    calling logging and log message.
    Prints out log message msg to the standard log file.
    """
    if os.path.exists(LOG_PATH):
        log_file = open(LOG_PATH + LOG_FILE, 'a')
    else:
        return 1
    try:
        fcntl.flock(log_file, fcntl.LOCK_EX|fcntl.LOCK_NB)
    except IOError:
        fcntl.flock(log_file, fcntl.LOCK_EX)
    log_file.write("ERROR: %s %s: %s\n" % (str(datetime.datetime.now()), str(name), str(msg)))
    fcntl.lockf(log_file, fcntl.LOCK_UN)
    log_file.close()
    return 0

################################################################################
#                                                                              #
#                                E R R O R                                     #
#                                                                              #
################################################################################

def error(name, msg):
    """
    _error_

    Identical to log but prints out error messages to the same file.
    """
    if os.path.exists(LOG_PATH):
        log_file = open(LOG_PATH + LOG_FILE, 'a')
    else:
        return 1
    try:
        fcntl.flock(log_file, fcntl.LOCK_EX|fcntl.LOCK_NB)
    except IOError:
        fcntl.flock(log_file, fcntl.LOCK_EX)
    log_file.write("ERROR: %s %s: %s\n" % (str(datetime.datetime.now()), str(name), str(msg)))
    fcntl.lockf(log_file, fcntl.LOCK_UN)
    log_file.close()
    return 0

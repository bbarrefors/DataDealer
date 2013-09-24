#!/usr/bin/env python26

"""
_PhEDExRoutine_

Keep database up to date and analyze it to make subscribtion and deletion
decisions.

Created by Bjorn Barrefors on 23/9/2013
for CMSDATA (CMS Data Analyzer and Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
################################################################################
#                                                                              #
#                        P h E D E x   R O U T I N E                           #
#                                                                              #
################################################################################

from PhEDExDatabase import delete

################################################################################
#                                                                              #
#                             J A N I T O R                                    #
#                                                                              #
################################################################################

def janitor():
    """
    _janitor_

    Delete entries in database that are expired.
    Update SetAccess based on deletions.
    """
    name = "RoutineJanitor"
    log(name, "Updating database")
    delete()

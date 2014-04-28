#!/usr/bin/python -B

"""
_DynDTADatabase_

Created by Bjorn Barrefors on 22/9/2013
for DynDTA (Dynamic Data Transfer Agent)

Holland Computing Center - University of Nebraska-Lincoln
"""
__author__       = 'Bjorn Barrefors'
__organization__ = 'Holland Computing Center - University of Nebraska-Lincoln'
__email__        = 'bbarrefo@cse.unl.edu'


import sys
import os.path
import datetime
import urllib
import sqlite3 as lite

from DynDTALogger import DynDTALogger


################################################################################
#                                                                              #
#                       D Y N D T A   D A T A B A S E                          #
#                                                                              #
################################################################################

class DynDTADatabase():
    """
    _DynDTADatabase_

    Manage the database keeping track of which datasets have been accessed and when.

    Class variables:
    connection -- Established connection to the database
    logger     -- Used to print log and error messages to log file
    """
    def __init__(self, db_path='/home/bockelman/barrefors/db/', db_file='cmsdata.db'):
        """
        __init__

        Create logger object
        Establish database connection and set up database

        Keyword arguments:
        db_path -- Path to database file
        db_file -- File name of database
        """
        # Alternative db paths:
        # /home/barrefors/cmsdata/db/
        # /home/bockelman/barrefors/db/

        self.name = "DynDTADatabase"
        self.logger = DynDTALogger()
        try:
            if not os.path.isdir(db_path):
                os.makedirs(db_path)
        except OSError, e:
            # Couldn't create path to db file
            self.logger.error(name, "Couldn\'t access db file. Reason: %s" % (e,))
            sys.exit(1)

        self.connection = lite.connect(db_path + db_file)
        try:
            with self.connection:
                cur = self.connection.cursor()
                cur.execute('CREATE TABLE IF NOT EXISTS DatasetRanking (Dataset TEXT, Ranking REAL, n_Replicas INTEGER, size REAL, n_tUsers INTEGER, n_tAccesses INTEGER, n_2tUsers INTEGER, n_2tAccesses INTEGER)')EXT
                cur.execute('CREATE TABLE IF NOT EXISTS DatasetAvailability (Dataset TEXT, Site T)')
        except lite.IntegrityError:
            self.logger.error(self.name, "Couldn't initialize database")
            sys.exit(1)


    ############################################################################
    #                                                                          #
    #                                L O O K U P                               #
    #                                                                          #
    ############################################################################

    def lookup(self, dir_name):
        """
        _lookup_

        Find which dataset files in a directory belongs to in cache if available

        If not return 1

        Arguments:
        dir_name -- Base directory of file
        """
        try:
            with self.connection:
                cur = self.connection.cursor()
                cur.execute("SELECT Dataset FROM DirectoryDataset WHERE Directory=?", (dir_name,))
                dataset = cur.fetchone()
                if not dataset:
                    return 1, "Not in db"
        except lite.IntegrityError:
            self.logger.error(self.name, "Exception while querying database")
            return 1, "Error"
        return 0, dataset[0]


    ############################################################################
    #                                                                          #
    #                        I N S E R T   D I R E C T O R Y                   #
    #                                                                          #
    ############################################################################

    def insertDirectory(self, dir_name, dataset):
        """
        _insertDirectory_

        Insert dir_name and dataset to FileSet table
        Set an expiration time for the cache to avoid too much data in database

        Arguments:
        dir_name -- Base director of file accessed
        dataset  -- Name of datatset file belongs to
        """
        try:
            with self.connection:
                cur = self.connection.cursor()
                # Set cache expiration to 24h for now
                expiration = datetime.datetime.now() + datetime.timedelta(hours=24)
                cur.execute('INSERT INTO DirectoryDataset VALUES(?,?,?)', (dir_name, dataset, expiration))
        except lite.IntegrityError:
            self.logger.error(self.name, "Exception while inserting data")
        return 0

    ############################################################################
    #                                                                          #
    #                            C L E A N   C A C H E                         #
    #                                                                          #
    ############################################################################

    def cleanCache(self):
        """
        _cleanCache_

        Delete all entries in the cache which have expired
        See insertDirectory for expiration time
        """
        try:
            with self.connection:
                cur = self.connection.cursor()
                cur.execute('DELETE FROM DirectoryDataset WHERE Expiration<?', (datetime.datetime.now(),))
        except lite.IntegrityError:
            self.logger.error(self.name, "Exception while deleting data")
        return 0


    ############################################################################
    #                                                                          #
    #                        I N S E R T   D A T A S E T                       #
    #                                                                          #
    ############################################################################

    def insertDataset(self, dataset):
        """
        _insertDataset_

        Insert dataset with expiration time to the dataset
        Expiration time is based on how long we want to keep track of # accesses

        For now the expiration is set to 3 days

        Arguments:
        dataset -- Name of datatset that was accessed
        """
        try:
            with self.connection:
                cur = self.connection.cursor()
                # Set time window to 24h for now
                expiration = datetime.datetime.now() + datetime.timedelta(hours=24)
                cur.execute('INSERT INTO DatasetAccess VALUES(?,?)', (dataset, expiration))
        except lite.IntegrityError:
            self.logger.error(self.name, "Exception while inserting data")
        return 0


    ############################################################################
    #                                                                          #
    #                              D A T A S E T S                             #
    #                                                                          #
    ############################################################################

    def datasets(self):
        """
        _datasets_

        Get all unique datasets in database
        """
        try:
            with self.connection:
                cur = self.connection.cursor()
                cur.execute("SELECT DISTINCT Dataset FROM DatasetAccess")
                datasets = []
                for dataset in cur:
                    datasets.append(dataset[0])
        except lite.IntegrityError:
            self.logger.error(self.name, "Exception while querying database")
        return datasets


    ############################################################################
    #                                                                          #
    #                          A C C E S S   C O U N T                         #
    #                                                                          #
    ############################################################################

    def accessCount(self, dataset):
        """
        _accessCount_

        Get the number of accesses for the given set

        Arguments:
        dataset -- Name of dataset
        """
        try:
            with self.connection:
                cur = self.connection.cursor()
                cur.execute("SELECT Dataset FROM DatasetAccess WHERE Dataset=?", (dataset,))
                count = len(cur.fetchall())
        except lite.IntegrityError:
            self.logger.error(self.name, "Exception while querying database")
            return 0
        return count


    ############################################################################
    #                                                                          #
    #                           C L E A N   A C C E S S                        #
    #                                                                          #
    ############################################################################

    def cleanAccess(self):
        """
        _cleanAccess_

        Delete all entries in the DatasetAccess table which have expired
        See insertDataset for expiration time
        """
        try:
            with self.connection:
                cur = self.connection.cursor()
                cur.execute('DELETE FROM DatasetAccess WHERE Expiration<?', (datetime.datetime.now(),))
        except lite.IntegrityError:
            self.logger.error(self.name, "Exception while deleting data")
        return 0


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
    db = DYNDTADatabase()
    #db.insertDataset("SET2")
    #db.insertDirectory("DIR1", "SET2")
    #check, datasets = db.datasets()
    #for dataset in datasets:
    #    print dataset
    #    count = db.accessCount(dataset)
    #    print count
    sys.exit(0)

#!/usr/bin/python

import socket
import ast
import re
import urllib2
import json
import time
import datetime
import sqlite3 as lite
from multiprocessing import Manager, Process, Pool

def checkDataset(dataset):
    # Accumulate all block sizes and calculate total dataset size
    # Size returned in GB
    phedex_call = "http://cmsweb.cern.ch/phedex/datasvc/json/prod/data?dataset=" + dataset
    try:
        response = urllib2.urlopen(phedex_call)
    except:
        return 0
    json_data = json.load(response)
    dataset = json_data.get('phedex').get('dbs')[0].get('dataset')[0].get('block')
    size_dataset = 0
    for block in dataset:
        size_dataset += block.get('bytes')

    size_dataset = size_dataset / 10**9
    return size_dataset

def checkPhedex():
    avail_space_util = 0
    
    return avail_space_util

def checkSize(dataset):
    # Check available space at phedex.unl.edu
    # See if dataset size is small enough to be moved
    fs2 = open('Sizes', 'a')
    size_dataset = checkDataset(dataset)
    if (size_dataset == 0):
        # dataset request didn't succeed, exit out maybe?
        return 0
    else:
        # Everything went well, do our thing instead
        # Find available space in phedex
        phedex_avail_util = checkPhedex()
        fs2.write("Dataset size: " + str(size_dataset) + "GB for set " + dataset + " | PhEDEx available space to utilize: " + phedex_avail_util)
        if (phedex_avail_util >= size_dataset):
            return 1
        else:
            return 0

def update():
    # Delete entries where the expiration timestamp is older than current time
    # Update SetCount to reflect database after deletions
    # Delete sets from SetCount if count is 0 or less
    con = lite.connect("dataset_cache.db")
    with con:
        cur = con.cursor()
        cur.execute('SELECT DataSet FROM SetCount')
        while True:
            dataSet = cur.fetchone()
            if dataSet == None:
                break
            del_count = 0;
            cur.execute('DELETE FROM AccessTimestamp WHERE Expiration<? AND DataSet=?', (datetime.datetime.now(),dataSet[0]))
            del_count = cur.rowcount
            cur.execute('UPDATE SetCount SET Count=Count-? WHERE DataSet=?',(del_count, dataSet[0]))
            
        cur.execute('DELETE FROM FileToSet WHERE Expiration<?', [datetime.datetime.now()])
        minCount = 1
        cur.execute('DELETE FROM SetCount WHERE Count<?', [minCount])
        cur.execute('DELETE FROM UnknownSet WHERE Expiration<?', [datetime.datetime.now()])
    con.close()
    return 1

def printer():
    # Print anything that might be of interest
    # Currently print out the SetCount database table
    fc = open('Setcount', 'w')
    con = lite.connect("dataset_cache.db")
    with con:
        cur = con.cursor()
        cur.execute('SELECT * FROM SetCount')
        while True:
            row = cur.fetchone()
            if row == None:
                break
            fc.write(str(datetime.datetime.now()) + " " + str(row[0]) + " " + str(row[1]) + "\n")
    con.close()
    fc.close()
    return 1

def subscriptions():
    # Decide which subscriptions to make
    # Current rule: 
    # Ratio setAcces / filesCount <= 100
    # Total setAccess >= 100
    fs = open('Subscriptions', 'a')
    con = lite.connect("dataset_cache.db")
    with con:
        cur = con.cursor()
        min_count = 200
        cur.execute('SELECT * FROM SetCount WHERE Count>=?', [min_count])
        while True:
            row = cur.fetchone()
            if row == None:
                break
            dataset = row[0]
            setAccess = row[1]
            filesCount = 0;
            cur.execute('SELECT * FROM AccessTimestamp WHERE DataSet=?', [dataset])
            while True:
                access = cur.fetchone()
                if access == None:
                    break
                filesCount += 1
            if filesCount > 0:
                if (setAccess/filesCount) <= 100:
                    if (checkSize(str(dataset)) == 1):
                    fs.write(str(datetime.datetime.now()) + " Move data set: " + str(dataset) + " because it had " + str(setAccess) + " set accesses to " + str(filesCount) + " different files.\n")
    con.close()
    fs.close()
    return 1

def report():
    # Run every hour
    while True:
        time.sleep(3600)
        # Update database, delete entries older than 12h
        update()
        # Print out stuff
        printer()
        # Check if should make subscriptions
        subscriptions()
    return 1

def data_handler(d):
    # Extract file name from data
    # If it is in cache fetch from db
    # Else fetch from PhEDEx
    # the file may not have dataset, this could be a bug. we will store log this in database
    # Insert into cache if not already there
    # Increment count table for dataset
    con = lite.connect("dataset_cache.db")
    lfn = str(d['file_lfn'])
    with con:
        cur = con.cursor()
        cur.execute("SELECT EXISTS(SELECT * FROM FileToSet WHERE File=?)", [lfn])
        test = cur.fetchone()[0]
        if int(test) == int(1):
            cur.execute('SELECT DataSet FROM FileToSet WHERE File=?', [lfn])
            dataset = cur.fetchone()[0]
            timestamp = datetime.datetime.now()
            delta = datetime.timedelta(hours=72)
            expiration = timestamp + delta
            cur.execute('UPDATE SetCount SET Count=Count+1 WHERE DataSet=?', [dataset])
            cur.execute('UPDATE FileToSet SET Expiration=? WHERE File=?', (lfn, expiration))
            cur.execute('UPDATE AccessTimestamp SET Expiration=? WHERE DataSet=?', (expiration, dataset))
        else:
            phedex_call = "http://cmsweb.cern.ch/phedex/datasvc/json/prod/data?file=" + lfn
            try:
                response = urllib2.urlopen(phedex_call)
            except:
                return 0
            json_data = json.load(response)
            if json_data.get('phedex').get('dbs'):
                dataset = json_data.get('phedex').get('dbs')[0].get('dataset')[0].get('name')
                timestamp = datetime.datetime.now()
                delta = datetime.timedelta(hours=72)
                expiration = timestamp + delta
                cur.execute('INSERT INTO AccessTimestamp VALUES(?,?)', (dataset, expiration))
                cur.execute('INSERT INTO FileToSet VALUES(?,?,?)', (lfn, dataset, expiration))
                cur.execute("SELECT EXISTS(SELECT * FROM SetCount WHERE DataSet=?)", [dataset])
                test = cur.fetchone()[0]
                if int(test) == int(1):
                    cur.execute('UPDATE SetCount SET Count=Count+1 WHERE DataSet=?', [dataset])
                else:
                    in_count = 1
                    cur.execute('INSERT INTO SetCount VALUES(?,?)', (dataset, in_count))
            else:
                # Unknown log is kept for 1 week
                delta = datetime.timedelta(hours=168)
                timestamp = datetime.datetime.now()
                dataset = "UNKNOWN"
                cur.execute('INSERT INTO UnknownSet VALUES(?,?,?)', (lfn, dataset, timestamp))
    con.close()
    return 1

def work(q):
    while True:
        d = q.get()
        data_handler(d)

def data_parser(data):
    # Extract data and insert into dictionary
    d = {}
    for line in data.split('\n'):
        if '=' in line:
            k, v = line.strip().split('=',1)
            if v:
                d[k] = v
    return d

if __name__ == '__main__':
    # Listen for UDP packets
    # Spawn pool of processes to handle data
    # Have a seperate process print out the collected dataset count every 1h
    # Create database and tables if they don't already exist
    connection = lite.connect("dataset_cache.db")
    with connection:
        cur = connection.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS FileToSet (File TEXT, DataSet TEXT, Expiration TIMESTAMP)')
        cur.execute('CREATE TABLE IF NOT EXISTS AccessTimestamp (DataSet TEXT, Expiration TIMESTAMP)')
        cur.execute('CREATE TABLE IF NOT EXISTS SetCount (DataSet TEXT, Count INTEGER)')
        cur.execute('CREATE TABLE IF NOT EXISTS UnknownSet (File TEXT, DataSet TEXT, Expiration TIMESTAMP)')
    
    # Spawn worker processes that will parse data and insert into database
    pool = Pool(processes=4)
    manager = Manager()
    queue = manager.Queue()
    # process will clean out database and make reports every 1h
    process = Process(target=report, args=())
    process.start()
    workers = pool.apply_async(work, (queue,))
    # UDP packets containing information about file access
    UDPSock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    listen_addr = ("0.0.0.0", 9345)
    UDPSock.bind(listen_addr)
    buf = 64*1024
    try:
        while True:
            data,addr = UDPSock.recvfrom(buf)
            dictionary = data_parser(data)
            queue.put(dictionary)
    finally:
        UDPSock.close()
        pool.close()
        pool.join()
        process.join()

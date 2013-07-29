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

def report(l):
    # Prints out the database every hour
    while True:
        time.sleep(3600)
        con = lite.connect("dataset_cache.db")
        with con:
            cur = con.cursor()
            # Update database, delete entries older than 12h
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
            # Check if should make subscriptions
            fc = open('Setcount', 'a')
            cur.execute('SELECT * FROM SetCount')
            while True:
                row = cur.fetchone()
                if row == None:
                    break
                fc.write(str(datetime.datetime.now()) + " " + str(row[0]) + " " + str(row[1]) + "\n")
            fc.close()
            min_count = 100
            cur.execute('SELECT * FROM SetCount WHERE Count>=?', [min_count])
            fs = open('Subscriptions', 'a')
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
                    if (setAccess/filesCount) <= 10:
                        fs.write(str(datetime.datetime.now()) + " Move data set: " + str(dataset) + " because it had " + str(setAccess) + " set accesses to " + str(filesCount) + " different files.\n")
            fs.close()
        con.close()

def data_handler(d, l):
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
            cur.execute('INSERT INTO Log VALUES(?,?,?)', (lfn, dataset, timestamp))
        else:
            phedex_call = "http://cmsweb.cern.ch/phedex/datasvc/json/prod/data?file=" + lfn
            response = urllib2.urlopen(phedex_call)
            json_data = json.load(response)
            if json_data.get('phedex').get('dbs'):
                dataset = json_data.get('phedex').get('dbs')[0].get('dataset')[0].get('name')
                timestamp = datetime.datetime.now()
                delta = datetime.timedelta(hours=72)
                expiration = timestamp + delta
                cur.execute('INSERT INTO AccessTimestamp VALUES(?,?)', (dataset, expiration))
                cur.execute('INSERT INTO FileToSet VALUES(?,?,?)', (lfn, dataset, expiration))
                cur.execute('INSERT INTO Log VALUES(?,?,?)', (lfn, dataset, timestamp))
                cur.execute("SELECT EXISTS(SELECT * FROM SetCount WHERE DataSet=?)", [dataset])
                test = cur.fetchone()[0]
                if int(test) == int(1):
                    cur.execute('UPDATE SetCount SET Count=Count+1 WHERE DataSet=?', [dataset])
                else:
                    in_count = 1
                    cur.execute('INSERT INTO SetCount VALUES(?,?)', (dataset, in_count))
            else:
                timestamp = datetime.datetime.now()
                dataset = "UNKNOWN"
                cur.execute('INSERT INTO Log VALUES(?,?,?)', (lfn, dataset, timestamp))
    con.close()
    return 1

def work(q, l):
    while True:
        d = q.get()
        data_handler(d, l)

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
        cur.execute('CREATE TABLE IF NOT EXISTS Log (File TEXT, DataSet TEXT, Timestamp TIMESTAMP)')
    
    # Spawn worker processes that will parse data and insert into database
    pool = Pool(processes=4)
    manager = Manager()
    queue = manager.Queue()
    lock = manager.Lock()
    # process will clean out database and make reports every 1h
    process = Process(target=report, args=(lock,))
    process.start()
    workers = pool.apply_async(work, (queue, lock))
    # UDP packets containing information about file access
    UDPSock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    listen_addr = ("0.0.0.0",9345)
    UDPSock.bind(listen_addr)
    buf = 64*1024
    try:
        while True:
            data,addr = UDPSock.recvfrom(buf)
            dictionary = data_parser(data)
            queue.put(dictionary)
    finally:
        UDPSock.close()
        #queue.close()
        pool.close()
        pool.join()
        process.join()
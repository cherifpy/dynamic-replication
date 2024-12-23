from communication.messages import SendObject, RequestObject, Task

import pickle
import queue
import requests
import multiprocessing 
import time
from experiments.params import (
    BD_LISTENING_PORT,  
    EXECUTION_LOCAL, 
)

import numpy as np
import redis
import os
import threading

class RedisClient:

    def __init__(self, storage_size, node_id):
        self.id_node = node_id
        self.storage_size = storage_size
        self.ids_data = []
        self.datas_sizes = {}
        self.slot_time = 1
        self.time_to_live = np.zeros((len(self.ids_data,)))
        self.data_requested_hostoric = []
        self.data_access_frequency = {}
        self.memory_used = 0
        self.is_memcached_installed = False
        self.last_recently_used_item = []

    def sendDataSetTo(self, ip_dst, id_dataset,size_ds):
        if EXECUTION_LOCAL:
            return True, None

        file_name = '/tmp/tmp.bin'
        file_size_octet = int(size_ds)*1024
        with open(file_name, "wb") as p:
            p.write(os.urandom(file_size_octet))
        
        with open(file_name, "rb") as p:
            content = p.read()
        
        servers = [f"{ip_dst}:{BD_LISTENING_PORT}"]  # Adresse du serveur Memcached

        r = redis.Redis(host=ip_dst, port=BD_LISTENING_PORT, db=0, decode_responses=True)

        try:
            s = r.set(id_dataset, content)

            return True,None
        except Exception as e:
            return False, e
    
    def sendDataSetToOnthread(self, ip_dst, id_dataset,size_ds):
        sending_process = threading.Thread(target=self.sendDataSetTo, args=(ip_dst,id_dataset,size_ds))
        #sending_process = multiprocessing.Process(target=self.sendDataSet, args=[ip_node, id_dataset, ds_size])
        sending_process.start()
        return sending_process
    
    #TODO en cas de modification de politique d'eviction
    def accessData(self, id_dataset):
        client = redis.Redis(host='0.0.0.0', port=BD_LISTENING_PORT,db=0)
        value = client.get(id_dataset) if not EXECUTION_LOCAL else True
        
        return True if value else False

    def getKeys(self):
        r = redis.Redis(host='0.0.0.0', port=BD_LISTENING_PORT,db=0, decode_responses=True)
        keys = r.keys('*')
        return keys

    def getStats(self, verbos=False):
        if EXECUTION_LOCAL:
            return [("0", {"used_memory":f'{self.memory_used}',"maxmemory":f'{self.storage_size}'})]

        r = redis.Redis(host='0.0.0.0', port=BD_LISTENING_PORT,db=0)
        memory_info = r.info('memory')
        stats = [('this',memory_info)]
        return stats
    
    def checkOnCacheMemorie(self, id_data):
        client = redis.Redis(host='0.0.0.0', port=BD_LISTENING_PORT, db=0)
        s = client.exists(id_data)
        return True if s == 1 else False
    
    def addData(self, id_data, ds_size):
        self.datas_sizes[id_data] = ds_size
        while id_data in self.ids_data: self.ids_data.remove(id_data)
        self.ids_data.append(id_data)
        return True

    def connectToRedis(self):
        try:
            self.client = redis.Redis(host='0.0.0.0', port=BD_LISTENING_PORT, db=0)
            return self.client
        except Exception as e:
            print(f"Error connecting to Redis: {e}")
            return None
        
    #TODO a revoire le return true dans except
    def deleteFromCache(self, key,ds_size=0):
        """Deletes a value from Memcached."""
        if EXECUTION_LOCAL:
            self.memory_used-=(ds_size*1024+100)
            return True
        try:

            client = redis.Redis(host='0.0.0.0', port=BD_LISTENING_PORT, db=0,decode_responses=True)
            r = client.delete(key)
            return True
            #ca retourne une exption la 
            while key in self.last_recently_used_item: self.last_recently_used_item.remove(key)
            while key in self.ids_data: self.ids_data.remove(key)
            
        
        except Exception as e:
            print(f"Error deleting from Memcached: {e}")
            return False

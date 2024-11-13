from communication.messages import SendObject, RequestObject, Task

import pickle
import queue
from communication.nodeManagerServer import NodeManagerServer
import requests
import multiprocessing 
import time

import numpy as np
import redis
import os
import copy 
import re
import threading


class NodeClient(object):

    def __init__(self, id, storage_space,listner_port,neighbors, data_manager_ip,data_manager_port,host):
        self.id_node = id
        self.host = host
        self.storage_space = storage_space
        self.time_limite = 0
        self.neighbors = neighbors
        self.listner_port = listner_port   
        self.future_task = queue.Queue()
        self.server_is_running = False
        self.data_manager_ip = data_manager_ip
        self.data_manager_port = data_manager_port
        self.processes_working = None
        self.node_server = NodeManagerServer(
            storage_space=self.storage_space,
            id_node=self.id_node,
            host=self.host,
            port=self.listner_port,
            neighbors=neighbors,
            node_client = self
        )
        self.waiting_list = []

    def start(self):
        self.server_is_running = self.node_server.run()
        return True    

    def startTask(self,execution_time):
        p = multiprocessing.Process(target=self.task, args=(self, execution_time))
        self.processes_working = p
        p.start()
        return p.pid
    
    def task(self,execution_time):
        time.sleep(execution_time)
        self.processes_working = None
        return "Done"
        
    """    
    def startManagerFlaskServer(self, cache):
        self.node_server = NodeManagerServer(cache,host=self.host,port=self.listner_port)
        self.server_is_running = self.node_server.run()
    

    def startThread(self):
        flask_process = multiprocessing.Process(target=self.startManagerFlaskServer)
        flask_process.start()
        time.sleep(0.2)
        return flask_process
    """


#here i have to manage replica
import copy
import redis
from experiments.params import  (
    SERVER_REPLICA_MANAGER_PORT, 
    BD_LISTENING_PORT,
    MAX_EXECUTION_TIME,
    NB_REPLICAS_INIT,
    MAX_DATA_SIZE,
    MAX_NB_TASKS,
    BANDWIDTH,
    NB_NODES,
    NB_JOBS,
)

from communication.send_data import recieveObject

from classes.data import Data
from classes.replica import Replica
from classes.djikstra import djikstra

from typing import Optional, Dict
import multiprocessing 
import pandas as pd
import numpy as np
import time
import requests
import os
import threading
import random

class JobInjector:
    def __init__(self,nb_nodes ,graphe, ip, local_execution) -> None:
        self.nb_nodes = nb_nodes-1
        self.id = nb_nodes-1
        self.nodes_infos = dict()
        self.api_server = None
        self.graphe_infos = graphe
        self.location_table = {}
        self.port = SERVER_REPLICA_MANAGER_PORT
        self.ip = ip
        self.nb_data_trasnfert = 0
        self.output = open(f"/tmp/log  .txt",'a')
        str = "/tmp/transfert.txt"
        self.transfert = open(str,'w')
        self.local_execution = local_execution
        self.num_evection = 0
        self.last_node_recieved = None
        self.data_sizes = {}
        self.nb_data_trasnfert_avoided = 0
        self.data: Dict[str, Data] = {}
        self.replicas: Dict[tuple, Replica] = {}
        self.previous_stats: Dict[str, Data] = {}
        self.requests_processed = {}
        self.waiting_list = []
        self.executing_task = [] # task with this format (job, node, start_time, execution_time)
        self.jobs_list = {} 
        self.dataset_counter = 0
        self.nb_jobs = 0


    def start(self,):
        if not self.nodes_infos:
            return False
        
        for i_job in range(NB_JOBS):
            print(f"Job {i_job}")
            time.sleep(5)
            self.dataset_counter += 1
            job_id, job = self.generateJob() # (nb_tasks, execution_time, file_size)
            host_nodes = self.selectHostsNodes()

            for host in host_nodes:
                r = self.replicate(host, job_id, self.dataset_counter, job[2])
                if r: print("Replica sended")
                else: print("no replica sended")
            for host in host_nodes:
                """
                    id_node: Any,
                    job_id: Any,
                    execution_time: Any,
                    id_dataset: Any
                """
                self.sendTaskToNode(host, job_id, job[1],self.dataset_counter)
            
            



    def generateJob(self,):
        nb_tasks = random.randint(1, MAX_NB_TASKS)
        file_size = random.randint(1, MAX_DATA_SIZE)
        execution_time = random.randint(1, MAX_EXECUTION_TIME)

        self.jobs_list[self.nb_jobs] = (nb_tasks, execution_time, file_size)
        self.nb_jobs +=1
        
        return self.nb_jobs-1, (nb_tasks, execution_time, file_size)
    

    def selectHostsNodes(self):
        availabel_nodes = self.getAvailabledNodes()
        if len(availabel_nodes) < NB_REPLICAS_INIT:
            return copy.deepcopy(availabel_nodes)
        else:
            return copy.deepcopy(random.sample(availabel_nodes, NB_REPLICAS_INIT))


    def getAvailabledNodes(self):
        nodes = [id for id in range(self.nb_nodes)]
        for i, task in self.executing_task:
            time_second = time.time()
            if int(time_second - task[2]) < task[3] and task[1] in nodes:
                nodes.remove(task[1])
        return nodes
                
    
    def replicate(self, host,job_id, id_dataset, ds_size):

        node_ip = self.nodes_infos[int(host)]["node_ip"]
        added = self.sendDataSet(id_node=host,ip_node=node_ip, id_ds=id_dataset, ds_size=ds_size) 
        if added:
            self.nb_data_trasnfert +=1
            cost = self.transfertCost(self.graphe_infos[self.id][host], ds_size)
            self.writeTransfert(f"{job_id},{id_dataset},{self.id},{host},{ds_size},{cost},transfert1\n")
        return added
            
    
    def sendDataSet(self,id_node, ip_node, id_ds, ds_size):

        file_name = '/tmp/tmp.bin'
        file_size_octet = int(ds_size)*1024
        with open(file_name, "wb") as p:
            p.write(os.urandom(file_size_octet))
        with open(file_name, "rb") as p:
            content = p.read()
        servers = [f"{ip_node}:{BD_LISTENING_PORT}"]  # Adresse du serveur Memcached
        
        client = redis.Redis(host=ip_node, port=BD_LISTENING_PORT, db=0, decode_responses=True)
        self.last_node_recieved = ip_node
        try:
            r = client.set(f"{self.dataset_counter}", content)
            print(f'ajouter {r}\n')
            return True
        except:
            return False



    def sendTaskToNode(self, id_node, job_id, execution_time, id_dataset): 
        ip = self.nodes_infos[int(id_node)]["node_ip"]
        port = self.nodes_infos[int(id_node)]["node_port"]

        data = {
                "job_id": job_id, 
                "execution_time": execution_time,
                "id_dataset": id_dataset
            }

        url = f'http://{ip}:{port}/execut'
        
        response = requests.post(url, json=data)
        print(f"reponse recu {response.json()}")
        return response.json(), self.graphe_infos[id][id_node]
    
    
    def transfertCost(self, latency, data_size):
        bandwith_in_bits = BANDWIDTH*1024*1024*8
        size_in_bits = data_size*1024*8
        latency_in_s = latency/1000

        return latency_in_s + (size_in_bits/bandwith_in_bits)
    
    def writeTransfert(self,str):
        path = "/tmp/transfert.txt"
        self.transfert = open(path,'a')
        self.transfert.write(str)
        self.transfert.close()
    
    def writeOutput(self, str):
        self.output = open(f"/tmp/log.txt",'a')
        self.output.write(str)
        self.output.close() 
        
if __name__ == "__main__":

    data = recieveObject()

    
    job_injector = JobInjector(
        nb_nodes = NB_NODES,
        graphe= data["graphe_infos"],
        ip=data["IP_ADDRESS"],
        local_execution=False
    )
    job_injector.writeOutput(f"{data}")
    
    
    job_injector.nodes_infos = data["infos"]
    job_injector.start()
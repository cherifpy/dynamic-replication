
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
from classes.job import Job
from classes.task import Task

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
        self.id_dataset = 0
        self.running_job = {}


    def start(self,):
        if not self.nodes_infos:
            return False
        
        job_id, job = self.generateJob() # (nb_tasks, execution_time, file_size)
        self.waiting_list.append((job_id,job))
        j = 0
        while True:
            
            
            while j < len(self.waiting_list):
                job_started = False
                print(f"Job {job_id}")
                job_id, job = self.waiting_list[j]
                self.dataset_counter += 1
                
                host_nodes = self.selectHostsNodes()
                host_with_replica = []
                for i, host in enumerate(host_nodes):
                    
                    r = self.replicate(host, job_id, job.id_dataset, job.size_dataset)
                    if r: 
                        print(f"{i+1} Replica sended")
                        host_with_replica.append(host)
                        job.tasks_list[i].host_node = host
                    else: print("no replica sended")

                for i,host in enumerate(host_with_replica):
                    """
                        id_node: Any,
                        job_id: Any,
                        execution_time: Any,
                        id_dataset: Any
                    """
                    
                    rep, latency = self.sendTaskToNode(host, job_id, job.execution_times,job.id_dataset)
                    if rep['started'] and rep['starting_time']:
                        print(f"========= Task of job {job_id} started")
                        job.executing_tasks.append(job.tasks_list[i])
                        self.executing_tasks.append(i)
                        self.executing_task.append((job_id, host, rep['starting_time'],job.execution_times))
                        job.ids_nodes.append(host)
                        job_started = True
                        
                    job.starting_times.append(rep['starting_time'])
                    job.nb_task_not_lunched -=1
                
                if job_started:
                    print("========= Job started")
                    #self.waiting_list.append(job)
                    self.running_job[job_id] = job

                    j+=1
            self.updateRunningJobsList()
            if len(self.running_job.keys()) ==0:
                print("========= All jobs executed")
                break


    def updateRunningJobsList(self,):
        delete = []
        for job_id in self.running_job.keys():
            end = False
            job = self.running_job[job_id]
            for i in range(len(job.starting_times)):
                if job.starting_times[i] + job.execution_times < time.time():
                    print(f"========= task on job {job_id} finished")
                    if job.nb_task_not_lunched > 0:
                        end = False
                        rep, latency = self.sendTaskToNode(job.ids_nodes[i],job_id,job.execution_times,job.id_dataset)
                        if rep["started"]:
                            job.nb_task_not_lunched -=1
                            job.starting_times[i] = rep['starting_time']
                            print(f"========= other task on job {job_id} started")
                    else:
                        print(f"========= job {job_id} finished")
                        job.finishing_time = time.time()
                        end = True
                else:
                    end = False
            if end: delete.append(job_id)
        for id in delete :
            del self.running_job[id]

        return True
        
            

    def generateJob(self,):
        self.id_dataset +=1
        nb_tasks = 6 #random.randint(1, MAX_NB_TASKS)
        file_size = 1024 #random.randint(1, MAX_DATA_SIZE)
        execution_time = 5 #random.randint(1, MAX_EXECUTION_TIME)

        execution_times = []

        for i in range(nb_tasks):
            execution_times.append(random.randint(1, MAX_EXECUTION_TIME))

        job = Job(
            nb_task=nb_tasks,
            execution_times=execution_time,
            id_dataset=self.id_dataset,
            size_dataset=file_size
        )

        self.tasks_list = [Task(f'task_{i}', execution_time, self.id_dataset) for i in range(nb_tasks)]

        self.jobs_list[self.nb_jobs] = job
        self.nb_jobs +=1
        return job.id, job#(nb_tasks, execution_times, file_size) 
    
    def checkIfNeedForAddingReplication(self,job, hosts):
        for index, job in enumerate(self.waiting_list):
            if job.nb_task_not_lunched != 0:
                #here lancer les autres tache si ya moyenne sinon attendre 
                pass
            else:
                self.waiting_list.pop(index)

    def addReplica(self,job_id):
        job = self.jobs_list[job_id]
        pass

    def selectHostsNodes(self):
        availabel_nodes = self.getAvailabledNodes()
        if len(availabel_nodes) < NB_REPLICAS_INIT:
            return copy.deepcopy(availabel_nodes)
        else:
            return copy.deepcopy(random.sample(availabel_nodes, NB_REPLICAS_INIT))


    def getAvailabledNodes(self):
        nodes = [id for id in range(self.nb_nodes)]
        for i, task in enumerate(self.executing_task):
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
        if response.json()["started"]:#(job, node, start_time, execution_time)
            self.executing_task.append((job_id, int(id_node), response.json()['starting_time']))

        return response.json(), self.graphe_infos[self.id][id_node]
    
    
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

    data =  {'IP_ADDRESS': '172.16.97.9', 'graphe_infos': [[ -1.,  -1.,  -1.,  -1.,  -1.,  -1.,  -1.,  -1., 100.],
       [  0.,  -1.,  -1.,  -1.,  -1.,  -1.,  -1.,  -1., 100.],
       [  0.,   0.,  -1.,  -1.,  -1.,  -1.,  -1.,  -1.,  60.],
       [  0.,   0.,   0.,  -1.,  -1.,  -1.,  -1.,  -1.,  30.],
       [  0.,   0.,   0.,   0.,  -1.,  -1.,  -1.,  -1., 100.],
       [  0.,   0.,   0.,   0.,   0.,  -1.,  -1.,  -1.,  20.],
       [  0.,   0.,   0.,   0.,   0.,   0.,  -1.,  -1.,  20.],
       [  0.,   0.,   0.,   0.,   0.,   0.,   0.,  -1.,  20.],
       [100., 100.,  60.,  30., 100.,  20.,  20.,  20.,  -1.]], 'IPs_ADDRESS': ['172.16.97.19', '172.16.97.2', '172.16.97.27', '172.16.97.4', '172.16.97.5', '172.16.97.6', '172.16.97.7', '172.16.97.8'], 'infos': {0: {'latency': 100.0, 'id': 0, 'node_ip': '172.16.97.19', 'node_port': 8880}, 1: {'latency': 100.0, 'id': 1, 'node_ip': '172.16.97.2', 'node_port': 8881}, 2: {'latency': 60.0, 'id': 2, 'node_ip': '172.16.97.27', 'node_port': 8882}, 3: {'latency': 30.0, 'id': 3, 'node_ip': '172.16.97.4', 'node_port': 8883}, 4: {'latency': 100.0, 'id': 4, 'node_ip': '172.16.97.5', 'node_port': 8884}, 5: {'latency': 20.0, 'id': 5, 'node_ip': '172.16.97.6', 'node_port': 8885}, 6: {'latency': 20.0, 'id': 6, 'node_ip': '172.16.97.7', 'node_port': 8886}, 7: {'latency': 20.0, 'id': 7, 'node_ip': '172.16.97.8', 'node_port': 8887}}} #recieveObject()

    
    job_injector = JobInjector(
        nb_nodes = NB_NODES,
        graphe= data["graphe_infos"],
        ip=data["IP_ADDRESS"],
        local_execution=False
    )
    job_injector.writeOutput(f"{data}")
    
    
    job_injector.nodes_infos = data["infos"]
    job_injector.start()
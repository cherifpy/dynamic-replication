
#here i have to manage replica
import copy
import redis
from experiments.params import  (
    SERVER_REPLICA_MANAGER_PORT, 
    BD_LISTENING_PORT,
    MAX_EXECUTION_TIME,
    NB_REPLICAS_INIT,
    BANDWIDTH,
    NB_NODES,
)

from communication.send_data import recieveObject

from functions.costs import transfertTime

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
random.seed(1)

class JobInjector:
    def __init__(self,nb_nodes ,graphe, ip, local_execution) -> None:
        self.nb_nodes = NB_NODES-1
        self.id = nb_nodes-1
        self.nodes_infos = dict()
        self.api_server = None
        self.graphe_infos = graphe
        self.location_table = {}
        self.port = SERVER_REPLICA_MANAGER_PORT
        self.ip = ip
        self.nb_data_trasnfert = 0
        
        self.output = open(f"/tmp/log.txt",'w')
        
        str = "/tmp/temps_execution.txt"
        self.stats = open(str,'w')#self.writeStates(f"{job.id},{job.nb_task},{job.job_starting_time},{job.finishing_time}")
        self.stats.write('job_id,nb_tasks, starting_time,finishing_time')
        self.stats.close()

        self.stats_on_task = open("/tmp/stats_on_tasks.txt",'w')
        self.stats_on_task.write('job,task,node,starting_time,finishing_time,execution_time,id_dataset,transfert_time')
        self.stats_on_task.close()
        
        self.local_execution = local_execution
        self.num_evection = 0
        self.last_node_recieved = None
        self.data_sizes = {}
        self.nb_data_trasnfert_avoided = 0
        self.data: Dict[str, Data] = {} # type: ignore
        self.replicas: Dict[tuple, Replica] = {}
        self.previous_stats: Dict[str, Data] = {} # type: ignore
        self.requests_processed = {}
        self.waiting_list = []
        self.executing_task = [] # task with this format (job, node, start_time, execution_time)
        self.jobs_list = {} 
        self.dataset_counter = 0
        self.nb_jobs = 0
        self.id_dataset = 0
        self.running_job = {}
        self.historiques = {}


    def start(self,):
        if not self.nodes_infos:
            return False
        self.exp_start_time = time.time()
        job_id, job = self.generateJob()
        self.waiting_list.append((job_id,job))

        job_id, job = self.generateJob()
        self.waiting_list.append((job_id,job))
        j = 0
        while True:
            while j < len(self.waiting_list):
                job_started = False
                
                job_id, job = self.waiting_list[j]
                print(f"Job {job_id}")
                self.dataset_counter += 1
                
                host_nodes = self.selectHostsNodes()
                host_with_replica = []
                for i, host in enumerate(host_nodes):
                    
                    r = self.replicate(host, job_id, job.id_dataset, job.size_dataset)
                    if r: 
                        print(f"{i+1} Replica sended")
                        self.writeOutput(f"Replica of dataset {job.id_dataset} sended to {host}")
                        host_with_replica.append(host)
                        job.tasks_list[i].host_node = host
                    
                    else: 
                        print("no replica sended")

                for i,host in enumerate(host_with_replica):
                    
                    rep, latency = self.sendTaskToNode(host, job_id, job.tasks_list[i].execution_time,job.id_dataset)
                    if rep['started']:
                        self.writeOutput(f"Job {job_id} started")
                        self.writeOutput(f"Task {i} of job {job_id} started on node {host}")
                        print("========= Job started")
                        print(f"========= Task of job {job_id} started on node {host}")
                        job.tasks_list[i].starting_time = rep['starting_time']
                        job.tasks_list[i].host_id = host_nodes
                        job.tasks_list[i].executed = True
                        job.tasks_list[i].state = "Started"
                        job.executing_tasks.append((i, job.tasks_list[i].task_id))
                        job.ids_nodes.append(host)
                        job_started = True
                        job.starting_times.append(rep['starting_time'])
                        job.nb_task_not_lunched -=1
                        job.job_starting_time = time.time()
                if job_started:
                    self.running_job[job_id] = job
                    j+=1

            self.analyseOnCaseTwo()
            if len(self.running_job.keys()) == 0:
                print("========= All jobs executed")
                break


    def analyseOnCaseOne(self):
        """
            Cas 1: we have heterogenose task and dataset with 1 and only replica

        """
        delete = []
        for job_id in self.running_job.keys():
            end = False
            job = self.running_job[job_id]

            for i, task_id in job.executing_tasks:

                task = job.tasks_list[i]
                if  task.state == "Started" and task.starting_time + task.execution_time < time.time():
                    self.writeOutput(f"Task {task.task_id} on job {job_id} finished")
                    print(f"========= task on job {job_id} finished")
                    task.is_finished = True
                    task.state = "Finished"
                    self.wrtieStatsOnTasks(f"{job_id},{task.task_id},{task.host_node},{task.starting_time},{task.execution_time + task.starting_time},{task.execution_time},{task.id_dataset}")
                    if job.nb_task_not_lunched > 0: #arrived here
                        end = False
                        for n_task in job.tasks_list:
                            if n_task.state != "Finished":
                                new_task = n_task
                                break
                        rep, latency = self.sendTaskToNode(task.host_node,job_id,new_task.execution_time,job.id_dataset)

                        if rep["started"]:
                            job.executing_tasks.append((len(job.executing_tasks),new_task.task_id))
                            job.nb_task_not_lunched -=1
                            new_task.starting_time = rep['starting_time']
                            new_task.executed = True
                            new_task.state = "Started"
                            new_task.host_node = task.host_node
                            print(f"========= new task on job {job_id} started at node {task.host_node}")
                            self.writeOutput(f"Task {new_task.task_id} of job {job_id} started on node {task.host_node}")
                            print(job.executing_tasks)
                        else:
                            print("didn't start")
                    else:
                        job.finishing_time = time.time()
                        end = True
                else:
                    end = False

            if end:
                delete.append(job_id)

        for id in delete :
            print(f"========= job {job_id} finished")
            self.writeOutput(f"Job {id} finished")
            job = self.running_job[id]
            self.writeStates(f"{job.id},{job.nb_task},{job.job_starting_time},{job.finishing_time}")
            self.historiques[id] = copy.deepcopy(self.running_job[id])
            del self.running_job[id]
        return True
    
    def analyseOnCaseTwo(self):
        delete = []
        for job_id in self.running_job.keys():
            added = False
            end = False
            job = self.running_job[job_id]
            for i, task_id in job.executing_tasks:
                
                task = job.tasks_list[i]
                if  task.state == "Started" and task.starting_time + task.execution_time < time.time():
                    print(f"========= task on job {job_id} finished")
                    self.writeOutput(f"Task {task.task_id} on job {job_id} finished")
                    task.is_finished = True
                    task.state = "Finished"
                    t_time = transfertTime(BANDWIDTH, self.graphe_infos[self.id][task.host_node], job.size_dataset)
                    self.wrtieStatsOnTasks(f"{job_id},{task.task_id},{task.host_node},{task.starting_time},{task.execution_time + task.starting_time},{task.execution_time},{task.id_dataset},{t_time}")
                    if job.nb_task_not_lunched > 0: #arrived here
                        end = False
                        for n_task in job.tasks_list:
                            if n_task.state == "NotStarted":
                                new_task = n_task
                                break
                        rep, latency = self.sendTaskToNode(task.host_node,job_id,new_task.execution_time,job.id_dataset)

                        if rep["started"]:
                            job.executing_tasks.append((len(job.executing_tasks),new_task.task_id))
                            job.nb_task_not_lunched -=1
                            new_task.starting_time = rep['starting_time']
                            new_task.executed = True
                            new_task.state = "Started"
                            new_task.host_node = task.host_node
                            print(f"========= new task on job {job_id} started at node {task.host_node}")
                            self.writeOutput(f"Task {new_task.task_id} of job {job_id} started on node {task.host_node}")
                            #print(job.executing_tasks)
                        else:
                            print("didn't start")
                    else:
                        job.finishing_time = time.time()
                        end = True
                if task.state == "Started" and time.time() - task.starting_time > 3:
                    end = False
                    t_time = transfertTime(BANDWIDTH, self.graphe_infos[self.id][task.host_node], job.size_dataset)
                    added = self.addNewTaskOnNewNode(job_id)

                    if added: 
                        job.nb_task_not_lunched -=1
                        break

            if end: delete.append(job_id)

        for id in delete :
            print(f"========= job {id} finished")
            job = self.running_job[id]
            self.writeStates(f"{job.id},{job.nb_task},{job.job_starting_time},{job.finishing_time}")
            self.historiques[id] = copy.deepcopy(self.running_job[id])
            del self.running_job[id]
        return True

    def addNewTaskOnNewNode(self, job_id):

        job = self.running_job[job_id]
        if job.nb_task_not_lunched == 0:
            return False
        id_node = self.getAvailabelNodeForReplicating()
        if id_node:
            job.ids_nodes.append(id_node)
            print(-job.nb_task_not_lunched)
            task = job.tasks_list[-job.nb_task_not_lunched]
            r = self.replicate(id_node,job.id, id_dataset=job.id_dataset, ds_size=job.size_dataset)
            if r:
                self.writeOutput(f"Replica of dataset {job.id_dataset} sended to {id_node}")
                job.ids_nodes.append(id_node)
                rep, latency = self.sendTaskToNode(id_node,job.id,task.execution_time,job.id_dataset)
                if rep["started"]:
                    task.state = "Started"
                    job.executing_tasks.append((len(job.executing_tasks), task.task_id))
                    #print(job.executing_tasks)
                    task.starting_time = rep['starting_time']
                    task.executed = True
                    task.host_node = id_node
                    job.starting_times.append(rep['starting_time'])
                    print(f"========= other task on job {job.id} started on node {job.tasks_list[-job.nb_task_not_lunched].host_node} at {job.tasks_list[-job.nb_task_not_lunched].starting_time}")
                    self.writeOutput(f"Task {task.task_id} of job {job_id} started on node {task.host_node}")
                    return True
                
        return False
        
    def generateJob(self,):
        self.id_dataset +=1
        nb_tasks = 5 #random.randint(1, MAX_NB_TASKS)
        file_size = 1024*6 #random.randint(1, MAX_DATA_SIZE)
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

        job.tasks_list = [Task(f'task_{i}', random.randint(2,10), self.id_dataset) for i in range(nb_tasks)]

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

    def selectHostsNodes(self):
        availabel_nodes = self.getAvailabledNodes()
        if len(availabel_nodes) < NB_REPLICAS_INIT:
            return copy.deepcopy(availabel_nodes)
        else:
            return copy.deepcopy(random.sample(availabel_nodes, NB_REPLICAS_INIT))


    def getAvailabledNodes(self): 
        nodes = [id for id in range(self.nb_nodes)]
        candidates = copy.deepcopy(nodes)
        for i, job_id in enumerate(self.running_job.keys()):
            job = self.running_job[job_id]
            for node in nodes:
                if node in job.ids_nodes:
                    candidates.remove(node)
                
        return [] if len(candidates) == 0 else candidates
    
    def getAvailabelNodeForReplicating(self):
        nodes = [id for id in range(self.nb_nodes)]
        candidates = copy.deepcopy(nodes)
        for i, job_id in enumerate(self.running_job.keys()):
            job = self.running_job[job_id]
            for node in nodes:
                if node in job.ids_nodes:
                    candidates.remove(node)
                
        return None if len(candidates) == 0 else random.sample(candidates,1)[0]
    


    def replicate(self, host,job_id, id_dataset, ds_size):

        node_ip = self.nodes_infos[int(host)]["node_ip"]
        added = self.sendDataSet(id_node=host,ip_node=node_ip, id_ds=id_dataset, ds_size=ds_size) 
        if added:
            self.nb_data_trasnfert +=1
            cost = self.transfertCost(self.graphe_infos[self.id][host], ds_size)
            #self.writeTransfert(f"{job_id},{id_dataset},{self.id},{host},{ds_size},{cost},transfert1\n")
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
            print(f'replica ajoutée sur le noued {id_node} {r}\n')
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
        print(f"reponse recu {response.json()} du noeud {id_node}")
        if response.json()["started"]:#(job, node, start_time, execution_time)
            self.executing_task.append((job_id, int(id_node), response.json()['starting_time']))

        return response.json(), self.graphe_infos[self.id][id_node]
    
    
    def transfertCost(self, latency, data_size):
        bandwith_in_bits = BANDWIDTH*1024*1024*8
        size_in_bits = data_size*1024*8
        latency_in_s = latency/1000

        return latency_in_s + (size_in_bits/bandwith_in_bits)
    
    def writeStates(self,str):
        path = "/tmp/temps_execution.txt"
        self.stats = open(path,'a')
        self.stats.write(f"{str}\n")
        self.stats.close()

    def wrtieStatsOnTasks(self,str):
        self.stats_on_task = open("/tmp/stats_on_tasks.txt",'a')
        self.stats_on_task.write(f"{str}\n")
        self.stats_on_task.close()
    
    def writeOutput(self, str):
        self.output = open(f"/tmp/log.txt",'a')
        self.output.write(f"time: {self.getTime()}: {str}\n")
        self.output.close() 

    def getTime(self):
        return time.time() - self.exp_start_time
        
if __name__ == "__main__":

    data ={'IP_ADDRESS': '172.16.193.6', 'graphe_infos': [[-1., 20., 20., 20., 20.],
       [20., -1., 20., 20., 20.],
       [20., 20., -1., 20., 20.],
       [20., 20., 20., -1., 20.],
       [20., 20., 20., 20., -1.]], 'IPs_ADDRESS': ['172.16.193.20', '172.16.193.36', '172.16.193.37', '172.16.193.45'], 'infos': {0: {'latency': 20.0, 'id': 0, 'node_ip': '172.16.193.20', 'node_port': 8880}, 1: {'latency': 20.0, 'id': 1, 'node_ip': '172.16.193.36', 'node_port': 8881}, 2: {'latency': 20.0, 'id': 2, 'node_ip': '172.16.193.37', 'node_port': 8882}, 3: {'latency': 20.0, 'id': 3, 'node_ip': '172.16.193.45', 'node_port': 8883}}}
    job_injector = JobInjector(
        nb_nodes = NB_NODES,
        graphe= data["graphe_infos"],
        ip=data["IP_ADDRESS"],
        local_execution=False
    )
    #job_injector.writeOutput(f"{data}")
    
    
    job_injector.nodes_infos = data["infos"]
    job_injector.start()
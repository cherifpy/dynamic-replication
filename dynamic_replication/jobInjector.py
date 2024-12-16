
#here i have to manage replica
import copy
import multiprocessing.process
import redis
from experiments.params import  (
    SERVER_REPLICA_MANAGER_PORT, 
    MAX_EXECUTION_TIME,
    MAX_REPLICA_NUMBER,
    BD_LISTENING_PORT,
    NB_REPLICAS_INIT,
    MAX_DATA_SIZE,
    MAX_TIME_EXP,
    MAX_NB_TASKS,
    BANDWIDTH,
    NB_NODES,
    NB_JOBS,
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
#random.seed(1)

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
        self.exp_start_time = 0
        self.output = open(f"/tmp/log.txt",'w')
        
        str = "/tmp/temps_execution.txt"
        self.stats = open(str,'w')#self.writeStates(f"{job.id},{job.nb_task},{job.job_starting_time},{job.finishing_time}")
        self.stats.write('job_id,nb_tasks,task_execution_time,arriving_time,starting_time,finishing_time,dataset size,transfert time,nb replicas\n')
        self.stats.close()

        self.stats_on_task = open("/tmp/stats_on_tasks.txt",'w')
        self.stats_on_task.write('job,task,node,starting_time,finishing_time,execution_time,id_dataset,transfert_time\n')
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
        self.running_job: Dict[int, Job] = {}
        self.historiques = {}
        self.running_tasks = []
        self.index = 0

        self.df_jobs = pd.read_json("/home/csimohammed/code/dynamic_replication/experiments/jobs/jobs.json")
        self.nb_arriving_job = self.df_jobs.shape[0]
        self.end = False
        self.job_executed = []

    def SimulateArrivingJobs(self,lambda_rate=1,job_to_inject = 10):
        if not self.nodes_infos:
            return False
        self.exp_start_time = time.time()

        self.job_list = []
        j = 0
        i_job = 0
        inter_arrival_time = 2
        current_time = 0

        while True:
            
            while j < len(self.job_list):

                job_started = False                
                job_id, job = self.job_list[j]
                self.dataset_counter += 1                
                host_nodes = self.selectHostsNodes()                
                host_with_replica = []

                for i, host in enumerate(host_nodes):
                    r, t_transfert = self.replicate(host, job_id, job.id_dataset, job.size_dataset)
                    if r: 
                        job.nb_replicas +=1
                        job.job_starting_time = time.time()
                        print(f"{i+1} Replica sended")
                        self.writeOutput(f"Replica of dataset {job.id_dataset} sended to {host}")
                        host_with_replica.append(host)
                        t_start = time.time()
                        job.transfert_time = transfertTime(BANDWIDTH, 100, job.size_dataset)
                        self.wrtieStatsOnTasks(f"{-1},{job_id},{host},{t_start},{t_start + job.transfert_time},{job.transfert_time},{job.id_dataset}")
                    else: 
                        print("no replica sended")

                for i,host in enumerate(host_with_replica):
                    rep, latency = self.sendTaskToNode(host, job_id, job.tasks_list[i].execution_time,job.id_dataset)
                    if rep['started']:
                        
                        self.writeOutput(f"Job {job_id} started")
                        self.writeOutput(f"Task {i} of job {job_id} started on node {host}")
                        print("========= Job started")
                        print(f"========= Task of job {job_id} started on node {host}")
                        job.tasks_list[i].starting_time = rep['starting_time']+job.transfert_time
                        job.tasks_list[i].host_node = host
                        job.tasks_list[i].executed = True
                        job.tasks_list[i].state = "Started"
                        job.executing_tasks.append((i, job.tasks_list[i].task_id))
                        self.running_tasks.append((job_id, job.tasks_list[i].task_id, job.tasks_list[i].starting_time, job.tasks_list[i].execution_time, job.tasks_list[i].host_node))
                        job.ids_nodes.append(host)
                        job_started = True
                        job.starting_times.append(rep['starting_time'])
                        job.nb_task_not_lunched -=1
                        
                if job_started:
                    self.running_job[job_id] = job
                    self.waiting_list.append((job_id, job))
                    j+=1
                self.startOtherTasksWithoutReplication()

            ## inject n jobs
            _ = self.injectJobs(self.job_list)
            self.startOtherTasksWithoutReplication()
            if len(self.running_job.keys()) == 0 and self.index >= self.nb_arriving_job:
                print("========= All jobs executed")
                break

        print(j, len(self.job_list))

    def SimulateArrivingJobsWithMaxReplication(self):
        if not self.nodes_infos:
            return False
        self.exp_start_time = time.time()
        self.job_list = []
        j = 0
        i_job = 0
        inter_arrival_time = 2
        current_time = 0

        while True:
            
            while j < len(self.job_list):
                job_started = True
                job_id, job = self.job_list[j]
                self.dataset_counter += 1
                host_nodes = self.AllNodesNeeded(job)
                host_with_replica = []
                if len(host_nodes) != 0 and len(host_nodes) == job.nb_task:
                    for i, host in enumerate(host_nodes):
                        
                        r, t_transfert = self.replicateForAllNode(host, job_id, job.id_dataset, job.size_dataset)
                        if r: 
                            job.nb_replicas +=1
                            job.job_starting_time = time.time()
                            print(f"{i+1} Replica sended")
                            self.writeOutput(f"Replica of dataset {job.id_dataset} sended to {host}")
                            host_with_replica.append(host)
                            t_start = time.time()
                            if job.transfert_time == float("inf"):
                                job.transfert_time = transfertTime(BANDWIDTH, 100, job.size_dataset)
                            self.wrtieStatsOnTasks(f"{-1},{job_id},{host},{t_start},{t_start + job.transfert_time},{job.transfert_time},{job.id_dataset}")
                        else: 
                            print("no replica sended")
                    for i,host in enumerate(host_nodes):
                        
                        rep, latency = self.sendTaskToNode(host, job_id, job.tasks_list[i].execution_time,job.id_dataset)
                        if rep['started']:
                            
                            self.writeOutput(f"Job {job_id} started")
                            self.writeOutput(f"Task {i} of job {job_id} started on node {host}")
                            print("========= Job started")
                            print(f"========= Task of job {job_id} started on node {host}")
                            job.tasks_list[i].starting_time = rep['starting_time']+job.transfert_time
                            job.tasks_list[i].host_node = host
                            job.tasks_list[i].executed = True
                            job.tasks_list[i].state = "Started"
                            job.executing_tasks.append((i, job.tasks_list[i].task_id))
                            self.running_tasks.append((job_id, job.tasks_list[i].task_id, job.tasks_list[i].starting_time, job.tasks_list[i].execution_time, job.tasks_list[i].host_node))
                            job.ids_nodes.append(host)
                            job.starting_times.append(rep['starting_time'])
                            job.nb_task_not_lunched -=1
                        else:
                            job_started=False
                            
                    if job_started:
                        self.running_job[job_id] = job
                        j+=1
                        self.waiting_list.append((job_id, job))

                self.startOtherTasksWithoutReplication()

            _ = self.injectJobs(self.job_list)
            
            self.startOtherTasksWithoutReplication()

            if len(self.job_executed) == self.nb_arriving_job:
                print("========= All jobs executed")
                break

    def SimulateArrivingJobsWithSemiDynamic(self):
        if not self.nodes_infos:
            return False
        self.exp_start_time = time.time()
        self.job_list = []
        j = 0
        i_job = 0
        inter_arrival_time = 2
        current_time = 0

        while True:
            
            while j < len(self.job_list):
                job_started = False
                job_id, job = self.job_list[j]
                self.dataset_counter += 1
                host_nodes = self.AllAvailableNodes(job)
                host_with_replica = []
                
                for i, host in enumerate(host_nodes):
                    
                    r, t_transfert = self.replicateForAllNode(host, job_id, job.id_dataset, job.size_dataset)
                    if r: 
                        job.nb_replicas +=1
                        job.job_starting_time = time.time()
                        print(f"{i+1} Replica sended")
                        self.writeOutput(f"Replica of dataset {job.id_dataset} sended to {host}")
                        host_with_replica.append(host)
                        t_start = time.time()
                        if job.transfert_time == float("inf"):
                            job.transfert_time = transfertTime(BANDWIDTH, 100, job.size_dataset)
                        self.wrtieStatsOnTasks(f"{-1},{job_id},{host},{t_start},{t_start + job.transfert_time},{job.transfert_time},{job.id_dataset}")
                    else: 
                        print("no replica sended")
                for i,host in enumerate(host_with_replica):
                    
                    rep, latency = self.sendTaskToNode(host, job_id, job.tasks_list[i].execution_time,job.id_dataset)
                    if rep['started']:
                        
                        self.writeOutput(f"Job {job_id} started")
                        self.writeOutput(f"Task {i} of job {job_id} started on node {host}")
                        print("========= Job started")
                        print(f"========= Task of job {job_id} started on node {host}")
                        job.tasks_list[i].starting_time = rep['starting_time']+job.transfert_time
                        job.tasks_list[i].host_node = host
                        job.tasks_list[i].executed = True
                        job.tasks_list[i].state = "Started"
                        job.executing_tasks.append((i, job.tasks_list[i].task_id))
                        self.running_tasks.append((job_id, job.tasks_list[i].task_id, job.tasks_list[i].starting_time, job.tasks_list[i].execution_time, job.tasks_list[i].host_node))
                        job.ids_nodes.append(host)
                        job.starting_times.append(rep['starting_time'])
                        job.nb_task_not_lunched -=1
                        job_started = True
                    else:
                        job_started=False
                            
                if job_started:
                    self.running_job[job_id] = job
                    j+=1
                    self.waiting_list.append((job_id, job))

                self.startOtherTaskIfNodeAvailabel()

            _ = self.injectJobs(self.job_list)
            
            self.startOtherTaskIfNodeAvailabel()

            if len(self.job_executed) == self.nb_arriving_job:
                print("========= All jobs executed")
                break


    def AllNodesNeeded(self,job:Job):
        availabel_nodes = self.getAvailabledNodes()
        if len(availabel_nodes) >  job.nb_task:
            return copy.deepcopy(random.sample(availabel_nodes, job.nb_task))
        elif len(availabel_nodes) == job.nb_task:
            return copy.deepcopy(availabel_nodes)
        else: 
            return []
    
    def AllAvailableNodes(self,job:Job):
        availabel_nodes = self.getAvailabledNodes()
        if len(availabel_nodes) >  job.nb_task:
            return copy.deepcopy(random.sample(availabel_nodes, job.nb_task))
        elif len(availabel_nodes) <= job.nb_task:
            return copy.deepcopy(availabel_nodes)

    
    def reschedulOtherTasks(self):
        """
            in this strategies we can adapte the strategies of replicating to avoid having a blocked state
        """

        delete = []
        added = False
        jobs_keys = self.orderJobs()
        for job_id in jobs_keys:
            
            added = False
            end = True #False
            job = self.running_job[job_id]
            z = 0
            while z < len(job.executing_tasks):
                i, task_id = job.executing_tasks[z]
                task = job.tasks_list[i]

                if task.state != "Finished": end = False
                if  task.state == "Started" and task.starting_time + task.execution_time < time.time():
                    #end = False
                    print(f"========= task on job {job_id} finished")
                    self.writeOutput(f"Task {task.task_id} on job {job_id} finished")
                    task.state = "Finished"
                    job.execution_time = task.execution_time
                    #t_time = transfertTime(BANDWIDTH, self.graphe_infos[self.id][task.host_node], job.size_dataset)
                    self.wrtieStatsOnTasks(f"{job_id},{task.task_id},{task.host_node},{task.starting_time},{task.execution_time + task.starting_time},{task.execution_time},{task.id_dataset},{job.transfert_time}")
                    if job.nb_task_not_lunched > 0: #arrived here
                        for n_task in job.tasks_list:
                            if n_task.state == "NotStarted":
                                new_task = n_task
                                break
                        rep, latency = self.sendTaskToNode(task.host_node,job_id,new_task.execution_time,job.id_dataset)
                        if rep["started"]:
                            job.executing_tasks.append((len(job.executing_tasks),new_task.task_id))
                            job.nb_task_not_lunched -=1
                            new_task.starting_time = rep['starting_time']
                            new_task.state = "Started"
                            new_task.host_node = task.host_node
                            print(f"========= new task on job {job_id} started at node {task.host_node}")
                            self.writeOutput(f"Task {new_task.task_id} of job {job_id} started on node {task.host_node}")
                            self.running_tasks.append((job_id, new_task.task_id, new_task.starting_time, new_task.execution_time, new_task.host_node))
                            print(job.executing_tasks)
                        else:
                            print("didn't start")

                z+=1  


            for n_task in job.tasks_list:
                if n_task.state == "NotStarted":          
                    end = False
                    added = self.addNewTaskOnNewNode(job_id,job.transfert_time)
                    if added: 
                        job.nb_replicas +=1
                        job.nb_task_not_lunched -=1
                        print(f'une replica ajouter au job {job_id}')                 

            if end: delete.append(job_id)

        for id in delete :
            print(f"========= job {id} finished")
            job = self.running_job[id]
            job.finishing_time = time.time()
            self.writeStates(f"{job.id},{job.nb_task},{job.execution_time},{job.arriving_time},{job.job_starting_time},{job.finishing_time},{job.size_dataset},{job.transfert_time},{job.nb_replicas}")
            self.historiques[id] = copy.deepcopy(self.running_job[id])
            del self.running_job[id]
        return True  

    def injectJobs(self, job_list):

        if self.index < self.nb_arriving_job:
            a_time = int(time.time() - self.exp_start_time)
            jobs = self.df_jobs[self.df_jobs["arriving_time"] <= a_time]
            if jobs.shape[0] != 0:
                self.index += jobs.shape[0]
                for i, row in jobs.iterrows():
                    print(f"job {row['job_id']} arriving at {a_time} added at time {row['arriving_time']}")
                    job = Job(
                        arriving_time = time.time(),
                        nb_task=int(row['nb_tasks']),
                        execution_times=row['time'],
                        id_dataset=self.id_dataset,
                        size_dataset=row['dataset_size']
                    )

                    job.tasks_list = [Task(f'task_{i}', row['time'], self.id_dataset) for i in range(int(row['nb_tasks']))]

                    self.jobs_list[self.nb_jobs] = job
                    self.id_dataset +=1
                    self.nb_jobs +=1
                    job_list.append((self.nb_jobs, job))
                print(len(self.job_list))
            
                self.df_jobs = self.df_jobs[self.df_jobs["arriving_time"] > a_time]
        return self.job_list
    

    def injectJobsOnProcess(self, job_list):

        while self.index < self.nb_arriving_job:
            a_time = int(time.time() - self.exp_start_time)
            jobs = self.df_jobs[self.df_jobs["arriving_time"] == a_time]
            if jobs.shape[0] != 0:
                self.index += jobs.shape[0]
                for i, row in jobs.iterrows():
                    print(f"job {row['job_id']} arriving at {a_time} added at time {row['arriving_time']}")
                    job = Job(
                        arriving_time = time.time(),
                        nb_task=int(row['nb_tasks']),
                        execution_times=row['time'],
                        id_dataset=self.id_dataset,
                        size_dataset=row['dataset_size']
                    )

                    job.tasks_list = [Task(f'task_{i}', row['time'], self.id_dataset) for i in range(int(row['nb_tasks']))]

                    self.jobs_list[self.nb_jobs] = job
                    self.id_dataset +=1
                    self.nb_jobs +=1
                    job_list.append((self.nb_jobs, job))
                print(len(self.job_list))
            
            self.df_jobs = self.df_jobs[self.df_jobs["arriving_time"] != a_time]
        return self.job_list
    

    def replicateWithInjectingJobs(self,):
        """
            in this function i will inject 10 job on the infrastructure
        """

        delete = []
        added = False
        jobs_keys = self.orderJobs()
        for job_id in jobs_keys:
            
            added = False
            end = True #False
            job = self.running_job[job_id]
            z = 0
            while z < len(job.executing_tasks) and not added:
                i, task_id = job.executing_tasks[z]
                task = job.tasks_list[i]

                if task.state != "Finished": end = False
                if  task.state == "Started" and task.starting_time + task.execution_time < time.time():
                    #end = False
                    print(f"========= task on job {job_id} finished")
                    self.writeOutput(f"Task {task.task_id} on job {job_id} finished")
                    task.state = "Finished"
                    job.execution_time = task.execution_time
                    #t_time = transfertTime(BANDWIDTH, self.graphe_infos[self.id][task.host_node], job.size_dataset)
                    self.wrtieStatsOnTasks(f"{job_id},{task.task_id},{task.host_node},{task.starting_time},{task.execution_time + task.starting_time},{task.execution_time},{task.id_dataset},{job.transfert_time}")
                    if job.nb_task_not_lunched > 0: #arrived here
                        for n_task in job.tasks_list:
                            if n_task.state == "NotStarted":
                                new_task = n_task
                                break
                        rep, latency = self.sendTaskToNode(task.host_node,job_id,new_task.execution_time,job.id_dataset)
                        if rep["started"]:
                            job.executing_tasks.append((len(job.executing_tasks),new_task.task_id))
                            job.nb_task_not_lunched -=1
                            new_task.starting_time = rep['starting_time']
                            new_task.state = "Started"
                            new_task.host_node = task.host_node
                            print(f"========= new task on job {job_id} started at node {task.host_node}")
                            self.writeOutput(f"Task {new_task.task_id} of job {job_id} started on node {task.host_node}")
                            self.running_tasks.append((job_id, new_task.task_id, new_task.starting_time, new_task.execution_time, new_task.host_node))
                            print(job.executing_tasks)
                        else:
                            print("didn't start")
                    else:

                        job.ids_nodes.remove(task.host_node)
                        
                        #end = True
                #t_time = transfertTime(BANDWIDTH, self.graphe_infos[self.id][task.host_node], job.size_dataset)       
                if task.state == "Started" and time.time() - task.starting_time > job.transfert_time and not added and ((job.nb_task_not_lunched > 1) or (job.nb_task_not_lunched == 1 and job.execution_time != float('inf') and job.transfert_time < (job.execution_time/2))): #and job.nb_replicas < MAX_REPLICA_NUMBER:
                    
                    end = False
                    #t_time = transfertTime(BANDWIDTH, self.graphe_infos[self.id][task.host_node], job.size_dataset)
                    added = self.addNewTaskOnNewNode(job_id,job.transfert_time)

                    if added: 
                        #pass
                        job.nb_replicas +=1
                        job.nb_task_not_lunched -=1
                        #This change thinks in this cas i only add one replica peer job
                        print(f'une replica ajouter au job {job_id}')
                z+=1       

            if end: delete.append(job_id)

        for id in delete :
            print(f"========= job {id} finished")
            job = self.running_job[id]
            job.finishing_time = time.time()
            self.writeStates(f"{job.id},{job.nb_task},{job.execution_time},{job.arriving_time},{job.job_starting_time},{job.finishing_time},{job.size_dataset},{job.transfert_time},{job.nb_replicas}")
            self.historiques[id] = copy.deepcopy(self.running_job[id])
            del self.running_job[id]
        return True 
    
    def startOtherTaskIfNodeAvailabel(self):
        """
            dans cette fontion une tache est injectée si un noued et libre
        """

        delete = []
        added = False
        jobs_keys = self.orderJobs()
        for job_id in jobs_keys:
            
            added = False
            end = True #False
            job = self.running_job[job_id]
            z = 0
            nb_available_nodes = self.nbAvailabelNodes()
            while z < len(job.executing_tasks) and not added:
                i, task_id = job.executing_tasks[z]
                task = job.tasks_list[i]

                if task.state != "Finished": end = False
                if  task.state == "Started" and task.starting_time + task.execution_time < time.time():
                    #end = False
                    print(f"========= task on job {job_id} finished")
                    self.writeOutput(f"Task {task.task_id} on job {job_id} finished")
                    task.state = "Finished"
                    job.execution_time = task.execution_time
                    
                    self.wrtieStatsOnTasks(f"{job_id},{task.task_id},{task.host_node},{task.starting_time},{task.execution_time + task.starting_time},{task.execution_time},{task.id_dataset},{job.transfert_time}")
                    if job.nb_task_not_lunched > 0: 
                        for n_task in job.tasks_list:
                            if n_task.state == "NotStarted":
                                new_task = n_task
                                break
                        rep, latency = self.sendTaskToNode(task.host_node,job_id,new_task.execution_time,job.id_dataset)
                        if rep["started"]:
                            job.executing_tasks.append((len(job.executing_tasks),new_task.task_id))
                            job.nb_task_not_lunched -=1
                            new_task.starting_time = rep['starting_time']
                            new_task.state = "Started"
                            new_task.host_node = task.host_node
                            print(f"========= new task on job {job_id} started at node {task.host_node}")
                            self.writeOutput(f"Task {new_task.task_id} of job {job_id} started on node {task.host_node}")
                            self.running_tasks.append((job_id, new_task.task_id, new_task.starting_time, new_task.execution_time, new_task.host_node))
                            print(job.executing_tasks)
                        else:
                            print("didn't start")
                    else:
                        job.ids_nodes.remove(task.host_node)

                if job.nb_task_not_lunched > 0 and nb_available_nodes != 0:
                    end = False
                    added = self.addNewTaskOnNewNode(job_id,job.transfert_time)
                    if added: 
                        #pass
                        job.nb_replicas +=1
                        job.nb_task_not_lunched -=1
                        print(f'une replica ajouter au job {job_id}')
                        nb_available_nodes = self.nbAvailabelNodes()
                z+=1       

            if end: delete.append(job_id)

        for id in delete :
            print(f"========= job {id} finished")
            job = self.running_job[id]
            job.finishing_time = time.time()
            self.writeStates(f"{job.id},{job.nb_task},{job.execution_time},{job.arriving_time},{job.job_starting_time},{job.finishing_time},{job.size_dataset},{job.transfert_time},{job.nb_replicas}")
            self.historiques[id] = copy.deepcopy(self.running_job[id])
            self.job_executed.append(id)
            del self.running_job[id]
        return True 

    def startOtherTasksWithoutReplication(self):
        """
            in this function i will inject 10 job on the infrastructure
        """

        delete = []
        added = False
        jobs_keys = self.orderJobs()
        for job_id in jobs_keys:
            
            added = False
            end = True #False
            job = self.running_job[job_id]
            z = 0
            while z < len(job.executing_tasks) and not added:
                i, task_id = job.executing_tasks[z]
                task = job.tasks_list[i]

                if task.state != "Finished": end = False
                if  task.state == "Started" and task.starting_time + task.execution_time < time.time():
                    #end = False
                    print(f"========= task on job {job_id} finished")
                    self.writeOutput(f"Task {task.task_id} on job {job_id} finished")
                    task.state = "Finished"
                    job.execution_time = task.execution_time
                    #t_time = transfertTime(BANDWIDTH, self.graphe_infos[self.id][task.host_node], job.size_dataset)
                    self.wrtieStatsOnTasks(f"{job_id},{task.task_id},{task.host_node},{task.starting_time},{task.execution_time + task.starting_time},{task.execution_time},{task.id_dataset},{job.transfert_time}")
                    if job.nb_task_not_lunched > 0: #arrived here
                        for n_task in job.tasks_list:
                            if n_task.state == "NotStarted":
                                new_task = n_task
                                break
                        rep, latency = self.sendTaskToNode(task.host_node,job_id,new_task.execution_time,job.id_dataset)
                        if rep["started"]:
                            job.executing_tasks.append((len(job.executing_tasks),new_task.task_id))
                            job.nb_task_not_lunched -=1
                            new_task.starting_time = rep['starting_time']
                            new_task.state = "Started"
                            new_task.host_node = task.host_node
                            print(f"========= new task on job {job_id} started at node {task.host_node}")
                            self.writeOutput(f"Task {new_task.task_id} of job {job_id} started on node {task.host_node}")
                            self.running_tasks.append((job_id, new_task.task_id, new_task.starting_time, new_task.execution_time, new_task.host_node))
                            print(job.executing_tasks)
                        else:
                            print("didn't start")
                    else:
                        job.ids_nodes.remove(task.host_node)
                z+=1       

            if end: delete.append(job_id)

        for id in delete :
            print(f"========= job {id} finished")
            job = self.running_job[id]
            job.finishing_time = time.time()
            self.writeStates(f"{job.id},{job.nb_task},{job.execution_time},{job.arriving_time},{job.job_starting_time},{job.finishing_time},{job.size_dataset},{job.transfert_time},{job.nb_replicas}")
            self.historiques[id] = copy.deepcopy(self.running_job[id])
            self.job_executed.append(id)
            del self.running_job[id]
        return True 

    def replicatWithThreeStrategies(self,):
        """
            in this strategies we can adapte the strategies of replicating to avoid having a blocked state
        """

        delete = []
        added = False
        jobs_keys = self.orderJobs()
        for job_id in jobs_keys:
            
            added = False
            end = True #False
            job = self.running_job[job_id]
            z = 0
            while z < len(job.executing_tasks) and not added:
                i, task_id = job.executing_tasks[z]
                task = job.tasks_list[i]

                if task.state != "Finished": end = False
                if  task.state == "Started" and task.starting_time + task.execution_time < time.time():
                    #end = False
                    print(f"========= task on job {job_id} finished")
                    self.writeOutput(f"Task {task.task_id} on job {job_id} finished")
                    task.state = "Finished"
                    job.execution_time = task.execution_time
                    #t_time = transfertTime(BANDWIDTH, self.graphe_infos[self.id][task.host_node], job.size_dataset)
                    self.wrtieStatsOnTasks(f"{job_id},{task.task_id},{task.host_node},{task.starting_time},{task.execution_time + task.starting_time},{task.execution_time},{task.id_dataset},{job.transfert_time}")
                    if job.nb_task_not_lunched > 0: #arrived here
                        for n_task in job.tasks_list:
                            if n_task.state == "NotStarted":
                                new_task = n_task
                                break
                        rep, latency = self.sendTaskToNode(task.host_node,job_id,new_task.execution_time,job.id_dataset)
                        if rep["started"]:
                            job.executing_tasks.append((len(job.executing_tasks),new_task.task_id))
                            job.nb_task_not_lunched -=1
                            new_task.starting_time = rep['starting_time']
                            new_task.state = "Started"
                            new_task.host_node = task.host_node
                            print(f"========= new task on job {job_id} started at node {task.host_node}")
                            self.writeOutput(f"Task {new_task.task_id} of job {job_id} started on node {task.host_node}")
                            self.running_tasks.append((job_id, new_task.task_id, new_task.starting_time, new_task.execution_time, new_task.host_node))
                            print(job.executing_tasks)
                        else:
                            print("didn't start")
                    else:

                        job.ids_nodes.remove(task.host_node)
                        
                        #end = True
                #t_time = transfertTime(BANDWIDTH, self.graphe_infos[self.id][task.host_node], job.size_dataset)       
                if task.state == "Started" and time.time() - task.starting_time > job.transfert_time and not added and job.nb_task_not_lunched > 1 and job.nb_replicas < MAX_REPLICA_NUMBER:
                    end = False
                    #t_time = transfertTime(BANDWIDTH, self.graphe_infos[self.id][task.host_node], job.size_dataset)
                    added = self.addNewTaskOnNewNode(job_id,job.transfert_time)

                    if added: 
                        #pass
                        job.nb_replicas +=1
                        job.nb_task_not_lunched -=1
                        #This change thinks in this cas i only add one replica peer job
                        print(f'une replica ajouter au job {job_id}')
                z+=1       

            if end: delete.append(job_id)

        for id in delete :
            print(f"========= job {id} finished")
            job = self.running_job[id]
            job.finishing_time = time.time()
            self.writeStates(f"{job.id},{job.nb_task},{job.execution_time},{job.arriving_time},{job.job_starting_time},{job.finishing_time},{job.size_dataset},{job.transfert_time},{job.nb_replicas}")
            self.historiques[id] = copy.deepcopy(self.running_job[id])
            del self.running_job[id]
        return True  

    def orderJobs(self):
        return [key for key in self.running_job.keys()]
        jobs = copy.deepcopy(self.running_job)
        nb_jobs, jobs_ids = self.nbJobsToReplicat()
        nb_availabel_nodes = self.nbAvailabelNodes()

        sorted_keys =[]
        jobs_objcts = []
        
        for key in self.running_job.keys():
            job = self.running_job[key]
            objective = job.execution_time
            
            jobs_objcts.append((key, objective))

        sorted_keys = [t[0] for t in sorted(jobs_objcts, key=lambda k:k[1], reverse=True)]
        return sorted_keys      
        
        """if nb_availabel_nodes >= nb_jobs:
            return jobs_ids
        
        

        if nb_availabel_nodes < nb_jobs: # condition a changer
            sorted_keys =[]
            jobs_objcts = []
            
            for key in jobs_ids:
                job = self.running_job[key]
                if (job.transfert_time < job.execution_time and job.nb_task_not_lunched <= 2) or job.transfert_time > job.execution_time:
                    objective = float('-inf')
                else: objective = (job.execution_time + job.transfert_time) * (1/job.nb_task_not_lunched)
                jobs_objcts.append((key, objective))

            sorted_keys = [t[0] for t in sorted(jobs_objcts, key=lambda k:k[1], reverse=True)]
            return sorted_keys       
        
        #return sorted(self.running_job.keys(), key=lambda k: self.running_job[k].execution_time*self.running_job[k].execution_time, reverse=False)
        
        return jobs_ids"""

    def nbJobsToReplicat(self):
        nb_job = 0
        jobs_keys = []
        for job_id, job in self.running_job.items():
            """if job.transfert_time == float('inf') or job.execution_time == float('inf'):
                continue"""

            if job.transfert_time < job.execution_time and job.nb_task_not_lunched >= 2:
                nb_job +=1
            
            jobs_keys.append(job_id)
        
        return nb_job, jobs_keys

    def nbAvailabelNodes(self):
        #Ordonner les keys 
        nodes = [id for id in range(self.nb_nodes)]
        for i, job_id in enumerate(self.running_job.keys()):
            job = self.running_job[job_id]
            for task in job.tasks_list:
                if task.state == "Started" and task.host_node in nodes:
                    nodes.remove(task.host_node)
        return len(nodes)
        
    def addNewTaskOnNewNode(self, job_id,t_time):

        job = self.running_job[job_id]
        if job.nb_task_not_lunched == 0:
            return True
        id_node = self.getAvailabelNodeV2()

        if id_node:
            job.ids_nodes.append(id_node)
            print(-job.nb_task_not_lunched)
            task = job.tasks_list[-job.nb_task_not_lunched]
            t_start = time.time()
            r = self.replicate(id_node,job.id, id_dataset=job.id_dataset, ds_size=job.size_dataset)
            self.wrtieStatsOnTasks(f"{-1},{job_id},{id_node},{t_start},{t_start + job.transfert_time},{job.transfert_time},{job.id_dataset}")
            if True:
                self.writeOutput(f"Replica of dataset {job.id_dataset} sended to {id_node}")
                rep, latency = self.sendTaskToNode(id_node,job.id,task.execution_time,job.id_dataset)

                if rep["started"]:
                    job.ids_nodes.append(id_node)
                    task.state = "Started"
                    job.executing_tasks.append((len(job.executing_tasks), task.task_id))
                    print(job.executing_tasks)
                    task.starting_time = rep['starting_time']+t_time
                    task.executed = True
                    task.host_node = id_node
                    job.starting_times.append(rep['starting_time'])
                    print(f"========= other task on job {job.id} started on node {job.tasks_list[-job.nb_task_not_lunched].host_node} at {job.tasks_list[-job.nb_task_not_lunched].starting_time}")
                    self.writeOutput(f"Task {task.task_id} of job {job_id} started on node {task.host_node}")
                    self.running_tasks.append((job_id, task.task_id, task.starting_time, task.execution_time, task.host_node))
                    return True
                
        return False
    
    def analyseJobExecution(self): 
        ignored_job_list = []
        
        for job_id in self.running_job.keys():
            job = self.running_job[job_id]
            if job.execution_time  != float('inf') and job.execution_time < job.transfert_time and job.nb_task_not_lunched > 0:
                ignored_job_list.append(job_id)
        
        to_replicate = []
        for job_id in self.running_job.keys():
            for ignored_job_id in ignored_job_list:
                if ignored_job_id != job_id:
                    job = self.running_job[job_id]
                    ignored_job = self.running_job[ignored_job_id]

                    if job.transfert_time != float('inf') and ignored_job.transfert_time + ignored_job.execution_time < job.transfert_time:
                        to_replicate.append(ignored_job_id)

        #print(f'"""""""""""""""""" {to_replicate}""""""""""""""""')
        return to_replicate

    def addNewTasksOnNewNodes(self, job_id,t_time):

        job = self.running_job[job_id]
        if job.nb_task_not_lunched == 0:  
            return False
        id_node = self.getAvailabelNodeV2()
        while id_node is not None :
            job.ids_nodes.append(id_node)
            print(-job.nb_task_not_lunched)
            task = job.tasks_list[-job.nb_task_not_lunched]
            t_start = time.time()
            r = self.replicate(id_node,job.id, id_dataset=job.id_dataset, ds_size=job.size_dataset)
            self.wrtieStatsOnTasks(f"{-1},{job_id},{id_node},{t_start},{t_start + job.transfert_time},{job.transfert_time},{job.id_dataset}")
            if True:
                self.writeOutput(f"Replica of dataset {job.id_dataset} sended to {id_node}")
                rep, latency = self.sendTaskToNode(id_node,job.id,task.execution_time,job.id_dataset)

                if rep["started"]:
                    job.ids_nodes.append(id_node)
                    task.state = "Started"
                    job.executing_tasks.append((len(job.executing_tasks), task.task_id))
                    print(job.executing_tasks)
                    task.starting_time = rep['starting_time']+t_time
                    task.executed = True
                    task.host_node = id_node
                    job.starting_times.append(rep['starting_time'])
                    print(f"========= other task on job {job.id} started on node {job.tasks_list[-job.nb_task_not_lunched].host_node} at {job.tasks_list[-job.nb_task_not_lunched].starting_time}")
                    self.writeOutput(f"Task {task.task_id} of job {job_id} started on node {task.host_node}")
                    self.running_tasks.append((job_id, task.task_id, task.starting_time, task.execution_time, task.host_node))
                    job.nb_task_not_lunched -=1
                id_node = self.getAvailabelNodeV2()
        return False

    def generateJob(self,):
        
        nb_tasks = random.randint(2, MAX_NB_TASKS)
        file_size = random.randint(1024, MAX_DATA_SIZE)
        execution_time = random.uniform(0.1, MAX_EXECUTION_TIME)

        execution_times = []

        """for i in range(nb_tasks):
            execution_times.append(random.randint(1, MAX_EXECUTION_TIME))"""

        job = Job(
            arriving_time = time.time(),
            nb_task=nb_tasks,
            execution_times=execution_time,
            id_dataset=self.id_dataset,
            size_dataset=file_size
        )

        job.tasks_list = [Task(f'task_{i}', execution_time, self.id_dataset) for i in range(nb_tasks)]

        self.jobs_list[self.nb_jobs] = job
        self.id_dataset +=1
        self.nb_jobs +=1
        return job.id, job#(nb_tasks, execution_times, file_size) 
    
    def generateJobs(self,):
        job_list = []
        for i in range(NB_JOBS):
            id, job = self.generateJob()
            job_list.append((id,job))
        return job_list
    
    def staticJobs(self,):
        infos = [
            (5,0.3, 0,4120),
            (3,1, 1,3048),
            (4,0.2, 2,1024),
            (5,0.6, 3,2504),
            (4,0.8, 4,3304),
        ]
        job_list = []
        for i, info in enumerate(infos):
            job = Job(
                nb_task=info[0],
                execution_times=info[1],
                id_dataset=info[2],
                size_dataset=info[3],
                arriving_time=time.time(),
            )

            job.tasks_list = [Task(f'task_{i}', info[1], self.id_dataset) for i in range(info[0])]

            self.jobs_list[self.nb_jobs] = job
            self.id_dataset +=1
            self.nb_jobs +=1

            job_list.append((i,job))

        return job_list

    def staticJobsFromJSON(self):
        # Load the JSON file
        df = pd.read_json("/home/csimohammed/code/dynamic_replication/jobs.json", lines=True)
        job_list = []

        # Process each job in the DataFrame
        for _, info in df.iterrows():
            job = Job(
                nb_task=int(info["nb_tasks"]),
                execution_times=info["time"],
                id_dataset=info["id_dataset"],
                size_dataset=info["dataset_size"],
                arriving_time=time.time(),
            )

            # Generate the list of tasks for this job
            job.tasks_list = [
                Task(f'task_{i}', info["time"], info["id_dataset"])
                for i in range(int(info["nb_tasks"]))
            ]

            # Store the job in the jobs_list and increment counters
            self.jobs_list[self.nb_jobs] = job
            self.id_dataset += 1
            self.nb_jobs += 1

            # Append to job_list for returning
            job_list.append((self.nb_jobs - 1, job))

        return job_list

    def selectHostsNodes(self):

        availabel_nodes = self.getAvailabledNodes()
        if len(availabel_nodes) < NB_REPLICAS_INIT:
            return copy.deepcopy(availabel_nodes)
        else:
            print(availabel_nodes)
            return copy.deepcopy([random.choice(availabel_nodes)]) if NB_REPLICAS_INIT == 1 else copy.deepcopy(random.sample(availabel_nodes, NB_REPLICAS_INIT))
    
    def getAvailabledNodes(self): 
        nodes = [id for id in range(self.nb_nodes)]
        for i, job_id in enumerate(self.running_job.keys()):
            job = self.running_job[job_id]
            for task in job.tasks_list:
                if task.state == "Started" and task.host_node in nodes:
                    nodes.remove(task.host_node)

        return nodes 
    
    def getAvailabelNodeForReplicating(self):
        nodes = [id for id in range(self.nb_nodes)]
        candidates = copy.deepcopy(nodes)
        for i, job_id in enumerate(self.running_job.keys()):
            job = self.running_job[job_id]
            for node in nodes:
                if node in job.ids_nodes:
                    candidates.remove(node)
                
        return None if len(candidates) == 0 else random.sample(candidates,1)[0]  

    def getAvailabelNodeV2(self):
        nodes = [id for id in range(self.nb_nodes)]
        for i, job_id in enumerate(self.running_job.keys()):
            job = self.running_job[job_id]
            for task in job.tasks_list:
                if task.state == "Started" and task.host_node in nodes:
                    nodes.remove(task.host_node)

        return None if len(nodes) == 0 else copy.deepcopy([random.choice(nodes)])[0]                
    
    def getAvailabelNodes(self):
        nodes = [id for id in range(self.nb_nodes)]
        for i, job_id in enumerate(self.running_job.keys()):
            job = self.running_job[job_id]
            for task in job.tasks_list:
                if task.state == "Started" and task.host_node in nodes:
                    nodes.remove(task.host_node)

        return nodes 

    def replicate(self, host,job_id, id_dataset, ds_size):

        node_ip = self.nodes_infos[int(host)]["node_ip"]
        t_start = time.time()
        added = self.sendDataSet(id_node=host,ip_node=node_ip, id_ds=id_dataset, ds_size=ds_size) 
        t_end = time.time()
        t_transfert = time.time() - t_start
        print(f"temps de transfert {t_transfert}")
        
        if added:
            self.nb_data_trasnfert +=1
            cost = self.transfertCost(self.graphe_infos[self.id][host], ds_size)
            #self.writeTransfert(f"{job_id},{id_dataset},{self.id},{host},{ds_size},{cost},transfert1\n")
        return added,t_transfert


    def replicateForAllNode(self, host,job_id, id_dataset, ds_size):
        #Cette fonction juste pour replique sur tout les noeds 
        node_ip = self.nodes_infos[int(host)]["node_ip"]
        t_start = time.time()
        added = True 

        t_transfert = time.time() - t_start
        print(f"temps de transfert {t_transfert}")
        
        if added:
            self.nb_data_trasnfert +=1
            cost = self.transfertCost(self.graphe_infos[self.id][host], ds_size)
        return added,t_transfert     
    
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
            r = client.set(id_ds, content)
            #r = client.exists()
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
    
    def replicateOnThread(self, host,job_id, id_dataset, ds_size):
        p = multiprocessing.Process(target=self.replicate, args=(host,job_id, id_dataset, ds_size))
        p.start()
        return p

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

    data = {'IP_ADDRESS': '172.16.97.8', 'graphe_infos': [[ -1., 100., 100., 100., 100., 100., 100., 100., 100., 100., 100.],
       [100.,  -1., 100., 100., 100., 100., 100., 100., 100., 100., 100.],
       [100., 100.,  -1., 100., 100., 100., 100., 100., 100., 100., 100.],
       [100., 100., 100.,  -1., 100., 100., 100., 100., 100., 100., 100.],
       [100., 100., 100., 100.,  -1., 100., 100., 100., 100., 100., 100.],
       [100., 100., 100., 100., 100.,  -1., 100., 100., 100., 100., 100.],
       [100., 100., 100., 100., 100., 100.,  -1., 100., 100., 100., 100.],
       [100., 100., 100., 100., 100., 100., 100.,  -1., 100., 100., 100.],
       [100., 100., 100., 100., 100., 100., 100., 100.,  -1., 100., 100.],
       [100., 100., 100., 100., 100., 100., 100., 100., 100.,  -1., 100.],
       [100., 100., 100., 100., 100., 100., 100., 100., 100., 100.,  -1.]], 'IPs_ADDRESS': ['172.16.97.12', '172.16.97.13', '172.16.97.2', '172.16.97.24', '172.16.97.25', '172.16.97.28', '172.16.97.3', '172.16.97.5', '172.16.97.6', '172.16.97.7'], 'infos': {0: {'latency': 100.0, 'id': 0, 'node_ip': '172.16.97.12', 'node_port': 8880}, 1: {'latency': 100.0, 'id': 1, 'node_ip': '172.16.97.13', 'node_port': 8881}, 2: {'latency': 100.0, 'id': 2, 'node_ip': '172.16.97.2', 'node_port': 8882}, 3: {'latency': 100.0, 'id': 3, 'node_ip': '172.16.97.24', 'node_port': 8883}, 4: {'latency': 100.0, 'id': 4, 'node_ip': '172.16.97.25', 'node_port': 8884}, 5: {'latency': 100.0, 'id': 5, 'node_ip': '172.16.97.28', 'node_port': 8885}, 6: {'latency': 100.0, 'id': 6, 'node_ip': '172.16.97.3', 'node_port': 8886}, 7: {'latency': 100.0, 'id': 7, 'node_ip': '172.16.97.5', 'node_port': 8887}, 8: {'latency': 100.0, 'id': 8, 'node_ip': '172.16.97.6', 'node_port': 8888}, 9: {'latency': 100.0, 'id': 9, 'node_ip': '172.16.97.7', 'node_port': 8889}}}
    
    job_injector = JobInjector(
        nb_nodes = NB_NODES,
        graphe= data["graphe_infos"],
        ip=data["IP_ADDRESS"],
        local_execution=False
    )
    job_injector.writeOutput(f"{data}")
    
    
    job_injector.nodes_infos = data["infos"]
    job_injector.SimulateArrivingJobsWithSemiDynamic()
                
            





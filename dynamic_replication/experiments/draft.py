import copy
import time
from params import *

def updateRunningJobsList(self,):
    """
        Cas 1: we have heterogenose task and dataset with 1 and only replica

    """
    delete = []
    for job_id in self.running_job.keys():
        end = False
        job = self.running_job[job_id]
        for i, task_id in job.executing_tasks:
            task = job.tasks_list[i]
            if not task.state != "Finished" and task.starting_time + task.execution_time < time.time(): 
                print(f"========= task on job {job_id} finished")
                task.is_finished = True
                task.state = "Finished"
                if job.nb_task_not_lunched > 0: #arrived here
                    end = False
                    for n_task in job.tasks_list:
                        if n_task.task_id in job.executing_task:
                            new_task = n_task
                            break
                    rep, latency = self.sendTaskToNode(task.hode_node,job_id,new_task.execution_times,task.id_dataset)
                    if rep["started"]:
                        job.executing_tasks.append((len(job.executing_tasks),new_task.task_id))
                        job.nb_task_not_lunched -=1
                        new_task.starting_time = rep['starting_time']
                        new_task.executed = True
                        new_task.host_node = task.host_node
                        job.starting_times[i] = rep['starting_time']
                        print(f"========= other task on job {job_id} started")
                else:
                    job.finishing_time = time.time()
                    end = True
            else:
                end = False
        if end: delete.append(job_id)

    for id in delete :
        print(f"========= job {job_id} finished")
        job = self.running_job[id]
        self.writeStates(f"{job.id},{job.nb_task},{job.job_starting_time},{job.finishing_time}")
        self.historiques[id] = copy.deepcopy(self.running_job[id])
        del self.running_job[id]
    return True

def updateRunningJobsWithDynamicReplication(self,):
    delete = []
    for job_id in self.running_job.keys():
        end = False
        job = self.running_job[job_id]
        for i, task_id in job.executing_tasks:
            task = job.tasks_list[i]
            if not task.is_finished and task.starting_time + task.execution_time < time.time(): #Faut voir avec ca
                print(f"========= task on job {job_id} finished")
                task.state = "Finished"
                task.is_finished = True
                if job.nb_task_not_lunched > 0: #arrived here
                    end = False
                    for n_task in job.tasks_list:
                        if n_task.task_id not in job.executing_task:
                            new_task = n_task
                            break
                    
                    rep, latency = self.sendTaskToNode(task.hode_node,job_id,new_task.execution_times,task.id_dataset)
                    if rep["started"]:
                        new_task.state = "Started"
                        job.executing_tasks.append((len(job.executing_tasks),new_task.task_id))
                        job.nb_task_not_lunched -=1
                        new_task.starting_time = rep['starting_time']
                        new_task.executed = True
                        new_task.host_node = task.host_node
                        job.starting_times[i] = rep['starting_time']
                        print(f"========= other task on job {job_id} started")
                else:
                    job.finishing_time = time.time()
                    end = True
            else:
                end = False
                t_time = transfertTime(BANDWIDTH, self.graphe_infos[0][task.host_node], job.size_dataset)
                if time.time() - task.starting_time < t_time:
                    self.addNewTaskOnNewNode(job_id)
        if end: delete.append(job_id)

    for id in delete :
        print(f"========= job {job_id} finished")
        job = self.running_job[id]
        self.writeStates(f"{job.id},{job.nb_task},{job.job_starting_time},{job.finishing_time}")
        self.historiques[id] = copy.deepcopy(self.running_job[id])
        del self.running_job[id]
    return True


def startAJob(self, index):
        job_started = False
                
        job_id, job = self.waiting_list[index]
        print(f"Job {job_id}")
        self.dataset_counter += 1
        
        host_nodes = self.selectHostsNodes()
        host_with_replica = []
        for i, host in enumerate(host_nodes):
            
            r, t_transfert = self.replicate(host, job_id, job.id_dataset, job.size_dataset)
            if r: 
                print(f"{i+1} Replica sended")
                self.writeOutput(f"Replica of dataset {job.id_dataset} sended to {host}")
                host_with_replica.append(host)
                job.transfert_time = t_transfert
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

def startAJobOnThread(self, index):
        p = multiprocessing.Process(target=self.startAJob, args=(index,))
        p.start()
        return p

def addNewTaskOnNewNode(self, job_id,t_time):

        job = self.running_job[job_id]
        if job.nb_task_not_lunched == 0:
            return True
        id_node = self.getAvailabelNodeV2()

        if id_node:
            job.ids_nodes.append(id_node)
            print(-job.nb_task_not_lunched)
            task = job.tasks_list[-job.nb_task_not_lunched]

            r = self.replicate(id_node,job.id, id_dataset=job.id_dataset, ds_size=job.size_dataset)

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


def checkIfNeedForAddingReplication(self,job, hosts):
        for index, job in enumerate(self.waiting_list):
            if job.nb_task_not_lunched != 0:
                #here lancer les autres tache si ya moyenne sinon attendre 
                pass
            else:
                self.waiting_list.pop(index)



def getAvailabledNodes(self): 
        nodes = [id for id in range(self.nb_nodes)]
        candidates = copy.deepcopy(nodes)
        for i, job_id in enumerate(self.running_job.keys()):
            job = self.running_job[job_id]
            for node in nodes:
                if node in job.ids_nodes:
                    candidates.remove(node)
                
        return [] if len(candidates) == 0 else candidates
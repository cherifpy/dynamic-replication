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

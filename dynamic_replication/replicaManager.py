#here i have to manage replica
import copy
import redis
from exp.params import  (
    NB_NODES, 
    SERVER_REPLICA_MANAGER_PORT, 
    PATH_TO_TASKS, 
    SERVER_REPLICA_MANAGER_PORT, 
    MEMCACHED_LISTENING_PORT,
    BANDWIDTH,
    ENABEL_MIGRATION,
    TIME_SLOT,
    TTL_MIN,
    EXECUTION_LOCAL,
    MAX_MIGRATIONS,
    MIN_REQUESTS,
    THRESHOLD
)

from communication.send_data import recieveObject
from communication.messages import Task
from communication.replicaManagerServer import ReplicaManagerServer
from protos.migration_using_KNN import manageUsingKNN
from protos.migration_best_case import (
    manageEvictionForBest,
    predictNextUssage,
    bestMigration,
)

from functions.costs import (
    nodeImportanceV2, 
    transefrtWithGain, 
    transfertTime,
    nodeImportance,
    minimizingTimeTransfert
    )

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


class ReplicaManager:
    
    def __init__(self,nb_nodes ,traces_path,graphe, ip, local_execution) -> None:
        self.nb_nodes = nb_nodes-1
        self.id = nb_nodes-1
        self.traces_path = PATH_TO_TASKS#"/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/exp/traces/random_subset.csv"
        self.nodes_infos = dict()
        self.api_server = None
        self.graphe_infos = graphe
        self.location_table = {}
        self.port = SERVER_REPLICA_MANAGER_PORT
        self.ip = ip
        self.nb_data_trasnfert = 0
        self.output = open(f"/tmp/log_M.txt",'a')
        #str = "/tmp/transfert_M.txt" if ENABEL_MIGRATION else "/tmp/transfert.txt"
        #self.transfert = open(str,'w')
        self.local_execution = local_execution
        self.num_evection = 0
        self.last_node_recieved = None
        self.data_sizes = {}
        self.nb_data_trasnfert_avoided = 0
        self.data: Dict[str, Data] = {}
        self.replicas: Dict[tuple, Replica] = {}
        self.previous_stats: Dict[str, Data] = {}
        self.requests_processed = {}
           
    
    def startV5(self):

        if not self.nodes_infos:
            return False
        traces = pd.read_csv(self.traces_path)

        popularities = self.getPopularities(traces)
        time = 0
        for index, row in traces.iterrows():
            
            b, self.nodes_infos = self.collecteData()
            task_infos = {'time' : row["time_compute (s)"],  'application_type': row["application_type"]}
            task = Task(id_task=row["id_task"],id_node= row["node_id"],infos= task_infos,id_dataset= row["dataset"],ds_size=row["dataset_size"])

            self.data_sizes[task.id_dataset] = task.ds_size
            
            if task.id_dataset not in self.data.keys(): 
                self.data[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None,nb_requests_on_traces=popularities[task.id_dataset])
                if task.id_dataset not in self.previous_stats:
                    self.previous_stats[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None,nb_requests_on_traces=popularities[task.id_dataset])
            
            self.data[task.id_dataset].updateDataState(task.id_node)
            
            node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
            node_port = self.nodes_infos[int(task.id_node)]["node_port"]
            response, latency = self.sendTask(task,node_port, node_ip)
            eviction = True  
            self.writeOutput(f"==============================Task {index} {task.id_task}\n")
            
            if response["sendData"]:
                eviction = self.sendDataToTask(task=task, latency=latency)
                
                if eviction and ENABEL_MIGRATION:
                    i = 0
                    if 'keys' in self.nodes_infos[task.id_node].keys():
                        candidates = copy.deepcopy(self.nodes_infos[task.id_node]["keys"])
                    else:
                        candidates = []

                    while eviction and len(candidates) > 0:
                        condidate = candidates[i] 
                        r_eviction = self.managerAvectionM1(task.id_node, condidate)#, self.data[condidate].size)
                        if r_eviction["send"]: 
                            id_dst_node = r_eviction["id_dst_node"]
                            self.writeOutput(f"send {condidate} from {task.id_node} and send it to {id_dst_node}\n")
                            r = False
                            r = self.deleteAndSend(id_src_node=task.id_node,id_dst_node=id_dst_node, id_dataset=condidate, ds_size=self.data[condidate].size)

                        if not r_eviction["send"] or not r:
                            self.writeOutput(f"delete {condidate} from {task.id_node}\n")
                            self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
                            del self.replicas[(condidate, task.id_node)]
                            self.data[condidate].updateNbReplica(add=False)
                        b, self.nodes_infos = self.collecteData()
                        eviction = self.sendDataToTask(task=task, latency=latency)
                        i+=1
                    self.writeOutput(f"resultats de l'envoi de la donnée {not eviction}\n")   
                  
                if eviction and not ENABEL_MIGRATION:
                    i = 0

                    if 'keys' in self.nodes_infos[task.id_node].keys(): candidates = copy.deepcopy(self.nodes_infos[task.id_node]["keys"])
                    else: candidates = []
                    print(f"task {task.id_task}")
                    while eviction and len(candidates) > 0:
                        condidate = candidates[i] 
                        self.writeOutput(f"delete {condidate} from {task.id_node}\n") 
                        d = self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
                        print(f"delete {d}\n")
                        self.data[condidate].updateNbReplica(add=False)
                        b, self.nodes_infos = self.collecteData()
                        eviction = self.sendDataToTask(task=task, latency=latency)
                        
                        i+=1
                    self.writeOutput(f"resultats de l'envoi de la donnée {not eviction}")  
                if not eviction:
                    if (task.id_dataset, task.id_node) not in self.replicas.keys(): self.replicas[(task.id_dataset, task.id_node)] = Replica(task.id_dataset, task.id_node)
            else:
                self.writeTransfert(f"{task.id_task},{task.id_dataset},-1,{task.id_node},{task.ds_size},0,NoTransfert\n")
            if time == TIME_SLOT:
                self.data, self.previous_stats = Data.iniTDataTTL(self.data)
                self.initNodeImportance()
                self.initReplicaTTL()
                time = 0
            else:
                time+=1
        return True
    #used a copie
    def collecteData(self, node=-1):
        if len(self.nodes_infos.keys()) == 0:
            return False, {}
        
        if node!=-1:
            url = f'http://{self.nodes_infos[node]["node_ip"]}:{self.nodes_infos[node]["node_port"]}/infos'
            response = requests.get(url).json()
            self.nodes_infos[node]["storage_space"] = response["storage_space"]
            self.nodes_infos[node]["remaining_space"] = response["remaining_space"]
            self.nodes_infos[node]["keys"] = response['keys']
            self.nodes_infos[node]["popularities"] = response["popularities"] 
            return True, self.nodes_infos
        
        self.location_table = {}
        for key in self.nodes_infos.keys():
            url = f'http://{self.nodes_infos[key]["node_ip"]}:{self.nodes_infos[key]["node_port"]}/infos'
            response = requests.get(url).json()
    
            self.nodes_infos[key]["storage_space"] = response["storage_space"]
            self.nodes_infos[key]["remaining_space"] = response["remaining_space"]
            self.nodes_infos[key]["keys"] = response['keys']
            self.nodes_infos[key]["popularities"] = response["popularities"] 
        return True, self.nodes_infos
    
    #used
    #TODO aussi
    def searchForDataOnNeighbors(self, id_node, dataset):
        """
            need to change this to select from where to get th data
        """
        locations = []
        latency = []
        for node, c in enumerate(self.graphe_infos[id_node][:-1]):
            if c > 0 and c < self.graphe_infos[self.id][node] and dataset in self.nodes_infos[node]["keys"]:
                locations.append(node)                
                latency.append(c)
            
        if len(locations) == 0:
            return None, None
        
        i_min = np.argmin(latency)

        return latency[i_min], locations[i_min]
    
    def searchOnAllNetwork(self, id_node, dataset):
        """
            need to change this to select from where to get th data
        """
        locations = []
        latency = []
        min_cost_location = None
        min_cost = float('inf')
        for node, c in enumerate(self.graphe_infos[id_node][:-1]):
            if node!=id_node and dataset in self.nodes_infos[node]["keys"]:
                _, cost =  djikstra(self.graphe_infos, node, id_node)
                locations.append(node)                
                latency.append(cost)
                if cost < min_cost:
                    min_cost = cost
                    min_cost_location = node
        _, cost_from_repo =  djikstra(self.graphe_infos, 8, id_node)    
        if min_cost > cost_from_repo:
            return None, None
        
        return min_cost, min_cost_location
    
    
    def sendDataToTask(self, task, latency=None):
        node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
        node_port = self.nodes_infos[int(task.id_node)]["node_port"]
        #_,l = self.searchForDataOnNeighbors(id_node=task.id_node, dataset=task.id_dataset)
        lat, l = self.searchOnAllNetwork(id_node=task.id_node, dataset=task.id_dataset)
        added = False
        if l != None:
            added = self.askForATransfert( 
                src= l,
                dst=task.id_node,
                id_dataset=task.id_dataset,
                size_ds=task.ds_size
            )
            if added: 
                self.data[task.id_dataset].updateNbReplica(add=True)
                cost = self.transfertCost(lat, task.ds_size)
                self.nb_data_trasnfert +=1
                self.writeTransfert(f"{task.id_task},{task.id_dataset},{l},{task.id_node},{task.ds_size},{cost},transfert2\n")
                self.replicas[(task.id_dataset, l)].nb_requests+=1
                return not added
            
        if not added :
            added = self.sendDataSet(id_node=task.id_node,ip_node=node_ip, id_ds=task.id_dataset, ds_size=task.ds_size) 
            if added:
                self.data[task.id_dataset].updateNbReplica(add=True)
                self.nb_data_trasnfert +=1
                cost = self.transfertCost(latency, task.ds_size)
                self.writeTransfert(f"{task.id_task},{task.id_dataset},{self.id},{task.id_node},{task.ds_size},{cost},transfert1\n")
        
        return not added
    
    #used a copie
    def sendTask(self, task:Task, port, ip="localhost"):
        data = {"task": task.to_json(), "type":"task"}
        url = f'http://{ip}:{port}/execut'
        
        if self.isNeighbors(task.id_node):
            response = requests.post(url, json=data)
            return response.json(), self.graphe_infos[self.id][task.id_node]
        
        else:
            path, cost =  djikstra(self.graphe_infos, self.id, task.id_node)
            n = path.pop(0)
            next_dst = path.pop(0)
            ip_n = self.nodes_infos[int(next_dst)]["node_ip"]
            port_n = self.nodes_infos[int(next_dst)]["node_port"] 
            
            data_to_send = {
                    'data' : data,
                    'url':url,
                    'methode':'POST',
                    'path': path,
                    'target':task.id_node, 
                    'id_ds':task.id_dataset
                }
            
            url2 = f'http://{ip_n}:{port_n}/process'
            
            response = requests.post(url2, json=data_to_send)
            self.updateNodeImportance(path=path)
            return response.json(), cost

    #used a copie
    def manageEviction(self, id_node, id_ds, ds_size):
        """
            ici je supprimer direct si ka données et dans les voisie
        """
        if self.data[id_ds].nb_replica >= TTL_MIN :
            return {"delete":True, "send":False}

        n = self.isOnNeighbords(id_node, id_ds)
        if len(n) != 0:
            return {"delete":True, "send":False} #demander au noeud de juste supprimer la données

        else:
            min_access_and_transfet_time = float('inf')
            node = None

            for id_neighbors in range(self.nb_nodes):
                if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and self.nodes_infos[id_neighbors]["remaining_space"] > (((ds_size+5120)*1024)):
                    cost = self.transfertCost(self.graphe_infos[int(id_node)][id_neighbors], ds_size) 
                    if cost <= min_access_and_transfet_time:
                        min_access_and_transfet_time = cost
                        node = id_neighbors

            return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}

    def manageEvictionV2(self, id_node, id_ds, ds_size, space_availabel):
        """
            here i will use the TTL to decide if a had to migrate or send 
        """
        if self.data[id_ds].nb_replica >= TTL_MIN :
            return {"delete":True, "send":False}

        n = self.isOnNeighbords(id_node, id_ds)
        if len(n) != 0:
            return {"delete":True, "send":False} #demander au noeud de juste supprimer la données

        else:
            min_access_and_transfet_time = float('inf')
            node = None

            for id_neighbors in range(self.nb_nodes):
                if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and space_availabel > (((ds_size+5120)*1024)):
                    cost = self.transfertCost(self.graphe_infos[int(id_node)][id_neighbors], ds_size) 
                    if cost <= min_access_and_transfet_time:
                        min_access_and_transfet_time = cost
                        node = id_neighbors

        return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}
    
    def manageEvictionWITHTTL(self, id_node, id_ds, ds_size):
        """
            ici je supprimer direct si ka données et dans les voisie
            pourquoi pas choisir la donnée avec un tres TTL pour la supprimer du cache
        """
        n = self.isOnNeighbords(id_node, id_ds)
        if len(n) != 0:
            return {"delete":True, "send":False} #demander au noeud de juste supprimer la données

        else:
            min_access_and_transfet_time = float('inf')
            node = None

            for id_neighbors in range(self.nb_nodes):
                if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and self.nodes_infos[id_neighbors]["remaining_space"] > (((ds_size+5120)*1024)):
                    cost = self.transfertCost(self.graphe_infos[int(id_node)][id_neighbors], ds_size) 
                    if cost <= min_access_and_transfet_time:
                        min_access_and_transfet_time = cost
                        node = id_neighbors

            return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}
        
    def sendDataSet(self,id_node, ip_node, id_ds,ds_size):

        file_name = '/tmp/tmp.bin'
        file_size_octet = int(ds_size)*1024
        with open(file_name, "wb") as p:
            p.write(os.urandom(file_size_octet))
        with open(file_name, "rb") as p:
            content = p.read()
        servers = [f"{ip_node}:{MEMCACHED_LISTENING_PORT}"]  # Adresse du serveur Memcached
        
        #client = pylibmc.Client(servers, binary=True, behaviors={"tcp_nodelay": True})
        client = redis.Redis(host=ip_node, port=MEMCACHED_LISTENING_PORT, db=0, decode_responses=True)
        self.last_node_recieved = ip_node
        try:
            r = client.set(id_ds, content)
            print(f'ajouter {r}\n')
            return True
        except:
            return False

        return not r 
    
    def accessData(self, id_node, id_dataset, ip="localhost"):

        url = f'http://{self.nodes_infos[id_node]["node_ip"]}:{self.nodes_infos[id_node]["node_port"]}/access-data'

        response = requests.get(url,params={
            'id_dataset':id_dataset
        })
        
        return response.json()

    def accessDataOnThread(self, id_node, id_dataset, ip="localhost"):
        process = threading.Thread(target=self.accessData, args=(id_node, id_dataset))
        process.start()

        return process
    
    def askForATransfert(self, src, dst, id_dataset,size_ds):
        
        url = f'http://{self.nodes_infos[src]["node_ip"]}:{self.nodes_infos[src]["node_port"]}/transfert'
        
        data = {
            "dst_id": dst,
            "dst_ip": self.nodes_infos[dst]["node_ip"], 
            "id_dataset": id_dataset,
            "size_ds": size_ds
        }

        response = requests.post(url, json=data)
        return response.json()["response"] #Todo this shit
    
    def deleteAndSend(self, id_src_node, id_dst_node, id_dataset, ds_size):
        url = f'http://{self.nodes_infos[id_src_node]["node_ip"]}:{self.nodes_infos[id_src_node]["node_port"]}/send-and-delete'

        response = requests.get(url,params={
            'id_dataset':id_dataset,
            'ds_size': ds_size,
            'ip_dst_node': self.nodes_infos[id_dst_node]["node_ip"],
            'port_dst_node':self.nodes_infos[id_dst_node]["node_port"]
        })

        if response.json()["sended"]:
            cost = self.transfertCost(self.graphe_infos[int(id_src_node)][int(id_dst_node)],ds_size)
            self.updateReplica(id_dataset,id_src_node, id_dst_node)
            self.writeTransfert(f"null,{id_dataset},{id_src_node},{id_dst_node},{ds_size},{cost},migration\n")
            print("migration faite \n")
            self.nodes_infos[id_src_node]['remaining_space'] = response.json()['remaining_space']
            self.notifyNode(id_dst_node,self.nodes_infos[id_dst_node]['node_ip'],self.nodes_infos[id_dst_node]['node_port'] , id_dataset, add=True)

        self.writeOutput(f"resultat du transfert {response.json()}\n")
        return response.json()["sended"]
    
    def deleteFromCache(self,node_id, node_ip, node_port, id_dataset):
        url = f'http://{node_ip}:{node_port}/delete-data'

        response = requests.get(url,params={
            'id_dataset':id_dataset,
        })

        self.nodes_infos[node_id]["remaining_space"] = response.json()["remaining_space"]

        self.notifyNode(node_id,node_ip,node_port , id_dataset, add=False)
        if response.json()['reponse'] == 1:
            self.writeOutput(f"{id_dataset} deleted from {node_id}\n")
        self.writeOutput(f"resulta de sup de {id_dataset} = {response.json()}\n")
        return True if response.json()['reponse'] == 1 else False

    def notifyNode(self, id_node,ip_node, port_node, id_dataset, add):
        url = f'http://{ip_node}:{port_node}/notify'
        
        data = { 
            "id_dataset": id_dataset,
            "add":1 if add else 2
        }
        
        response = requests.post(url, json=data)
        self.nodes_infos[id_node]['remaining_space'] = response.json()['remaining_space']
        #print(response.json()["response"]),
        #self.writeOutput(f"resultat du transfert {response.json()}\n")
        return response.json()

    def deleteAndSendOnThread(self, id_src_node, id_dst_node, id_dataset, ds_size):
        
        sending_process = threading.Thread(target=self.deleteAndSend, args=(id_src_node,id_dst_node, id_dataset, id_dataset,ds_size))
        #sending_process = multiprocessing.Process(target=self.sendDaaSet, args=[ip_node, id_dataset, ds_size])
        sending_process.start()

        return sending_process
        

    def isOnNeighbords(self,node,id_ds):
        n = []
        for id_node, infos in self.nodes_infos.items():
            if self.graphe_infos[int(id_node)][int(node)] > 0 and id_ds in infos["keys"]:
                n.append(id_node)
        return n
    

    def dataOnNode(self,id_node, id_dataset):
        if id_node in self.local_execution[id_dataset]:
            return True
        
        return False

    def searchReplicaDistination(self,id_node,id_ds, ds_size):
        """
            here i will use the TTL to decide if a had to migrate or send 
            And this also serach for the distination node form here
        """

        #partie TTL
        data = self.data[id_ds]
        p = 0 if id_node not in self.data[id_ds].popularity_peer_noed.keys() else self.data[id_ds].popularity_peer_noed[id_node]
        if p == 0 : return {"delete":True, "send":False} #supp si le TTL l'exige => bcp de donnée dans l'infra


        #partie ENVOI
        if len(self.isOnNeighbords(id_node, id_ds)) != 0: return {"delete":True, "send":False} #demander au noeud de juste supprimer la données
        
        optimal_cost = -1
        #optimal_cost = float('inf')
        node = None

        for id_neighbors in range(self.nb_nodes):
            space_availabel = self.nodes_infos[id_neighbors]["remaining_space"]
            if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and (space_availabel > (((ds_size+1024)*1024))):
                self.writeOutput(f"why not to send {id_ds} from {id_node} to {id_neighbors} {self.graphe_infos[int(id_node)][id_neighbors]}\n")
                popularity = 0 if id_node not in self.data[id_ds].popularity_peer_noed.keys() else self.data[id_ds].popularity_peer_noed[id_neighbors]

                cost =  transefrtWithGain(
                    b=BANDWIDTH,
                    l=self.graphe_infos[int(id_node)][id_neighbors],
                    s=ds_size,
                    n=p, 
                )
                """
                cost = transfertTime(
                    b=BANDWIDTH,
                    l=self.graphe_infos[int(id_node)][id_neighbors],
                    s=ds_size,
                )"""
                if cost > optimal_cost:
                    optimal_cost = cost
                    node = id_neighbors

        return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}

    def sendDataSetOnThread(self, id_node,ip_node, id_dataset, ds_size, process:Optional[threading.Thread]):
        
        sending_process = threading.Thread(target=self.sendDataSet, args=(id_node,ip_node, id_dataset, ds_size))
        #sending_process = multiprocessing.Process(target=self.sendDataSet, args=[ip_node, id_dataset, ds_size])
        sending_process.start()

        return sending_process
    
    #Note used
    def getEmptyNode(self):
        nodes = []
        for i, node in enumerate(self.nodes_infos):
            if node["remaining_space"] == node["storage_space"]:
                nodes.append(i)
        return nodes   
         
    #Note used
    def selectNodeBySpace(self, task:Task):
        nodes = []
        if self.datasetOnNeigbors(task.id_node, task.id_dataset):
            return 
        for i, node in enumerate(self.nodes_infos):
            if not node["remaining_space"] == 0:
                if node["remaining_space"] < task.ds_size:
                    continue
                else:
                    nodes.append(i)
            else:
                nodes.append(i)
        return nodes
    
    #Note used
    def choseNode(self, task:Task):
        if self.datasetOnNeigbors(task.id_node, task.id_dataset):
            return 
        for i, node in enumerate(self.nodes_infos):
            if node["remaining_space"] == 0 or node["remaining_space"] < task.ds_size:
                continue
            else:
                return i
        return False
    
    def getDataSetLocation(self,id_ds):
        return self.location_table[id_ds] if id_ds in self.location_table.keys() else []

    
    
    def startFlaskServer(self):
        pass
        self.api_server = ReplicaManagerServer( 
            host=self.ip,
            port=self.port,
            replica_manager=self
            )
        server_is_running = self.api_server.run()


    def addToLocationTable(self, id_node, id_dataset):
        if id_dataset in self.location_table.keys():
            if id_node not in self.location_table[id_dataset]:
                self.location_table[id_dataset].append(id_node)
            return True
            
        else:
            self.location_table[id_dataset] = [id_node]
            return True
        
        return False

    def addDataToTable(self,id_node, id_dataset):
        if 'keys' not in self.nodes_infos[id_node].keys():
            self.nodes_infos[id_node]['keys'] = []
        if id_dataset not in self.nodes_infos[id_node]['keys']:
            self.nodes_infos[id_node]['keys'].append(id_dataset)
            #self.writeOutput(f'{self.nodes_infos[id_node]['keys']}')
            return True
        else:
            self.nodes_infos[id_node]['keys'].remove(id_dataset)
            self.nodes_infos[id_node]['keys'].append(id_dataset)
            #self.writeOutput(f'{self.nodes_infos[id_node]['keys']}')
            return True
        return False

    def deleteDataFromTable(self, id_node, id_dataset):
        if id_dataset in self.nodes_infos[id_node]['keys']:
            self.nodes_infos[id_node]['keys'].remove(id_dataset)
            #self.writeOutput(f'{self.nodes_infos[id_node]['keys']}')
            return True
    
    def managerAvectionM1(self,id_node,id_ds):

        #partie TTL
        data_item = self.data[id_ds]
        #p = 0 if id_node not in self.previous_stats[id_ds].popularity_peer_noed.keys() else self.previous_stats[id_ds].popularity_peer_noed[id_node]
        #Ca revien a l'exp 5
        # this is used only
        
        p = self.data[id_ds].TTL
        if p == -1:
            self.writeOutput("deleted cause of TTL\n")
            return {"delete":True, "send":False} #supp si le TTL l'exige => bcp de donnée dans l'infra
        self.writeOutput("TTL esquivé \n")
        if self.replicas[(id_ds, id_node)].nb_migrations > MAX_MIGRATIONS:
            self.writeOutput("deleted cause of Migration limite\n")
            return {"delete":True, "send":False}
            
        """
        
        if self.replicas[(id_ds,id_node)].nb_requests < MIN_REQUESTS:
            print("deleted cause of TTL\n")
            return {"delete":True, "send":False}
        p =  self.previous_stats[id_ds].nb_requests
        if p == 0 : 
            print("deleted cause of TTL\n")
            return {"delete":True, "send":False} #supp si le TTL l'exige => bcp de donnée dans l'infra
        print("TTL esquivé \n")
        
        data_item = self.data[id_ds]

        if data_item.nb_replica > TTL_MIN:
            print("deleted cause of TTL\n")
            return {"delete":True, "send":False} #supp si le TTL l'exige => bcp de donnée dans l'infra
        print("TTL esquivé \n")
        """
        
        neighbors = []
        storage_on_node = []
        for n in range(len(self.graphe_infos)-1):
            if self.graphe_infos[id_node][n] > 0 and self.nodes_infos[n]["remaining_space"] > (((data_item.size+1024)*1024)) and id_ds not in self.nodes_infos[n]['keys']:
                neighbors.append((n, self.nodes_infos[n]["remaining_space"]))
        
        sorted_neighbors_by_space = sorted(neighbors, key=lambda x: x[1], reverse=True)
        optimal_cost = 0
        node = None

        keys_peer_node = {}
        for n in self.nodes_infos.keys():
            keys_peer_node[n] = copy.deepcopy(self.nodes_infos[n]['keys'])

        for id_n, _ in sorted_neighbors_by_space:
            space_availabel = self.nodes_infos[id_n]["remaining_space"]
            if  self.graphe_infos[int(id_node)][id_n] > 0 and (space_availabel > (((data_item.size+1024)*1024))):
                self.writeOutput(f"why not to send {id_n} from {id_node} to {id_n} {self.graphe_infos[int(id_node)][id_n]}\n")
                #p = 0 if id_node not in self.data[id_ds].popularity_peer_node.keys() else self.data[id_ds].popularity_peer_node[id_n]

                """cost =  nodeImportanceV2(
                    b=BANDWIDTH,
                    graphe_infos=self.graphe_infos,
                    s=data_item.size,
                    id_node=id_n,
                    nb_requests= 1 if id_n not in self.requests_processed.keys() else self.requests_processed[id_n]
                )

                
                cost = transfertTime(
                    b=BANDWIDTH,
                    l=self.graphe_infos[int(id_node)][id_n],
                    s=data_item.size,
                )
                """
                
                cost = minimizingTimeTransfert(
                    dataset=id_ds,
                    ds_size=data_item.size,
                    id_src=id_node,
                    id_dst=id_n,
                    key_peer_node=keys_peer_node,
                    graphe_infos=self.graphe_infos
                )

                if cost > optimal_cost:
                    optimal_cost = cost
                    node = id_n

        return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}
    
    def managerAvectionWithLimite(self,id_node,id_ds):
        """
            i use this function to impose limites on data migration 
            in this function i will use the class replica 
        """
        data_item = self.data[id_ds]

        if self.data[id_ds].nb_requests_on_traces == 0 or self.replicas[(id_ds, id_node)].nb_migrations > MAX_MIGRATIONS:
            print("deleted cause of TTL or max migration for the replica\n")
            return {"delete":True, "send":False}
        
        neighbors = []
        storage_on_node = []
        for n in range(len(self.graphe_infos)-1):
            if self.graphe_infos[id_node][n] > 0 and self.nodes_infos[n]["remaining_space"] > (((data_item.size+1024)*1024)) and id_ds not in self.nodes_infos[n]['keys']:
                neighbors.append((n, self.nodes_infos[n]["remaining_space"]))
        
        sorted_neighbors_by_space = sorted(neighbors, key=lambda x: x[1], reverse=True)
        optimal_cost = 0
        node = None

        keys_peer_node = {}
        for n in self.nodes_infos.keys():
            keys_peer_node[n] = copy.deepcopy(self.nodes_infos[n]['keys'])

        for id_n, _ in sorted_neighbors_by_space:
            space_availabel = self.nodes_infos[id_n]["remaining_space"]
            if  self.graphe_infos[int(id_node)][id_n] > 0 and (space_availabel > (((data_item.size+1024)*1024))):
                self.writeOutput(f"why not to send {id_n} from {id_node} to {id_n} {self.graphe_infos[int(id_node)][id_n]}\n")
                
                cost = minimizingTimeTransfert(
                    dataset=id_ds,
                    ds_size=data_item.size,
                    id_src=id_node,
                    id_dst=id_n,
                    key_peer_node=keys_peer_node,
                    graphe_infos=self.graphe_infos
                )

                if cost > optimal_cost:
                    optimal_cost = cost
                    node = id_n

        return {"delete":True, "send": True if not node is None else False, "id_dst_node":node} 

        #je suis arrivé la je continu le choix du noeud comme dicuté

    def decideOnMigration(self, id_ds, id_node, model):

        replica = self.replicas[(id_ds, id_node)]
        data = self.data[id_ds]

        pres = Replica.nbReplica(id_ds, self.replicas)/len(self.replicas.keys())

        if random.random() > THRESHOLD:
            pass
            
        else:
            print("deleted cause of random")
            return {"delete":True, "send":False}
        """
            je vais prendre le stockage en consideration 
            le derniere fois que la donnée a etais utilisé
            la preence est le nombre de replica de la donnée sur le nombre de noeuds
            
        """
        return None


    def updateReplica(self, id_ds, id_node, destination):
        
        replica  = self.replicas[(id_ds, id_node)]  

        del self.replicas[(id_ds, id_node)]

        self.replicas[(id_ds, destination)] = replica
        self.replicas[(id_ds, destination)].nb_migrations +=1
        self.replicas[(id_ds, destination)].id_node = destination    

        return True

    def getPopularities(self, traces):
        unique, count = np.unique(traces['dataset'], return_counts=True)

        popularities = {}
        for i, ds in enumerate(unique):
            popularities[ds] = int(count[i])
        
        return popularities

    def deleteFromLocationTable(self,id_node, id_dataset):
        if id_dataset in self.location_table.keys():
            while id_node in self.location_table[id_dataset]:
                self.location_table[id_dataset].remove(id_node)
        return True

    def isNeighbors(self, id_node):
        if self.graphe_infos[self.id][int(id_node)] > 0:
            return True
        else: 
            return False
    
    def startThread(self):
        flask_process = multiprocessing.Process(target=self.startFlaskServer)
        flask_process.start()
        time.sleep(0.2)
        return flask_process

    def transfertCost(self, latency, data_size):
        bandwith_in_bits = BANDWIDTH*1024*1024*8
        size_in_bits = data_size*1024*8
        latency_in_s = latency/1000

        return latency_in_s + (size_in_bits/bandwith_in_bits)

    def writeOutput(self, str):
        self.output = open(f"/tmp/log_M.txt",'a')
        self.output.write(str)
        self.output.close()
    
    def writeTransfert(self,str):
        path = "/tmp/transfert_M.txt" if ENABEL_MIGRATION else "/tmp/transfert.txt"
        self.transfert = open(path,'a')
        self.transfert.write(str)
        self.transfert.close()
    
    def updateNodeImportance(self, path):
        for n in path:
            if n in self.requests_processed.keys():
                self.requests_processed[n] +=1
            else:
                self.requests_processed[n] = 2

    def initNodeImportance(self,):
        for n in self.requests_processed.keys():
            self.requests_processed[n] = 1

    def initReplicaTTL(self):
        for key in self.replicas.keys():
            self.replicas[key].nb_requests = 0
        
    def lookUpNextLocation(self, index):
        pass

if __name__ == "__main__":

    data = recieveObject()

    
    task_manager = ReplicaManager(
        nb_nodes = NB_NODES,
        traces_path=PATH_TO_TASKS,
        graphe= data["graphe_infos"],
        ip=data["IP_ADDRESS"],
        local_execution=False
    )
    task_manager.writeOutput(f"{data}")
    
    
    task_manager.nodes_infos = data["infos"]
    manageUsingKNN(task_manager)
import sys
import os
import threading
import requests
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flask import Flask, request, jsonify
from communication.messages import Task
from queue import Queue
from experiments.params import EXECUTION_LOCAL
from redisClient import RedisClient
import multiprocessing

class NodeManagerServer:
    def __init__(self,storage_space, id_node,neighbors:dict, node_client, host='localhost', port=8888):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.recieved_task = Queue()
        self.setup_routes()
        self.node_manager = RedisClient(storage_space, id_node)
        self.node_client = node_client
        self.neighbors = neighbors
        self.nb_requests_processed = {}
        self.processes = []
        
        
        self.client = self.node_manager.connectToRedis()
        if self.client != None:
            self.writeOutput("connected to redis\n")
        else:
            self.writeOutput("not connected to redis\n")
    
    def setup_routes(self):
        
        @self.app.route("/process", methods=["POST"])
        def process():

            data_r = request.json
            self.writeOutput(f"{data_r} {self.neighbors.keys()}\n")
            if data_r["target"] in self.neighbors.keys():
                self.writeOutput(f"neu\n")
                if data_r["methode"] == "POST":
                    reponse = requests.post(url=data_r["url"],json=data_r["data"])
                else:
                    reponse = requests.get(url=data_r["url"], params=data_r["data"])
            
            else:
                new_target = data_r['path'].pop(0)
                data_send = {
                    'data' : data_r["data"],
                    'url':data_r["url"],
                    'methode':data_r["methode"],
                    'path': data_r['path'],
                    'target':data_r['target'], 
                    'id_ds': data_r["id_ds"]
                }

                if data_r['id_ds'] in self.nb_requests_processed.keys():
                    self.nb_requests_processed[data_r["id_ds"]] += 1
                else:
                    self.nb_requests_processed[data_r["id_ds"]] = 1

                url = f"http://{self.neighbors[new_target]['ip']}:{self.neighbors[new_target]['rep_port']}/process"
                reponse = requests.post(url, json=data_send)
            return jsonify(reponse.json())

        
        #used
        @self.app.route('/execut', methods=['POST'])    
        def process_data():

            self.writeOutput("recieved task")
            data = request.json

            task = {
                "job_id": data['job_id'], 
                "execution_time": data['execution_time'],
                "id_dataset": data['id_dataset']
            }

            b1 = self.node_manager.checkOnCacheMemorie(task["id_dataset"])
            
            if not b1:
                processed_data = {"sendData":True, "started": False, "PID": None}
            
            else:
                pid = self.node_client.startTask(task['execution_time'])
                self.writeOutput(f"Job {task['job_id']} started on node")

                processed_data = {"sendData":False, "started": True,  "PID": pid}
    
            return jsonify(processed_data)
        
        #used
        @self.app.route('/infos', methods=['GET'])
        def get_info():
            stats = self.node_manager.getStats()[0][1]
            keys = self.node_manager.getKeys()
            if stats:
                data = {
                    "id_node": self.node_manager.id_node,
                    "storage_space": int(stats["maxmemory"]),
                    "remaining_space":int(stats["maxmemory"]) - (int(stats["used_memory"])),
                    'keys': keys,
                    'popularities':self.nb_requests_processed
                }

                self.writeOutput("here\n")
                
                self.node_manager.cache_size = int(stats["maxmemory"])
                self.node_manager.memory_used  = int(stats["used_memory"])
            else:
                data = {
                    "id_node": self.node_manager.id_node,
                    "storage_space": self.node_manager.cache_size,
                    "remaining_space":self.node_manager.cache_size - self.node_manager.memory_used,
                    'keys': self.node_manager.ids_data #self.node_manager.getKeys()#
                }
                self.writeOutput("not here\n")
            for ds in self.node_manager.last_recently_used_item:
                if ds not in keys:
                    self.node_manager.last_recently_used_item.remove(ds)
            self.writeOutput(f"{data}")
            self.writeOutput("info sended\n")
            return jsonify(data)
        
        #used
        @self.app.route('/transfert', methods=['POST'])
        def transfert():
            data = request.json
            
            r,e = self.node_manager.sendDataSetTo(
                ip_dst=data["dst_ip"],
                id_dataset=data["id_dataset"],
                size_ds=data["size_ds"]
            ) 
            processed_data = {"response":r}
            
            return jsonify(processed_data)
        
        
        @self.app.route('/send-and-delete', methods=["GET"])
        def sendAndDelete():
            id_ds = request.args.get("id_dataset")
            ip_dst_node = request.args.get("ip_dst_node")
            ds_size = request.args.get("ds_size")
            port_dst = request.args.get("port_dst_node")

            t,e = self.node_manager.sendDataSetTo(
                    ip_dst=ip_dst_node,
                    id_dataset=id_ds,
                    size_ds=ds_size
                    )
            if not e is None:
                self.writeOutput(f"{e}")

            if t:
                b = self.node_manager.deleteFromCache(id_ds, ds_size=ds_size)
                self.writeOutput(b)
                stats = self.node_manager.getStats()[0][1]
                self.node_manager.memory_used = int(stats["used_memory"])
                response = {"sended":b, "remaining_space":int(stats["maxmemory"]) - int(stats["used_memory"])}
            else:
                response = {"sended":t}

            return jsonify(response)

        
        @self.app.route('/add-data', methods=['POST'])
        def add_data():
            data = request.json
            b = self.node_manager.checkOnCacheMemorie(data['id_dataset'])
            if b:
                processed_data = {"sendData":False}
            else:
                processed_data = {"sendData":True}

            return jsonify(processed_data) 
        
        @self.app.route('/delete-data', methods=['GET'])
        def delete_data():
            id_dataset = request.args.get("id_dataset")
            
            r = self.node_manager.deleteFromCache(id_dataset,ds_size=100)

            stats = self.node_manager.getStats()[0][1]
            self.node_manager.memory_used = int(stats["used_memory"])
            return jsonify({
                "reponse":r,
                "remaining_space":int(stats["maxmemory"]) - int(stats["used_memory"])
                })
        
        @self.app.route("/notify",methods=['POST'])
        def notify():
            data = request.json
            id_ds = data["id_dataset"]
            self.node_manager.ids_data = self.node_manager.getKeys()
            stats = self.node_manager.getStats()[0][1]
            return jsonify({"added":True,
                            "remaining_space":int(stats["maxmemory"]) - int(stats["used_memory"])
            })
            
        @self.app.route("/operations", methods=['POST'])
        def operation():
            data = request.json
            operations = data["operations"]
            threads = []
            for opt in operations:
                if opt[0] == 'migrate':
                    ip_dst_node = self.neighbors[opt[3]]['ip']
                    thread = threading.Thread(target=self.node_manager.migrateData, args=(opt[1], opt[2], ip_dst_node))
                    threads.append(thread)
                    thread.start()
            # Attendre la fin de tous les threads
            for thread in threads:
                thread.join()
            
            stats = self.node_manager.getStats()[0][1]
            return jsonify({"added":True,
                            "remaining_space":int(stats["maxmemory"]) - int(stats["used_memory"])
            })

        @self.app.route('/shutdown', methods=['POST'])
        def shutdown():
            shutdown_server = request.environ.get('werkzeug.server.shutdown')
            if shutdown_server is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            shutdown_server()
            return 'Server shutting down...'
        


        @self.app.route('/say', methods=['GET'])
        def say():
            param1 = request.args.get('num', 'Guest')
            #print(param1)
            processed_data = {"response":"good"}
            
            return jsonify(processed_data)
    
    
    def addRequest(self,id_dataset):
        if id_dataset in self.nb_requests_processed.keys():
            self.nb_requests_processed[id_dataset] +=1
        else:
            self.nb_requests_processed[id_dataset] =1
    def run(self):
        try:
            self.app.run(host="0.0.0.0", port=self.port)
            return True
        except :
            return False  
    
    def writeOutput(self, str):
        out = open(f"/tmp/log_{self.node_manager.id_node}.txt",'a')
        out.write(f"{str}")
        out.close()



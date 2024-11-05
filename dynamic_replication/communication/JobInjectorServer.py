from flask import Flask, request, jsonify
import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobInjector import JobInjector

class JobInjectorServer:
    def __init__(self, job_injector:JobInjector, host='localhost', port=5000):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.job_injector:JobInjector = job_injector
        self.setup_routes()

    def setup_routes(self):
        
        @self.app.route('/process', methods=['POST'])
        def process_data():
            data = request.json
            # Traiter les données reçues
            processed_data = {"received": data, "status": "processed"}
            return jsonify(processed_data)

        @self.app.route('/get-data', methods=['GET'])
        def get_data_set():
            param1 = request.args.get('id-ds')
            param2 = request.args.get('id-node')
            data = request.json
            # Traiter les données reçues
            self.job_injector.send_data()

            processed_data = {"status": "Good"}

            return jsonify(processed_data)
        
        
    
    
    def deleteAndSend(self, id_src_node, id_dst_node, id_dataset, ds_size):
        url = f'http://{self.nodes_infos[id_src_node]["node_ip"]}:{self.job_injector.nodes_infos[id_src_node]["node_port"]}/send-and-delete'

        response = requests.get(url,params={
            'id_dataset':id_dataset,
            'ds_size': ds_size,
            'ip_dst_node': self.job_injector.nodes_infos[id_dst_node]["node_ip"],
            'port_dst_node':self.job_injector.nodes_infos[id_dst_node]["node_port"]
        })
        self.job_injector.writeOutput("migration declanchée\n")
        self.job_injector.writeOutput(f"{response.text}")
        if response.json()["sended"]:
            cost = self.job_injector.transfertCost(self.job_injector.graphe_infos[int(id_src_node)][int(id_dst_node)],ds_size)
            self.job_injector.writeTransfert(f"null,{id_dataset},{id_src_node},{ds_size},{id_dst_node},{cost},migration\n")
            self.job_injector.nodes_infos[id_src_node]['remaining_space'] = response.json()['remaining_space']
            self.job_injector.location_table[id_dataset].append(id_dst_node)
            self.job_injector.location_table[id_dataset].remove(id_src_node)

            self.job_injector.notifyNode(id_dst_node,self.job_injector.nodes_infos[id_dst_node]['node_ip'],self.job_injector.nodes_infos[id_dst_node]['node_port'] , id_dataset, add=True)
            #self.accessData(id_src_node,id_dataset)
        return response.json() 

    def askForATransfert(self, src, dst, id_dataset,size_ds):
        
        url = f'http://{self.nodes_infos[src]["node_ip"]}:{self.job_injector.nodes_infos[src]["node_port"]}/transfert'
        
        data = {
            "dst_id": dst,
            "dst_ip": self.job_injector.nodes_infos[dst]["node_ip"], 
            "id_dataset": id_dataset,
            "size_ds": size_ds
        }

        response = requests.post(url, json=data)
        #print(response.json()["response"])
        return response.json()["response"]
        
    def run(self):
        self.app.run(host=self.host, port=self.port)


    def runLocal(self):
        self.app.run(port=5000)


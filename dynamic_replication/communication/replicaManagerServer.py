from flask import Flask, request, jsonify
import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class ReplicaManagerServer:
    def __init__(self, replica_manager, host='localhost', port=5000):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.replica_manager = replica_manager
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
            self.replica_manager.send_data()

            processed_data = {"status": "Good"}

            return jsonify(processed_data)
        
        @self.app.route('/evect-data', methods=['GET'])
        def delete_data():
            data = request.json
            decision = self.replica_manager.evectData(data["id_node"], data["id_dataset"], data["dataset_size"])
            processed_data = {"decision": decision}
            return jsonify(processed_data)
    
    @classmethod
    def deleteAndSend(self, id_src_node, id_dst_node, id_dataset, ds_size):
        url = f'http://{self.nodes_infos[id_src_node]["node_ip"]}:{self.nodes_infos[id_src_node]["node_port"]}/send-and-delete'

        response = requests.get(url,params={
            'id_dataset':id_dataset,
            'ds_size': ds_size,
            'ip_dst_node': self.nodes_infos[id_dst_node]["node_ip"],
            'port_dst_node':self.nodes_infos[id_dst_node]["node_port"]
        })
        self.writeOutput("migration declanchée\n")
        self.writeOutput(f"{response.text}")
        if response.json()["sended"]:
            cost = self.transfertCost(self.graphe_infos[int(id_src_node)][int(id_dst_node)],ds_size)
            self.writeTransfert(f"null,{id_dataset},{id_src_node},{ds_size},{id_dst_node},{cost},migration\n")
            self.nodes_infos[id_src_node]['remaining_space'] = response.json()['remaining_space']
            self.location_table[id_dataset].append(id_dst_node)
            self.location_table[id_dataset].remove(id_src_node)

            self.notifyNode(id_dst_node,self.nodes_infos[id_dst_node]['node_ip'],self.nodes_infos[id_dst_node]['node_port'] , id_dataset, add=True)
            #self.accessData(id_src_node,id_dataset)
        return response.json() 

    @classmethod
    def askForATransfert(self, src, dst, id_dataset,size_ds):
        
        url = f'http://{self.nodes_infos[src]["node_ip"]}:{self.nodes_infos[src]["node_port"]}/transfert'
        
        data = {
            "dst_id": dst,
            "dst_ip": self.nodes_infos[dst]["node_ip"], 
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

if __name__ == '__main__':
    service1 = ReplicaManagerServer()
    service1.run()
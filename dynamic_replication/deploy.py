import subprocess
import time
from configurations.configuration import Configuration
from communication.send_data import sendObject
from experiments.params import (
    REP_PORT,
    PATH_TO_CONFIG_FILE, 
    SERVER_REPLICA_MANAGER_PORT,
    BD_LISTENING_PORT,
    STORAGE_SPACE
)

def run_command(command):
    
    result = subprocess.run(command.split(), capture_output=True, text=True)
    print(result)

def InfosToSend(id_peer:int,graphe_info,ip_address, rep_port, storage_size):
    data = {}
    data["STORAGE_SIZE"] = storage_size*1024*1024 #touche pas a ca
    data["infos"] = []
    data["SITE_ID"] = id_peer
    data["REP_PORT"] = rep_port + id_peer
    data["IP_ADDRESS"] = str(ip_address[id_peer])
    data["MANAGER_IP"] = str(ip_address[-1])
    
    for i in range(len(graphe_info)):
        if graphe_info[id_peer,i] > 0:
            peer = {
                'latency' : graphe_info[id_peer,i],
                "id": i,
                "ip" : ip_address[i], 
                "rep_port" : rep_port+i,
            } 

            data["infos"].append(peer)
    return data

def SendInfoToManager(id_peer, config,ips_address, rep_port):
    data = {}
    data["IP_ADDRESS"] = str(ips_address[id_peer])
    data['graphe_infos'] = config
    data["IPs_ADDRESS"] =ips_address[:-1]
    data["infos"] = {}
    
    for i in range(len(config)-1):
        peer = {
            'latency' : config[id_peer,i],
            "id": i,
            "node_ip" : ips_address[i], 
            "node_port" : rep_port+i,
        } 
        data["infos"][i] = peer
    return data

###### Start a reservation
  
port_rep = REP_PORT

if True:

    config = Configuration(
        config_file_path = PATH_TO_CONFIG_FILE,
        nb_nodes=11,
        cluster='parasilo',
        latency='100ms'
    )
    

    provider = config.setReservation()
    #netem = config.setNetworkConstraintes()

    ## deplot memcached
    #config.deployMemcached(port=BD_LISTENING_PORT)   
    rep =config.deployRedis(port=BD_LISTENING_PORT, storage= STORAGE_SPACE, eviction=True)
    print("installation de redis:", rep)
    NB_NODES = config.nb_sites
    CONFIG_GRAPHE = config.getGraphe()
    IPS_ADDRESS = config.getAllIPs()


    print(CONFIG_GRAPHE)
    #config.provider.destroy()
    
    infos_nodes= []
    
    if config.execution_local:
        import threading
        for i, machine in enumerate(config.machines):
            if i == NB_NODES-1:

                data = SendInfoToManager(i, CONFIG_GRAPHE,IPS_ADDRESS, REP_PORT)
                
                thread = threading.Thread(
                    target=run_command, 
                    args=(f"python replicaManager.py ",))
                
                thread.start()
                
                print(f"adresse IP du node {i} : {IPS_ADDRESS[i]}")
                
                sendObject(data, IPS_ADDRESS[i])
                infos_nodes.append({"node_ip":IPS_ADDRESS[i], "node_port":SERVER_REPLICA_MANAGER_PORT})
                port_rep += 1
                time.sleep(1)
            else:
                data = InfosToSend(i,CONFIG_GRAPHE, IPS_ADDRESS,REP_PORT,config.storage_capacities[i])
                
                thread = threading.Thread(
                    target=run_command, 
                    args=(f"python cacheManager.py ",))
                
                thread.start()
                
                print(f"adresse IP du node {i} : {IPS_ADDRESS[i]}")
                
                sendObject(data, "localhost")
                infos_nodes.append({"node_ip":IPS_ADDRESS[i], "node_port":port_rep})
                port_rep += 1
                time.sleep(1)
 
    else:
        for i, machine in enumerate(config.machines):
            print(f"======= node {i} ========\n")
            print(f"{machine['roles'][0]}\n")
            with config.enoslib.actions(roles=config.roles[machine["roles"][0]]) as p:
                #p.ensure_python()
                #p.apt(name=["git","python3-pip"], state="present")
                p.command(
                    task_name = "Delete the last version of the repo",
                    cmd = "rm -rf /home/csimohammed/code"
                )
                p.command(
                    task_name="clone repo",
                    cmd="git clone https://github.com/cherifpy/dynamic-replication.git /home/csimohammed/code"
                )
                p.command(
                    task_name = "installing python libs",
                    cmd = "pip3 install pandas numpy sockets PyYAML Flask redis numpy"
                )
            
            
            if i==(NB_NODES-1):

                data = SendInfoToManager(i, CONFIG_GRAPHE,IPS_ADDRESS, REP_PORT)
                print(data)
                """with config.enoslib.actions(roles=config.roles[machine["roles"][0]]) as p:
                    p.command(
                        task_name = "Executing the code on a site",
                        cmd = f"python3 /home/csimohammed/code/dynamic_replication/jobInjector.py > /tmp/output{i}_and_errors.log 2>&1",
                        background=True
                    )
                print(IPS_ADDRESS[i])
                sendObject(data, IPS_ADDRESS[i])"""  

            else: 
                data = InfosToSend(i,CONFIG_GRAPHE, IPS_ADDRESS,REP_PORT,config.storage_capacities[i])
                with config.enoslib.actions(roles=config.roles[machine["roles"][0]]) as p:
                    p.command(
                        task_name = "Executing the code on a site",
                        cmd = f"python3 /home/csimohammed/code/dynamic_replication/nodeManager.py > /tmp/output{i}_and_errors.log 2>&1",
                        background=True
                    )
                print(data)
                print(f"address IP {data['IP_ADDRESS']}")
                sendObject(data, data["IP_ADDRESS"])
                
            
            #config.enoslib.ensure_python3(True,roles=config.roles[machine["roles"][0]])
            
            infos_nodes.append({"node_ip":IPS_ADDRESS[i], "node_port":port_rep})
            port_rep += 1



import re
import yaml
import numpy as np
import requests
import json
import time
import enoslib as en
import logging 
import copy 

class Configuration:

    def __init__(self, cluster = None, config_file_path="/", memcached_listening_port=11211):
        """
            classe constructor is used to create an instence of the class
            i dont know if i have to remove the param = cluster
        """
        
        self.config_file_path = config_file_path 
        self.parametres = self.readYamlFile(self.config_file_path)
        self.test = 0
        self.cluster = cluster
        self.username = self.parametres.get("username")
        self.walltime = self.parametres.get("exp_walltime")
        self.job_type = self.parametres.get("job_type",[])
        self.env_name = self.parametres.get("exp_env")
        self.execution_local = self.parametres.get("execution_local")
        self.user_id = self.parametres.get("user_id")
        self.provider = None
        self.emulation_conf = None
        self.monitoringDeployed = False
        self.start_time = -1
        self.machines = [machine for machine in self.parametres.get('machines', [])]
        self.sites = [machine["roles"][0] for machine in self.machines]
        self.storage_capacities = [machine['storage'] for machine in self.machines]
        self.roles = None
        self.contraintes = self.parametres.get('network_constraints',[{}])[0].get("constraints")
        self.is_docker_deployed = False
        self.nb_sites = len(self.machines)
        self.python_libs:list = None
        self.enoslib = None if self.execution_local else en #en
        self.memcached_listening_port = memcached_listening_port
        
        if not self.execution_local:
            self.enoslib.init_logging(level=logging.INFO)
            self.enoslib.check()
    
        #print(self.machines)

    def setReservation(self):
        if self.execution_local:
            return None
        """
            the role of this function is to directly reserve the noeud needed 
            params:
                - none
        """
        conf = self.enoslib.G5kConf.from_settings(job_type=self.job_type, walltime=self.walltime)

        for machine in self.machines :
            conf.add_machine(
                roles = machine["roles"],
                cluster = self.cluster if self.cluster else machine["cluster"],
                nodes = machine["nodes"]
            )
        # install fdifk directly on the machins
        self.provider = self.enoslib.G5k((conf))

        self.roles, self.networks = self.provider.init()
        self.roles = self.enoslib.sync_info(self.roles, self.networks)
        print("Reservation done !")
        return self.provider
    
    def deployMemcached(self, port=11211):
        if self.execution_local:
            return False

        print("storage restriction using memcached for All nodes ")
        with self.enoslib.actions(roles=self.roles) as p: 
            p.apt(name=['memcached'],state="present",)
            p.command(
                    task_name="Start memcached with a pecifique config",
                    cmd=f"memcached -m {15} -I {5}m -l 0.0.0.0 -p {self.memcached_listening_port} -u nobody -M", 
                    background=True
                )
        return True
    
    
        for i,machine in enumerate(self.machines):
            print("storage restriction using memcached for ", machine["roles"])
            with self.enoslib.actions(roles=self.roles[machine["roles"][0]]) as p:
                p.apt(name=['memcached'],state="present",)
                """p.command(
                    task_name="changine de size",
                    cmd=f"sed -i 's/-m 64/-m {machine['storage']}/g' /etc/memcached.conf"
                )
                p.command(
                    task_name="changing IP",
                    cmd=f"sed -i 's/-l 127.0.0.1/-l 0.0.0.0/g' /etc/memcached.conf"
                )
                # 
                p.command(task_name="start memcached",cmd="systemctl start memcached")
                p.command(task_name="enable memcached",cmd="systemctl enable memcached")
                p.command(task_name="restart memcached",cmd="service memcached restart")

                p.command(task_name="change max value size memcached",cmd="memcached -I 120m", background=True)"""
                p.command(
                    task_name="Start memcached with a pecifique config",
                    cmd=f"memcached -m {1024} -I {int(machine['storage'])//2}m -l 0.0.0.0 -p {self.memcached_listening_port} -u nobody", 
                    background=True
                )
                

                #
    def deployRedis(self, port=5555, storage=90):
        if self.execution_local:
            return False

        print("storage restriction using memcached for All nodes ")
        with self.enoslib.actions(roles=self.roles) as p: 
            p.apt(name=['redis-server'],state="present",)
            p.command(
                    task_name="Start redis with a pecifique config",
                    cmd=f"redis-server --bind 0.0.0.0 --protected-mode no --maxmemory {storage}mb --port {self.memcached_listening_port}", 
                    background=True
                )
        return True
    
    def setNetworkConstraintes(self):
        if self.execution_local:
            return None
        self.emulation_conf = {
            "default_delay" : self.parametres.get('network_constraints', [{}])[0].get('default_delay'),
            "default_rate" : self.parametres.get('network_constraints', [{}])[0].get('default_rate'),
            "constraints": self.contraintes
        }
        
        self.netem = self.enoslib.NetemHTB.from_dict(self.emulation_conf, self.roles, self.networks)
        self.netem.deploy()
        return self.netem

    def getIpAddress(self, role:str, node:int=None):
        """
            this functionis just used to 
        
        """
        if self.execution_local:
            return None
        
        if node == None:
            #get the IP address of a machine    
            machine = self.roles[role][0]
        else:
            #get the IP address of a specific node on a machine
            machine = self.roles[role][node-1]
        #print(machine)
        ip_address = machine.filter_addresses(networks=self.networks["prod"])[0]

        return str(ip_address.ip.ip)
    
    def getAllIPs(self): 
        if self.execution_local:
            return ["localhost" for i in range(self.nb_sites)]
        ips = []
        for i, role in enumerate(self.roles):
            ip = self.getIpAddress(role)
            ips.append(ip)
        return ips

    #Not tested yes
    def ApplyStorageRestriction(self,partition_name = None): #do a version without using ansible
        """
            this funtion is used to set the storage capacity restiction on each site
            it return nothing, it just a classe methode
        """
        if self.execution_local:
            return None
        list_results = {}
        for i,machine in enumerate(self.machines):
            print("storage restriction for ", machine["roles"])
            
            with self.enoslib.actions(roles=self.roles[machine["roles"][0]]) as p:
                p.command(
                    task_name="Creatition the virtual space",
                    cmd=f"dd if=/dev/zero of=virtual_storage_disk.img bs=1M count={machine['storage']}"
                    ) # create the file .img with to storage capacity needed 
                
                p.command(
                    task_name="format the partition",
                    cmd="mkfs.ext4 virtual_storage_disk.img"
                    ) 
                
                p.command(
                    task_name="create a mount point",
                    cmd="mkdir /dev/virtual_disk/"
                    ) 
        
                p.command(
                    task_name="Mount the partition",
                    cmd="mount virtual_storage_disk.img /dev/virtual_disk"
                    ) 
        
                p.command(
                    task_name="Change partition's permission",
                    cmd="chmod 777 /dev/virtual_disk/"
                    ) 

                list_results[machine["roles"][0]] = p.results
                
        return list_results     

    #Not yet tested 
    def installPackages(self):
        if(self.package != None):
            with self.enoslib.actions(roles=self.roles) as p:
                p.apt(
                    name= self.packages,
                    state="present",
                )
        else:
            print("Aucun package a installer")

    def clonGitReposiroty(self, repo_link:str, repo_name:str):

        projet_path = f"/home/{self.user_id}/{repo_name}"
        with self.enoslib.actions(roles=self.roles) as p:
            p.apt(name=["git"], state="present")
            p.command(
                task_name = "Delete the last version of the repository if exist",
                cmd = f"rm -rf {projet_path}"
            )
            p.git(repo=f"{repo_link}", dest=projet_path)
        
        
        return projet_path


    def preparePythonExecution(self):

        self.enoslib.ensure_python3(True, self.roles)

        with self.enoslib.actions(roles=self.roles) as p:
            p.apt(name=["python3-pip"], state="present")
        
        for lib in self.python_libs:
            self.enoslib.run_command(f"pip install {lib}", roles=self.roles)

        return True

    def installPythonPackages(self, packages:list=[]):

        if(len(packages) != 0 ):
            with self.enoslib.actions(roles=self.roles) as p:
                p.apt(
                    name= packages,
                    state="present",
                )
        else:
            print("There is no package to install")


    def deployTIGMonitoring(self, site,node,port = 3000):
        # The Monitoring service knows how to use this specific runtime
        m = self.enoslib.TIGMonitoring(
            collector=site[node], agent=site, ui=site[node]
        )
        m.deploy()

        ui_address = site[node].address
        print(f"The UI is available at http://{ui_address}:{port}")
        print("user=admin, password=admin")
        
        return ui_address

    def deployTIGOnAllSites(self):
        
        sites = [machine["roles"][0] for machine in self.machines]
        
        for site in sites:
            d = self.enoslib.Docker(agent=self.roles[site])
            d.deploy()
            # The Monitoring ]how to use this specific runtime
            m = self.enoslib.TIGMonitoring(
                collector=self.roles[site][0], agent=self.roles[site], ui=self.roles[site][0]
            )
            m.deploy()
            
            ui_address = self.roles[site][0].address
            print(f"The UI is available at http://{ui_address}:{3000}")
            print("user=admin, password=admin")
        
        self.monitoringDeployed = True

    def deployZipKinMonitoring(self):
        
        with self.enoslib.actions(roles=self.roles) as p:
            p.command(
                task_name="Deploy OpenZipkin",
                cmd="docker run -d -p 9411:9411 openzipkin/zipkin"
            ) 

    def installDockerOnAllMachine(self,port=80):
        # install docker
        for i,machine in enumerate(self.machines):
            
            registry_opts = dict(type="external", ip="docker-cache.grid5000.fr", port=80)
            d = self.enoslib.Docker(
                agent=self.roles[machine["roles"][0]], bind_var_docker="/tmp/docker", registry_opts=registry_opts
            )
            d.deploy()
        self.is_docker_deployed = True
    
    def collectMetricsFromBD(self):
        if self.monitoringDeployed:
            return True
        else:
            print("No influx installed")
    
        return False
        
    def getGraphe(self):
        self.graphe = np.zeros((self.nb_sites,self.nb_sites))

        for i in range(self.nb_sites):
            node_i = self.machines[i]["roles"][0]
            for j in range(i, self.nb_sites):
                node_j = self.machines[j]["roles"][0]
                if i == j: self.graphe[i,j] = -1
                else:
                    latency, is_symmetric = self.getLatencyBetweenNodes(node_i,node_j)
                    self.graphe[i,j] = latency
                    if is_symmetric:
                        self.graphe[j,i] = latency
        return self.graphe
    
    def getLatencyBetweenNodes(self, node1:str, node2:str):
        
        for const in self.contraintes:
            if (const['src'] == node1 and const['dst'] == node2) or ( const['src'] == node2 and const['dst'] == node1) :
                return int(const['delay'][0:-2]), const['symmetric'] 
        
        return -1, False
    
    def getStorageCapacities(self,):
        
        collector_address = self.roles['Qnode1'][0].address
        
        with self.enoslib.G5kTunnel(collector_address, 8086) as (local_address, local_port, tunnel):
            url = f"http://{local_address}:{local_port}/query"
            print(url)
            q = (
                'SELECT max("bytes_recv") FROM "net" WHERE time > now() - 5m GROUP BY time(1m), "index", "name", "host"'
            )
            r = requests.get(url, dict(db="telegraf", q=q))
            print(json.dumps(r.json(), indent=4))
        pass


    def quantityDataRecieved(self):
        pass

    def getActualResources(self):
        return self.roles, self.networks

    def readYamlFile(self,config_file_path):
        with open(config_file_path, 'r') as f:
            parametres = yaml.safe_load(f)
        return parametres
    
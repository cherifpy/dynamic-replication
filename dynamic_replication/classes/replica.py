import time

class Replica:
    num = 0
    def __init__(self, id_dataset, id_node):
        self.id_replica = self.num + 1
        self.id_dataset = id_dataset
        self.id_node = id_node
        self.creation_time = time.time()
        self.TTL = 2
        self.nb_access = 0
        self.nb_acces_peer_node = {}
        self.nb_migrations  = 0
        self.nb_requests = 0


    def updateTTL(self):
        self.TTL += 1
    
    def migration(self, id_new_node):
        self.id_node = id_new_node

    @staticmethod
    def nbReplica(id_data, replicas):
        nb = 0
        for key in replicas.keys():
            if key[0] == id_data:
                nb+=1

        return nb
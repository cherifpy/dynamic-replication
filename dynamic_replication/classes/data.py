

from numpy import size
import copy

class Data(object):
    def __init__(self, id_dataset,size, replicas_location,nb_requests_on_traces=0) -> None:
        self.id_dataset = id_dataset
        self.size = size
        self.popularity = 0
        self.nb_replica = 0
        self.replicas_location = replicas_location
        self.popularity_peer_noed = {}
        self.nb_requests = 0
        self.nb_transfert_between_node = {}
        self.TTL = 0
        self.nb_requests_on_traces = 0
        

    def updateDataState(self, id_node):

        self.nb_requests_on_traces-=1

        self.nb_requests +=1
        if self.TTL != 1:
            self.TTL = 1
        if id_node in self.popularity_peer_noed.keys():self.popularity_peer_noed[id_node] +=1
        else:self.popularity_peer_noed[id_node] = 1

    def updateNBrequests(self):
        self.nb_requests +=1

    def updatePopularity(self):
        self.popularity +=1

    def updateNbReplica(self, add=True):
        if add: self.nb_replica +=1
        else: self.nb_replica -= 1

    def addPopularityPeerNode(self, id_node):
        if id_node in self.popularity_peer_noed.keys():
            self.popularity_peer_noed[id_node]+=1
        else: self.popularity_peer_noed[id_node] = 1

    
    def iniTDataTTL(data_list):
        previous_data = copy.deepcopy(data_list)
        for d in data_list.keys():
            data_list[d].nb_requests = 0
            data_list[d].popularity_peer_noed = {}
            if data_list[d].TTL == 0:
                data_list[d].TTL = -1
            elif data_list[d].TTL == 1:
                data_list[d].TTL == 0

        return copy.deepcopy(data_list), previous_data
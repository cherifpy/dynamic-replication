
from calendar import day_abbr
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from classes.djikstra import djikstra
from exp.params import BANDWIDTH
import numpy as np
import copy

def transefrtWithGain(b, l, s, n):
    """
        params:
            b = bandwidth
            l = latency
            s = dataset size
            n = popularity

        this cost fonction try to minimise the transfert cost will maximising the gain

        the transfert cost is the time to transfert the data from Src to distiantion: here the src ith the node where the data may be evicted
        the gain is the latency times the popularity of a data on a node this mean the number of requete a node recieved for the data
    """
    bandwith_in_bits = b*1024*1024*8
    size_in_bits = s*1024*8
    latency_in_s = l/1000

    transfert_cost = 1/(bandwith_in_bits/(bandwith_in_bits*latency_in_s + size_in_bits))
    gain = n * latency_in_s
    return transfert_cost + gain

def transfertTime(b, l, s):
    """
        params:
            b = bandwidth
            l = latency
            s = dataset size

        this cost fonction try to minimise the transfert cost
    """

    bandwith_in_bits = b*1024*1024*8
    size_in_bits = s*1024*8
    latency_in_s = l/1000

    return latency_in_s + (size_in_bits/bandwith_in_bits)

def energieAndTransfertCost(b,l,s):
    pass


def nodeImportanceV2(id_node, b, s, graphe_infos, nb_requests):
    """
        dans cette fonction je me base sur la fonction node importance en ajoutant le nombre de requete qui passe par ce noued
        possible de saturé le noeud mais je dois voir commennt regler ce truc
    """

    sum = 0
    for l in graphe_infos[id_node]:
        if l > 0: sum += transfertTime(b,l,s)

    return sum*nb_requests

def nodeImportance(id_node, b,s, graphe_infos):

    sum = 0
    for l in graphe_infos[id_node]:
        if l > 0: sum += transfertTime(b,l,s)

    return sum


def fobjectif():
    """
        - dans cette fonction je dois tenir en contre
            - le nombre de voisin du noeud
            - le nombre de voisin en commun avec la source
            - l'espace de stockage

    
        - le nombre de demande passée par le noued
        - aussi parler sur le truc de julia qui m'as donné un papier sur la conso energitique
        selon la distance parcourue par la requet
    """
    pass

def searchTheNearst(id_node,dataset, graphe_infos, key_peer_node):
    locations = []
    latency = []
    if dataset in key_peer_node[id_node]:
        return id_node, 0
    
    for node, c in enumerate(graphe_infos[id_node][:-1]):
        if c > 0 and dataset in key_peer_node[node]:
            locations.append(node)                
            latency.append(c)
        elif node!=id_node and node in key_peer_node.keys():
            _, cost =  djikstra(graphe_infos, node, id_node)
            locations.append(node)                
            latency.append(cost)
        
    if len(locations) == 0:
        return -1, 0

    i_min = np.argmin(latency)

    return latency[i_min], locations[i_min]


def minimizingTimeTransfert(dataset, ds_size,id_src, id_dst, graphe_infos, key_peer_node):
    """
        this function used to see the def between cost befor and after migration 
        the goal is to maci this function 
    """
    keys = copy.deepcopy(key_peer_node)
    cost_with_ds_in_source = 0
    cost_with_ds_in_dst = 0
    for i in range(len(graphe_infos[id_src])-1):
        latency, node = searchTheNearst(i, dataset,graphe_infos,keys)
        cost_with_ds_in_source+= transfertTime(
            l=latency, 
            b=BANDWIDTH,
            s=ds_size)
    
    keys[id_src].remove(dataset) 
    keys[id_dst].append(dataset)
    
    for i in range(len(graphe_infos[id_src])-1):
        latency, node = searchTheNearst(i, dataset,graphe_infos,keys )
        cost_with_ds_in_dst+= transfertTime(
            l=latency, 
            b=BANDWIDTH,
            s=ds_size)
        
    return cost_with_ds_in_source - cost_with_ds_in_dst
from multiprocessing.synchronize import SemLock
import pickle
import socket
import time
PORT_FOR_OPENING_SOCKET = 8888

""" 
    J'aurais besoin de ca pour envoyer les données vers chaqu'un des nodes au debut de l'exp

"""

def sendObject(obj:object, ip:str):
    time.sleep(3)
    #serialize the object
    data = pickle.dumps(obj)
    # Envoi via un socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((ip, PORT_FOR_OPENING_SOCKET))
        s.send(data)
    return True

def recieveObject():
    print("opning socket")
    # Créer un socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', PORT_FOR_OPENING_SOCKET))
        s.listen()
        #attendre une connexion
        conn, addr = s.accept()
        
        data = conn.recv(5120)
        
        objet_recu = pickle.loads(data)
        
        s.close()

        return objet_recu
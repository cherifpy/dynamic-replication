import socket
import json

class RequestObject(object):
    def __init__(self, dist, sender,object_key) -> None:
        self.dist = dist
        self.object = object_key
        self.sender = sender


class SendObject:
    def __init__(self, dist, object_key, object) -> None:
        self.dist = dist
        self.key = object_key
        self.object = object

class Task:
    def __init__(self,id_task, id_node, infos, id_dataset, ds_size) -> None:
        self.dist = id_node
        self.id_task = id_task
        self.id_node = id_node
        self.infos = infos
        self.id_dataset = id_dataset
        self.ds_size = ds_size
    
    def to_dict(self):
        return {
            'id_task': self.id_task,
            'id_node': self.id_node,
            'infos': self.infos,
            'id_dataset': self.id_dataset,
            'ds_size': self.ds_size
        }

    @staticmethod
    def from_dict(data):
        return Task(data['id_task'], data['id_node'], data['infos'], data['id_dataset'], data['ds_size'])

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(data):
        return Task.from_dict(json.loads(data))
    
    def __str__(self):
        return f"Task(id_task={self.id_task}, id_node={self.id_node}, infos={self.infos}, id_dataset={self.id_dataset}, ds_size={self.ds_size})"

class endMessage:
    def __init__(self,id_node, end_message) -> None:
        self.dist = id_node
        self.end_message = end_message
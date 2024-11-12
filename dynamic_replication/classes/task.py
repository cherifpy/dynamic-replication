
class Task:
    

    def __init__(self, task_id, execution_time, id_dataset):
        self.task_id = task_id
        self.execution_time = execution_time
        self.id_dataset = id_dataset   
        self.host_node = -1
        self.starting_time = None
        self.is_finished = False
        self.executed = False
        

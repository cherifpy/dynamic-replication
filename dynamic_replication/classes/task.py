
class Task:
    

    def __init__(self, task_id, execution_time, id_dataset):
        self.task_id = task_id
        self.execution_time = execution_time
        self.id_dataset = id_dataset   
        self.host_node = None
        self.starting_time = None
        self.finishing_time = 0
        self.size_dataset = 0
        self.is_finished = False
        self.executed = False
        self.state = "NotStarted" #🤣 Started Finished .....
        

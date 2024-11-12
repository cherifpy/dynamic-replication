

class Job:
    nb_job = 0 
    def __init__(self, nb_task, execution_times, id_dataset, size_dataset):
        """
            nb_task
            execution_times
            id_dataset
            size_dataset
        """
        self.id = Job.nb_job
        self.nb_task = nb_task
        self.execution_times = execution_times
        self.id_dataset = id_dataset
        self.size_dataset = size_dataset 
        self.nb_task_not_lunched = nb_task
        self.job_starting_time = 0
        self.job_finished = False
        self.starting_times = []
        self.ids_nodes = []
        self.tasks_list = []
        self.finishing_time = 0
        self.executing_tasks = []
        self.finished_tasks = []

    @classmethod
    def createJob():
        pass
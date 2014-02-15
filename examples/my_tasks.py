from monque.task import Task

class Add(Task):
    def run(self, a, b):
        return a + b

class Subtract(Task):
    def run(self, a, b):
        return a - b


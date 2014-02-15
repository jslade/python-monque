
import logging

from monque.queue import Monque
from monque.task import Task
import monque.config
import monque.instance

# Add a null handler to root logger to suppress "no handlers" warning
# The logging.NullHandler is new in python 2.7, so define our own:
class _NullHandler(logging.Handler):
    def emit(self,record): pass
    def handle(self,record): pass
    def createLock(self): 
        self.lock = None
        return None
logging.getLogger().addHandler(_NullHandler())

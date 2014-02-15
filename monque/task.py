
import logging

import monque.instance
from monque.config import Configuration


class Task(object):
    """
    A Task object can be executed remotely via the queue, or can be executed directly
    as a callable or just invoking the run() method.

    A Task object is actually more of an 'actor', in that the remote workers will only
    instantiate the Task class once, and reuse that instance every time it is processing
    a Task of that type. So implementation should be careful not to store state in the
    instance (except perhaps cached data to be reused for future invocations of the task)
    """

    def __init__(self,**kwargs):
        self.monque = kwargs.pop('monque',None)
        if not self.monque:
            self.monque = monque.instance.current_instance
        if not self.monque:
            raise Task.NoQueue("No Monque queue instance")

        self.logger = kwargs.pop('logger',None)
        if not self.logger:
            self.logger = self.monque.get_logger() if self.monque else \
                logging.getLogger('monque.task')

        self.init_config(kwargs)

    def init_config(self,kwargs):
        """
        Create a config object that encapsulates all the config settings
        for this task, including those inherited from base classes, from the
        queue, etc
        """

        self.config = Configuration(**kwargs)
        self.config.parent = self.monque.config

        # Iterate through the list of base classes to get configs from class members.
        # Iteration is done in reverse order, so the lowest in the class hierarchy
        # will 'stick' in the final config
        mro = [x for x in self.__class__.__mro__]
        mro = filter(lambda cls: cls != object,mro)
        mro.reverse()
        for cls in mro:
            #print "mro cls=%s" % (cls)
            for k,v in cls.__dict__.iteritems():
                if k.startswith('__'):
                    continue
                self.config.set(k,v)


    def get_name(self):
        return self.__class__.__name__

    def __call__(self,*args, **kwargs):
        return self.run(*args,**kwargs)

    def run(self, *args, **kwargs):
        """ This is to be implemented by subclasses """
        raise NotImplementedError()
        

    def post(self,args=[],kwargs={},**config):
        """
        Submit this task to the queue to be executed by a (remote) worker.
        Result is a TaskRemote instance that can be used to monitor progress and
        get results back.
        """
        return self.monque.post(self,args,kwargs,config)



    @classmethod
    def find_task_class(klass,class_name):
        all_subclasses = klass.find_all_task_classes()

        # Find fullname match if possible:
        for sub in all_subclasses:
            full_name = sub.__module__ + '.' + sub.__name__
            if full_name == class_name:
                return sub

        # Find short name match:
        short = class_name.split('.')[-1]
        for sub in all_subclasses:
            if sub.__name__ == short:
                return sub

        raise Task.ClassNotFound(class_name)


    @classmethod
    def find_all_task_classes(klass):
        all = []
        subs = [s for s in klass.__subclasses__()]
        while subs:
            sub = subs.pop(0)
            all.append(sub)
            for subsub in sub.__subclasses__():
                subs.append(subsub)

        not_obsolete = filter(lambda sub: '__obsolete__' not in sub.__dict__,all)

        return sorted(not_obsolete)


    class NoQueue(Exception):
        pass

    class ClassNotFound(Exception):
        pass



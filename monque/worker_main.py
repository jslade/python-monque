
import os, sys, logging, types, datetime, socket, threading, time
import pymongo


class WorkerMain(object):
    # When worker module is instantiated as the main module,
    # create a single worker instance and consume tasks:
    def main(self,worker_class,args):
        self.worker_class=worker_class
        self.parse_args(args)

        if self.options.control_msg:
            self.send_control_msg()

        elif self.options.find_orphaned:
            self.find_orphaned_tasks()
        elif self.options.reset_orphaned:
            self.reset_orphaned_tasks(self.options.reset_orphaned)
        elif self.options.abort_orphaned:
            self.abort_orphaned_tasks(self.options.abort_orphaned)

        else:
            self.work()

    def parse_args(self,args):
        import optparse
        op = optparse.OptionParser()
        op.add_option('--name', type='string', dest='name')
        op.add_option('--threads', type='int', dest='thread_count',
                      default=1)
        op.add_option('--queue', type='string',action='append', dest='queues')

        op.add_option('--include', type='string', action='append', dest='includes',
                      default=[])
        op.add_option('--include-dir', type='string', action='append', dest='include_dirs',
                      default=[])

        op.add_option('--max-count', type='int', dest='max_run_count')
        op.add_option('--max-time', type='float', dest='max_run_time')
        op.add_option('--max-errors', type='int', dest='max_exception_count')
        op.add_option('--max-idle', type='float', dest='max_idle_time')

        op.add_option('--verbose', action='store_true', dest='verbose')

        op.add_option('--control', type='string', dest='control_msg')
        op.add_option('--find-orphaned', action='store_true', dest='find_orphaned')
        op.add_option('--reset-orphaned', type='string', dest='reset_orphaned')
        op.add_option('--abort-orphaned', type='string', dest='abort_orphaned')

        self.options, self.args = op.parse_args(args)

    def work(self):
        kwargs = { 'name': self.options.name,
                   'num_threads': self.options.thread_count,
                   'debug': self.options.verbose,
                   'include_dirs': self.options.include_dirs,
                   'includes': self.options.includes,
                   'queues': self.options.queues }

        if self.options.max_run_count:
            kwargs['max_run_count'] = self.options.max_run_count
        if self.options.max_run_time:
            kwargs['max_run_time'] = self.options.max_run_time
        if self.options.max_exception_count:
            kwargs['max_exception_count'] = self.options.max_exception_count
        if self.options.max_idle_time:
            kwargs['max_idle_time'] = self.options.max_idle_time

        worker = self.worker_class(**kwargs)
        worker.run()

    
    def get_util_worker(self):
        """
        Create and return a Worker instance that can be used for various utility work,
        but not to consume tasks
        """
        return self.worker_class(name=self.options.name,
                                 debug=self.options.verbose,
                                 queues=self.options.queues)

    def send_control_msg(self):
        """
        Broadcast a control message to the configured queues (all by default)
        """
        worker = self.get_util_worker()
        worker.send_control_msg(self.options.control_msg)


    def find_orphaned_tasks(self):
        worker = self.get_util_worker()
        orph = WorkerOrphanage(worker)
        all_tasks = orph.find_orphaned_tasks()

        if not all_tasks:
            worker.logger.info("No orphaned tasks found")
            return

        for worker_name in sorted(all_tasks.keys()):
            tasks = all_tasks[worker_name]
            worker.logger.warning("Worker process '%s' left behind %d orphaned tasks" %
                                  (worker_name,len(tasks)))
            

    def reset_orphaned_tasks(self,worker_name):
        worker = self.get_util_worker()
        orph = WorkerOrphanage(worker)

        tasks = orph.find_orphaned_tasks(worker_name)
        if tasks:
            worker.logger.warning("Resetting %d orphaned tasks for worker process '%s'" %
                                  (len(tasks),worker_name))
            orph.reset_orphaned_tasks(worker_name,tasks)

    def abort_orphaned_tasks(self):
        worker = self.get_util_worker()
        orph = WorkerOrphanage(worker)

        tasks = orph.find_orphaned_tasks(worker_name)
        if tasks:
            worker.logger.warning("Resetting %d orphaned tasks for worker process '%s'" %
                                  (len(tasks),worker_name))
            orph.reset_orphaned_tasks(worker_name,tasks)



class WorkerOrphanage(object):
    """
    Helper class that can find (and cleanup) tasks that have been orphaned: taken by a worker,
    but not completed before the worker process went away.
    """

    def __init__(self,worker):
        self.queue = worker
        self.logger = self.queue.logger


    def find_orphaned_tasks(self,worker_name=None):
        """
        Find orphaned tasks for the specific worker, or for all.
        Orphaned tasks are identified by the worker.name field in the task,
        and there not being a corresponding current worker entry for the given
        worker name
        """
        
        if worker_name:
            return self.group_orphaned_tasks(self.find_orphaned_tasks_for_worker(worker_name))
        else:
            return self.group_orphaned_tasks(self.find_all_orphaned_tasks())


    def find_orphaned_tasks_for_worker(self,worker_name):
        worker_exists = self.queue.workers_collection.find({'name':worker_name})
        if worker_exists:
            self.logger.warning("No orphaned tasks for worker '%s' because it's still alive" %
                                (worker_name))
            return None

        return self.queue.tasks_collection.find({'worker.name':worker_name},
                                                sort=[('taken_at',pymongo.ASCENDING)])

        

    def find_all_orphaned_tasks(self):
        all_workers = self.queue.workers_collection.find({},fields={'name':True})
        worker_names = map(lambda w: w['name'],all_workers)

        return self.queue.tasks_collection.find({'$and': [{'worker':{'$exists': True}},
                                                          {'worker.name':{'$nin': worker_names}}] },
                                                sort=[('worker.name',pymongo.ASCENDING),
                                                      ('taken_at',pymongo.ASCENDING)])


    def group_orphaned_tasks(self,all_tasks):
        grouped = {}
        curr_name = None
        curr_group = None

        for task in all_tasks:
            worker_name = task.get('worker',{}).get('name','unknown')
            if worker_name != curr_name:
                curr_name = worker_name
                curr_group = []
                groupd[curr_name] = curr_group

            curr_group.append(task)

        return grouped
                


import pymongo
import pymongo.errors
import os, sys, logging, types, datetime, socket, threading, time
import traceback

from bson import BSON

if __name__ == '__main__':
    # When loaded directly as the main module, need to be able to
    # access other modules from the parent:
    import inspect 
    dirname = os.path.dirname(inspect.getfile(inspect.currentframe()))
    updir = os.path.dirname(dirname)
    sys.path.append(updir)


from monque.queue import Monque, PostedTask
from monque.config import Configuration
from monque.task import Task


class Worker(Monque):
    """
    Class to consume tasks from the queue and execute them, storing the results back in the queue.
    Workers are actually created and managed by the WorkerPool class (below).

    """

    def __init__(self,**kwargs):
        super(Worker,self).__init__(**kwargs)

        self.started_at = datetime.datetime.utcnow()
        self.idle_since = datetime.datetime.utcnow()

        self.worker_name = kwargs.pop('name',None)
        if not self.worker_name:
            self.worker_name = '%s:pid=%s:started=%s' % \
                (socket.gethostname(),
                 os.getpid(),
                 self.started_at.strftime('%Y%m%d-%H:%M'))

        self.worker_id = {
            'name': self.worker_name,
            'host': socket.gethostname(),
            'pid': os.getpid(),
            }

        self.num_threads = kwargs.pop('num_threads',1)
        if self.num_threads < 1:
            raise RuntimeError("num_threads must be at least 1 (%s given)" % (self.num_threads))

        self.queues = kwargs.pop('queues',None)
        self.includes = kwargs.pop('includes',[])
        self.include_dirs = kwargs.pop('include_dirs',[])

        self.lock = threading.Condition(threading.RLock())

        self.control_initialized = False
        self.running = False
        self.paused = False
        self.throttled = False

        self.current_workers_update_interval = float(self.config.get('worker.update_interval',30))
        self.wait_interval = float(self.config.get('worker.wait_interval',10))
        self.max_run_count = int(kwargs.pop('max_run_count',
                                            self.config.get('worker.max_run_count',0)))
        self.max_run_time = float(kwargs.pop('max_run_time',
                                             self.config.get('worker.max_run_time',0)))
        self.max_exception_count = int(kwargs.pop('max_exception_count',
                                                  self.config.get('worker.max_exception_count',0)))
        self.max_idle_time = float(kwargs.pop('max_idle_time',
                                              self.config.get('worker.max_idle_time',0)))

        self.loaded_modules = {}
        self.known_tasks = {}
        self.current_task = None

        self.run_count = 0
        self.run_time = 0
        self.exception_count = 0

        # A thread to monitor new activity in the queues:
        self.activity_thread = threading.Thread(target=self.activity_loop,name='activity_loop')
        self.activity_thread.daemon = True

        # A thread to consume control messages:
        self.control_thread = threading.Thread(target=self.control_loop,name='control_loop')
        self.control_thread.daemon = True

        # Threads to consume tasks
        self.task_threads = []
        for i in range(self.num_threads):
            task_thread = threading.Thread(target=self.task_loop,name='task_loop[%d]'%(i))
            task_thread.daemon = True
            self.task_threads.append(task_thread)


    def run(self):
        """
        Run the worker: Start the various threads, then run forever
        """

        self.logger.info("%s: run()" % (self.worker_name))

        self.load_includes()
        self.report_known_tasks()

        self.running = True
        for th in self.task_threads: th.start()
        self.activity_thread.start()
        self.control_thread.start()

        try:
            self.main_loop()
        except KeyboardInterrupt:
            self.logger.warning("%s: run() interrupted" % (self.worker_name))
        finally:
            self.stop_running()

        self.logger.info("%s: run() ended" % (self.worker_name))

        self.logger.debug("%s: waiting for control_thread" % (self.worker_name))
        with self.lock: self.lock.notifyAll()
        self.control_thread.join()

        self.logger.debug("%s: waiting for activity_thread" % (self.worker_name))
        with self.lock: self.lock.notifyAll()
        self.activity_thread.join()

        self.logger.debug("%s: waiting for task_threads" % (self.worker_name))
        for th in self.task_threads:
            with self.lock: self.lock.notifyAll()
            th.join()

        self.logger.info("%s: run() exiting" % (self.worker_name))
        

    def load_includes(self):
        """
        Load all of the specified include modules
        """
        self.logger.info("%s: Loading includes" % (self.worker_name))

        if not len(self.loaded_modules):
            for dir in self.include_dirs:
                sys.path.insert(0,dir)
            
        for module_name in self.includes:
            self.logger.info("%s: Loading: %s" % (self.worker_name,module_name))
            mod = __import__(module_name)
            self.loaded_modules[module_name] = mod


    def report_known_tasks(self):
        """
        Report all the known Task classes:
        """
        modules = sorted(self.loaded_modules.keys())
        formatted = "\n    ".join([str(m) for m in modules])
        self.logger.info("%s: All loaded modules:\n    %s" % (self.worker_name,formatted))

        task_classes = Task.find_all_task_classes()
        formatted = "\n    ".join([t.__name__ for t in task_classes])
        self.logger.info("%s: All known task classes:\n    %s" % (self.worker_name,formatted))


    def reload_tasks(self):
        """
        Called when the worker gets a 'reload' control message.
        """
        modules = sorted(self.loaded_modules.keys())
        self.logger.info("%s: Re-loading modules: %s" % (self.worker_name,', '.join(modules)))
        
        # First, find all existing Task subclasses and mark the as obsolete,
        # so that the newly-loaded version (which will have the same name) can take precedence
        old_task_classes = Task.find_all_task_classes()
        for task_class in old_task_classes:
            task_class.__obsolete__ = True

        # Toss any previously-instantiated tasks, so newly-loaded version of class can be used.
        self.known_tasks = {}

        # Now request to reload each of the originally-loaded modules:
        for module_name in modules:
            self.logger.info("%s: Re-loading module: %s" % (self.worker_name,module_name))
            mod = self.loaded_modules[module_name]
            try:
                new_mod = reload(mod)
                self.loaded_modules[module_name] = new_mod
            except:
                ex = sys.exc_info()
                self.logger.error("%s: Failed to reload module: %s: %s\n%s" % 
                                  (self.worker_name,module_name,
                                   str(ex[1]),traceback.format_exc(ex[2])))

        # Report on the new 
        self.report_known_tasks()



    def main_loop(self):
        """
        Main loop for the worker, runs in the 'main' thread.
        Just waits indefinitely while the task thread consumes tasks,
        and checks if timeout or other limits are reached.
        """

        last_update = 0

        while self.running:
            # Poll for a task:
            # Actually, just signal the lock so the other thread wakes
            # up to do the polling:
            # So this lock.wait() will stop either when the wait_interval is reached,
            # or when the lock is signaled from another thread -- which will happen
            # each time a task is run. So this loop may execute very frequently, if there
            # are pending tasks in the queue. Therefore, the loop should be pretty short.
            with self.lock:
                self.lock.wait(self.wait_interval)

            # After waking up, check if timeout conditions are reached
            if self.check_timeout():
                self.logger.warning("%s: run() finishing on run limit" % (self.worker_name))
                self.stop_running()

            # Periodically update the "current workers" record
            last_update = self.update_current_workers(last_update)


    def check_timeout(self):
        """
        Check if the worker has 'timed out' and should exit
        The worker can be terminated by certain conditions:
        - max number of executed jobs
        - max runtime duration
        - max idle time
        """

        with self.lock:
            current = self.current_task
            idle_since = self.idle_since

        if self.max_run_count and \
                self.run_count >= self.max_run_count:
            self.logger.warning("%s: worker reached max_run_count (%d)" %
                                (self.worker_name,self.run_count))
            return True

        if self.max_exception_count and \
                self.exception_count >= self.max_exception_count:
            self.logger.warning("%s: worker reached max_exception_count (%d)" %
                                (self.worker_name,self.exception_count))
            return True

        if self.max_run_time and current:
            # use total run time, not current
            if self.run_time >= self.max_run_time:
                self.logger.warning("%s: worker reached max_run_time (%s)" %
                                    (self.worker_name,self.run_time))
                return True

        if self.max_idle_time and not current:
            # Use current idle time, not total
            idle_time = datetime.datetime.utcnow() - idle_since
            try: idle_secs = idle_time.total_seconds()
            except:
                # total_seconds new in python 2.7
                idle_secs = (idle_time.microseconds +
                             (idle_time.seconds + idle_time.days * 24 * 3600) * 10**6) / 10**6

            if idle_secs >= self.max_idle_time:
                self.logger.warning("%s: worker reached max_idle_time (%s)" %
                                    (self.worker_name,idle_secs))
                return True

        return False


    def update_current_workers(self,last_update):
        """
        Update the entry in the 'current_workers' collection for this worker, so that it stays fresh.
        The workers need to periodically update themselves to stay fresh in the collection,
        otherwise they will be automatically removed from the collection (based on a TTL index)

        The current workers record includes the current task (if any), but it does not always
        stay current with the latest test -- the update frequency is potentially far lower than
        the frequency of new tasks.
        """

        now = time.time()
        elapsed = now - last_update
        if elapsed < self.current_workers_update_interval:
            # Not time to update yet
            return last_update

        query = { 'name': self.worker_name }
        update = { 'name': self.worker_name,
                   'queues': self.queues,
                   'host': socket.gethostname(),
                   'pid': os.getpid(),
                   'started_at': self.started_at,
                   'updated_at': datetime.datetime.utcnow(),
                   'idle_since': self.idle_since,
                   'current_task': { 'task': None,
                                     'started_at': None },
                   'run_totals': { 'count': self.run_count,
                                   'exceptions': self.exception_count,
                                   'elapsed': self.run_time },
                   }
        current = self.current_task
        if current:
            (task,started_at) = current
            update['current_task']['task'] = task.doc
            update['current_task']['started_at'] = started_at

        self.workers_collection.find_and_modify(query,update,upsert=True)

        return now


    def task_loop(self):
        """
        Loop in which the worker waits for a task to be available, then executes it.
        Multiple threads may be running the same loop.
        """
        self.logger.debug("%s: task_loop() start" % (self.worker_name))

        while not self.control_initialized:
            with self.lock:
                self.lock.wait()

        while self.running:
            try:
                # Poll for the next task, then execute it
                posted_task = self.get_next_task()
                if posted_task:
                    self.execute_task(posted_task)
                
                    # After executing each task, notfiy the lock
                    # to wake up the other threads
                    with self.lock:
                        self.lock.notifyAll()

                else:
                    # No task was available, so wait some time before asking for another
                    if self.running:
                        with self.lock:
                            self.lock.wait(self.wait_interval)

            except pymongo.errors.OperationFailure, ex:
                self.logger.error("%s: failed in task_loop due to: %s" %
                                  (self.worker_name,ex))
                
                # Remove this thread from the pool of task threads. If all the task
                # threads are gone, then time to exit:
                th = threading.current_thread()
                if th in self.task_threads:
                    self.task_threads.remove(th)
                if not self.task_threads():
                    self.logger.error("%s: All task threads are gone" % (self.worker_name))
                    self.stop_running()


    def get_next_task(self):
        """
        Poll the task queue for the next available task.
        """

        if not self.running:
            return None
        if self.paused:
            return None

        task_doc = PostedTask.get_next(collection=self.tasks_collection,
                                       queue=self.queues,
                                       worker=self.worker_id)
        if not task_doc:
            #self.logger.debug("get_next_task: got nothing")
            return None

        
        #self.logger.debug("get_next_task: got doc = %s" % (task_doc))

        payload = task_doc['payload']
        args = payload['args']
        kwargs = payload['kwargs']
        constraints = task_doc['constraints']

        task_name = task_doc['name']
        class_name = task_doc['class']
        
        task = self.get_task_instance(task_name,class_name)

        posted_task = PostedTask(self,task,args,kwargs,constraints)
        posted_task.collection = self.tasks_collection
        posted_task.id = task_doc['_id']
        posted_task.doc = task_doc

        if self.can_run_task(posted_task):
            return posted_task
        else:
            posted_task.unget()


    def get_task_instance(self,task_name,class_name):
        """
        Get a Task instance matching the given class name.
        Task instances are actually re-used, so it is more of an 'Actor'
        than an individual task instance
        """

        if task_name in self.known_tasks:
            return self.known_tasks[task_name]

        task_class = Task.find_task_class(class_name)
        task = self.known_tasks[task_name] = task_class(monque=self)
        return task


    def can_run_task(self,posted_task):
        """
        Check task constraints prior to running tgis task instance
        """
        # TODO: not implemented...
        return True


    def execute_task(self,posted_task):
        """
        Execute the task, and save the result.
        """
        posted_task.mark_running()
        with self.lock:
            self.current_task = (posted_task,datetime.datetime.utcnow())
            self.idle_since = None
                                                
        task = posted_task.task
        args = posted_task.args
        kwargs = posted_task.kwargs
        self.logger.info("%s: run[%d]: %s args=%s kwargs=%s" %
                         (self.worker_name,self.run_count,
                          posted_task.name,args,kwargs))

        started = time.time()
        try:
            result = task.run(*args,**kwargs)
            ended = time.time()
            self.store_task_result(posted_task,result)
        except:
            ended = time.time()
            self.store_task_exception(posted_task,sys.exc_info())

            with self.lock:
                self.exception_count += 1

        with self.lock:
            self.run_count += 1
            self.current_task = None
            self.run_time += ended - started
            self.idle_since = datetime.datetime.utcnow()


    def store_task_result(self,posted_task,result):
        """
        Store result of a 'successful' task run.
        The task is removed from the original collection (tasks), and put in the
        results collection.
        """
        posted_task.remove()

        # TODO: Need special serialization?
        try:
            # Can the result be safely BSON-encoded?
            if type(result) != dict:
                safe_result = BSON.encode({'_':result})
            else:
                safe_result = BSON.encode(result)
        except:
            safe_result = str(result)
            self.logger.warning("%s: result of %s cannot be BSON-encoded: %s: %s" %
                                (self.worker_name,posted_task.name,safe_result,
                                 sys.exc_info()[1]))
            result = safe_result

        posted_task.doc['result'] = result
        posted_task.doc['status'] = 'completed'
        posted_task.doc['completed_at'] = datetime.datetime.utcnow()

        posted_task.save_into(self.results_collection)

        posted_task.notify_results(self.activity_log)


    def store_task_exception(self,posted_task,ex):
        posted_task.remove()

        posted_task.doc['exception'] = {
            'msg': str(ex[1]),
            'trace': traceback.format_exc(ex[2]),
        }
        posted_task.doc['status'] = 'failed'
        posted_task.doc['completed_at'] = datetime.datetime.utcnow()
        
        posted_task.save_into(self.results_collection)

        posted_task.notify_results(self.activity_log)


    def activity_loop(self):
        """
        In order to keep from activity polling the task queue (tasks collection),
        the activity_log is used. activity_log is a capped collection, which allows a tailable
        cursor to be used to efficiently signal when new tasks are queued.
        """
        self.logger.debug("%s: activity_loop() start" % (self.worker_name))

        last_id = None
        for latest in self.activity_log.find().sort([('$natural',-1)]).limit(1):
            last_id = latest['_id']

        while self.running:
            # Tailable cursor for activity log, to quickly know when new tasks are available
            query = {}

            if last_id:
                query['_id'] = {'$gt':last_id}

            if self.queues:
                if len(self.queues) == 1:
                    query['queue'] = self.queues[0]
                else:
                    query['queue'] = {'$in':self.queues}
            

            tail = self.activity_log.find(query,
                                          tailable=True,
                                          await_data=False)

            # Tail the cursor until end is reached:
            try: 
                for new_task in tail:
                    last_id = new_task['_id']

                    # Just wait up the task loop:
                    with self.lock:
                        self.lock.notifyAll()
            except pymongo.errors.OperationFailure, ex:
                self.logger.error("%s: failed in activity_loop due to: %s" %
                                  (self.worker_name,ex))
                self.stop_running()

            time.sleep(0.5)


    def check_control_state(self):
        """
        Check the current 'static' control state, via the control_collection.
        This is intended to get the initial state/configuration when starting up,
        and once the initial state is established, future control state changes
        will be handled via control messages.

        i.e. determine at startup of the worker should be paused, etc.
        """
        
        for control in self.control_collection.find():
            self.handle_control_state(control)


    def handle_control_state(self,state):
        if state['name'] == 'paused':
            if state['paused']:
                self.pause()
            else:
                self.resume()

        elif state['name'] == 'stopped':
            if state['stopped']:
                self.stop()


    def control_loop(self):
        """
        Wait for new messages on the control channel (pause, etc)
        """
        self.logger.debug("%s: control_loop() start" % (self.worker_name))

        self.check_control_state()

        last_id = None
        for latest in self.control_log.find().sort([('$natural',-1)]).limit(1):
            last_id = latest['_id']

        with self.lock:
            self.control_initialized = True
            self.lock.notifyAll()

        while self.running:
            # Tailable cursor for control log, to quickly know when new control messages are available
            # By default, monitors the special queue name '*'
            query = {'queue': '*'}

            if last_id:
                query['_id'] = {'$gt':last_id}

            if self.queues:
                query['queue'] = {'$in':['*'] + self.queues}

            tail = self.control_log.find(query,
                                         tailable=True,
                                         await_data=False)

            # Tail the cursor until end is reached:
            try:
                for msg in tail:
                    last_id = msg['_id']

                    self.logger.info("%s: control msg = %s" % (self.worker_name,msg))

                    try:
                        self.handle_control_msg(msg)
                    except:
                        pass
            except pymongo.errors.OperationFailure, ex:
                self.logger.error("%s: failed in control_loop due to: %s" %
                                  (self.worker_name,ex))
                self.stop_running()


            time.sleep(0.5)


    def handle_control_msg(self,msg):
        """
        Interpret a message received on the control channel
        """

        command = msg['command']

        if command == 'reload':
            self.reload_tasks()

        elif command == 'pause':
            self.pause()

        elif command == 'resume':
            self.resume()

        elif command == 'stop':
            self.stop()


    def pause(self):
        if not self.paused:
            self.paused = True
            self.logger.warning("%s: PAUSED -- issue the 'resume' command to continue" % (self.worker_name))
            with self.lock:
                self.lock.notifyAll()

    def resume(self):
        if self.paused:
            self.paused = False
            self.logger.warning("%s: RESUMED" % (self.worker_name))
            with self.lock:
                self.lock.notifyAll()

    def stop(self):
        if self.running:
            self.logger.warning("%s: STOPPED -- issue the 'resume' command before starting workers" %
                                (self.worker_name))

            self.stop_running()


    def stop_running(self):
        self.running = False
        with self.lock:
            self.lock.notifyAll()


if __name__ == '__main__':
    from monque.worker_main import WorkerMain
    WorkerMain().main(Worker,sys.argv[1:])

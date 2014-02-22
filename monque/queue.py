
import pymongo
import pymongo.errors
import os, sys, logging, types, datetime, time

from monque.config import Configuration
import monque.instance


class Monque(object):
    """
    Class for accessing the task queue as a client to submit or control jobs.
    This is basically used for everything except the workers
    """

    def __init__(self,**kwargs):
        """
        Create an instance using the configuration given in the kwargs
        """
        self.logger = kwargs.pop('logger',logging.getLogger('monque'))
        if type(self.logger) == types.StringType:
            self.logger = logging.getLogger(self.logger)

        self.config = kwargs.pop('config',Configuration(**kwargs).load_from_env())

        self.setup_logging()
        self.connect()

        self.posted_count = 0

        monque.instance.current_instance = self


    def setup_logging(self):
        if self.config.get('debug'):
            self.logger.setLevel(logging.DEBUG)
        elif self.config.get('verbose'):
            self.logger.setLevel(logging.INFO)

        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        self.logger.debug("did setup_logging()")


    def get_logger(self):
        return self.logger


    def connect(self):
        self.logger.debug("Monque.connect()")
        host = self.config.get('mongo.host','localhost')
        if ':' in host:
            host, port = host.split(':',1)
            port = int(port)
        else:
            port = int(self.config.get('mongo.port',27017))

        db_name = self.config.get('mongo.db','monque')

        self.connection = pymongo.MongoClient(host,port)
        self.db = self.connection[db_name]

        self.global_config = Configuration.get_global(self.db,
                                                      self.config.get('mongo.config','config'))
        self.config.parent = self.global_config

        self.init_collections()


    def init_collections(self):
        """
        Setup the various collections used to manage tasks and record results.
        """

        # Pending tasks: tasks that are waiting to run, or are in the process of running
        # Nothing special about this collection
        tasks_name = self.config.get('tasks.collection_name','tasks')
        self.logger.debug("init_collections: tasks_name=%s" % (tasks_name))
        self.tasks_collection = self.db[tasks_name]

        self.tasks_collection.ensure_index([('class',pymongo.ASCENDING),
                                            ('status',pymongo.ASCENDING)])
        self.tasks_collection.ensure_index([('class',pymongo.ASCENDING),
                                            ('status',pymongo.ASCENDING),
                                            ('submitted_at',pymongo.ASCENDING)])

        # Retired tasks: tasks + results (success or failure)
        # Uses a TTL index so that results will automatically be removed over time.
        results_name = self.config.get('results.collection_name','results')
        results_ttl = int(self.config.get('results.ttl', 3600 * 24 * 7)) # 7 days default
        self.logger.debug("init_collections: results_name=%s" % (results_name))
        self.logger.debug("init_collections: results_ttl=%s" % (results_ttl))
        self.results_collection = self.db[results_name]

        # This index ensures results expire:
        self.results_collection.ensure_index([('completed_at',pymongo.ASCENDING)],
                                             expireAfterSeconds=results_ttl)

        # Are both of these indexes needed?
        self.results_collection.ensure_index([('status',pymongo.ASCENDING)])
        self.results_collection.ensure_index([('status',pymongo.ASCENDING),
                                              ('queue',pymongo.ASCENDING)])


        # Current workers:
        workers_name = self.config.get('worker.collection_name','current_workers')
        workers_ttl = 3 * int(self.config.get('worker.update_interval', 30))
        self.logger.debug("init_collections: workers_name=%s" % (workers_name))
        self.workers_collection = self.db[workers_name]

        self.workers_collection.ensure_index([('name',pymongo.ASCENDING)])

        # This index ensures workers expire:
        self.workers_collection.ensure_index([('updated_at',pymongo.ASCENDING)],
                                             expireAfterSeconds=workers_ttl)

        self.workers_collection.ensure_index([('task._id',pymongo.ASCENDING)])
        self.workers_collection.ensure_index([('task.class',pymongo.ASCENDING),
                                              ('task.queue',pymongo.ASCENDING)])
        self.workers_collection.ensure_index([('task.queue',pymongo.ASCENDING)])


        # Activity log: a capped collection that is updated when there is activity on
        # a queue (new tasks). This is used by workers to tail the collection to quickly
        # discover new tasks, without having to poll.
        activity_name = self.config.get('activity.collection_name','activity_log')
        activity_size = int(self.config.get('activity.collection_size',100e+6)) # default: 100M
        try:
            self.activity_log = self.db.create_collection(activity_name,
                                                          size=activity_size,
                                                          capped=True)
        except pymongo.errors.CollectionInvalid:
            self.activity_log = self.db[activity_name]
                                
        
        # Control state: current control settings
        control_name = self.config.get('control.collection_name','control_state')
        self.control_collection = self.db[control_name]
        self.control_collection.ensure_index([('name',pymongo.ASCENDING)])

        # Control log: a capped collection used to broadcast control messages to workers
        control_log_name = self.config.get('control_log.collection_name','control_log')
        control_log_size = int(self.config.get('control_log.collection_size',100e+6)) # default: 100M
        try:
            self.control_log = self.db.create_collection(control_log_name,
                                                         size=control_log_size,
                                                         capped=True)
        except pymongo.errors.CollectionInvalid:
            self.control_log = self.db[control_log_name]



    def post(self,task,args,kwargs,config):
        """
        Add a task to the queue. This is not typically called directly, but rather via Task.post()
        """

        post = PostedTask(self,task,args,kwargs,config)

        # Check whether the task can post.
        # If it can't be posted, this will raise an error
        # It's possible that configs / filters reject it, but in
        # a way that mean it should just be ignored, in which cast this
        # will return False:
        if not self.check_post(post):
            # post is filtered out, silently ignored
            self.logger.debug("Ignoring task post: task=%s args=%s kwargs=%s" %
                              (post.name,post.args,post.kwargs))
            return None 

        self.logger.debug("Posting task=%s args=%s kwargs=%s" %
                          (post.name,post.args,post.kwargs))

        post.save_into(self.tasks_collection)
        post.notify_workers(self.activity_log)

        self.logger.info("Posted id=%s task=%s args=%s kwargs=%s" %
                         (post.id,post.name,post.args,post.kwargs))

        self.posted_count += 1
        return post


    def check_post(self,post):
        """
        Check whether the task can be posted.
        - is valid
        - satisfies constraints
        - ...
        """
        
        # Not implemented ...
        return True


    def send_control_msg(self,command):
        """
        This is intended to be used from WorkerMain to broadcast control messages to 
        all workers (or all workers for specific queues).
        
        It can also be used from a client, however, to control the state of the queues it 
        is connected to.
        """

        queues = self.queues or ['*']
        for queue in queues:
            msg = { 'command': command, 'queue': queue }
            self.logger.warning("Sending control message: %s" % (msg))
            self.control_log.insert(msg)

            self.update_control_state(queue,command)


    def update_control_state(self,queue,command):
        """
        The pause command will cause all workers to stop executing jobs, but they
        will continue to run, waiting for the signal to resume.

        The stop command will cause all workers to terminate, and it will prevent
        any new workers from starting up until the resume signal is given.

        The resume command un-pauses any paused workers, as well as allowing new
        workers to start up (depending on some other mechanism to spawn them)
        """

        if command == 'pause':
            obj = self.control_collection.find_and_modify({'name':'paused',
                                                           'queue':queue},
                                                          {'$set':{'name':'paused',
                                                                   'queue':queue,
                                                                   'paused':True}},
                                                          upsert=True,new=True)
            self.logger.debug("after pause: %s" % (obj))

        elif command == 'resume':
            obj = self.control_collection.find_and_modify({'name':'paused',
                                                           'queue':queue},
                                                          {'$set':{'name':'paused',
                                                                   'queue':queue,
                                                                   'paused':False}},
                                                          upsert=True,new=True)
            self.logger.debug("after resume: %s" % (obj))

            obj = self.control_collection.find_and_modify({'name':'stopped',
                                                           'queue':queue},
                                                          {'$set':{'name':'stopped',
                                                                   'queue':queue,
                                                                   'stopped':False}},
                                                          upsert=True,new=True)
            self.logger.debug("after un-stop: %s" % (obj))

        elif command == 'stop':
            obj = self.control_collection.find_and_modify({'name':'stopped',
                                                           'queue':queue},
                                                          {'$set':{'name':'stopped',
                                                                   'queue':queue,
                                                                   'stopped':True}},
                                                          upsert=True,new=True)
            self.logger.debug("after stop: %s" % (obj))

        else:
            self.logger.error("Unrecongized control message: %s" % (msg))


    def count_posted(self):
        return self.posted_count


    def count_pending(self,queue=None,queues=None):
        query = {'status':'pending'}

        if queue:
            query['queue'] = queue
        elif queues:
            query['queue'] = {'$in':queues}

        return self.tasks_collection.find(query).count()


    def count_running(self,queue=None,queues=None):
        query = {'status':'running'}

        if queue:
            query['queue'] = queue
        elif queues:
            query['queue'] = {'$in':queues}

        return self.tasks_collection.find(query).count()

    
    def count_completed(self,queue=None,queues=None):
        query = {'status':'completed'}

        if queue:
            query['queue'] = queue
        elif queues:
            query['queue'] = {'$in':queues}

        return self.results_collection.find(query).count()

    
    def count_failed(self,queue=None,queues=None):
        query = {'status':'failed'}

        if queue:
            query['queue'] = queue
        elif queues:
            query['queue'] = {'$in':queues}

        return self.results_collection.find(query).count()

    

class PostedTask(object):
    """
    In-memory representation of a task posted (or to be posted) to a queue.
    """

    def __init__(self,monque,task,args,kwargs,config):
        self.config = Configuration(**config)
        self.config.parent = task.config

        self.monque = monque
        self.task = task
        self.name = task.get_name()
        self.args = args
        self.kwargs = kwargs

        self.collection = None
        self.id = None
        self.doc = None

        self.logger = self.task.logger

        self.queue = self.config.get('queue','default')

        self.start_time = self.get_start_time()
        self.result = None

        self.max_in_queue = int(self.config.get('max_in_queue',0))
        self.max_running = int(self.config.get('max_running',0))
        self.must_be_unique = self.config.get('must_be_unique',False)
        self.unique_kwargs = self.config.get('unique_kwargs',None)
    

    def get_start_time(self):
        absolute = self.config.get('at')
        if absolute:
            if isinstance(absolute,datetime.datetime):
                return absolute
            elif type(absolute) == int or \
                    type(absolute) == float:
                return datetime.datetime.fromtimestamp(absolute)
            raise ValueError("Unrecognized format of 'at': %s" % (absolute))

        delay = self.config.get('delay')
        if delay:
            if isinstance(absolute,datetime.timedelta):
                return datetime.datetime.utcnow() + delay
            elif type(delay) == int or \
                    type(delay) == float:
                return datetime.datetime.utcnow() + datetime.timedelta(seconds=delay)
            raise ValueError("Unrecognized format of 'delay': %s" (delay))

        return None
        

    def save_into(self,collection):
        """
        This is where the task actually gets inserted into the collection.
        TODO: options for write concern, etc?
        """
        #self.logger.debug("Task save_into() collection=%s task=%s args=%s kwargs=%s" %
        #                  (collection,self.name,self.args,self.kwargs))
        if not self.doc:
            self.doc = self.serialize()
        #self.logger.debug("Task save_into() doc=%s" % (self.doc))
        collection.save(self.doc)
        self.id = self.doc['_id']
        #self.logger.debug("Task save_into() id=%s" % (self.id))


    def mark_running(self):
        if self.doc:
            self.doc['status'] = 'running'
            self.doc['started_at'] = datetime.datetime.utcnow()

        if self.collection and self.id:
            self.collection.find_and_modify(query={'_id':self.id},
                                           update={'$set':{'status':self.doc['status'],
                                                           'started_at':self.doc['started_at']}},
                                            )

    def remove(self):
        if self.collection and self.id:
            self.collection.find_and_modify(query={'_id':self.id},
                                            remove=True)


    def serialize(self):
        """
        Return a serialized version (dict) of the task, as it is to be stored
        in the collection
        """
        doc = {'name': self.name,
               'class': self.task.__module__ + '.' + self.task.__class__.__name__,
               'queue': self.queue,
               'payload': { 'args': self.args,
                            'kwargs': self.kwargs },

               'constraints': { },

               'created_at': datetime.datetime.utcnow(),
               'submitted_at': datetime.datetime.utcnow(),
               'status': 'pending',
               }

        # Add constraints:
        if self.start_time:
            doc['constraints']['start_time'] = self.start_time
        if self.max_in_queue:
            doc['constraints']['max_in_queue'] = self.max_in_queue
        if self.max_running:
            doc['constraints']['max_running'] = self.max_running
        if self.must_be_unique:
            doc['constraints']['must_be_unique'] = True
            if self.unique_kwargs:
                doc['constraints']['unique_kwargs'] = self.unique_kwargs

        return doc
            

    def notify_workers(self,collection):
        """
        Add a doc to the collection (the activity log) that indicates new tasks in the queue,
        so workers that are tailing the collection can immediately pick it up
        """
        collection.insert({'task':self.id,
                           'queue':self.queue})


    def notify_results(self,collection):
        """
        Add a doc to the collection (the activity log) that indicates task results are available,
        so clients waiting for the results can immediately pick it up
        """
        collection.insert({'result':self.id})


    @classmethod
    def get_next(klass,**kwargs):
        collection = kwargs.pop('collection')
        queue = kwargs.pop('queue',None)

        # Set up the queury filters:
        query = {'status':'pending'}

        if queue:
            if type(queue) == str:
                query['queue'] = queue
            elif type(queue) == list:
                if len(queue) == 1:
                    query['queue'] = queue[0]
                else:
                    query['queue'] = {'$in':queue}

        now = datetime.datetime.utcnow()
        query['$or'] = [{'constraints.start_time':{'$exists':False}},
                        {'constraints.start_time':{'$lte':now}}]

        # As soon as it is picked up, mark it as 'taken',
        # which is the pre-cursor state to 'running', 
        # in which pre-run conditions are checked, etc
        update = {'$set':{'status':'taken',
                          'taken_at':datetime.datetime.utcnow()}}

        found = collection.find_and_modify(query=query,
                                           update=update,
                                           new=True,
                                           sort={'_id':1})

        return found


    def unget(self):
        """
        Put a task back into the queue that was 'incorrectly' taken.
        Usually this is for a task that is taken, then one or more pre-execution tasks
        fails (e.g. too many running tasks of a given type)
        """
        if not self.collection or not self.id:
            return

        self.logger.debug("Task unget() id=%s" % (self.id))
        
        self.collection.find_and_modify(query={'_id':self.id},
                                        update={'$et':{'status':'pending'}})



    def wait(self,timeout=None):
        """
        Wait for the results of the task to be posted to the result queue.
        If timeout (given in seconds) is not None, then wait at least that long
        for the result. If no result is available within that time, returns None.
        If the result is received, the result is returned back.
        """

        query = {'result': self.id}

        expire_at = None
        if timeout: expire_at = time.time() + timeout

        while expire_at is None or time.time() < expire_at:
            tail = self.monque.activity_log.find(query,
                                                 tailable=True,
                                                 await_data=False)
            got = False
            for doc in tail:
                got = True
            
            if got:
                break

            time.sleep(.1)

        result = self.monque.results_collection.find_one(self.id)
        if result:
            return self.handle_result(result)

        return None
        
    def handle_result(self,result):
        self.result = result

        status = self.result.get('status',None)

        if status == 'completed':
            return self.result['result']

        elif status == 'failed':
            exception = self.result['exception']
            raise PostedTask.RuntimeException(exception)


    class RuntimeException(Exception):
        pass




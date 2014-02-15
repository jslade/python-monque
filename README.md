monque
====================

Persistent task queue built on top of MongoDB.


Quick Start
====================

Define tasks in a module:

In my_tasks.py:

    from monque.task import Task
    
    class Add(Task):
        def run(self, a, b):
            return a + b
    
    class Subtract(Task):
        def run(self, a, b):
            return a - b

Submit tasks:

    from monque import Monque
    from my_tasks import *
    
    q = Monque()
    
    plus = Add().post([1,2])
    minus = Subtract().post([1,2])
    
    print "result of plus:", plus.wait()
    print "result of minus:", minus.wait()

Run a worker to consume the tasks from the queue:

    python -m monque.worker --include my_tasks --verbose


Why
====================

This motivation is simple: a persistent queue for distributing tasks
to multiple compute nodes. I like MongoDB, it's a great persistent store for my case.

I'm not trying to create an ultra-fast queue. Handling hundreds of
transactions per second is plenty fast for my needs, but there's no
reason monque can't got lots faster than that.

On the other hand, I do want to be able to actively inspect what's
going on in the queue. See what's waiting, see what's running, have
the ability to pause/resume the queue, and rate-limit tasks by various
criteria. I didn't find those capabilities in other MongoDB-based task
queues that I looked at.


Similar projects
--------------------

Why not [Celery](http://www.celeryproject.org/)?

There are plenty of good things about [celery](http://www.celeryproject.org/),
but it just didn't work for me:

- The queue itself was too opaque -- to hard to see what's queued, what's running, etc
- Too limited in terms of flow control options -- ability to pause/resume queue globally, 

Maybe it was mostly a matter of style. But Celery is not
MongoDB-specific, and therefore does not make the best use of MongoDB.



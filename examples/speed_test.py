#!/usr/bin/env python
"""
speed_test -- test how fast tasks can be submitted and processed.

The client/submitter side will run forever, submitting dummy tasks (that don't do anything)
in a loop. Periodically (every few seconds), a status line is printed showing how many
tasks are in the queue, how many running, etc.

Start the submitter side as:

    python speed_tests.py

Interrupt with Ctrl-C to end the test.

One or more workers should also be started to consume/execute the tasks:

   python -m monque.worker --include speed_test --queue speed_test


"""

from monque import Monque
from monque.task import Task


class NopTask(Task):
    queue = 'speed_test'
    def run(self):
        pass


if __name__ == '__main__':

    q = Monque()
    nop = NopTask()

    import time
    started = time.time()

    def stats():
        return { 'posted': q.count_posted(),
                 'pending': q.count_pending(queue='speed_test'),
                 'completed': q.count_completed(queue='speed_test'),
                 'failed': q.count_failed(queue='speed_test') }
    prev = stats()

    while True:
        nop.post()

        now = time.time()
        elapsed = now - started
        if elapsed >= 5:
            curr = stats()
            print "Posted: %d(%.1f/s), Pending: %d(%.1f/s), Completed: %d(%.1f/s), Failed: %d(%.1f/s)" % \
                (curr['posted'],    (curr['posted']-prev['posted'])/elapsed,
                 curr['pending'],   (curr['pending']-prev['pending'])/elapsed,
                 curr['completed'], (curr['completed']-prev['completed'])/elapsed,
                 curr['failed'],    (curr['failed']-prev['failed'])/elapsed)
        
            prev = curr
            started = now


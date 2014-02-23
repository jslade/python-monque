
import os, sys, logging, types, datetime, socket, threading, time


class WorkerMain(object):
    # When worker module is instantiated as the main module,
    # create a single worker instance and consume tasks:
    def main(self,worker_class,args):
        self.worker_class=worker_class
        self.parse_args(args)
        if self.options.control_msg:
            self.send_control_msg()
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

    def send_control_msg(self):
        """
        Broadcast a control message to the configured queues (all by default)
        """
        worker = self.worker_class(name=self.options.name,
                                   debug=self.options.verbose,
                                   queues=self.options.queues)
        worker.send_control_msg(self.options.control_msg)

    


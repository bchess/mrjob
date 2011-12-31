# vim: set expandtab tabstop=4 shiftwidth=4:
"""

General plan

initial mappers use s3 paths as inputs
Server knows all S3 paths.
Mapper nodes request an S3 path to process.
Each mapper reads that s3 path and runs the mapper on it itself.
Each map output is routed to a particular bucket (via the combiner).
Each bucket is a separate zermoq.  The mappers write to those zeromqs.  Reducers read from them


Who owns the bucket zeromqs?
There's an N-N relationship between mappers and buckets.  It doesn't make sense for the mappers to own them
There's an 1-1 relationship between buckets and reducers.  Reduce tasks, not reduce nodes.
So do we spin up a process per reduce task?  And then we just carefully throttle reducers to run one-at-a-time?
If the reduce task dies, we've lost our mapper output.  And we're boned.  But theres no way around that without a DFS.

Okay, so at startup time we spin up a reducer process *per bucket*.  They own the bucket zeromqs.
They communicate their zeromqs back to the server so that the server can inform the mappers where to direct to.

Once the mappers are all finished, the server directs individual reducer procs to start running.  It runs only a select few
at a time, based on what memory allows.

Now what about multi-step?

The reducer outputs to another mapper. (if just reducer, let's assume a nil mapper in-between).  These are zeromqs
managed by the reducer.  So the reducer doesn't go entirely away until its downstream mapper is finished.

1. Server starts up all reducer procs, specifying whether the output is an S3 path or a zeromq.
2. Server waits for all reducers to start. Each reducer provides to the server its bucket zeromq and, optionally, output zeromq
3. Server starts up individual mappers, feeding them an input (either zeromq or S3 path), plus output buckets (zeromqs)
4. Server waits for all mappers to finish.
5. Sorting
6. Server indicates to individual reducers to run their inputs.
7. Server waits for all reducers to finish (this isn't necessary, but fixing it should be a TODO)
8. If more than 1 step, runs 1-6 above, using the reducer's zeromq output as input for the next mapper.

Other things to worry about:
    Server control of other hosts: ssh & scp.
    Getting the files bootstrapped.
    Status server TODO
    Counters TODO
    Integration with MRJob TODO:
        "owning the server" is okay.  "owning the client" is not.
        The client should be implemented as a helper class.  Let someone else be the __main__
        
Responsibilities of the server:
    Know the steps
    Bootstrap the clients
    Startup the clients
    Feed control commands to clients
        

conclusion:
This is a lot of work.
"""
from collections import defaultdict
from collections import deque
import logging
import optparse
import os
import os.path
import re
import socket
import subprocess
import sys
import time
import urllib

from mrjob.parse import is_s3_uri
import tornado.httpserver
import tornado.ioloop
import tornado.web
import zmq.eventloop
import zmq.eventloop.zmqstream
import zmq.core.context

log = logging.getLogger('mrjob.zeromq.server')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(process)d:%(name)s:%(levelname)s %(message)s')
logging.getLogger('boto').setLevel(logging.INFO)

DEBUG_NO_SSH = False
EC2_RESERVATION_URL = 'http://169.254.169.254/latest/meta-data/reservation-id'
EC2_INSTANCE_ID_URL = 'http://169.254.169.254/latest/meta-data/instance-id'
EC2_AVAILABILITY_ZONE_URL = 'http://169.254.169.254/latest/meta-data/placement/availability-zone'

EC2_INSTANCE_TYPE_TO_CORES = {
    'm1.small': 1,
    'm1.large': 2,
    'm1.xlarge': 4,
    'c1.medium': 2,
    'c1.xlarge': 8,
    'm2.xlarge': 2,
    'm2.2xlarge': 4,
    'm2.4xlarge': 8,
    'cc1.4xlarge': 8,
    'cc2.8xlarge': 16,
    't1.micro': 1
}


class Server(object):
    def __init__(self):
        self.options = {}
        self.context = zmq.core.context.Context()
        self.client_addresses = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: None)))
        self.waiting_for_hello = set()
        self.num_tasks_done = defaultdict(lambda: 0)
        self.downstream_requests = []
        self.running_last = 0
        self.last_step_num = -1

    def start(self):
        self.build_option_parser()
        self.read_config()
        if not self.options.get('clients') or not self.options.get('inputs'):
            self.parser.print_help()
            sys.exit(-1)

        zmq.eventloop.ioloop.install()
        self.ioloop = tornado.ioloop.IOLoop.instance()

        web_application = tornado.web.Application(
            [
                (r'/', WebRequestHandler),
            ],
        )

        http_server = tornado.httpserver.HTTPServer(web_application, io_loop=self.ioloop)
        http_server.listen(2680)

        self.server_socket, self.server_socket_address = self.create_server_socket(zmq.core.socket.ROUTER)
        self.server_socket_stream = zmq.eventloop.zmqstream.ZMQStream(self.server_socket, self.ioloop)
        self.server_socket_stream.on_recv(self.on_server_socket_message)

        self.input_queue = deque(self.options['inputs'])

    def build_option_parser(self):
        self.parser = optparse.OptionParser()
        self.parser.add_option('-c', '--clients', dest='clients', action="append") # What hosts will be running the work?
        self.parser.add_option('-i', '--inputs', dest='inputs', action="append") # What are the paths to the input files?
        self.parser.add_option('-s', '--mrjob', dest='mrjob') # import path to MRJob class
        self.parser.add_option('-o', '--output', dest='output') # Where should the output go?

    def read_config(self):
        options, args = self.parser.parse_args()
        for each_option in self.parser.option_list:
            if not each_option.dest:
                continue
            self.options[each_option.dest] = getattr(options, each_option.dest)

        if not self.options['mrjob']:
            self.parser.print_help()
            sys.exit(1)

        # TODO: this is dupe in client.parse_args()
        mrjob_modulename, _, mrjob_classname = self.options['mrjob'].rpartition('.')
        mrjob_module = __import__(mrjob_modulename, globals(), locals(), [mrjob_classname])
        self.mrjob_args = args
        self.mrjob = getattr(mrjob_module, mrjob_classname)(args)

    def run(self):
        self.start_next_steps()
        self.ioloop.start()

    def start_next_steps(self):
        assert not self.waiting_for_hello
        assert not self.downstream_requests
        self.num_mappers_done = 0

        all_steps = self.get_steps()

        # Start everything up to and including the next reducer
        step_num_offset = self.last_step_num + 1
        for step_num, step in enumerate(all_steps[step_num_offset:]):
            step_num += step_num_offset

            self.last_step_num = step_num

            if step == 'M':
                self.start_mappers(step_num, {})
            elif step == 'R':
                self.start_reducers(step_num, {})
                break

    def get_steps(self):
        result = []
        for each_step in self.mrjob.steps():
            if each_step['mapper']:
                result.append('M')
            if each_step['reducer']:
                result.append('R')

        return ''.join(result)

    def create_server_socket(self, socket_type):
        zmqsocket = self.context.socket(socket_type)
        address = 'tcp://%s' % socket.gethostbyname(socket.gethostname())
        log.debug('address %r' % address)
        port = zmqsocket.bind_to_random_port(address)
        address = address + ':%d' % port
        return zmqsocket, address

    def run_on_client(self, client_addr, args):
        ssh_cmd = ['ssh',
            '-o', 'VerifyHostKeyDNS=no',
            '-o', 'StrictHostKeyChecking=no',
        ]
        command = ssh_cmd + [client_addr, 'python', '-m', 'mrjob.zeromq.client', '-s', self.options['mrjob']] + args + ['--'] + self.mrjob_args
        log.debug(' '.join(map(str, command)))
        if not DEBUG_NO_SSH:
            time.sleep(.1) # A little sleep to be nice in case we're ssh-ing to the same host
            subprocess.Popen(command)

    def start_mappers(self, step_num, step_data):
        # Start the mapper nodes
        mapper_cmd = ['--mapper', '--server-address', self.server_socket_address, '--step-num', str(step_num)]
        for mapper_task_num, mapper_addr in enumerate(self.get_clients()):
            self.run_on_client(mapper_addr, mapper_cmd + ['--task-num', str(mapper_task_num)])
            self.waiting_for_hello.add(('M', step_num, mapper_task_num))

            if step_num == len(self.get_steps()) - 1:
                self.running_last += 1

    def start_reducers(self, step_num, step_data):
        # Start all reducers
        reduce_args = ['--reducer', '--server-address', self.server_socket_address, '--step-num', str(step_num)]

        clients = self.get_clients()
        for reduce_task_num in xrange(self.num_reduce_tasks()):
            client_addr = clients[reduce_task_num % len(clients)]
            self.run_on_client(client_addr, reduce_args + ['--task-num', str(reduce_task_num)])
            log.info('Adding waiting for hello %r' % (('R', step_num, reduce_task_num),))
            self.waiting_for_hello.add(('R', step_num, reduce_task_num))

            if step_num == len(self.get_steps()) - 1:
                self.running_last += 1

    def run_reducers(self):
        log.info('run_reducers')

        # All data is now sitting on the reducers.  It still needs to be sorted.
        # Start up subsequent tasks so the reducers have downstreams to write to
        reduce_step_num = self.last_step_num

        self.start_next_steps()

        for each_reducer_address in self.client_addresses['R'][reduce_step_num].itervalues():
            # TODO: slowly indicate reducers to run, one host at a time
            bucket_socket = self.context.socket(zmq.core.socket.PUSH)
            bucket_socket.connect(each_reducer_address)
            bucket_socket.send('')

        log.info('done run_reducers')

    def num_reduce_tasks(self):
        # TODO
        return len(self.get_clients())

    def get_output_dir(self):
        if not self.options['output']:
            return None

        if is_s3_uri(self.options['output']):
            return self.options['output']

        return os.path.abspath(self.options['output'])

    def get_clients(self):
        if self.options['clients'] == ['ec2_auto']:
            # Return all machines in this reservation.
            return self._get_ec2_clients()
        return self.options['clients']

    def _get_ec2_clients(self):
        reservation_id = urllib.urlopen(EC2_RESERVATION_URL).read()
        instance_id = urllib.urlopen(EC2_INSTANCE_ID_URL).read()

        ec2_connection = self._get_ec2_connection()

        reservations = ec2_connection.get_all_instances(filters=dict(reservation_id=reservation_id))
        if len(reservations) != 1:
            raise Exception('Unable to find reservation id %s' % reservation_id)

        private_ips = []
        for each_instance in reservations[0].instances:
            if False: # TODO: make this an option
                if each_instance.id == instance_id:
                    continue # Skip ourself.

            for each_core in xrange(EC2_INSTANCE_TYPE_TO_CORES[each_instance.instance_type]):
                private_ips.append(each_instance.private_ip_address)

        return private_ips

    def _get_ec2_connection(self):
        import boto.ec2

        availability_zone = urllib.urlopen(EC2_AVAILABILITY_ZONE_URL).read()
        for each_region in boto.ec2.regions():
            if availability_zone.startswith(each_region.name):
                return each_region.connect()

    def on_server_socket_message(self, msg):
        log.info('Got server socket message %r' % msg[2])
        if msg[2].startswith('HELLO '):
            self.handle_hello(msg)

        if msg[2].startswith('INPUT '):
            self.handle_input_request(msg)

        if msg[2].startswith('DOWNSTREAM '):
            self.handle_downstream(msg)

        if msg[2].startswith('DONE '):
            self.handle_done(msg)

        if msg[2].startswith('STATUS '):
            self.handle_status(msg)

    RE_HELLO = re.compile('HELLO ([MR])\.(\d+)\.(\d+) (\S+)')
    def handle_hello(self, msg):
        match = self.RE_HELLO.match(msg[2])
        if not match:
            raise Exception('Invalid HELLO message')

        client_type, step_num, task_num, bucket_address = match.groups()
        step_num = int(step_num)
        task_num = int(task_num)

        try:
            self.waiting_for_hello.remove((client_type, step_num, task_num))
        except KeyError:
            raise Exception('Not waiting for HELLO: %s' % ((client_type, step_num, task_num),))

        self.client_addresses[client_type][step_num][task_num] = bucket_address

        self.server_socket.send_multipart([msg[0], msg[1], 'OKAY'])

        # Check to see if all clients are up and ready
        if not self.waiting_for_hello:
            downstream_requests = self.downstream_requests
            self.downstream_requests = []
            for each_msg in downstream_requests:
                self.handle_downstream(each_msg)

    def handle_input_request(self, msg):
        log.info('received input request %r' % msg)
        assert not self.waiting_for_hello

        log.info('replying to input request %r' % msg)
        if not self.input_queue:
            # Send empty string to indicate we're done
            self.server_socket.send_multipart([msg[0], msg[1], ''])
        else:
            self.server_socket.send_multipart([msg[0], msg[1], self.input_queue.popleft()])

    RE_DOWNSTREAM = re.compile('DOWNSTREAM ([MR])\.(\d+)\.(\d+)')
    def handle_downstream(self, msg):
        if self.waiting_for_hello:
            self.downstream_requests.append(msg)
            return

        match = self.RE_DOWNSTREAM.match(msg[2])
        if not match:
            raise Exception('Invalid DOWNSTREAM message')

        client_type, step_num, task_num = match.groups()
        step_num = int(step_num)
        task_num = int(task_num)

        # Get next step
        if step_num == len(self.get_steps()) - 1:
            # Last step. Return the actual output path
            addresses = [os.path.join(self.options['output'], 'part-%05d' % task_num)]

        else:
            next_step_type = self.get_steps()[step_num + 1]
            if next_step_type == 'M':
                # Provide just one downstream mapper -- ideally the same task number as the one its already using
                addresses = [self.client_addresses[next_step_type][step_num + 1][task_num]]
            elif next_step_type == 'R':
                # Provide all the downstream reducers
                addresses = self.client_addresses[next_step_type][step_num + 1].values()

        addresses = ' '.join(addresses)
        log.info('Replying to downstream request with %r' % addresses)
        self.server_socket.send_multipart([msg[0], msg[1], addresses])

    RE_DONE = re.compile('DONE ([MR])\.(\d+)\.(\d+)')
    def handle_done(self, msg):
        match = self.RE_DONE.match(msg[2])
        if not match:
            raise Exception('Invalid DONE message')

        task_type, step_num, task_num = match.groups()
        step_num = int(step_num)
        task_num = int(task_num)

        self.server_socket.send_multipart([msg[0], msg[1], 'OKAY'])

        self.num_tasks_done[step_num] += 1
        if self.num_tasks_done[step_num] == len(self.get_clients()):
            # All done with this step.  Now what do we do?
            if step_num == self.last_step_num - 1 and self.get_steps()[self.last_step_num] == 'R':
                # Run the reducers that are last_step_num
                self.run_reducers()
            elif step_num == self.last_step_num:
                # We're all done with the job.
                assert step_num == len(self.get_steps()) - 1
                log.info("All done!")
                self.ioloop.stop()
            else:
                # So we're something that finished processing its outputs, and we're upstream from another mapper
                assert self.get_steps()[step_num + 1] == 'M'

                # Send an empty string to all downstream mappers, indicating that their inputs are finished
                for each_reducer_address in self.client_addresses['M'][step_num + 1].itervalues():
                    bucket_socket = self.context.socket(zmq.core.socket.PUSH)
                    bucket_socket.connect(each_reducer_address)
                    bucket_socket.send('')

    RE_STATUS = re.compile('STATUS ([MR])\.(\d+)\.(\d+) (.*)')
    def handle_status(self, msg):
        match = self.RE_STATUS.match(msg[2])
        if not match:
            raise Exception('Invalid STATUS message')

        task_type, step_num, task_num, status_string = match.groups()
        step_num = int(step_num)
        task_num = int(task_num)

        self.server_socket.send_multipart([msg[0], msg[1], 'OKAY'])

        # TODO


class WebRequestHandler(tornado.web.RequestHandler):
        def get(self):
            log.info('RequestHandler')
            self.set_header('Content-type', 'text/html')
            self.write('O hai!!')

if __name__ == '__main__':
    server = Server()
    server.start()
    server.run()


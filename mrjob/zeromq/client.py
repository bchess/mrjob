# vim: set expandtab tabstop=4 shiftwidth=4:
import json
import logging
import optparse
import os
import socket
import subprocess
import sys
import tempfile
import urllib2

import boto.s3.connection
import zmq.core.context
import zmq.core.socket

from mrjob.emr import REGION_TO_S3_ENDPOINT
from mrjob.emr import s3_key_to_uri # TODO: move out of EMR
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri
from mrjob.util import buffer_iterator_to_line_iterator
from mrjob.util import read_file

import gzipstream # TODO
from server import EC2_AVAILABILITY_ZONE_URL

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(process)d:%(name)s:%(levelname)s %(message)s')
log = logging.getLogger('mrjob.zeromq.client')
logging.getLogger('boto').setLevel(logging.INFO)

class Client(object):
    def __init__(self):
        self.options = {}

    def parse_args(self, args):
        parser = optparse.OptionParser()
        parser.add_option('-m', '--mapper', dest='mapper', action="store_true", default=False)
        parser.add_option('-r', '--reducer', dest='reducer', action="store_true", default=False)
        parser.add_option('-a', '--server-address', dest='server_address') # required
        parser.add_option('-s', '--mrjob', dest='mrjob') # required
        parser.add_option('--step-num', dest='step_num', action='store', type='int')
        parser.add_option('--task-num', dest='task_num', action='store', type='int')
        options, args = parser.parse_args(args)

        for each_option in parser.option_list:
            if not each_option.dest:
                continue
            self.options[each_option.dest] = getattr(options, each_option.dest)

        mrjob_modulename, _, mrjob_classname = self.options['mrjob'].rpartition('.')
        mrjob_module = __import__(mrjob_modulename, globals(), locals(), [mrjob_classname])
        log.info('sys argv %r' % sys.argv)
        log.info('mrjob args %r' % args)
        self.mrjob = getattr(mrjob_module, mrjob_classname)(args)

    def run(self):
        self.context = zmq.Context()

        self.server_socket = self.context.socket(zmq.core.socket.REQ)
        self.server_socket.connect(self.options['server_address'])

        # Create a zeromq for this client's bucket
        self.bucket_socket, self.bucket_address = self.create_bucket_socket()

        self.server_socket.send('HELLO %s.%d.%d %s' % (self.get_task_type(), self.options['step_num'], self.options['task_num'], self.bucket_address))
        msg = self.server_socket.recv()
        if msg != 'OKAY':
            raise Exception('Got weird message %s' % msg)

        if self.options['mapper']:
            self.run_mapper()
        elif self.options['reducer']:
            self.run_reducer()

    def get_task_type(self):
        if self.options['mapper']:
            return 'M'
        if self.options['reducer']:
            return 'R'

    def create_bucket_socket(self):
        zmqsocket = self.context.socket(zmq.core.socket.PULL)
        zmqsocket.setsockopt(zmq.HWM, 10000)
        address = 'tcp://%s' % socket.gethostbyname(socket.gethostname())
        log.debug('address %r' % address)
        port = zmqsocket.bind_to_random_port(address)
        address = address + ':%d' % port
        return zmqsocket, address

    def run_mapper(self):
        log.info('run_mapper')
        # Create sockets for each bucket
        self.prepare_downstream()

        _, write_protocol = self.mrjob.pick_protocols(self.options['step_num'], 'M')

        if self.options['step_num'] == 0:
            # Get input paths from server
            input = self.file_input()
        else:
            # Get input paths from our socket
            input = self.socket_input()

        input_records, output_records = self.map_inputs(input)

        # TODO send a note to server that I finished that input
        # self.server_socket.send('STATUS Mapped %d input records to %d output records' % (input_records, output_records))

        self.close_downstream()

    def file_input(self):
        """ Request file paths from the server, yielding the key-value pair of each line of each file
        """
        read_protocol, _ = self.mrjob.pick_protocols(self.options['step_num'], 'M')
        while True:
            # Get a path to an input that should be processed
            log.info('mapper waiting for input')
            self.server_socket.send('INPUT M.%d.%d' % (self.options['step_num'], self.options['task_num']))

            input_path = self.server_socket.recv()
            if input_path == '':
                break
            log.info('mapper got input %s' % input_path)

            input_file = self._read_path(input_path)
            for input_line in input_file:
                input_k, input_v = read_protocol(input_line.rstrip('\r\n'))
                yield input_k, input_v
        log.info('mapper file input complete')

    def socket_input(self):
        """ Yield key-value pairs from the bucket_socket
        """
        read_protocol, _ = self.mrjob.pick_protocols(self.options['step_num'], 'M')
        while True:
            msg = self.bucket_socket.recv()
            if msg == '':
                break
            input_k, input_v = read_protocol(msg)
            yield input_k, input_v
        log.info('mapper bucket input complete')

    def prepare_downstream(self):
        self.server_socket.send('DOWNSTREAM %s.%d.%d' % (self.get_task_type(), self.options['step_num'], self.options['task_num']))
        downstream_result = self.server_socket.recv()

        log.info('%s.%d prepare_downstream %r' % (self.get_task_type(), self.options['step_num'], downstream_result))

        if downstream_result.startswith('tcp://'):
            # We are an intermediate step, and we'll be writing to one or more zeromq sockets.
            self.bucket_sockets = []
            self.output_file = None

            for bucket_address in downstream_result.split(' '):
                bucket_socket = self.context.socket(zmq.core.socket.PUSH)
                bucket_socket.setsockopt(zmq.HWM, 10000)
                bucket_socket.connect(bucket_address)
                self.bucket_sockets.append(bucket_socket)
        else:
            # We should write to the path specified 
            self.output_path = downstream_result
            if is_s3_uri(self.output_path):
                # Write to a local temporary file.  We'll copy it to S3 eventually.
                self.output_file = tempfile.NamedTemporaryFile(dir=self._get_tmp_dir())
            else:
                # Just a plain file stored locally?
                self.output_file = open(self.output_path, 'w')

    def map_inputs(self, input_itr):
        input_records = 0
        output_records = 0

        _, write_protocol = self.mrjob.pick_protocols(self.options['step_num'], 'M')

        this_step = self.flatten_steps(self.mrjob.steps())[self.options['step_num']]
        assert this_step['type'] == self.get_task_type()
        mapper = this_step['function']

        if this_step['init']:
            this_step['init']()

        for input_k, input_v in input_itr:
            # TODO: format the input_line input proper k, v form given whatever input_protocol 
            input_records += 1
            for k, v in mapper(input_k, input_v):
                try:
                    self.write_output(k, write_protocol(k, v))
                except Exception:
                    log.exception('k/v was %r %r' % (k, v)) # TODO: increment counter
                    # TODO: if strict protocol, raise
                    pass
                output_records += 1

            if input_records % 1000 == 0:
                log.debug("M.%d Read %d records\tWrote %d records" % (self.options['step_num'], input_records, output_records))
                # self.server_socket.send('STATUS %s.%d.%d %s' % (self.get_task_type(), self.options['step_num'], self.options['task_num'], "Read %d records\tWrote %d records" % (input_records, output_records)))

        if this_step['final']:
            for k, v in this_step['final']():
                self.write_output(k, write_protocol(k, v))
                output_records += 1

        return input_records, output_records

    def flatten_steps(self, mrjob_steps):
        steps = []
        for each_step in mrjob_steps:
            if each_step['mapper']:
                steps.append({'type': 'M', 'function': each_step['mapper'], 'init': each_step['mapper_init'], 'final': each_step['mapper_final']})
            if each_step['reducer']:
                steps.append({'type': 'R', 'function': each_step['reducer'], 'init': each_step['reducer_init'], 'final': each_step['reducer_final']})

        return steps

    def combiner(self, k):
        # default
        return hash(repr(k))

    def run_reducer(self):
        # Read all inputs for bucket
        input_file = self.fill_reducer_bucket()

        self.prepare_downstream()

        # Now reduce
        input_records, output_records = self.reduce_input_file(input_file)
        log.info('R %d input_records, %d output_records' % (input_records, output_records))

        self.close_downstream()


    def fill_reducer_bucket(self):
        sort_tmp_args = []
        all_tmp_dirs = rotate(self._all_tmp_dirs(), self.options['task_num'])
        for each_tmp_dir in all_tmp_dirs:
            sort_tmp_args += ['-T', each_tmp_dir]

        sort_cmd = ['sort'] + sort_tmp_args
        log.info('sort_cmd: %r' % sort_cmd)

        sort_process = subprocess.Popen(['sort'] + sort_tmp_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

        # Write the inputs from bucket_socket to disk until there's nothing left
        while True:
            msg = self.bucket_socket.recv()
            if msg == '':
                break
            sort_process.stdin.write(msg)
            sort_process.stdin.write('\n')

        sort_process.stdin.close()
        log.info('R done filling sort bucket')

        return sort_process.stdout

    def reduce_input_file(self, input_file):
        log.info('R reduce_input_file')
        # TODO: support HTTP, S3, gzip
        input_records = [0]
        output_records = 0

        tab_splitter_itr = tab_splitter(input_file, input_records)

        try:
            input_k_v = list(tab_splitter_itr.next())
        except StopIteration:
            return 0, 0

        _, write_protocol = self.mrjob.pick_protocols(self.options['step_num'], 'R')

        this_step = self.flatten_steps(self.mrjob.steps())[self.options['step_num']]
        assert this_step['type'] == self.get_task_type()
        reducer = this_step['function']

        if this_step['init']:
            this_step['init']()

        while input_k_v:
            reduce_value_itr = reducer_takewhile(input_k_v, tab_splitter_itr)
            for output_k, output_v in reducer(input_k_v[0], reduce_value_itr):
                output_records += 1
                self.write_output(output_k, write_protocol(output_k, output_v))

            if input_records[0] % 1000 == 0:
                log.debug("R.%d Read %d records\tWrote %d records" % (self.options['step_num'], input_records[0], output_records))
                # self.server_socket.send('STATUS %s.%d.%d %s' % (self.get_task_type(), self.options['step_num'], self.options['task_num'], "Read %d records\tWrote %d records" % (input_records[0], output_records)))

        if this_step['final']:
            for k, v in this_step['final']():
                self.write_output(k, write_protocol(k, v))

        return input_records[0], output_records

    def _get_final_output_filename(self):
        return os.path.join(self.options['output'], 'part-%05d' % self.options['reducer_num'])

    def close_downstream(self):
        if self.output_file is not None:
            if is_s3_uri(self.output_path):
                # Upload this part to S3
                log.info('Uploading to S3 %s' % self.output_path)
                s3_conn = self._create_s3_connection()
                bucket_name, key_name = parse_s3_uri(self.output_path)
                s3_key = s3_conn.get_bucket(bucket_name).new_key(key_name)
                s3_key.set_contents_from_file(self.output_file)
                log.info('Done uploading to S3 %s' % self.output_path)

            self.output_file.close()

        # Tell the server we're all done
        self.server_socket.send('DONE %s.%d.%d %s' % (self.get_task_type(), self.options['step_num'], self.options['task_num'], self.bucket_address))
        msg = self.server_socket.recv()
        if msg != 'OKAY':
            raise Exception('Expected OKAY, received %s' % msg)


    def write_output(self, k, output_line):
        if self.output_file:
            self.output_file.write(output_line)
            self.output_file.write('\n')
        else:
            # send output to proper bucket
            if len(self.bucket_sockets) == 1:
                bucket_num = 0
            else:
                # TODO: this is not a combiner
                bucket_num = self.combiner(k) % len(self.bucket_sockets)

            self.bucket_sockets[bucket_num].send(output_line)

    def _create_s3_connection(self):
        if True: # === local mode
            from mrjob.emr import EMRJobRunner
            emr_runner = EMRJobRunner()
            return emr_runner.make_s3_conn()
        availability_zone = urllib2.urlopen(EC2_AVAILABILITY_ZONE_URL).read()
        for region, host in REGION_TO_S3_ENDPOINT.iteritems():
            if region and availability_zone.startswith(region):
                return boto.s3.connection.S3Connection(host=host)
        raise Exception('Unable to find S3 endpoint for %s' % availability_zone)

    def _read_path(self, path):
        # Currently supports S3, gzip S3, and HTTP urls. TODO the emr runner doesn't support HTTP urls
        log.info('_read_path')
        if is_s3_uri(path):
            s3_conn = self._create_s3_connection()
            bucket_name, key_name = parse_s3_uri(path)
            s3_key = s3_conn.get_bucket(bucket_name).get_key(key_name)
            if path.endswith('gz'):
                    s3_key.mode = 'rb'
                    return gzipstream.GzipStream(s3_key)

            buffer_iterator = read_file(s3_key_to_uri(s3_key), fileobj=s3_key)
            return buffer_iterator_to_line_iterator(buffer_iterator)

        return urllib2.urlopen(path)

    def _get_tmp_dir(self):
        tmp_dirs = self._all_tmp_dirs()
        return tmp_dirs[self.options['task_num'] % len(tmp_dirs)]

    def _all_tmp_dirs(self):
        return map(lambda s: '/' + s, filter(lambda s: s.startswith('mnt'), os.listdir('/')))

def tab_splitter(itr, count):
    for line in itr:
        count[0] += 1
        yield map(json.loads, line[:-1].split('\t')) # Remove the line endings and split on tabs
    log.info('R tab_splitter done')

def reducer_takewhile(first, itr):
    first_k, first_v = first
    yield first_v

    for k, v in itr:
        if first_k != k:
            first[0] = k
            first[1] = v
            return
        yield v

    del first[:] # Indicate that we should stop

def rotate(x, y=1):
    if len(x) == 0:
        return x
    y = y % len(x) # Normalize y, using modulo - even works for negative y
    return x[y:] + x[:y]

if __name__ == '__main__':
    client = Client()
    client.parse_args(sys.argv)
    client.run()


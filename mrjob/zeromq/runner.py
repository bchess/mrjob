import logging
import os
import os.path
import subprocess
import sys
import tempfile
import time

import boto
from mrjob.runner import MRJobRunner
from mrjob.emr import EMRJobRunner

log = logging.getLogger('mrjob.zeromq')

REGION_TO_EC2_ENDPOINT = {
    'EU': 'ec2.eu-west-1.amazonaws.com',
    'us-east-1': 'ec2.us-east-1.amazonaws.com',
    'us-west-1': 'ec2.us-west-1.amazonaws.com',
    'us-west-2': 'ec2.us-west-2.amazonaws.com',
    '': 'ec2.amazonaws.com',  # when no region specified
}

# ubuntu-images-us/ubuntu-lucid-10.04-amd64-server-20110930.manifest.xml
LUCID_EC2_AMIS = {
    'us-east-1': {'32-bit': 'ami-6936fb00', '64-bit': 'ami-1136fb78'},
    'us-west-1': {'32-bit': 'ami-d7227e92', '64-bit': 'ami-ed227ea8'},
    'us-west-2': {'32-bit': 'ami-cef77afe', '64-bit': 'ami-34f67b04'}
}

EC2_32_BIT_INSTANCE_TYPES = ('m1.small', 'c1.medium')

class ZeroMQJobRunner(MRJobRunner):
    alias = 'mrzeromq'

    def __init__(self, *args, **kwargs):
        self.mrjob_cls = kwargs.pop('mrjob_cls')
        self._emr_runner = EMRJobRunner(*args, **kwargs)
        super(ZeroMQJobRunner, self).__init__(*args, **kwargs)

    @classmethod
    def _allowed_opts(cls):
        """A list of which keyword args we can pass to __init__()"""
        return super(ZeroMQJobRunner, cls)._allowed_opts() + EMRJobRunner._allowed_opts() + cls._my_allowed_opts()

    @classmethod
    def _my_allowed_opts(cls):
        return ['ec2_reservation_id']

    def _run(self):
        # Copy input files to a tmp place on S3
        self._emr_runner._prepare_for_launch()
        self._launch_job()

    def _launch_job(self):
        if self._opts['ec2_reservation_id']:
            self._reservation_id = self._opts['ec2_reservation_id']
        else:
            self._create_ec2_instances()

        self.bootstrap_instances()
        self.launch_server()

    def _create_ec2_instances(self):
        image_id = self._get_image_id()
        instance_type = self._emr_runner._opts['ec2_slave_instance_type'] or self._emr_runner._opts['ec2_instance_type']

        log.info('Creating connection')
        conn = self._create_ec2_conn()

        log.info('Starting instances')
        assert self._emr_runner._opts['ec2_key_pair']

        # create ssh-key
        log.info('Generating ssh key for this session')
        ssh_key_dir = tempfile.mkdtemp()
        ssh_key_filename = os.path.join(ssh_key_dir, 'id_rsa')
        status = subprocess.call(['ssh-keygen', '-N', '', '-f', ssh_key_filename])
        if status != 0:
            raise Exception('ssh-keygen returned %d' % status)
        log.info('SSH key is %s' % ssh_key_filename)

        ssh_pub_key = open(ssh_key_filename + '.pub', 'r').read()

        user_data = self._make_user_data(ssh_pub_key)
        log.info('User data %s' % user_data)

        reservation = conn.run_instances(
            image_id,
            min_count=self._emr_runner._opts['num_ec2_instances'],
            max_count=self._emr_runner._opts['num_ec2_instances'],
            key_name=self._emr_runner._opts['ec2_key_pair'],
            user_data=user_data, # TODO
            instance_type=instance_type,
            placement=self._emr_runner._opts['aws_availability_zone'],
            security_groups=['ec2-testing'] # TODO
        )

        while True:
            log.info('Waiting for instances...')
            all_ready = True
            for each_instance in reservation.instances:
                if each_instance.update() != 'running':
                    log.info('Status %s' % each_instance.update())
                    all_ready = False
            if all_ready:
                break
            time.sleep(5)

        master_hostname = reservation.instances[0].public_dns_name
        log.info('Okay, instance(s) ready. %s %s' % (master_hostname, reservation.id))
        self._reservation_id = reservation.id

        # Copy the private key to the master instance
        log.info('Sleeping 90 extra secs for the host to be ready.')
        time.sleep(90)

        args = ['scp',
            '-i', self._emr_runner._opts['ec2_key_pair_file'],
            '-o', 'VerifyHostKeyDNS=no',
            '-o', 'StrictHostKeyChecking=no',
            ssh_key_filename,
            'ubuntu@%s:.ssh/id_rsa' % master_hostname
        ]
        log.info(args)

        status = subprocess.call(args)
        if status != 0:
            raise Exception('scp returned %d' % status)

    def _get_image_id(self):
        image_ids = LUCID_EC2_AMIS[self._emr_runner._aws_region]

        instance_type = self._emr_runner._opts['ec2_slave_instance_type'] or self._emr_runner._opts['ec2_instance_type']
        if not instance_type or instance_type in EC2_32_BIT_INSTANCE_TYPES:
            return image_ids['32-bit']

        return image_ids['64-bit']

    def _create_ec2_conn(self):
        region = self._get_region_info_for_ec2_conn()
        return boto.ec2.connection.EC2Connection(
            self._emr_runner._opts['aws_access_key_id'],
            self._emr_runner._opts['aws_secret_access_key'],
            region=region)

    def bootstrap_instances(self):
        self._tmp_dir = self._emr_runner._s3_tmp_uri.split('/')[-2]
        cmd = ['s3cmd', 'get', '--recursive', self._emr_runner._s3_tmp_uri + 'files/']
        cmd = 'mkdir %s ; cd %s ; %s' % (self._tmp_dir, self._tmp_dir, ' '.join(cmd))
        self._run_ssh_command(cmd)

        cmd = ['python', 'b.py']
        cmd = 'cd %s ; %s' % (self._tmp_dir, ' '.join(cmd))
        self._run_ssh_command(cmd)

        cmd = ['python', 'wrapper.py', 'echo']
        cmd = 'cd %s ; %s' % (self._tmp_dir, ' '.join(cmd))
        self._run_ssh_command(cmd)

    def launch_server(self):
        module_path = self.mrjob_cls.__module__
        if module_path == '__main__':
            module_path = sys.modules[module_path].__file__
            assert module_path.endswith('.py')
            module_path = module_path[:-3]
        mrjob_path = '.'.join((module_path, self.mrjob_cls.__name__))
        cmd = ['python', '-m', 'mrjob.zeromq.server',
            '-c', 'ec2_auto',
            '-s', mrjob_path,
            '-o', self._emr_runner._output_dir
        ]
        for each_input_path in self._emr_runner._s3_input_uris:
            for each_file in self._emr_runner.ls(each_input_path):
                cmd.extend(['-i', each_file])

        self._run_ssh_command(cmd, instance_indices=[0])

    def _run_ssh_command(self, cmd, instance_indices=None):
        conn = self._create_ec2_conn()
        reservations = conn.get_all_instances(filters=dict(reservation_id=self._reservation_id))
        if len(reservations) != 1:
            raise Exception('Unable to find reservation id %s' % self._reservation_id)
        reservation = reservations[0]


        if instance_indices is None:
            # run on all the instances
            instance_indices = xrange(len(reservation.instances))

        ssh_procs = []
        for ami_launch_index in instance_indices:
            host = reservation.instances[ami_launch_index].public_dns_name
            if not host:
                raise Exception('No public dns name for instance %d of reservation id %s' % (ami_launch_index, self._reservation_id))

            args = self._emr_runner._opts['ssh_bin'] + [
                '-o', 'VerifyHostKeyDNS=no',
                '-o', 'StrictHostKeyChecking=no',
                '-i', self._emr_runner._opts['ec2_key_pair_file'],
            ]
            args.append('ubuntu@' + host)
            if isinstance(cmd, basestring):
                args.append(cmd)
            else:
                args.extend(cmd)

            log.info('running %s' % args)
            # ssh_proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # ssh_procs.append(ssh_proc)
            ssh_procs.append(subprocess.Popen(args))
            time.sleep(1.0)

        for each_ssh_proc in ssh_procs:
            each_ssh_proc.wait()

    def _get_region_info_for_ec2_conn(self):
        """Get a :py:class:`boto.ec2.regioninfo.RegionInfo` object to
        initialize EC2 connections with.
        """
        region = boto.ec2.regioninfo.RegionInfo(None, self._emr_runner._aws_region, REGION_TO_EC2_ENDPOINT[self._emr_runner._aws_region])
        return region

    def _make_user_data(self, ssh_pub_key):
        # TODO: in apt-get below, everything gcc and past is yelp
        user_data = """#!/usr/bin/env bash
for drive in /dev/sd*; do
        mount_path=${drive/\/dev\/sd/\/mnt};
        mounted=`mount`
        if [[ ! "$mounted" == *${drive}* ]]; then
                sudo mkdir $mount_path
                sudo mount $drive $mount_path
        fi
done
sudo chmod 777 /mnt*
sudo add-apt-repository ppa:chris-lea/zeromq
sudo add-apt-repository ppa:chris-lea/libpgm
sudo apt-get update
sudo apt-get install -y s3cmd python-pyzmq python-setuptools gcc python-dev python-lxml
sudo easy_install boto
sudo easy_install tornado
sudo -u ubuntu sh -c 'echo "[default]
access_key = %(aws_access_key_id)s
secret_key = %(aws_secret_access_key)s" > ~ubuntu/.s3cfg'
sudo -u ubuntu sh -c 'echo "[Credentials]
aws_access_key_id = %(aws_access_key_id)s
aws_secret_access_key = %(aws_secret_access_key)s" > ~ubuntu/.boto'
sudo -u ubuntu cat << *EOF* >> ~ubuntu/.ssh/authorized_keys
%(ssh_pub_key)s
*EOF*
echo "mysql-server-5.1 mysql-server/root_password password ''" | sudo debconf-set-selections
echo "mysql-server-5.1 mysql-server/root_password_again password ''" | sudo debconf-set-selections
"""
        return user_data % {'aws_access_key_id': self._emr_runner._opts['aws_access_key_id'],
                            'aws_secret_access_key': self._emr_runner._opts['aws_secret_access_key'],
                            'files_path': self._emr_runner._s3_tmp_uri + 'files/',
                            'ssh_pub_key': ssh_pub_key}



import os

import disco
from disco.core import Job, classic_iterator

from mrjob.runner import MRJobRunner
from mrjob.emr import EMRJobRunner
from mrjob.local import LocalMRJobRunner


##########
# Utilities
##########
def my_log(*msg):
	return
	msg = ' '.join(msg)
	log = open('/tmp/mrdisco_stderr', 'a')
	print >> log, msg
	log.flush()
	os.fsync(log.fileno())
	log.close()

def piped_subprocess_queue(params, subprocess_args):
	import Queue
	from subprocess import Popen, PIPE, STDOUT
	import thread
	import tempfile

	def my_thread(params):
		while True:
			line = params['stdout'].readline()
			if not line:
				break
			params['queue'].put(line[:-1])
			my_log('REDUCE read line', line)

	read_stdin, write_stdin = os.pipe()
	params['stdin_fds'] = (read_stdin, write_stdin)

	read_stdout, write_stdout = os.pipe()
	params['stdout_fds'] = (read_stdout, write_stdout)

	params['queue'] = Queue.Queue()

	# proc_stderr, proc_stderr_fn = tempfile.mkstemp()
	proc_stderr = open('/tmp/mrdisco_job_stderr', 'a')
	params['proc_stderr'] = proc_stderr
	env = dict(os.environ)
	for each_extra_path in params['extra_python_paths']:
		env['PYTHONPATH'] += ':' + each_extra_path



	def popen_close_pipes():
		os.close(write_stdin)
		os.close(read_stdout)
	mrjob_cwd = os.path.join(os.getcwd(), params['mrjob_working_dir'][1:])

	# It's friday; time to hack
	try:
		os.symlink(os.path.join(os.getcwd(), 'home'), os.path.join(mrjob_cwd, 'home'))
	except OSError:
		pass
	try:
		os.symlink(os.path.join(os.getcwd(), params['script_path']), os.path.join(mrjob_cwd, params['script_path']))
	except OSError:
		pass

	proc = Popen(subprocess_args, preexec_fn=popen_close_pipes, stdin=read_stdin, stdout=write_stdout, stderr=proc_stderr, env=env, cwd=mrjob_cwd)
	my_log('Launched process %d with cwd %s args %s' % (proc.pid, mrjob_cwd, subprocess_args))

	# we have no business using these.  These are for the subprocess
	os.close(read_stdin)
	os.close(write_stdout)

	params['stdout'] = os.fdopen(read_stdout, 'r')
        params['proc'] = proc

	thread.start_new_thread(my_thread, (params,))

def piped_subprocess_close(params):
	import os
	# close the stdin to the process
	os.close(params['stdin_fds'][1])

	my_log('closed stdin, waiting for proc to end')

	# wait for the stdout from the process to end
	params['proc'].wait() # probably not right? What happens to stdout?

	my_log('process ended')

##########
# Mapper
##########
def mapper_in(fd, url, size, params):
	from mrjob._disco import piped_subprocess_queue
	from mrjob._disco import piped_subprocess_close

        # run the process
	args = params['mapper_args']
	piped_subprocess_queue(params, args)

	for line in fd:
		yield line

	# TODO: Now close
	piped_subprocess_close(params)

	# yield one more time to flush out the queue
	yield None

def mapper_runner(line, params):
	import Queue
	from mrjob._disco import my_log

	if line is not None:
		try:
			os.write(params['stdin_fds'][1], line)
		except OSError, error:
			if error.errno == 32:
				import sys
				print >> sys.stderr, 'Shit crashed.'
				my_stderr = params['proc_stderr']
				print >> my_stderr, sys.path
				print >> my_stderr, os.getcwd()
				print >> my_stderr, os.getpid()
				#my_stderr.seek(0)
				#print >> sys.stderr, my_stderr.read()
				sys.exit(1)

		my_log('writing line', line)

	try:
		while True:
			line = params['queue'].get_nowait()
			my_log('popping line from queue >>>%s<<<' % line)

			k, v = line.split('\t')
			yield k, v
	except Queue.Empty:
		pass

##########
# Reducer
##########
def reducer_runner(iter, out, params):
	from mrjob._disco import my_log
	from mrjob._disco import piped_subprocess_queue
	from mrjob._disco import piped_subprocess_close

	my_log('REDUCE fired up reducer')

        # run the process
	args = params['reducer_args']
	piped_subprocess_queue(params, args)

	def reducer_flush():
		import Queue
		try:
			while True:
				line = params['queue'].get_nowait()
				my_log('REDUCE popping line from queue >>>%s<<<' % line)

				k, v = line.split('\t')
				out.add(k, v)
		except Queue.Empty:
			pass

	for k, v in iter:
		line = '\t'.join((k, v))
		os.write(params['stdin_fds'][1], line + '\n')
		my_log('REDUCE writing >>>%s<<<' % line)
		reducer_flush()

	# Now close the process
	piped_subprocess_close(params)

	# flush out the queue
	reducer_flush()


import logging
log = logging.getLogger('mrjob.disco')

class DiscoJobRunner(MRJobRunner):
	alias = 'disco'

	def __init__(self, job, *args, **kwargs):
		self._job = job
		self._emr_runner = EMRJobRunner(*args, **kwargs)
		self._local_runner = LocalMRJobRunner(*args, **kwargs)
		super(DiscoJobRunner, self).__init__(*args, **kwargs)

	def _run(self):
		self._emr_runner._setup_input()
		# self._emr_runner._upload_non_input_files()
		self._local_runner._files = self._files
		self._local_runner._setup_working_dir()
		self._local_runner._create_wrapper_script()

		all_files = []
		python_paths = ['.']
		for each_file in self._files:
			if each_file.get('upload', 'file') == 'file':
				all_files.append(each_file['path'])
			else:
				new_path = os.path.basename(each_file['path'])
				python_paths.append(new_path)
				#if not each_file['path'].startswith(self._local_runner._get_local_tmp_dir()):
				#	all_files.append(each_file['path'])

		all_files.extend(self._list_all_files(self._local_runner._get_local_tmp_dir()))

		# HACK HACK HACK
		all_files.append('/home/bchess/tricks/wingdbstub.py')
		all_files.extend(self._list_all_files('/home/bchess/disco/mrjob/mrjob'))

		wrapper_args = self._opts['python_bin']
		if self._local_runner._wrapper_script:
			all_files.append(self._local_runner._wrapper_script['path'])
			wrapper_args += [os.path.join('..', os.path.basename(self._local_runner._wrapper_script['path']))] + wrapper_args

		# specify the steps

		true_inputs = []
		s3_conn = self._emr_runner.make_s3_conn()
		for s3_input_glob in self._emr_runner._s3_input_uris:
			for s3_input in self._emr_runner.ls(s3_input_glob):
				key = self._emr_runner.get_s3_key(s3_input, s3_conn)
				url = key.generate_url(600, force_http=True)
				true_inputs.append(url)

		step_outputs = None
		steps = self._get_steps()
		for i, step in enumerate(steps):
			log.debug('running step %d of %d' % (i + 1, len(steps)))
			job_params = {}

			if i == 0:
				step_inputs = true_inputs
			else:
				step_inputs = step_outputs

			job_dict = dict(
				input=step_inputs,
				required_files=all_files,
				required_modules=[],
				sort=True,
				map_input_stream=[
					disco.worker.classic.func.map_input_stream,
					disco.worker.classic.func.gzip_line_reader,
					mapper_in
				],
				params=job_params
			)


			job_params['extra_python_paths'] = python_paths
			job_params['mrjob_working_dir'] = self._local_runner._working_dir
			job_params['script_path'] = self._script['path']

			if 'M' in step:
				job_params['mapper_args'] = wrapper_args + [self._script['path'],
					'--step-num=%d' % i, '--mapper'] + self._mr_job_extra_args()
				job_dict['map'] = mapper_runner

			if 'R' in step:
				job_params['reducer_args'] = wrapper_args + [self._script['path'],
					'--step-num=%d' % i, '--reducer'] + self._mr_job_extra_args()
				job_dict['reduce'] = reducer_runner


			job = Job().run(**job_dict)
			step_outputs = job.wait(show=True)

		self._output_files = step_outputs

	def _list_all_files(self, path):
		for dirpath, dirnames, filenames in os.walk(path):
			for each_filename in filenames:
				yield os.path.join(dirpath, each_filename)

	def stream_output(self):
		assert self._ran_job
		for k_v in classic_iterator(self._output_files):
			yield '\t'.join(k_v) + '\n'


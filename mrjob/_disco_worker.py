#!/usr/bin/env python
import disco.worker.classic.worker
import cProfile

class MyWorker(disco.worker.classic.worker.Worker):
	def map(self, *args):
		m = super(MyWorker, self).map
		# cProfile.runctx('m(*args)', globals(), {'m': m, 'args': args}, '/tmp/worker.prof')
		m(*args)

	pass


if __name__ == '__main__':
	MyWorker.main()

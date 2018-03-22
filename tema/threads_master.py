from threading import *
from debug_helper import Debug


class ThreadsMaster(Thread):
	MAX_NUM_THREADS = 7

	def __init__(self, device):
		"""
		Constructor.

		@type device: Device
		@param device: the device which owns this thread
		"""
		Thread.__init__(self, name="Thread Master for device %d" % device.device_id)
		self.device = device
		self.neighbours = None
		self.freed_thread = Event()
		self.free_threads = []
		self.all_threads = []
		self.worker_died = Event()
		for i in range(ThreadsMaster.MAX_NUM_THREADS):
			worker = Worker(self, self.device, i)
			self.free_threads.append(worker)
			self.all_threads.append(worker)
			worker.start()

	def run(self):

		while True:
			# print "Master %d waiting neighs on timepoint %d\n" % (self.device.device_id, self.device.current_timepoint)
			self.neighbours = self.device.supervisor.get_neighbours()
			# message = "Master %d received on timepoint %d: " % (self.device.device_id, self.device.current_timepoint)
			# if self.neighbours is not None:
			# 	for neigh in self.neighbours:
			# 		message += neigh.device_id + " "
			# print message +"\n"

			if self.neighbours is None:
				break
			for worker in self.free_threads:
				worker.set_neighbours(self.neighbours)

			self.device.script_received.wait()
			self.device.script_received.clear()

			if not self.device.scripts and self.neighbours:
				for neigh in self.neighbours:
					self.device.scripts = self.device.scripts + neigh.scripts

			for (script, location) in self.device.scripts:
				while not self.free_threads:
					self.freed_thread.wait()
					self.freed_thread.clear()
				worker = self.free_threads.pop()
				worker.give_script(script, location)

				if (script, location) == self.device.scripts[-1] and self.device.received_none is False:
					self.device.script_received.wait()
					self.device.script_received.clear()
					if (script, location) == self.device.scripts[-1] and self.device.received_none is True:
						break
			self.device.script_received.clear()

			while len(self.free_threads) != ThreadsMaster.MAX_NUM_THREADS:
				self.freed_thread.wait()
				self.freed_thread.clear()
			self.device.barrier.wait()


		for worker in self.all_threads:
			# print "Master %d announced worker %d\n" %(self.device.device_id, worker.id)
			worker.give_script(None, -1)

		# while self.free_threads:
		# 	print "Master %d has %d workers left\n" %(self.device.device_id, len(self.free_threads))
		# 	self.worker_died.wait()
		# 	self.worker_died.clear()

		for worker in self.all_threads:
			worker.join()

		# print "Master %d finished all workers\n" % self.device.device_id
		self.device.die_barrier.wait()
		return


class Worker(Thread):

	def __init__(self, master, device, id):
		"""
		Constructor.
		@type master: Master
		@param master: the master of this thread

		@type device: Device
		@param device: the device which owns this thread

		@type neighbours: List
		@param neighbours: the device's neighbours

		@type id: Integer
		@param id: the location on which the script will run
		"""
		Thread.__init__(self, name="Worker Thread for device %d" % device.device_id)
		self.master = master
		self.device = device
		self.neighbours = None
		self.script = None
		self.location = -1
		self.id = id
		self.received_script = Event()

	def set_neighbours(self, neighbours):
		self.neighbours = neighbours

	def give_script(self, script, location):
		self.script = script
		self.location = location
		self.received_script.set()

	def run(self):
		while True:
			self.received_script.wait()
			self.received_script.clear()
			if self.script is None:
				self.master.free_threads.remove(self)
				self.master.worker_died.set()
				return

			# run scripts received until now
			script_data = []
			for device in self.neighbours:
				data = device.get_data(self.location)
				if data is not None:
					script_data.append(data)
			data = self.device.get_data(self.location)
			if data is not None:
				script_data.append(data)

			if script_data:
				result = self.script.run(script_data)
				for device in self.neighbours:
					device.set_data(self.location, result)
				self.device.set_data(self.location, result)
			self.master.free_threads.append(self)
			self.master.freed_thread.set()



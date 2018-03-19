from threading import *


class ThreadsMaster(Thread):

	MAX_NUM_THREADS = 8

	def __init__(self, device):
		"""
		Constructor.

		@type device: Device
		@param device: the device which owns this thread
		"""
		self.device = device
		self.neighbours = None
		self.num_slaves_lock = Lock()
		self.num_slaves = 0
		self.freed_thread = Event()

	def run(self):
		self.neighbours = self.device.supervisor.get_neighbours()

		self.device.script_received.wait()
		self.device.script_received.clear()

		for (script, location) in self.device.scripts:
			while self.num_slaves == ThreadsMaster.MAX_NUM_THREADS:
				self.freed_thread.wait()
				self.freed_thread.clear()
			worker = Worker(self, self.device, self.neighbours, script, location)
			worker.run()

			if (script, location) == self.device.scripts[-1] and self.device.received_none is False:
				self.device.script_received.wait()
				self.device.script_received.clear()
				if self.device.received_none is True:
					break
		self.device.script_received.clear()

		while self.num_slaves > 0:
			self.freed_thread.wait()
			self.freed_thread.clear()
		self.device.barrier.wait()


class Worker(Thread):

	def __init__(self, master, device, neighbours, script, location):
		"""
		Constructor.
		@type master: Master
		@param master: the master of this thread

		@type device: Device
		@param device: the device which owns this thread

		@type neighbours: List
		@param neighbours: the device's neighbours

		@type script: Script
		@param script: this thread will run this script

		@type location: Integer
		@param location: the location on which the script will run
		"""
		self.master = master
		self.device = device
		self.neighbours = neighbours
		self.script = script
		self.location = location
		self.master.num_slaves_lock.acquire()
		self.master.num_slaves += 1
		self.master.num_slaves_lock.acquire()

	def run(self):
		# run scripts received until now
		# Device.data_log_message += "ID %d starts work on location %d\n" % (self.device.device_id, location)
		script_data = []
		# collect data from current neighbours
		for device in self.neighbours:
			data = device.get_data(self.location)
			if data is not None:
				script_data.append(data)
		# add our data, if any
		data = self.device.get_data(self.location)
		if data is not None:
			script_data.append(data)

		if script_data != []:
			# run script on data
			result = self.script.run(script_data)

			# update data of neighbours, hope no one is updating at the same time
			for device in self.neighbours:
				device.set_data(self.location, result)
			# update our data, hope no one is updating at the same time
			self.device.set_data(self.location, result)

		self.master.num_slaves_lock.acquire()
		self.master.num_slaves -= 1
		self.master.freed_thread.set()
		self.master.num_slaves_lock.release()

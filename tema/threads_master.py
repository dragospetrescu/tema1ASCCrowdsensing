from threading import *
from debug_helper import Debug

class ThreadsMaster(Thread):

	MAX_NUM_THREADS = 8

	def __init__(self, device):
		"""
		Constructor.

		@type device: Device
		@param device: the device which owns this thread
		"""
		Thread.__init__(self, name="Thread Master for device %d" % device.device_id)
		self.device = device
		self.neighbours = None
		self.num_slaves_lock = Lock()
		# self.num_slaves_lock.release()
		self.num_slaves = 0
		self.freed_thread = Event()

	def run(self):

		while True:

			self.neighbours = self.device.supervisor.get_neighbours()
			if self.neighbours is None:
				break
			# print "Master %d waiting for script\n" % (self.device.device_id)
			self.device.script_received.wait()
			self.device.script_received.clear()
			# print "Master %d received script\n" % (self.device.device_id)

			for (script, location) in self.device.scripts:
				while self.num_slaves == ThreadsMaster.MAX_NUM_THREADS:
					# print "Started script_received wait in while"
					# print "RAHAT num_slaves: %d == max_num_threads: %d with balue %r\n" %(self.num_slaves, ThreadsMaster.MAX_NUM_THREADS, self.num_slaves == ThreadsMaster.MAX_NUM_THREADS)
					self.freed_thread.wait()
					self.freed_thread.clear()
					# print "Started script_received wait in while"
				worker = Worker(self, self.device, self.neighbours, script, location, self.num_slaves)
				# print "Master %d started thread %d\n" %(self.device.device_id, self.num_slaves)
				worker.start()

				if (script, location) == self.device.scripts[-1] and self.device.received_none is False:
					# print "Started script_received wait in exit if"
					# print "SHIT\n"

					self.device.script_received.wait()
					self.device.script_received.clear()
					# print "Finished script_received wait in exit if"
					if (script, location) == self.device.scripts[-1] and self.device.received_none is True:
						break
			self.device.script_received.clear()
			# print "Master %d done starting new scripts\n" % (self.device.device_id)

			while self.num_slaves > 0:
				# print "Started freed_thread"
				# print "Master %d waiting for scripts to finish\n" % (self.device.device_id)
				self.freed_thread.wait()
				self.freed_thread.clear()
				# print "Master %d finished scripts\n" % (self.device.device_id)
				# print "Finished freed_thread"
			# print "Master %d waiting for barrier on timepoint %d\n" % (self.device.device_id, self.device.current_timepoint)
			self.device.barrier.wait()
			# print "Master %d finished barrier on timepoint %d\n" % (self.device.device_id, self.device.current_timepoint)

		# print "Master %d died\n" % (self.device.device_id)



class Worker(Thread):

	def __init__(self, master, device, neighbours, script, location, id):
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
		Thread.__init__(self, name="Worker Thread for device %d" % device.device_id)
		self.master = master
		self.device = device
		self.neighbours = neighbours
		self.script = script
		self.location = location
		self.id = id
		# print "Starting num_slaves acquire"
		self.master.num_slaves_lock.acquire()
		self.master.num_slaves += 1
		self.master.num_slaves_lock.release()
		# print "Finished num_slaves acquire"

	def run(self):
		# run scripts received until now
		# Device.data_log_message += "ID %d starts work on location %d\n" % (self.device.device_id, location)
		script_data = []
		# collect data from current neighbours
		for device in self.neighbours:
			# print "Worker from device %d getting data from device %d on location %d\n" % (self.device.device_id, device.device_id, self.location)
			data = device.get_data(self.location)
			# print "Worker from device %d done getting data from device %d on location %d\n" % (
			# self.device.device_id, device.device_id, self.location)
			if data is not None:
				script_data.append(data)
		# add our data, if any
		# print "Worker from device %d getting data from device %d on location %d\n" % (
		# self.device.device_id, self.device.device_id, self.location)
		data = self.device.get_data(self.location)
		# print "Worker from device %d done getting data from device %d on location %d\n" % (
		# self.device.device_id, self.device.device_id, self.location)
		if data is not None:
			script_data.append(data)

		if script_data != []:
			# run script on data
			result = self.script.run(script_data)

			# update data of neighbours, hope no one is updating at the same time
			for device in self.neighbours:
				# print "Worker from device %d writting data %f on device %d on location %d\n" % (
				# self.device.device_id, result, device.device_id, self.location)
				device.set_data(self.location, result)
				# print "Worker from device %d done writting data %f on device %d on location %d\n" % (
				# 	self.device.device_id, result, device.device_id, self.location)
			# update our data, hope no one is updating at the same time
			# print "Worker from device %d writting data %f on device %d on location %d\n" % (
			# 	self.device.device_id, result, self.device.device_id, self.location)
			self.device.set_data(self.location, result)
			# print "Worker from device %d done writting data %f on device %d on location %d\n" % (
			# 	self.device.device_id, result, self.device.device_id, self.location)


		# print "Starting num_slaves acquire"
		self.master.num_slaves_lock.acquire()
		self.master.num_slaves -= 1
		self.master.freed_thread.set()
		self.master.num_slaves_lock.release()
		# print "Finished num_slaves acquire"
		# print "Worker no %d/%d of master %d finished\n" %(self.id, self.master.num_slaves, self.device.device_id)

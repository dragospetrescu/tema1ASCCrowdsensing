"""
This module represents a device.

Computer Systems Architecture Course
Assignment 1
March 2018
"""

from threading import *

from barrier import ReusableBarrierSem


class Device(object):
	"""
	Class that represents a device.
	"""
	data_log_message = ""
	NO_THREADS = 1

	def __init__(self, device_id, sensor_data, supervisor):
		"""
		Constructor.

		@type device_id: Integer
		@param device_id: the unique id of this node; between 0 and N-1

		@type sensor_data: List of (Integer, Float)
		@param sensor_data: a list containing (location, data) as measured by this device

		@type supervisor: Supervisor
		@param supervisor: the testing infrastructure's control and validation component
		"""
		self.device_id = device_id
		self.sensor_data = sensor_data
		self.supervisor = supervisor
		self.script_received = Event()
		self.script_received.clear()
		self.scripts = []
		self.timepoint_done = Event()
		self.timepoint_done.clear()
		self.barrier = None
		self.current_timepoint = 0
		self.thread = None
		self.workers_barrier = None
		self.received_none = False

		self.nr = 0
		self.nw = 0
		self.e = Lock()
		self.r = Lock()
		self.r.acquire()
		self.w = Lock()
		self.w.acquire()
		self.dr = 0
		self.dw = 0


	def __str__(self):
		"""
		Pretty prints this device.

		@rtype: String
		@return: a string containing the id of this device
		"""
		return "Device %d" % self.device_id

	def setup_devices(self, devices):
		"""
		Setup the devices before simulation begins.

		@type devices: List of Device
		@param devices: list containing all devices
		"""
		if self.device_id == 0:
			self.barrier = ReusableBarrierSem(len(devices))
			self.workers_barrier = ReusableBarrierSem(len(devices) * Device.NO_THREADS)
			self.thread = DeviceThread(self, self.workers_barrier)
		else:
			id0_device = None
			for device in devices:
				if device.device_id == 0:
					id0_device = device
					break

			while self.barrier is None:
				self.barrier = id0_device.__get_barrier()

			while self.workers_barrier is None:
				self.workers_barrier = id0_device.__get_workers_barrier()

		self.thread = DeviceThread(self, self.workers_barrier)
		self.thread.start()

	def __get_barrier(self):
		return self.barrier

	def __get_workers_barrier(self):
		return self.workers_barrier

	def assign_script(self, script, location):
		"""
		Provide a script for the device to execute.

		@type script: Script
		@param script: the script to execute from now on at each timepoint; None if the
			current timepoint has ended

		@type location: Integer
		@param location: the location for which the script is interested in
		"""
		if script is not None:
			self.scripts.append((script, location))
			self.script_received.set()
			Device.data_log_message += "Device %d received script for location %d on timepoint %d\n" %(self.device_id, location, self.current_timepoint)
		else:
			Device.data_log_message += "Device %d received NONE\n" % self.device_id
			self.received_none = True
			self.script_received.set()
			self.barrier.wait()
			self.current_timepoint += 1
			self.timepoint_done.set()
			self.received_none = False

	def get_data(self, location):
		"""
		Returns the pollution value this device has for the given location.

		@type location: Integer
		@param location: a location for which obtain the data

		@rtype: Float
		@return: the pollution value
		"""
		Device.data_log_message += "Device %d is waiting for read on timepoint %d\n" %(self.device_id, self.current_timepoint)
		self.e.acquire()
		if self.nw > 0 or self.dw > 0:
			self.dr += 1
			self.e.release()
			self.r.acquire()

		self.nr += 1

		if self.dr > 0:
			self.dr -= 1
			self.r.release()
		elif self.dr == 0 :
			self.e.release()

		data = self.sensor_data[location] if location in self.sensor_data else None
		Device.data_log_message += "Device %d is reading %s from location %d on timepoint %d\n" % (self.device_id, str(data), location, self.current_timepoint)

		self.e.acquire()
		self.nr -= 1
		if self.nr == 0 and self.dw > 0:
			self.dw -= 1
			self.w.release()
		elif self.nr > 0 or self.dw == 0:
			self.e.release()
		Device.data_log_message += "Device %d finished reading on timepoint %d\n" % (self.device_id, self.current_timepoint)
		return data

	def set_data(self, location, data):
		"""
		Sets the pollution value stored by this device for the given location.

		@type location: Integer
		@param location: a location for which to set the data

		@type data: Float
		@param data: the pollution value
		"""
		Device.data_log_message += "Device %d is waiting to write on timepoint %d\n" % (self.device_id, self.current_timepoint)
		self.e.acquire()
		if self.nr > 0 or self.nw > 0:
			self.dw += 1
			self.e.release()
			self.w.acquire()

		self.nw += 1
		self.e.release()
		Device.data_log_message += "Device %d is writing %s in location %d on timepoint %d\n" % (self.device_id, str(data), location, self.current_timepoint)
		if location in self.sensor_data:
			self.sensor_data[location] = data

		self.e.acquire()
		self.nw -= 1

		if self.dr > 0 and self.dw == 0:
			self.dr -= 1
			self.r.release()
		elif self.dw > 0:
			self.dw -= 1
			self.w.release()
		elif self.dr == 0 and self.dw == 0:
			self.e.release()
		Device.data_log_message += "Device %d finished writing on timepoint %d\n" % (
			self.device_id, self.current_timepoint)



	def shutdown(self):
		"""
		Instructs the device to shutdown (terminate all threads). This method
		is invoked by the tester. This method must block until all the threads
		started by this device terminate.
		"""
		self.thread.join()


class DeviceThread(Thread):
	"""
	Class that implements the device's worker thread.
	"""

	def __init__(self, device, workers_timepoint_barrier):
		"""
		Constructor.

		@type device: Device
		@param device: the device which owns this thread
		"""
		Thread.__init__(self, name="Device Thread %d" % device.device_id)
		self.device = device
		self.workers_timepoint_barrier = workers_timepoint_barrier

	def run(self):
		# hope there is only one timepoint, as multiple iterations of the loop are not supported
		while True:
			# get the current neighbourhood

			Device.data_log_message += "ID %d apeleaza get_neighbours\n" % (self.device.device_id)
			neighbours = self.device.supervisor.get_neighbours()
			if neighbours is None:
				Device.data_log_message += "ID %d has no neighbours\n" % (self.device.device_id)
				break

			while not self.device.received_none:
				Device.data_log_message += "ID %d asteapta script_received\n" % (self.device.device_id)
				self.device.script_received.wait()
				self.device.script_received.clear()
				Device.data_log_message += "ID %d terminat script_received\n" % (self.device.device_id)
				self.__work(neighbours)

			self.device.script_received.clear()
			Device.data_log_message += "ID %d asteapta timepoint_done\n" % (self.device.device_id)
			self.device.timepoint_done.wait()
			self.device.timepoint_done.clear()
			Device.data_log_message += "ID %d terminat timepoint_done\n" % (self.device.device_id)
			self.workers_timepoint_barrier.wait()


	def __work(self, neighbours):
		# run scripts received until now
		for (script, location) in self.device.scripts:
			Device.data_log_message += "ID %d starts work on location %d\n" % (self.device.device_id, location)
			script_data = []
			# collect data from current neighbours
			for device in neighbours:
				data = device.get_data(location)
				if data is not None:
					script_data.append(data)
			# add our data, if any
			data = self.device.get_data(location)
			if data is not None:
				script_data.append(data)

			if script_data != []:
				# run script on data
				result = script.run(script_data)

				# update data of neighbours, hope no one is updating at the same time
				for device in neighbours:
					device.set_data(location, result)
				# update our data, hope no one is updating at the same time
				self.device.set_data(location, result)

"""
This module represents a device.

Computer Systems Architecture Course
Assignment 1
March 2018
"""

from threading import *
from threads_master import *
from debug_helper import Debug
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
		self.barrier = None
		self.current_timepoint = 0
		self.thread = None
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
			self.barrier = ReusableBarrierSem(len(devices) * 2)
		else:
			id0_device = None
			for device in devices:
				if device.device_id == 0:
					id0_device = device
					break

			while self.barrier is None:
				self.barrier = id0_device.__get_barrier()

		self.thread = ThreadsMaster(self)
		# print "Starting Thread Master\n"
		self.thread.start()

	def __get_barrier(self):
		return self.barrier

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
		else:
			self.received_none = True
			self.script_received.set()
			self.barrier.wait()
			self.current_timepoint += 1
			self.received_none = False

	def get_data(self, location):
		"""
		Returns the pollution value this device has for the given location.

		@type location: Integer
		@param location: a location for which obtain the data

		@rtype: Float
		@return: the pollution value
		"""
		self.e.acquire()
		if self.nw > 0 or self.dw > 0:
			self.dr += 1
			self.e.release()
			self.r.acquire()

		self.nr += 1

		if self.dr > 0:
			self.dr -= 1
			self.r.release()
		elif self.dr == 0:
			self.e.release()

		data = self.sensor_data[location] if location in self.sensor_data else None

		self.e.acquire()
		self.nr -= 1
		if self.nr == 0 and self.dw > 0:
			self.dw -= 1
			self.w.release()
		elif self.nr > 0 or self.dw == 0:
			self.e.release()
		return data

	def set_data(self, location, data):
		"""
		Sets the pollution value stored by this device for the given location.

		@type location: Integer
		@param location: a location for which to set the data

		@type data: Float
		@param data: the pollution value
		"""
		self.e.acquire()
		if self.nr > 0 or self.nw > 0:
			self.dw += 1
			self.e.release()
			self.w.acquire()

		self.nw += 1
		self.e.release()
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



	def shutdown(self):
		"""
		Instructs the device to shutdown (terminate all threads). This method
		is invoked by the tester. This method must block until all the threads
		started by this device terminate.
		"""
		# print "Device %d is waiting to die" % self.device_id
		self.thread.join()


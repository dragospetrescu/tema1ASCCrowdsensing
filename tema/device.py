"""
This module represents a device.

Computer Systems Architecture Course
Assignment 1
March 2018
"""

from threading import Event

from DeviceThread import DeviceThread
from barrier import *

class Device(object):
    """
    Class that represents a device.
    """

    time_point_barrier = None
    time_point_barrier_initialization = Event()
    nr_devices = 0

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
        self.scripts = []
        self.timepoint_done = Event()
        self.timepoint_done.clear()
        self.thread = DeviceThread(self)
        self.current_timepoint = 0
        Device.nr_devices += 1

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
            Device.time_point_barrier = ReusableBarrierCond(Device.nr_devices)
            print "Sunt " + str(Device.nr_devices) + " device-uri"
            Device.time_point_barrier_initialization.set()
        else:
            Device.time_point_barrier_initialization.wait()

        self.thread.start()

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
            print "Device " + str(self.device_id) + " received script " + str(script) + " location " + str(location) + "\n"
            self.script_received.set()
        else:
            print "Device " + str(self.device_id) + " is waiting for end of timepoint " + str(self.current_timepoint) + "\n"
            self.time_point_barrier.wait()
            self.current_timepoint +=1
            self.script_received.set()
            self.timepoint_done.set()
            print "Device " + str(self.device_id) + " incepe timepoint-ul " + str(self.current_timepoint) + "\n"

    def get_data(self, location):
        """
        Returns the pollution value this device has for the given location.

        @type location: Integer
        @param location: a location for which obtain the data

        @rtype: Float
        @return: the pollution value
        """
        return self.sensor_data[location] if location in self.sensor_data else None

    def set_data(self, location, data):
        """
        Sets the pollution value stored by this device for the given location.

        @type location: Integer
        @param location: a location for which to set the data

        @type data: Float
        @param data: the pollution value
        """
        if location in self.sensor_data:
            self.sensor_data[location] = data

    def shutdown(self):
        """
        Instructs the device to shutdown (terminate all threads). This method
        is invoked by the tester. This method must block until all the threads
        started by this device terminate.
        """
        self.thread.join()
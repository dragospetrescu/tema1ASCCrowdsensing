"""
This module represents a ThreadMaster

Computer Systems Architecture Course
Assignment 1
March 2018
"""

from threading import Thread, Event


class ThreadsMaster(Thread):
    """
    Class that represents a ThreadMaster - manages workers that run scripts
    """
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
        """
        The run method. Is executed until it receives None as neighbours.
        1). Waits for neighbours. If it is None stops all Workers and finishes
        2). Wait for script and starts workers that execute the scripts
        3). Waits for new scripts until receives None. Goes back to point 1).

        """
        while True:
            self.neighbours = self.device.supervisor.get_neighbours()

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
                # while there are not free threads we wait for one of them to notify us
                while not self.free_threads:
                    self.freed_thread.wait()
                    self.freed_thread.clear()
                # Starting a worker that will execute (script and location)
                worker = self.free_threads.pop()
                worker.give_script(script, location)

                # If I got to the end of the scripts but haven't received
                # none then I have to wait for more scripts
                if (script, location) == self.device.scripts[-1] \
                        and self.device.received_none is False:
                    self.device.script_received.wait()
                    self.device.script_received.clear()

            # Waiting for all threads to finish what they still have to do
            while len(self.free_threads) != ThreadsMaster.MAX_NUM_THREADS:
                self.freed_thread.wait()
                self.freed_thread.clear()
            self.device.barrier.wait()

        # Signaling all workers to stop
        for worker in self.all_threads:
            worker.give_script(None, -1)
        for worker in self.all_threads:
            worker.join()


class Worker(Thread):
    """
    Class that represents a Worker - runs scripts
    """
    def __init__(self, master, device, worker_id):
        """
        Constructor.
        @type master: ThreadsMaster
        @param master: the master of this thread

        @type device: Device
        @param device: the device which owns this thread

        @type worker_id: Integer
        @param worker_id: identifies he worker
        """
        Thread.__init__(self, name="Worker %d for device %d" % (worker_id, device.device_id))
        self.master = master
        self.device = device
        self.neighbours = None
        self.script = None
        self.location = -1
        self.worker_id = worker_id
        self.received_script = Event()

    def give_script(self, script, location):
        """
        Gives Worker a task and signals it to start

        @type script: Script
        @param script: the script that has to be executed

        @type location: Integer
        @param location: the location on which the script has to be executed
        """

        self.script = script
        self.location = location
        self.received_script.set()

    def set_neighbours(self, neighbours):
        """
        Assigns worker the device's neighbours on the current time point

        @type neighbours: List
        @param neighbours: list of the device's neighbours

        """

        self.neighbours = neighbours

    def run(self):
        """
        Worker's run method. Waits until it receives script and then executes it
        When it is done it reappends itself to master's free_threads list
        If receives None as script knows it's time to finish

        """

        while True:
            self.received_script.wait()
            self.received_script.clear()
            if self.script is None:
                return

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

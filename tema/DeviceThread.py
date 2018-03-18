from threading import Thread


class DeviceThread(Thread):
    """
    Class that implements the device's worker thread.
    """

    def __init__(self, device):
        """
        Constructor.

        @type device: Device
        @param device: the device which owns this thread
        """
        Thread.__init__(self, name="Device Thread %d" % device.device_id)
        self.device = device


    def run(self):
        # hope there is only one timepoint, as multiple iterations of the loop are not supported
        while True:
            # get the current neighbourhood
            # self.device.timepoint_done.wait()
            # self.device.timepoint_done.clear()

            # print "Thread id " + str(self.device.device_id) + " waiting for master to receive script" + "\n"
            self.device.script_received.wait()
            self.device.script_received.clear()
            # print "Device thread id " + str(self.device.device_id) + " master just received script" + "\n"
            print "Thread id " + str(self.device.device_id) + " VECINII %d" %( self.device.current_timepoint) + "\n"
            neighbours = self.device.supervisor.get_neighbours()
            if neighbours is None:
                break



            # run scripts received until now
            for (script, location) in self.device.scripts:
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

            # hope we don't get more than one script
            # print "Device thread id " + str(self.device.device_id) + " waiting for master timepoint stop on " + str(self.device.current_timepoint - 1) + "\n"
            self.device.timepoint_done.wait()
            self.device.timepoint_done.clear()
            # print "Device thread id " + str(self.device.device_id) + " master received timepoint stop" + str(self.device.current_timepoint - 1) + "\n"

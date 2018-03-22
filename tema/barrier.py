"""
This module represents a barrier.

Laborator 3 ASC

I do not own this module
"""
from threading import Lock, Semaphore


class ReusableBarrierSem(object):
    """ Bariera reentranta, implementata folosind semafoare """

    def __init__(self, num_threads):
        self.num_threads = num_threads
        self.count_threads1 = self.num_threads
        self.count_threads2 = self.num_threads
        self.counter_lock = Lock()
        self.threads_sem1 = Semaphore(0)
        self.threads_sem2 = Semaphore(0)

    def wait(self):
        """ Wait """
        self.phase1()
        self.phase2()

    def phase1(self):
        """ Phase 1 """
        with self.counter_lock:
            self.count_threads1 -= 1
            if self.count_threads1 == 0:
                for _ in range(self.num_threads):
                    self.threads_sem1.release()
                self.count_threads1 = self.num_threads

        self.threads_sem1.acquire()

    def phase2(self):
        """ Phase 2 """
        with self.counter_lock:
            self.count_threads2 -= 1
            if self.count_threads2 == 0:
                for _ in range(self.num_threads):
                    self.threads_sem2.release()
                self.count_threads2 = self.num_threads

        self.threads_sem2.acquire()

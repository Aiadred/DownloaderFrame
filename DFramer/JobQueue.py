# -*- coding: utf-8 -*-
from threading import Thread
from DFramer import Job
import time

class JobQueue:
    def __init__(self):
        self._queue = []

        pass

    def add_job(self,Job):
        if Job is None:
            return False
        self._queue.append(Job)

    def get_capacity(self):
        return len(self._queue)


    def get_next_job(self):
        for job in self._queue:
            yield job


class workers(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._job_queue = JobQueue()
        self._item_dict = {}
        self._quit_flags = False


    def get_queue_capacity(self):
        return self._job_queue.get_capacity()

    def add_type(self,type_in):
        if self._item_dict.get(type_in,True):
            self._item_dict[type_in] = []

    '''
    this function should check wheter the return value is False or not
    '''
    def add_job(self,job):
        if job is None:
            return False
        self._job_queue.add_job(job)

    def set_quit_flags(self,flags):
        self._quit_flags = flags

    '''
    this function should check wheter the return value is False or not
    '''
    def add_item(self,item):
        if item is None:
            return False
        TYPE = item['TYPE']
        self._item_dict[TYPE].append(item)


    def run(self):
        while self._quit_flags == False:
            for job in self._job_queue.get_next_job():
                if self._item_dict.get(job.get_type(),False):
                    if len(self._item_dict[job.get_type()]) > 0:        # 'item_dict[TYPE]' return a list,the list's elemtents is item
                        job.run(self._item_dict[job.get_type()].pop())
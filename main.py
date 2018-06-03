# -*- coding: utf-8 -*-
from DFramer.Downloader.PornhubDownloader import PornhubDownloder
from DFramer.Downloader.PornhubDownloader import FileLocker
from DFramer.Downloader.PornhubDownloader import Filefd
from DFramer.Downloader import PornhubDownloader
from DFramer.JobQueue import workers
from DFramer.Parser.PornhubParser import PornhubParser
from threading import Lock
import os
import time
'''
    TODO:
        1. 文件创建和文件锁 ok
        2. 下载完成后的事件通知机制
        3. 下载完成后应该把已经下载的数据进行标记，或者是直接删除当前json对象
        4. 下载完成后把文件关闭并且销毁文件锁

'''
file_is_downloaded = None

def main_loop():
    the_threads_num = 5
    capacity_max_full = 3
    thread_list = []
    pornhub_downloader = None
    # 1.create the worker threads
    for index in range(0,the_threads_num,1):
        tid = workers()
        # 2. create job and add it to the workers job queue
        pornhub_downloader = PornhubDownloder()
        tid.add_job(pornhub_downloader)
        tid.add_type("PORNHUB")
        thread_list.append(tid)
        tid.start()

    # 3. create the parser instance and set the json file path
    pornhub_parser = PornhubParser()
    pornhub_parser.set_json_path("./pornhub.json")
    pornhub_parser.set_thread_num(the_threads_num)
    # 4. create the semaphore
    RR_counter = 0
    while True:

        for item in pornhub_parser.get_item():
            if item is None:
                print "all works has been downloaded complete"
                break
            '''
            if thread_list[RR_counter].get_queue_capacity() == capacity_max_full:
                RR_counter += 1
                continue
        '''
            # open the file
                # warning ,create the file ,should check whether the file name is find or not
            if Filefd.get(item['source_video_title'],False) == False:
                source_video_filepath = pornhub_downloader.get_base_path() + item['source_video_title'] + ".mp4"
                if os.path.isfile(source_video_filepath):   #如果文件已经存在
                    print source_video_filepath+" has been existed"
                    continue
                fd = open(source_video_filepath,"wb")
                Filefd[item['source_video_title']] = fd
            # create the file lock
            if FileLocker.get(item['source_video_title'],False) == False:
                FileLock = Lock()
                FileLocker[item['source_video_title']] = FileLock

            thread_list[RR_counter].add_item(item)
            RR_counter += 1
            if RR_counter == the_threads_num:
                RR_counter = 0
                while PornhubDownloader.download_is_complete != the_threads_num:
                    pass
                if PornhubDownloader.download_is_complete == the_threads_num:
                    PornhubDownloader.download_is_complete = 0
                    Filefd[item['source_video_title']].close()
                    Filefd.pop(item['source_video_title'])
                    FileLocker.pop(item['source_video_title'])
        # close all threads
        for tid in thread_list:
            tid.set_quit_flags(True)
        time.sleep(3)
        return


if __name__ == "__main__":
    main_loop()

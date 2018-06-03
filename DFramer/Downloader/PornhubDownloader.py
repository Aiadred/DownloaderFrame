# -*- coding: utf-8 -*-
from DFramer.Job import Job
import requests
from requests import exceptions
import time
download_is_complete = 0
FileLocker = {}
Filefd = {}
class PornhubDownloder(Job):
    def __init__(self):
        super(PornhubDownloder,self).__init__()
        self._chunk_size = 1000000
        self._item = None
        self._base_path = "./PornhubStore/"
        self._type = "PORNHUB"

    def get_base_path(self):
        return self._base_path

    def get_type(self):
        return self._type

    def run(self,item):
        if item is None:
            return False
        self._item = item
        self._download_page()



    def _download_page(self):
        source_video_url = self._item['source_video_url']
        source_video_title = self._item['source_video_title']
        source_video_pstart = self._item['source_video_pstart']
        source_video_pend = self._item['source_video_pend']

        headers = {"Range": "bytes=%s-%s" % (source_video_pstart, source_video_pend)}
        while True:
            try:
                res = requests.get(source_video_url, headers=headers, stream=True, verify=False) # the verify is false,but it has problem and not perfection
                break
            except exceptions.SSLError:
                print "Download "+source_video_title+" range "+str(source_video_pstart)+" to "+str(source_video_pend)+" SSLError -- please wait 3 seconds"
                time.sleep(3)
            except:
                print "Download "+source_video_title+" range "+str(source_video_pstart)+" to "+str(source_video_pend)+" an Unkown Error Occured -- please wait 3 seconds"
                time.sleep(3)

        print source_video_pstart,source_video_pend # print

        pstart = source_video_pstart
        for chunk in res.iter_content(chunk_size=self._chunk_size):
            if chunk and FileLocker[source_video_title].acquire(True):
                Filefd[source_video_title].seek(pstart)
                Filefd[source_video_title].write(chunk)
                pstart += len(chunk)
                FileLocker[source_video_title].release()

        print "Download "+source_video_title+" "+str(source_video_pstart)+" to "+str(source_video_pend)+" "+" complete\n"
        global  download_is_complete
        download_is_complete += 1
        # there should be inform the main thread

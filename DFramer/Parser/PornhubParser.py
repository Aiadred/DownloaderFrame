# -*- coding: utf-8 -*-
from DFramer.ParsePage import ParsePage
from requests import exceptions
import requests
from bs4 import BeautifulSoup
import re
import codecs
import json
import time


class PornhubParser(ParsePage):
    def __init__(self):
        super(PornhubParser,self).__init__()
        self._json_path = "./pornhub.json"  # default json file path
        self._thread_num = 5
        self._start = 0
        self._end = 0
        self._type = "PORNHUB"


    def set_json_path(self,path):
        # there should be check the path ,TODO:
        self._json_path = path


    '''
        this function should check wheter the return value is False or not
    '''
    def set_thread_num(self,thread_num):
        if thread_num <= 0 or thread_num > 20:
            print "the thread num should larger than 0 and smaller than 20"
            return False
        self._thread_num = thread_num


    '''
    this function should check wheter the yield value is None or not
    '''
    def get_item(self):
        # there should check whether the json path is rightful,TODO:
        line_text = {}
        json_file_id = codecs.open(self._json_path,"r",encoding='utf-8')
        for line in json_file_id:
            try:
                line_text = json.loads(line)
            except ValueError,e:
                yield None
            page_url = line_text['Url']
            page_title = line_text['Title']
            if page_url is None or page_title is None:
                yield False
            # handle with the title's illegal character
            pattern = re.compile(r'[\\/?:*"<>|]')
            if re.findall(pattern,page_title):
                page_title = pattern.sub(" ", page_title)


            source_video_url = self._parse_page(page_url = page_url)    # get the source video url
            if source_video_url is None:
                print "parse page method return value is None\n"
                yield None
            user_agent = u"user_agent = Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299"
            headers = {'User-Agent': user_agent}

            while True:
                try:
                    res = requests.get(url=source_video_url, headers=headers, stream=True, verify=False)
                    break
                except exceptions.SSLError:
                    print "get " + page_title + " content-length SSLError -- please wait 3 seconds"
                    time.sleep(3)
                except:
                    print "get " + page_title + " an Unkown Error Occured -- please wait 3 seconds"
                    time.sleep(3)

            # slice and send item to workers which used the RR method
            for slice_range in self._get_range(res):
                pstart, pend = slice_range
                item = {'TYPE':self._type,'source_video_url': source_video_url, 'source_video_title': page_title,'source_video_pstart': pstart, 'source_video_pend': pend}
                yield item

        json_file_id.close()
        yield None


    '''
    this function should check wheter the return value is None or not
    '''
    def _parse_page(self,page_url):
        user_anget = u"user_agent = Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299"
        headers = {'User-Agent': user_anget}

        while True:
            try:
                res = requests.get(page_url, headers=headers, verify=False)
                break
            except exceptions.SSLError:
                print "get HTML SSLError -- please wait 3 seconds"
                time.sleep(3)
            except:
                print "get HTML SSLError an Unkown Error Occured -- please wait 3 seconds"
                time.sleep(3)

        soup = BeautifulSoup(res.content, "lxml", from_encoding="utf-8")
        # parse the url
        items = soup.find_all('script', type="text/javascript")
        pattern = re.compile(r'var flashvar')
        for item in items:
            if (len(item.contents) > 0):
                resutls = re.findall(pattern, item.text)
                if (len(resutls) > 0):
                    # 解析文本
                    patten_urls = [
                        re.compile(
                            r'https:\\/\\/cv\.phncdn\.com\\/videos\\/\d+\\/\d+\\/\d+\\/.*?\.mp4\?[a-z0-9A-Z=&.]*'),
                        re.compile(
                            r'https:\\/\\/dv1\.phncdn\.com\\/videos\\/\d+\\/\d+\\/\d+\\/.*?\.mp4\?[a-z0-9A-Z=&.]*'),
                        re.compile(
                            r'https:\\/\\/dv\.phncdn\.com\\/videos\\/\d+\\/\d+\\/\d+\\/.*?\.mp4\?[a-z0-9A-Z=&.]*')]
                    for pattern_url in patten_urls:
                        urls = re.findall(pattern_url, res.content)
                        if len(urls) == 0:
                            continue
                        else:
                            url = re.sub(r"\\", "", urls[0])
                            return url
        return None


    def _get_range(self,response):
        # get the response header's Content-Length
        content_length = response.headers.get("Content-Length")
        print "the content-length: ", content_length

        default_range_limit = int(content_length) / self._thread_num
        self._start = self._end = 0
        for i in range(0, self._thread_num, 1):
            if i == self._thread_num - 1:
                self._start = i * default_range_limit
                self._end = content_length
                yield [self._start, self._end]
            else:
                self._start = i * default_range_limit
                self._end = (i + 1) * default_range_limit - 1
                yield [self._start, self._end]
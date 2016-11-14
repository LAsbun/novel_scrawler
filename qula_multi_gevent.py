#!/usr/bin/env python
#coding:utf-8
__author__ = 'sws'

import gevent
from util import db
from util.logger import logger
import gevent
from gevent.pool import Pool
from lxml.html import fromstring, tostring
import requests
from gevent.queue import Queue, Empty
from gevent import monkey
monkey.patch_all()

class Crawler():

    def __init__(self, base_url):
        self.base_url = base_url
        self.que = Queue()
        self.pool = Pool(200)


    def get_content(self, url):
        res = requests.get(url)

        if len(res.content)>3000:
            return res.content

    def run(self):
        self.get_url()
        for i in xrange(200):
            self.pool.spawn(self.crawl_single_chapter)
        self.pool.join()


    def get_url(self):
        content = self.get_content(self.base_url)
        root = fromstring(content)

        dd_list = root.xpath('//*[@id="list"]//dd')
        print dd_list
        for dd in dd_list:
            try:
                tmp_url = dd.xpath('./a/@href')[0]
                temp_name = dd.xpath('./a/text()')[0].encode('utf-8')
            except Exception as e:
                import traceback
                traceback.print_exc(e)
                logger.error('提取url 失败')
            url = self.base_url+tmp_url
            self.que.put((temp_name, url))

    def insert_db(self, result):
        sql = '''insert ignore into app01_chapter (book_id, source_id, chapter_name,
              chapter_url, chapter_content) values (%s, %s, '%s', '%s', '%s')''' %result
        print sql
        db.ExecuteSQL(sql)

    def crawl_single_chapter(self):
        while True:
            try:
                print self.que.qsize(), 'sss'
                chapter_name, url = self.que.get()
            except Empty:
                break
            content = self.get_content(url=url)
            root = fromstring(content)
            content = root.xpath('//*[@id="content"]')[0]
            content = tostring(content, encoding='utf-8')

            self.insert_db((1,1, chapter_name, url, content))

base_url = 'http://www.qu.la/book/168/'
craw = Crawler(base_url)
craw.run()

#!/usr/bin/env python
#coding:utf-8
__author__ = 'sws'

import gevent
from user_agent import generate_user_agent
from util import db
from util.logger import logger
import gevent
import urllib
from gevent.pool import Pool
from lxml.html import fromstring, tostring
import requests
from gevent.queue import Queue, Empty
from gevent import monkey
monkey.patch_all()



class Crawler():
    '''
        主要是抓取www.qu.la网站上的小说,并入库
    '''

    def __init__(self, base_url = None, max_pool_size = 200):
        '''
        @base_url 是小说的章节的主页
        @max_pool_size: 协程的数量
        '''
        self.base_url = base_url
        self.que = Queue()
        self.max_pool_size = max_pool_size
        self.pool = Pool(max_pool_size)


    def get_content(self, url):
        '''
        :param url:
        :return: 抓取回来的内容
        '''
        user_agen = generate_user_agent()
        header = {
            'User-Agent':user_agen,
        }
        res = requests.get(url, headers = header)

        if len(res.content)>3000:
            return res.content

    def run(self):
        '''
        :return:
        '''
        if self.base_url is None:
            logger.error('没有指定书url')
            return
        self.get_url()
        for i in xrange(self.max_pool_size):
            self.pool.spawn(self.crawl_single_chapter)
        self.pool.join()


    def get_url(self):
        content = self.get_content(self.base_url)
        root = fromstring(content)

        dd_list = root.xpath('//*[@id="list"]//dd')
        print len(dd_list)
        temp_list = []
        for count in xrange(len(dd_list)):
            dd = dd_list[count]
            try:
                tmp_url = dd.xpath('./a/@href')[0]
                temp_name = dd.xpath('./a/text()')[0].encode('utf-8')
                print temp_name,tmp_url
                temp_list.append((temp_name, tmp_url))
            except Exception as e:
                import traceback
                traceback.print_exc(e)
                logger.error('提取url 失败')
                print tostring(dd)
                continue
            url = self.base_url+tmp_url
            self.que.put((count+1, temp_name, url))
        print self.que.qsize(), len(temp_list)

    def insert_db(self, result):
        sql = '''insert ignore into app01_chapter (book_id, source_id, chapter_id, chapter_name,
              chapter_url, chapter_content) values (%s, %s, %s, %s, %s, %s)'''
        # print sql
        try:
            ret = db.ExecuteSQLs(sql, result)
            print ret, '-'*100
        except Exception as e:
            import traceback
            traceback.print_exc(e)
            print sql

    def crawl_single_chapter(self):
        content_list = []
        while self.que.qsize():
            try:
                print self.que.qsize(), 'sss'
                count, chapter_name, url = self.que.get_nowait()
            except Empty:
                break
            content = self.get_content(url=url)
            root = fromstring(content)
            content = root.xpath('//*[@id="content"]')[0]
            content = tostring(content, encoding='utf-8')
            content_list.append((2,1, count, chapter_name, url, content))
        self.insert_db(content_list)
            # gevent.sleep(0.001)

class Crawl_book():
    '''
        输入书名，查找是否存在书，
        如果不存在，返回False
        如果存在 返回url
        抓取详细的书单 使用Crawler
    '''
    def __init__(self, bookname):
        self.bookname = bookname
        self.bookname_url = self.get_bookname_url()
        self.crawler = Crawler(self.bookname_url)

    def run(self):
        if self.bookname_url:
            self.crawler.run()

    def get_content(self, url):
        '''
        :param url:
        :return: 抓取回来的内容
        '''
        user_agen = generate_user_agent()
        header = {
            'User-Agent':user_agen,
        }
        res = requests.get(url, headers = header)

        if len(res.content)>3000:
            return res.content

    def get_bookname_url(self):
        base_url = 'http://zhannei.baidu.com/cse/search?s=920895234054625192&q='
        url = base_url+urllib.quote(self.bookname)
        content =self.get_content(url)

        root = fromstring(content)

        # 选取第一个查找的，如果名字不是为bookname， 那么就是找不到
        book_list = root.xpath('//*[@class="result-item result-game-item"]')
        if len(book_list) == 0:
            logger.error('对不起找不到对应的书籍')
            return False
        else:
            bookname = book_list[0].xpath('.//a[@cpos="title"]//text()')
            bookname = ''.join([ bk.replace('\n', '').strip() for bk in bookname]).encode('utf-8')

            if bookname == self.bookname:
                try:
                    bookname_url = book_list[0].xpath('.//a[@cpos="title"]/@href')[0]
                except Exception as e:
                    logger.error('找不到对应书的网址')
                    return False
                return bookname_url


base_url = 'http://www.qu.la/book/168/'
bookname = '永夜君王'
craw = Crawl_book(bookname)
craw.run()

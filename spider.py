import sys
import requests
import hashlib
import time
from lxml import etree
from elasticsearch import Elasticsearch


class Poetry:
    def __init__(self, title="", content="", author="", tag=None, dynasty=""):
        self.title = title
        self.dynasty = dynasty
        self.content = content
        self.author = author
        self.tag = tag


class PoetrySpider:
    def __init__(self):
        self.db = Elasticsearch([{"host": "localhost", "port": 9200}])
        self.domain = ''
        
    def create_poetry_index(self):
        self.db.indices.create('poetry_index', ignore=400, body={
            "mapping": {
                "poetry": {
                    "properties": {
                        "title": {"type": "text"},
                        "dynasty": {"type": "text"},
                        "author": {"type": "text"},
                        "content": {"type": "text"},
                        "tag": {"type": "text"},
                    }
                }
            }
        })
    
    def download(self, origin_url):
        print('origin_url:{}'.format(origin_url))
        self.domain = origin_url.split('/')[2]
        data = requests.get(origin_url)
        if data:
            self.parse(data.text)

    def parse(self, data):
        response = etree.HTML(data)
        for row in response.xpath('//div[@class="left"]/div[@class="sons"]'):
            poetry = Poetry()
            poetry.title = row.xpath('div[@class="cont"]/p/a/b/text()')[0] \
                if row.xpath('div[@class="cont"]/p/a/b/text()') else ''
            poetry.dynasty = row.xpath('div[@class="cont"]/p[@class="source"]//text()')[0] \
                if row.xpath('div[@class="cont"]/p[@class="source"]//text()') else ''
            poetry.author = row.xpath('div[@class="cont"]/p[@class="source"]//text()')[-1] \
                if row.xpath('div[@class="cont"]/p[@class="source"]//text()') else ''
            poetry.content = ''.join(row.xpath('div[@class="cont"]/div[@class="contson"]//text()')).\
                replace('　　', '').replace('\n', '') \
                if row.xpath('div[@class="cont"]/div[@class="contson"]//text()') else ''
            poetry.tag = ','.join(row.xpath('div[@class="tag"]/a/text()')) \
                if row.xpath('div[@class="tag"]/a/text()') else ''
            print('Title: {}'.format(poetry.title))
            print('Dynasty: {}'.format(poetry.dynasty))
            print('Author: {}'.format(poetry.author))
            print('Content: {}'.format(poetry.content))
            print('Tag: {}'.format(poetry.tag))
            self.write_to_es(poetry)
        if response.xpath('//div[@class="pagesright"]/a[@class="amore"]/@href'):
            time.sleep(2)
            self.download('http://' + self.domain +
                          response.xpath('//div[@class="pagesright"]/a[@class="amore"]/@href')[-1])
            
    def write_to_es(self, poetry):
        doc = {
            "title": poetry.title,
            "dynasty": poetry.dynasty,
            "author": poetry.author,
            "content": poetry.content,
            "tag": poetry.tag,
            "timestamp": int(time.time()*1000)
        }
        poetry_id = hashlib.sha1(poetry.content.encode("utf-8")).hexdigest()[0:15]
        if self.db.exists("poetry_index", "poetry", poetry_id):
            self.db.delete("poetry_index", "poetry", poetry_id)
        res = self.db.create(index="poetry_index", doc_type="poetry", id=poetry_id, body=doc)
        print('Put Success: {}'.format(res))
        
    
if __name__ == '__main__':
    sys.setrecursionlimit(100000)
    ori_url = 'http://so.gushiwen.org/type.aspx'
    do = PoetrySpider()
    #do.create_poetry_index()
    do.download(ori_url)

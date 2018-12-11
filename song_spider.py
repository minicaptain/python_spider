import sys
import requests
import hashlib
import time
from lxml import etree
from elasticsearch import Elasticsearch
from spider_error import NetWorkError


class Song:
    def __init__(self, title="", content="", author="", tag=None):
        self.title = title
        self.content = content
        self.author = author
        self.tag = tag
        self.country = "cn"
        self.language = "zh"
        self.url = ""


class SongSpider:
    def __init__(self):
        self.db = Elasticsearch([{"host": "localhost", "port": 9200}])
        self.domain = ''
    
    def create_poetry_index(self):
        self.db.indices.put_mapping('poetry_index', ignore=400, body={
            "mapping": {
                "song": {
                    "properties": {
                        "title": {"type": "text"},
                        "author": {"type": "text"},
                        "content": {"type": "text"},
                        "tag": {"type": "text"},
                        "country": {"type": "text"},
                        "language": {"type": "text"},
                    }
                }
            }
        })
    
    def download(self, origin_url):
        data = requests.get(origin_url)
        self.domain = origin_url.split('/')[2]
        print(self.domain)
        if data:
            main_feed = etree.HTML(data.text)
            for song_list_url in main_feed.xpath('//span[@class="TAG"]/a/@href'):
                self.parse_song_list(song_list_url)
                
    def parse_song_list(self, list_url):
        try:
            list_data = requests.get(list_url)
            list_data.encoding = "gb2312"
            if list_data:
                list_page = etree.HTML(list_data.text)
                for song_page_suffix in \
                        list_page.xpath('//div[@class="newslist"]//span/a[@target="_blank"]/@href'):
                    song_page_url = 'http://' + self.domain + '/'+song_page_suffix
                    self.parse_song(song_page_url)
                if list_page.xpath('//div[@id="pagenum"]//a[@title="下一页"]/@href'):
                    next_page_suffix = list_page.xpath('//div[@id="pagenum"]//a[@title="下一页"]/@href')[0]
                    next_page_url = 'http://' + self.domain + '/' + next_page_suffix
                    print('next page url: {}'.format(next_page_url))
                    self.parse_song_list(next_page_url)
                else:
                    print("next page error")
            else:
                raise NetWorkError(list_data.reason)
        except NetWorkError as e:
            print(e.args)
            
    def parse_song(self, song_page_url):
        try:
            print('song_page_url is:{}'.format(song_page_url))
            song_page = requests.get(song_page_url)
            song_page.encoding = 'gb2312'
            if song_page:
                response = etree.HTML(song_page.text)
                song = Song()
                song.title = response.xpath('//div[@id="entrytitle"]/h1/text()')[0] \
                    if response.xpath('//div[@id="entrytitle"]/h1/text()') else ''
                song.author = response.xpath('//div[@id="entrymeta"]//font/text()')[0] \
                    if response.xpath('//div[@id="entrymeta"]//font/text()') else ''
                song.content = '\n'.join(response.xpath('//table[@class="content"]//td[@class="content_word"]/div/text()')) \
                    if response.xpath('//table[@class="content"]//td[@class="content_word"]/div/text()') else ''
                song.tag = response.xpath('//div[@class="position"]/a/text()')[-1] \
                    if response.xpath('//div[@class="position"]/a/text()') else ''
                song.url = song_page_url
                print(song.title)
                print(song.author)
                print(song.content)
                print(song.tag)
                self.write_to_es(song)
            else:
                raise NetWorkError("parse song page error")
        except NetWorkError as e:
            print(e.args)
        
    def write_to_es(self, song):
        doc = {
            "title": song.title,
            "author": song.author,
            "content": song.content,
            "language": song.language,
            "country": song.country,
            "tag": song.tag,
            "timestamp": int(time.time() * 1000),
            "url": song.url,
        }
        if song.content:
            poetry_id = hashlib.sha1(song.content.encode("utf-8")).hexdigest()[0:15]
            if self.db.exists("poetry_index", "poetry", poetry_id):
                self.db.delete("poetry_index", "poetry", poetry_id)
            res = self.db.create(index="poetry_index", doc_type="poetry", id=poetry_id, body=doc)
            print('Put Success: {}'.format(res))


if __name__ == '__main__':
    sys.setrecursionlimit(100000)
    ori_url = 'http://www.gecicn.com/list.php?fid=1'
    do = SongSpider()
    #do.create_poetry_index()
    do.download(ori_url)

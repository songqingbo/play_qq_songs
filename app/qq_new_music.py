# -*- coding:utf-8
import json
import requests
import MySQLdb
import time
import datetime


class QqMusicNew():
    def __init__(self):
        self.insert_list = []
        self.host = '101.200.159.42'
        self.user = 'java'
        self.pw = 'inspero'
        self.database = 'musicnew'
        self.time_stamp = []
        self.database = MySQLdb.connect(self.host, self.user, self.pw, self.database, charset='utf8')
        self.cursor = self.database.cursor()
        self.cursor.execute('select version()')
        data = self.cursor.fetchone()
        print int(time.time()), 'Database version : %s' % data
        del data

    def get_urls(self):
        '''
        :description:get the urls that to be requested
        :return:urls
        '''
        today = datetime.date.today()
        one_day = datetime.timedelta(days=1)
        yesterday = str(today - one_day)
        head_url_1 = 'https://c.y.qq.com/v8/fcg-bin/fcg_v8_toplist_cp.fcg?tpl=3&page=detail&date=' + yesterday
        tail_url_1 = '&topid=27&type=top&song_begin=0&song_num=30&g_tk=5381&jsonpCallback=MusicJsonCallbacktoplist&loginUin=0&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0'
        head_url_2 = 'https://c.y.qq.com/v8/fcg-bin/fcg_v8_toplist_cp.fcg?tpl=3&page=detail&date=' + yesterday
        tail_url_2 = '&topid=27&type=top&song_begin=30&song_num=30&g_tk=5381&jsonpCallback=MusicJsonCallbacktoplist&loginUin=0&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0'
        head_url_3 = 'https://c.y.qq.com/v8/fcg-bin/fcg_v8_toplist_cp.fcg?tpl=3&page=detail&date=' + yesterday
        tail_url_3 = '&topid=27&type=top&song_begin=60&song_num=30&g_tk=5381&jsonpCallback=MusicJsonCallbacktoplist&loginUin=0&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0'
        head_url_4 = 'https://c.y.qq.com/v8/fcg-bin/fcg_v8_toplist_cp.fcg?tpl=3&page=detail&date=' + yesterday
        tail_url_4 = '&topid=27&type=top&song_begin=90&song_num=30&g_tk=5381&jsonpCallback=MusicJsonCallbacktoplist&loginUin=0&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0'
        url_1 = head_url_1 + tail_url_1
        url_2 = head_url_2 + tail_url_2
        url_3 = head_url_3 + tail_url_3
        url_4 = head_url_4 + tail_url_4
        urls = [url_1, url_2, url_3, url_4]
        return urls

    def get_all_data(self, urls):
        '''
        :param urls:urls to be requested
        :return: all data
        '''
        all_data = []
        for url in urls:
            response = requests.request(method='GET', url=url)
            head_index = len('MusicJsonCallbacktoplist(')
            response = response.text[head_index + 1:-2]
            qq_json = json.loads(response)
            s = self.parse_get_dict_list(qq_json)
            all_data += s
        return all_data

    def parse_get_dict_list(self, qq_json):
        '''
        :param qq_json: the json data from qq music (only one page)
        :return: list that contains the dict data of one page
        '''
        all_songs = []
        ALL_UPDATE_TIME = qq_json['update_time']
        ALL_DATE = qq_json['update_time']
        song_list = qq_json['songlist']
        for song in song_list:
            try:
                mb = song['mb']
                album_desc = song['data']['albumdesc']
                album_mid = song['data']['albummid']
                song_id = song['data']['songid']
                album_name = song['data']['albumname']
                song_orig = song['data']['songorig']
                song_name = song['data']['songname']
                interval = song['data']['interval']
                song_mid = song['data']['songmid']
                singer = song['data']['singer']
                singer_id = ''
                singer_mid = ''
                singer_name = ''
                singer_num = len(singer)
                if singer_num == 1:
                    singer_id += str(singer[0]['id'])
                    singer_name += singer[0]['name']
                    singer_mid += singer[0]['mid']
                elif singer_num >= 2:
                    for i, s in enumerate(singer):
                        if i == 0:
                            singer_id += str(singer[i]['id'])
                            singer_name += singer[i]['name']
                            singer_mid += singer[i]['mid']
                        else:
                            singer_id += '+' + str(singer[i]['id'])
                            singer_name += '+' + singer[i]['name']
                            singer_mid += '+' + singer[i]['mid']
                else:
                    pass
                song_dict = {'update_time': ALL_UPDATE_TIME, 'date': ALL_DATE, 'mb': mb, 'album_desc': album_desc,
                             'album_mid': album_mid, 'song_id': song_id,
                             'album_name': album_name, 'song_orig': song_orig, 'song_name': song_name,
                             'interval': interval,
                             'song_mid': song_mid, 'singer_id': singer_id, 'singer_name': singer_name,
                             'singer_mid': singer_mid}
                all_songs.append(song_dict)
            except Exception, e:
                fn = open('exception.log', 'a')
                fn.write(str(e))
                fn.write('\n')
                fn.flush()
                fn.close()
        return all_songs

    def process_data(self, all_data):
        '''
        :param all_data:all songs crawled from qq music
        :return: all can be inserted into mysql
        '''
        insert_data = []
        exist_mid = []
        exist_song = open('exist_song.txt', 'r')
        lines = exist_song.readlines()
        for line in lines:
            mid = line.strip()
            exist_mid.append(mid)
        exist_song.close()
        for song in all_data:
            if song['song_mid'] not in exist_mid:
                insert_data.append(song)
                self.insert_list.append(song['song_mid'])
        return insert_data

    def get_inserted_list(self, insert_data):
        '''
        :param insert_data:to be inserted into mysql
        :return: list contains tuple
        '''
        insert_list = []
        timestamp = str(int(time.time()))
        for song in insert_data:
            song_name = song['song_name']
            song_mid = song['song_mid']
            song_orig = song['song_orig']
            singer_name = song['singer_name']
            song_id = str(song['song_id'])
            singer_mid = song['singer_mid']
            singer_id = song['singer_id']
            album_name = song['album_name']
            album_desc = song['album_desc']
            album_mid = song['album_mid']
            mb = song['mb']
            update_time = song['update_time']
            date_var = song['date']
            interval_var = str(song['interval'])
            insert_timestamp = timestamp
            insert_list.append(
                (insert_timestamp, song_name, song_mid, song_orig, song_id, singer_name, singer_mid, singer_id,
                 album_name,
                 album_desc, album_mid, interval_var, mb, update_time, date_var))
        return insert_list

    def insert_into_mysql(self, insert_list):
        sql = 'insert into qq_songs(insert_timestamp,song_name,song_mid,song_orig,' \
              'song_id,singer_name,singer_mid,singer_id,album_name,album_desc,album_mid,' \
              'interval_var,mb,update_time,date_var) values ' \
              '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        try:
            self.cursor.executemany(sql, insert_list)
            self.database.commit()
            fn = open('exist_song.txt', 'a')
            for mid in self.insert_list:
                fn.write(mid)
                fn.write('\n')
                fn.flush()
            fn.close()
            fx = open('work.log', 'a')
            fx.write(str(time.time()) + '  ' + '插入' + str(len(self.insert_list)) + '条数据')
            fx.write('\n')
            fx.flush()
            fx.close()

        except Exception, e:
            fn = open('error.log', 'a')
            fn.write(str(e))
            fn.write('\n')
            fn.flush()
            fn.close()
            self.database.rollback()

    def interface(self):
        urls = self.get_urls()
        all_data = self.get_all_data(urls=urls)
        insert_data = self.process_data(all_data)
        insert_list = self.get_inserted_list(insert_data)
        self.insert_into_mysql(insert_list)
        
    def git_test(self):
        pass

if __name__ == '__main__':
    while True:
        QqMusicNew().interface()
        time.sleep(86400)

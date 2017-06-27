#-encoding:utf8-
import requests
import pymysql
from tqdm import tqdm
import time
import sys
import Database_config



class BilibiliSpider():

#url = 'http://api.bilibili.com/archive_rank/getarchiverankbypartion?type=jsonp&tid=33&pn=' + pn
    def __init__(self, action, tid, pn, table_name, rangeX, rangeY, type = 'json', base_url = 'http://api.bilibili.com/archive_rank/'):
        '''
        :param action: url动作
        :param tid: 栏目名
        :param base_url: base_url
        :param type:数据类型 type=json
        :param pn: 页码
        :param table_name: 数据库表名
        :param rangeX:
        :param rangeY:
        :param host:
        :param user:
        :param pwd:
        :param database:
        :param charset:
        '''
        self.action = action
        self.tid = tid
        self.base_url = base_url
        self.type = type
        self.pn = pn
        self.table_name = table_name
        self.rangeX = rangeX
        self.rangeY = rangeY


    def results(self):
        url = self.base_url + self.action + '?type=' + self.type + '&tid=' + str(self.tid) + '&pn=' + str(self.pn)
        try:
            r = requests.get(url)
            data = r.json()
        except Exception as e:
            print(e)

        try:
            results = data['data']['archives']
            return results
        except Exception as e:
            print(e,'An error occurred in'+ self.pn)

    def save(self):
        db = self.db()
        cursor = db.cursor()
        cursor.execute('SET NAMES utf8;')
        cursor.execute('SET CHARACTER SET utf8;')
        cursor.execute('SET character_set_connection=utf8;')
        results = self.results()
        # print(results[0]['aid'])
        # print(results[1]['tid'])
        # sys.exit(0)

        try:
            for result in tqdm(results, desc="存储进度"):
                aid = int(result['aid'])
                query = "SELECT * from "+ self.table_name +"   WHERE aid = ('%d')" % (aid)
                if cursor.execute(query):
                    # cursor.close()
                    continue
                else:
                    title = pymysql.escape_string(str(result['title']))
                    description = pymysql.escape_string(str(result['description']))
                    mid = int(result['mid'])
                    try:
                        play = int(result['play'])
                    except:
                        play = 0
                    try:
                        coin = int(result['stat']['coin'])
                    except:
                        coin = 0
                    try:
                        danmaku = int(result['stat']['danmaku'])
                    except:
                        danmaku = 0
                    try:
                        favorite = int(result['stat']['favorite'])
                    except:
                        favorite = 0
                    try:
                        share = int(result['stat']['share'])
                    except:
                        share = 0
                    try:
                        reply = int(result['stat']['reply'])
                    except:
                        reply = 0
                    try:
                        creattime = int(time.mktime(time.strptime(result['create'], '%Y-%m-%d %H:%M')))
                    except:
                        creattime = 0
                        # int(time.mktime(time.strptime(timestring, '%Y-%m-%d %H:%M:%S')))将时间转换为时间戳
                    # print(aid, title, description, creattime, mid, play, coin, danmaku, favorite, share, reply)
                    # sys.exit(0)
                    sql = "INSERT INTO "+ self.table_name +" (AID, TITLE, DESCRIPTION, CREATTIME, MID, PLAY, COIN, DANMAKU, FAVORITE, SHARE, REPLY) VALUES ('%d', '%s', '%s', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d')" % (
                        aid, title, description, creattime, mid, play, coin, danmaku, favorite, share, reply)
                    try:
                        cursor.execute(sql)
                        db.commit()
                        time.sleep(0.1)
                    except Exception as e:
                        print(e)
                        db.rollback()
            db.close()
        except Exception as e:
            print(e)

    def create_table(self):
        try:
            cursor = self.cursors()
            sql = 'CREATE TABLE `' + self.table_name + '` (' \
                                                       '`id` int(11) unsigned NOT NULL AUTO_INCREMENT,' \
                                                       '`aid` int(11) DEFAULT NULL,' \
                                                       '`title` varchar(255) DEFAULT NULL,' \
                                                       '`description` longtext,' \
                                                       '`creattime` int(11) DEFAULT NULL,' \
                                                       '`mid` int(11) DEFAULT NULL,' \
                                                       '`play` int(11) DEFAULT NULL,' \
                                                       '`danmaku` int(11) DEFAULT NULL,' \
                                                       '`favorite` int(11) DEFAULT NULL,' \
                                                       '`coin` int(11) DEFAULT NULL,' \
                                                       '`share` int(11) DEFAULT NULL,' \
                                                       '`reply` int(11) DEFAULT NULL,' \
                                                       'PRIMARY KEY (`id`) )ENGINE=InnoDB DEFAULT CHARSET=utf8'
            cursor.execute(sql)
            self.db().commit()
        except Exception as e:
            print(e)

    def db(self):
        try:
            db = pymysql.connect(Database_config.HOST, Database_config.USER, Database_config.POSSWOED, Database_config.DATABASE, charset="utf8", use_unicode=True)
            return db
        except Exception as e:
            print(e)

    def start(self):
        for x in range(self.rangeX, self.rangeY):
            self.pn = x
            self.save()
            time.sleep(0.3)
        print('It\'s OK!')

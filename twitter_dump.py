# -*- coding:UTF-8 -*-
#######################################################################
###this program is used to dump twitter data into postgresql database##
##raw twitter data will be stored as .json type in postgis database####
##version: 1.0.0 updated in 10/24/2015#################################
#######################################################################
__author__ = 'Guixng(Fred) Wei email:g_w38@txstate.edu'

import json
import re
import glob
import psycopg2
import copy
import os
from psycopg2.extensions import register_adapter
from psycopg2.extras import Json



class twitter_dump():
    def __init__(self):
        self.dir = r"D:/Data/twitter/*.json"
        self.host = r'localhost'
        self.database = 'twitter'
        self.user = 'postgres'
        self.password = 'pgsql2015'# unix passwd?
        self.data_dict={}
        self.list_twitts=[]
        register_adapter(dict,Json)


    def conn(self):
        self.conn= None
        try:
            self.conn = psycopg2.connect(host=self.host,database=self.database,user=self.user,password=self.password)
            self.cur = self.conn.cursor()
        except Exception as err:
            print "---------->error happened at opening twitter database"
            print err.message

    def del_conn(self):
        if self.conn:
            self.conn.close()
            print "-------->the database is closed now"

    def create_tables(self):
        try:
            self.cur.execute('CREATE SCHEMA IF NOT EXISTS twitts')
            self.cur.execute("CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public")
            self.conn.commit()
            #firstly create table-> test.users
            self.cur.execute('CREATE TABLE IF NOT EXISTS twitts.users('
                             'uid character varying(30) NOT NULL,'
                             'seri bigserial NOT NULL,'
                             'raw_data json,'
                             'CONSTRAINT pri_uid PRIMARY KEY (uid)) '
                             'WITH (OIDS=TRUE)'
                             )
            #and then create table->test.statuses
            self.cur.execute('CREATE TABLE IF NOT EXISTS twitts.statuses('
                             'seri bigserial NOT NULL,'
                             'tid character varying(30) NOT NULL,'
                             'uid character varying(30) NOT NULL,'
                             'raw_data json,'
                             'CONSTRAINT pri_tid PRIMARY KEY (tid),'
                             'CONSTRAINT foreign_uid FOREIGN KEY (uid)'
                             'REFERENCES twitts.users (uid) MATCH SIMPLE '
                             'ON UPDATE NO ACTION ON DELETE NO ACTION)'
                             'WITH (OIDS=TRUE)'
                             )
            self.conn.commit()
        except Exception as err:
            print "---> error happened during table users&statuses creation!!"
            print err.message

    def insert_table(self):
        try:
            self.cur.executemany('INSERT INTO twitts.users(uid,raw_data) '
                                 'SELECT %(uid)s, %(user_raw)s WHERE NOT EXISTS '
                                '(SELECT * FROM twitts.users as tem WHERE '
                                 'tem.uid = %(uid)s)',self.list_twitts)

            self.conn.commit()
            print 'inserted users data successfully for {0}'.format(self.cur_file)
            self.cur.executemany('INSERT INTO twitts.statuses(tid,uid,raw_data) '
                                 'SELECT %(tid)s,%(uid)s,%(twitt_raw)s WHERE NOT EXISTS '
                                '(SELECT * FROM twitts.statuses as tem WHERE '
                                 'tem.tid = %(tid)s)',self.list_twitts)
            self.conn.commit()
            print 'inserted statuses data successfully for {0}'.format(self.cur_file)
        except Exception as err:
            print "--------> error happened during data  insertation!"
            print err.message


    def read_file(self):
        files_list = glob.glob(self.dir)
        for fl in files_list:
            self.cur_file = fl
            print ("---->the current processed file is: "+fl)
            with open(fl,'r') as f:
                json_str = f.read().splitlines()
                num = json_str.__len__()
                #print ('the # of total rows is {0}'.format(num))
                for i in range(0,num,1):
                    twitt_raw = json_str[i]
                    twitt_json = json.loads(twitt_raw)
                    #print ('the {0} row has no problem'.format(i))
                    self.data_dict['tid'] = str(twitt_json['id_str'])
                    self.data_dict['uid'] = str(twitt_json['user']['id_str'])
                    self.data_dict['user_raw'] = twitt_json['user']
                    self.data_dict['twitt_raw'] = twitt_json
                    self.list_twitts.append(copy.deepcopy(self.data_dict))
                self.insert_table()
                del json_str[:]
                self.data_dict.clear()
                del self.list_twitts[:]





if __name__ == '__main__':
    t_obj = twitter_dump()
    t_obj.conn()
    t_obj.create_tables()
    t_obj.read_file()
    t_obj.del_conn()




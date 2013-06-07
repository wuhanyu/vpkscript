#! /usr/bin/env python
#coding=gbk
import urllib,urllib2  
import cookielib
import base64
import re
import json
import gl
from hashlib import md5
# import MySQLdb
import time, logging

import pymongo, random

settings = dict()

settings['MONGODB_SERVER']     = '192.168.12.254'
settings['MONGODB_PORT']       = 27017
settings['MONGODB_DB']         = 'vpk-development'
settings['MONGODB_USER_COLLECTION'] = 'users'
settings['MONGODB_LOG_COLLECTION'] = 'log'
settings['MONGODB_REC_COLLECTION'] = 'recs'


connection = pymongo.Connection(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
db = connection[settings['MONGODB_DB']]
collection_log = db[settings['MONGODB_LOG_COLLECTION']]
collection_rec = db[settings['MONGODB_REC_COLLECTION']]
collection_rec.create_index('rid', unique=True)
collection_usr = db[settings['MONGODB_USER_COLLECTION']]

prefix = "https://mp.weixin.qq.com"
download_url = "/cgi-bin/downloadfile?source="
cj = cookielib.LWPCookieJar()
cookie_support = urllib2.HTTPCookieProcessor(cj)
opener = urllib2.build_opener(cookie_support, urllib2.HTTPHandler)
opener.addheaders = [('User-agent', 'Opera/9.23')]
urllib2.install_opener(opener)
postdata = {
            "imgcode":"",
            "f":"json",
             "register" : 0
            }

def login():
    username = gl.username
    password = gl.password
    url = 'http://mp.weixin.qq.com/cgi-bin/login'
    wx_user = username
    m = md5(password[0:16])
    m.digest()
    wx_pwd = m.hexdigest()
    print wx_pwd
    global postdata
    postdata['username'] = wx_user
    postdata['pwd'] = wx_pwd
    postdata = urllib.urlencode(postdata)
    req  = urllib2.Request(
        url = url,
        data = postdata
    ) 
    response = urllib2.urlopen(req)
    text = response.read()
    # print text
    msg = json.loads(text)
    print msg["ErrMsg"]
    token = msg["ErrMsg"].split("&token=")[-1]
    response.close()
    return token

def saveSoundByUrl(url, mid):
    response = urllib2.urlopen(url)
    fout = open( "C:\\AppServ\\www\\sound\\ori\\" + mid + ".mp3", 'wb')
    fout.write(response.read())
    fout.close()

def saveLogoByUrl(url, fake_id):
    # print url
    response = urllib2.urlopen(url)
    fout = open( "C:\\AppServ\\www\\img\\" + fake_id + ".jpg", 'wb')
    fout.write(response.read())
    fout.close()    

def process_user( fake_id, nick_name, token ):
    # print fake_id, collection_usr.find( {"uid":fake_id} ).count()
    if collection_usr.find( {"uid":fake_id} ).count() != 0:
        return

    # save logo
    url = "https://mp.weixin.qq.com/cgi-bin/getheadimg?token=" + token + "&fakeid=" + fake_id 
    saveLogoByUrl(url, fake_id)
        
    new_item = dict()
        
    new_item['uid'] = fake_id
        
    new_item['name'] = nick_name
    
    new_item['avatar_url'] = "http://124.16.139.178:8080/img/" + fake_id + ".jpg"
        
    rating_item = dict()
    new_item['ratings'] = rating_item
        
    new_item['overall_rating'] = 0
        
    wins_item = dict()
    new_item['wins'] = wins_item
        
    matches_item = dict()
    new_item['matches'] = matches_item
        
    recs_item = dict()
    new_item['recs'] = recs_item
        
    collection_usr.update( {'uid': new_item['uid']}, dict(new_item), upsert=True)    


def insert_rec( msg_id, fake_id, category ):

    item = dict()
    item['rid'] = msg_id
    item['uid'] = fake_id
    
    item['rec_url'] = "http://124.16.139.178:8080/sound/ori/" + msg_id + ".mp3"
    
    item['category'] = str( category )
    
    collection_rec.update( {'rid': item['rid']}, dict(item), upsert=True)    

def update_usr( msg_id, fake_id, category ):    
    item = dict( collection_usr.find( {"uid":fake_id} )[0] )
    recs_dict = item['recs']
    if recs_dict.has_key( str( category ) ):
        t_dict = recs_dict[ str( category ) ]
        t_dict[ msg_id ] = True
        recs_dict[ str( category ) ] = t_dict
    else:
        recs_dict[ str( category ) ] = dict()
        t_dict = recs_dict[ str( category ) ]
        t_dict[ msg_id ] = True
        recs_dict[ str( category ) ] = t_dict        
    
    item['recs'] = recs_dict
    
    collection_usr.update( {'uid': item['uid']}, dict(item), upsert=True)   
    
    
def refreshAndCheck(token):

    if collection_log.count() == 0:
        last_checked_mid = 0
    else:
        last_checked_mid = dict( collection_log.find()[0] )['last_checked_mid']

    offset = 0
    is_first = True
    new_checked_mid = 0
    dict_ready4save = dict()
    while True:
        url = "https://mp.weixin.qq.com/cgi-bin/getmessage?t=wxm-message&token=" + str(token) + "&lang=zh_CN&count=50&offset=" + str(offset)
        response = urllib2.urlopen(url)
        page = response.read()
        text = page.split('<script type="json" id="json-msgList">')[-1].split('</script>')[0]
        msgs = json.loads(text)
        
        
        # finish 1
        if len( msgs ) == 0:
            return new_checked_mid
        for msg in msgs:
            if is_first:
                is_first = False
                new_checked_mid = int( msg['id'] )
            
            # finish 2
            if int( msg['id'] ) <= last_checked_mid:
                return new_checked_mid
            
            print "Processing:", msg['fakeId'], msg['nickName'], msg['content'], msg['id'], msg['playLength']
            
            process_user( msg['fakeId'], msg['nickName'], token )
            
            if msg['content'] == u"保存":
                dict_ready4save[ msg['fakeId'] ] = True
                
            playLength = int(msg['playLength'])
            mid = msg['id']
            nickname = msg['nickName']
            if playLength > 0 and dict_ready4save.has_key( msg['fakeId'] ):
                del dict_ready4save[ msg['fakeId'] ]
                url = prefix + download_url + "&msgid=" + mid + "&token=" + token
                saveSoundByUrl(url, mid)
                
                # let category = 1
                category = 1
                insert_rec( msg['id'], msg['fakeId'], category )
                update_usr( msg['id'], msg['fakeId'], category )
         
        offset += 50
        response.close()

if __name__ == '__main__':
    token = login()
    new_checked_mid = refreshAndCheck(token)
    
    # write last checked mid
    item = dict()
    item['last_checked_mid'] = new_checked_mid
    collection_log.remove()
    collection_log.insert(dict(item))
    
    logging.basicConfig(filename = 'batch_log.txt', level = logging.INFO, filemode = 'w', format = '%(asctime)s - %(levelname)s: %(message)s')  
     
    logging.info('check_messages.py last checked msg_id -> ' + str( new_checked_mid ) )    
    print 'check_messages.py last checked msg_id -> ' + str( new_checked_mid )
    
    # a = raw_input()
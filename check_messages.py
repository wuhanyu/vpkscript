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

import pymongo, random, time

# Re process messages
PRE_RE_PROCESS = 100

settings = dict()

settings['MONGODB_SERVER']     = '42.121.98.220'
settings['MONGODB_PORT']       = 22222
settings['MONGODB_DB']         = 'vpk-development'
settings['MONGODB_USER_COLLECTION'] = 'users'
settings['MONGODB_LOG_COLLECTION'] = 'log'
settings['MONGODB_LOGS_COLLECTION'] = 'logs'
settings['MONGODB_REC_COLLECTION'] = 'recs'


connection = pymongo.Connection(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
db = connection[settings['MONGODB_DB']]
collection_log = db[settings['MONGODB_LOG_COLLECTION']]
collection_rec = db[settings['MONGODB_REC_COLLECTION']]
collection_rec.create_index('rid', unique=True)
collection_usr = db[settings['MONGODB_USER_COLLECTION']]

collection_logs = db[settings['MONGODB_LOGS_COLLECTION']]

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
    fout = open( "C:\\Program Files\\Apache Software Foundation\\Apache2.2\\htdocs\\sound\\ori\\" + mid + ".mp3", 'wb')
    fout.write(response.read())
    fout.close()

def saveLogoByUrl(url, fake_id):
    # print url
    response = urllib2.urlopen(url)
    data = response.read()
    # print fake_id, "[ResponseSize]", len( data )
    if len( data ) == 0:
        return False
    fout = open( "C:\\Program Files\\Apache Software Foundation\\Apache2.2\\htdocs\\img\\" + fake_id + ".jpg", 'wb')        
    fout.write( data )
    fout.close()
    
    return True
    
    # time.sleep(1)
    
    
    
def process_user( fake_id, real_name, token, given_name, gender ):
    # print fake_id, collection_usr.find( {"uid":fake_id} ).count()
    if collection_usr.find( {"uid":fake_id} ).count() != 0:
        print "Register: Sarred ERROR!!!"
        return

    if collection_usr.find( {"name":given_name} ).count() != 1:
        print "Register: Name ERROR"
        return
    else:
        new_item = collection_usr.find( {"name":given_name} )[0]
        
    # save logo
    url = "https://mp.weixin.qq.com/cgi-bin/getheadimg?token=" + token + "&fakeid=" + fake_id 
    saveLogoByUrl(url, fake_id)
    # saveLogoByUrl(url, "real_" + fake_id)
        
    new_item['uid'] = fake_id
        
    new_item['real_name'] = real_name
    
    new_item['avatar_url'] = "http://124.16.139.178:8080/img/" + fake_id + ".jpg"
    # new_item['real_avatar_url'] = "http://124.16.139.178:8080/img/" + fake_id + ".jpg"
        
    rating_item = dict()
    new_item['ratings'] = rating_item
        
    new_item['overall_rating'] = 0
        
    wins_item = dict()
    new_item['wins'] = wins_item
        
    matches_item = dict()
    new_item['matches'] = matches_item
        
    recs_item = dict()
    new_item['recs'] = recs_item
        
    collection_usr.update( {'name': new_item['name']}, dict(new_item), upsert=True)    


def insert_rec( msg_id, fake_id, category, date_time ):

    # update category
    if collection_rec.find( {'rid': msg_id} ).count() == 1:
        print "[Update Category] msg_id=", msg_id
        item = collection_rec.find( {'rid': msg_id} )[0]
        item['category'] = str( category )
        collection_rec.update( {'rid': item['rid']}, dict(item), upsert=True) 
        return
    
    # new rec
    item = dict()
    item['rid'] = msg_id
    item['uid'] = fake_id
    
    item['created_at'] = int( date_time )
    
    item['rec_url'] = "http://124.16.139.178:8080/sound/ori/" + msg_id + ".mp3"
    
    item['category'] = str( category )
    
    # find mediaId
    try:
    # 1. get open_id
        open_id = collection_usr.find({"uid":item['uid']})[0]['openid']
        
        print collection_logs.find({'fromUser':open_id, 'CreateTime':date_time}).count()
        # a = raw_input()
        # +-1
        if collection_logs.find({'fromUser':open_id, 'CreateTime':date_time}).count() == 1: 
            MediaId = collection_logs.find({'fromUser':open_id, 'CreateTime':date_time})[0]['MediaId']
        elif collection_logs.find({'fromUser':open_id, 'CreateTime' : str( int( date_time ) + 1 ) }).count() == 1: 
            MediaId = collection_logs.find({'fromUser':open_id, 'CreateTime' : str( int( date_time ) + 1 ) })[0]['MediaId']
        else:
            MediaId = collection_logs.find({'fromUser':open_id, 'CreateTime' : str( int( date_time ) - 1 ) })[0]['MediaId']
        
        item['MediaId'] = MediaId
        
        print "For MediaID, OpenId = ", open_id, "MediaId=", MediaId
        
    except Exception,e:
        print e
    
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

# update user avatar
def update_avatar( fake_id, token, msg_id ):
    print "update_avatar: ", fake_id, token, msg_id
    if collection_usr.find( {"uid":fake_id} ).count() != 1:
        print "ERROR: Update avatar without a uid!"
        return
    
    try:
        user_item = collection_usr.find( {"uid":fake_id} )[0]
        # save avatar
        
        # samll
        url = "https://mp.weixin.qq.com/cgi-bin/getimgdata?token=" + token + "&msgid=" + msg_id + "&mode=small&source=&fileId=0"
        success_small = saveLogoByUrl(url, fake_id + "_S")
        # large
        url = "https://mp.weixin.qq.com/cgi-bin/getimgdata?token=" + token + "&msgid=" + msg_id + "&mode=large&source=&fileId=0"
        success_large = saveLogoByUrl(url, fake_id + "_L")
        
        if success_small and success_large:
            user_item['avatar_url'] = "http://124.16.139.178:8080/img/" + fake_id + "_S" + ".jpg"    
            user_item['avatar_url_L'] = "http://124.16.139.178:8080/img/" + fake_id + "_L" + ".jpg"
            collection_usr.update( {'uid': user_item['uid']}, dict(user_item), upsert=True) 
        else:
            print "update_avatar request failure, fake_id=", fake_id, "msg_id=", msg_id
        
    except Exception, e:
        print e
    
def refreshAndCheck(token):

    if collection_log.count() == 0:
        last_checked_mid = 0
    else:
        last_checked_mid = dict( collection_log.find()[0] )['last_checked_mid']

    offset = 0
    is_first = True
    new_checked_mid = 0
    dict_ready4save = dict()
    dict_ready4category = dict()
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
            if int( msg['id'] ) <= last_checked_mid - PRE_RE_PROCESS:
                return new_checked_mid
            
            # print "Processing:", msg['fakeId'], msg['nickName'], msg['content'], msg['id'], msg['playLength'], msg['starred'], msg['type']
            
            if int( msg['id'] ) > last_checked_mid and msg['starred'] == "1" and msg['content'] != "" and ( msg['content'].split("&nbsp;")[-1] == (u"男") or msg['content'].split("&nbsp;")[-1] == (u"女") ):
                process_user( msg['fakeId'], msg['nickName'], token, msg['content'].split("&nbsp;")[0], msg['content'].split("&nbsp;")[-1] )
            
            # update avatar
            if int( msg['id'] ) > last_checked_mid and msg['content'] == "" and msg['type'] == "2":
                update_avatar( msg['fakeId'], token, msg['id'] )          
            
            # should be in new messages
            if int( msg['id'] ) > last_checked_mid and msg['content'] == u"保存" and collection_usr.find({"uid":msg['fakeId']}).count() == 1:
                dict_ready4save[ msg['fakeId'] ] = True
            
            # if msg['content'].startswith(u"修改昵称"):
            #     process_user( msg['fakeId'], msg['nickName'], token, update_nick_name = msg['content'].split(" ")[1].strip() )
                
            playLength = int(msg['playLength'])
            mid = msg['id']
            nickname = msg['nickName']
            if playLength > 0 and dict_ready4save.has_key( msg['fakeId'] ):
                del dict_ready4save[ msg['fakeId'] ]
                
                dict_ready4category[ msg['fakeId'] ] = msg['id']
                
                url = prefix + download_url + "&msgid=" + mid + "&token=" + token
                saveSoundByUrl(url, mid)
                
                # let category = 1
                category = 1
                insert_rec( msg['id'], msg['fakeId'], category, msg['dateTime'] )
                update_usr( msg['id'], msg['fakeId'], category )
                
            # update category
            if msg['content'] == u"台词" and dict_ready4category.has_key( msg['fakeId'] ):
                insert_rec( dict_ready4category[ msg['fakeId'] ], msg['fakeId'], 2, msg['dateTime'] )
                del dict_ready4category[ msg['fakeId'] ]
            if ( msg['content'] == u"清唱" or msg['content'] == u"唱给你听" ) and dict_ready4category.has_key( msg['fakeId'] ):
                insert_rec( dict_ready4category[ msg['fakeId'] ], msg['fakeId'], 3, msg['dateTime'] )
                del dict_ready4category[ msg['fakeId'] ]
            if ( msg['content'] == u"三行情书" ) and dict_ready4category.has_key( msg['fakeId'] ):
                insert_rec( dict_ready4category[ msg['fakeId'] ], msg['fakeId'], 5, msg['dateTime'] )
                del dict_ready4category[ msg['fakeId'] ]
            if ( msg['content'] == u"梦想" ) and dict_ready4category.has_key( msg['fakeId'] ):
                insert_rec( dict_ready4category[ msg['fakeId'] ], msg['fakeId'], 6, msg['dateTime'] )
                del dict_ready4category[ msg['fakeId'] ]                
            if ( msg['content'] == u"正毕业" ) and dict_ready4category.has_key( msg['fakeId'] ):
                insert_rec( dict_ready4category[ msg['fakeId'] ], msg['fakeId'], 7, msg['dateTime'] )
                del dict_ready4category[ msg['fakeId'] ]                  
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
#! /usr/bin/env python
#coding=utf8
import os
import pymongo
import random
import time

settings = dict()

settings['MONGODB_SERVER']     = '42.121.98.220'
settings['MONGODB_PORT']       = 22222
settings['MONGODB_DB']         = 'vpk-development'
settings['MONGODB_RATE_COLLECTION'] = 'rates'
settings['MONGODB_REC_COLLECTION'] = 'recs'
settings['MONGODB_USER_COLLECTION'] = 'users'
prefix = "C:/\"Program Files\"/\"Apache Software Foundation\"/Apache2.2/htdocs/sound/"
prefix_ori = prefix + "ori/"
prefix_pair = prefix + "pair/"
prefix_url = "http://124.16.139.178:8080/sound/"

def mergemp3(filepath1, filepath2, outputpath):
    cmd = "sox.exe A.mp3 %s B.mp3 %s %s" % (filepath1, filepath2, outputpath)
    print cmd
    os.system(cmd)

def getRecord(user, collection):
    tmplist = []
    recs_dict = user["recs"]
    for key in recs_dict.keys():
        tmplist += recs_dict[ key ].keys()
    
    print user["uid"], len(tmplist)
    if (len(tmplist) == 0): return None
    rid = random.choice(tmplist)
    record = collection.find_one({"rid":rid})
    return record

def getFilepath(records):
    result = []
    for record in records:
        result.append(prefix_ori + record["rid"] + ".mp3")
    output_path = "%s%s_%s.mp3" % (prefix_pair, records[0]["rid"], records[1]["rid"])
    return result, output_path

def getUrl(path):
    return prefix_url + path.split("sound/")[-1].replace("\\", "/")
        
if __name__ == '__main__':
    flag = True
    tmpuser = None
    while (True):
        connection = pymongo.Connection(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
        db = connection[settings['MONGODB_DB']]
        rat_collection = db[settings['MONGODB_RATE_COLLECTION']]
        rec_collection = db[settings['MONGODB_REC_COLLECTION']]
        user_collection = db[settings['MONGODB_USER_COLLECTION']]
        rat_collection.create_index("pairstr")
        if (rat_collection.find({"rated_count":0}).count() > 100):
            time.sleep(60)
            continue
        for user in user_collection.find().sort( "priority", pymongo.ASCENDING ):
            user = dict(user)
            if not user.has_key("priority"): continue
            if flag:
                tmpuser = user
                flag = False
                continue
            else:
                flag = True
                records = []
                records.append(getRecord(tmpuser, rec_collection))
                records.append(getRecord(user, rec_collection))
                if (records[0] == None or records[1] == None): continue
                inputs, output = getFilepath(records)
                mergemp3(inputs[0], inputs[1], output)
                
                url = getUrl(output)
                print url
                rate = {}
                rate["sound_url"] = url
                rate["rid_a"] = records[0]["rid"]
                rate["rid_b"] = records[1]["rid"]
                rate["uid_a"] = records[0]["uid"]
                rate["uid_b"] = records[1]["uid"]
                rate["category"] = records[0]["category"]
                rate["wincount_a"] = 0
                rate["wincount_b"] = 0
                rate["text"] = "A:%s vs B:%s" % (tmpuser["name"], user["name"])
                rate["rated_count"] = 0
                rat_collection.save(rate)
                
            
        time.sleep(60)


import pymongo, random
import logging

settings = dict()

settings['MONGODB_SERVER']     = '192.168.12.254'
settings['MONGODB_PORT']       = 27017
settings['MONGODB_DB']         = 'vpk-development'

if __name__ == "__main__":

    connection = pymongo.Connection(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
    db = connection[settings['MONGODB_DB']]
    collection_rec = db['recs']
    collection_usr = db['users']
    collection_rat = db['rates']
    
    uid = ""
    category = ""
    # delete rec
    for rec in collection_rec.find({'deleted':True}):
        rec_dict = dict( rec )
        if rec_dict.has_key('deleted'):
            uid = rec_dict['uid']
            category = rec_dict['category']
            rid = rec_dict['rid']
            collection_rec.remove( {'rid':rec_dict['rid']} )
            
            # update user rec field
            try:
                user_item = dict( collection_usr.find( {'uid':uid} )[0] )
                del user_item['recs'][ category ][ rid ]
                collection_usr.update( {'uid': user_item['uid']}, dict(user_item), upsert=True)
            except Exception,e:
                print e
            
            # update rates collection
            for rate_item in collection_rat.find( { 'rid_a':rid } ):
                collection_rat.remove( {'_id':rate_item['_id']} )
            for rate_item in collection_rat.find( { 'rid_b':rid } ):
                collection_rat.remove( {'_id':rate_item['_id']} )            
                
    # a = raw_input()
            
            
            
            
    
    
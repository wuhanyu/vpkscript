import pymongo, random
import logging

MAX_INCREASE = 32
INIT_RATING = 1500

settings = dict()

settings['MONGODB_SERVER']     = '42.121.98.220'
settings['MONGODB_PORT']       = 22222
settings['MONGODB_DB']         = 'vpk-development'
settings['MONGODB_COLLECTION'] = 'newmatches'
settings['MONGODB_OLD_COLLECTION'] = 'oldmatches'
settings['MONGODB_USER_COLLECTION'] = 'users'
settings['MONGODB_UNIQ_KEY']   = 'mid'
# settings['MONGODB_UNIQ_KEY_USER']   = 'uid'


#===========================================

class NewMatchesPoper():
    def __init__(self):
        connection = pymongo.Connection(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
        self.db = connection[settings['MONGODB_DB']]
        self.collection = self.db[settings['MONGODB_COLLECTION']]
        
        if self.__get_uniq_key() is not None:
            self.collection.create_index(self.__get_uniq_key(), unique=True)

    def __get_uniq_key(self):
        if not settings['MONGODB_UNIQ_KEY'] or settings['MONGODB_UNIQ_KEY'] == "":
            return None
        return settings['MONGODB_UNIQ_KEY']

class OldMatchesWriter():
    def __init__(self):
        connection = pymongo.Connection(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
        self.db = connection[settings['MONGODB_DB']]
        self.collection = self.db[settings['MONGODB_OLD_COLLECTION']]
        
        if self.__get_uniq_key() is not None:
            self.collection.create_index(self.__get_uniq_key(), unique=True)

    def process_item(self, item):
        if self.__get_uniq_key() is None:
            self.collection.insert(dict(item))
        else:
            self.collection.update(
                            {self.__get_uniq_key(): item[self.__get_uniq_key()]},
                            dict(item),
                            upsert=True)
        return item

    def __get_uniq_key(self):
        if not settings['MONGODB_UNIQ_KEY'] or settings['MONGODB_UNIQ_KEY'] == "":
            return None
        return settings['MONGODB_UNIQ_KEY']        

class EloRatingUpdater():
    def __init__(self, match):
        self.uid_a = match['uid_a']
        self.uid_b = match['uid_b']
        self.category = match['category']
        self.result = match['result']

        connection = pymongo.Connection(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
        self.db = connection[settings['MONGODB_DB']]
        self.collection = self.db[settings['MONGODB_USER_COLLECTION']]
        
        #if self.__get_uniq_key() is not None:
        #    self.collection.create_index(self.__get_uniq_key(), unique=True)        
 
    def inv(self, res):
        if res == 1:
            return -1
        if res == 0:
            return 0
        if res == -1:
            return 1

    def rating_change(self, ratA, ratB, res, k = MAX_INCREASE):
        WinProbA = 1.0/((10.0**((ratB-ratA)/400.0)) +1.0)
        RatingDiffA = round(k*( (res + 1.0)/2.0 - WinProbA))
        #print(WinProbA, RatingDiffA)
        return RatingDiffA
            
    def update_elo_rating(self):
        dict_player_a = self.collection.find( {"uid":self.uid_a} )[0]
        dict_player_b = self.collection.find( {"uid":self.uid_b} )[0]
        
        print dict_player_a['uid'], dict_player_b['uid']
        
        dict_ratings_a = dict( dict_player_a['ratings'] )
        dict_ratings_b = dict( dict_player_b['ratings'] )
        
        # print type( dict_ratings_a ), type( dict_ratings_b )
        
        if dict_ratings_a.has_key( self.category ):
            self.rating_a = dict_ratings_a[ self.category ]
        else:
            self.rating_a = INIT_RATING
            dict_ratings_a[ self.category ] = INIT_RATING

        if dict_ratings_b.has_key( self.category ):
            self.rating_b = dict_ratings_b[ self.category ]
        else:
            self.rating_b = INIT_RATING
            dict_ratings_b[ self.category ] = INIT_RATING

        
        dict_wins_a = dict(  dict_player_a['wins'] )
        dict_wins_b = dict( dict_player_b['wins'] )
        
        if self.result == 1:
            if dict_wins_a.has_key( self.category ):
                dict_wins_a[ self.category ] += 1
            else:
                dict_wins_a[ self.category ] = 1
        elif self.result == -1:
            if dict_wins_b.has_key( self.category ):
                dict_wins_b[ self.category ] += 1
            else:
                dict_wins_b[ self.category ] = 1
        else:
            pass
        dict_player_a['wins'] = dict_wins_a
        dict_player_b['wins'] = dict_wins_b

        
        dict_matches_a = dict ( dict_player_a['matches'] )
        dict_matches_b = dict( dict_player_b['matches'] )
        
        print type( dict_matches_a ), type( dict_matches_b )
        
        if dict_matches_a.has_key( self.category ):
            dict_matches_a[ self.category ] += 1
        else:
            dict_matches_a[ self.category ] = 1
        if dict_matches_b.has_key( self.category ):
            dict_matches_b[ self.category ] += 1
        else:
            dict_matches_b[ self.category ] = 1
        dict_player_a['matches'] = dict_matches_a
        dict_player_b['matches'] = dict_matches_b
            
        dict_ratings_a[ self.category ] += int( self.rating_change( self.rating_a, self.rating_b, self.result ) )
        dict_ratings_b[ self.category ] += int( self.rating_change( self.rating_b, self.rating_a, self.inv( self.result ) ) )
        
        dict_player_a['ratings'] = dict_ratings_a
        dict_player_b['ratings'] = dict_ratings_b

        # cal overall rating
        sumA = 0
        sumB = 0.0000000001
        for key in dict_ratings_a.keys():
            sumA += dict_ratings_a[ key ] * dict_matches_a[ key ]
            sumB += dict_matches_a[ key ]
        dict_player_a['overall_rating'] = int( round( sumA / sumB ) )
        
        sumA = 0
        sumB = 0.0000000001
        for key in dict_ratings_b.keys():
            sumA += dict_ratings_b[ key ] * dict_matches_b[ key ]
            sumB += dict_matches_b[ key ]
        dict_player_b['overall_rating'] = int( round( sumA / sumB ) )
        
        self.process_item(dict_player_a)
        self.process_item(dict_player_b)
        # print dict_player_a['overall_rating'], dict_player_b['overall_rating']
        
        # a= raw_input()
 
    def process_item(self, item):
            #print type( item['uid'] ), self.__get_uniq_key()
            #print item
            #a = raw_input()
            self.collection.update( {'uid': item['uid']}, dict(item), upsert=True)
 
 
if __name__ == "__main__":
    
    new_matches_poper = NewMatchesPoper()
    
    old_matches_writer = OldMatchesWriter()
    
    processed_items = 0

    for match in new_matches_poper.collection.find():
    
        print "[update_rating] Processing new_match - ", match['mid']
    
        old_matches_writer.process_item( match )
        
        # update elo rating
        elo_rating_updater = EloRatingUpdater( match )
        elo_rating_updater.update_elo_rating()
        
        processed_items += 1
    
    # clean new_matches
    new_matches_poper.collection.remove()
    
    # calculate rank
    connection = pymongo.Connection(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
    db = connection[settings['MONGODB_DB']]
    collection = db[settings['MONGODB_USER_COLLECTION']]
    
    # set rank    
    rank = 1
    for user in collection.find().sort( "overall_rating", pymongo.DESCENDING ):
        user['rank'] = rank
        rank +=1
        collection.update( {'_id': user['_id']}, dict(user), upsert=True)     

    
    # set priority, no priority field if the user doesn't have a rec
    priority4elite = 1
    priority4new = 0
    for user in collection.find().sort( "overall_rating", pymongo.DESCENDING ):
        # for new
        user = dict(user)
        recs_count = 0
        if user.has_key('recs'):
            recs_dict = dict( user['recs'] )
            for key in recs_dict.keys():
                recs_count += len( recs_dict[ key ].keys() )
        
        # print user['name'], recs_count
        # without a priority field
        if recs_count == 0:
            if user.has_key( 'priority' ):
                del user['priority']
                collection.update( {'_id': user['_id']}, dict(user), upsert=True)  
            continue
            
        # for new
        if user['overall_rating'] == 0:
            user['priority'] = priority4new
            priority4new -= 1
        else:
            user['priority'] = priority4elite
            priority4elite +=1
        collection.update( {'_id': user['_id']}, dict(user), upsert=True)        
        
    logging.basicConfig(filename = 'batch_log.txt', level = logging.INFO, filemode = 'w', format = '%(asctime)s - %(levelname)s: %(message)s')  
     
    logging.info( 'updating_rating.py processed -> ' + str( processed_items ) + " items" )
    print 'updating_rating.py processed -> ' + str( processed_items ) + " items" 
    
    # a = raw_input()
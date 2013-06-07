import os, time

SLEEP_INTERVAL = 60

while True:
    os.system( 'python check_messages.py' )
    os.system( 'python delete_recs.py' )   
    
    os.system( 'python update_rating.py' )        
    time.sleep( SLEEP_INTERVAL )

    os.system( 'python update_rating.py' )        
    time.sleep( SLEEP_INTERVAL )
    
    os.system( 'python update_rating.py' )        
    time.sleep( SLEEP_INTERVAL )

    os.system( 'python update_rating.py' )        
    time.sleep( SLEEP_INTERVAL )

    os.system( 'python update_rating.py' )        
    time.sleep( SLEEP_INTERVAL )    
"""
Use Case: Faceted search.
"""
import redis
import os
import hashlib
import json
import keynamehelper
import logging

class facet_search(object):
    def __init__(self, lookup_attrs):
        self.lookup_attrs = lookup_attrs

    def put(self, entity_id, entity, redisdb):
        for attr in self.lookup_attrs:
            if hasattr(entity, attr):
                fs_key = keynamehelper.create_key_name('fs',
                                      attr,
                                      str(getattr(entity, attr)))
                redisdb.sadd(fs_key, entity_id)

    def search(self, redisdb, *keys):
        facets = []
        for k, v in keys:
            fs_key = keynamehelper.create_key_name('fs',
                                   k, str(v))
            facets.append(fs_key)

        return redisdb.sinter(facets)

class hash_facet_search(facet_search):
    def put(self, entity_id, entity, redisdb):
        for attr in self.lookup_attrs:
            if hasattr(entity, attr):
                hfs_key = self._get_hash_key(attr, getattr(entity, attr))
                redisdb.sadd(hfs_key, entity_id)

    def _get_hash_key(self, attr, val):
        hfs = (attr, str(val))
        hashed_val = hashlib.sha256( str(hfs).encode('utf-8') ).hexdigest()
        return keynamehelper.create_key_name("hfs", hashed_val)

    def search(self, redisdb, *keys):
        facets = []
        for k, v in keys:
            hfs_key = self._get_hash_key(k, v)
            facets.append(hfs_key)

        return redisdb.sinter(facets)
            
class sp_event(object):
    def __init__(self, sku, name, venue, category, 
                       medal_event = False,
                       disabled_access = True):
        self.sku = sku
        self.name = name
        self.venue = venue
        self.category = category
        self.medal_event = medal_event
        self.disabled_access = disabled_access

    def __repr__(self):
        '''
        override repr function for the class
        '''
        return json.dumps(self.__dict__)

    def put(self, redisdb):
        '''
        Save the event to redis db as json
        '''
        key = keynamehelper.create_key_name('event', self.sku)
        redisdb.set(key, repr(self))

    @classmethod
    def load(cls, json_obj):
        try:
            return sp_event(json_obj['sku'], 
                        json_obj['name'],
                        json_obj['venue'],
                        json_obj['category'],
                        json_obj['medal_event'],
                        json_obj['disabled_access'])
        except Exception as err:
            logging.error("Failed to load sp_event from json", err)
            return None

    @classmethod
    def get(cls, sku, redisdb, is_key = False):
        '''
        Get the event from redis db
        '''
        ev = cls(None, None, None, None)
        if is_key:
            key = sku
        else:    
            key = keynamehelper.create_key_name('event', sku)

        try:
            ev.__dict__ = json.loads(redisdb.get(key))
            return ev
        except Exception as err:
            logging.error("Failed to load event %s" % sku, err)
            return None 

    @classmethod
    def search_greedy(cls, redisdb, *keys):
        '''
        Find all matching keys, retreive value and exeamine
        '''
        matches = []
        key = keynamehelper.create_key_name('event', '*')
        for k in redisdb.scan_iter(key):
            ev = cls.get(k, redisdb, True)
            match = False
            for tk, tv in keys:
                if hasattr(ev, tk) and getattr(ev, tk) == tv:
                   match = True
                else:
                   match = False
                   break

            if match:
                matches.append(ev.sku)

        return matches
        
if __name__ == '__main__':
    # Redis setup
    DB_HOST = 'localhost'
    DB_PORT = 6379
    DB_NO = 0

    redisdb = redis.StrictRedis(
                    host=DB_HOST,
                    port=DB_PORT,
                    db=DB_NO)

    events = [{'sku': "123-ABC-723",
               'name': "Men's 100m Final",
               'disabled_access': True,
               'medal_event': True,
               'venue': "Olympic Stadium",
               'category': "Track & Field"
              },
              {'sku': "737-DEF-911",
               'name': "Women's 4x100m Heats",
               'disabled_access': True,
               'medal_event': False,
               'venue': "Olympic Stadium",
               'category': "Track & Field"
              },
              {'sku': "320-GHI-921",
               'name': "Womens Judo Qualifying",
               'disabled_access': False,
               'medal_event': False,
               'venue': "Nippon Budokan",
               'category': "Martial Arts"
              }]

    lookup_attrs = ['disabled_access', 'medal_event', 'venue', 'tbd']
    facet_srch = facet_search(lookup_attrs)
    hash_facet_srch = hash_facet_search(lookup_attrs)

    for ev_json in events:
       ev =  sp_event.load(ev_json)
       ev.put(redisdb)
       facet_srch.put(ev.sku, ev, redisdb)
       hash_facet_srch.put(ev.sku, ev, redisdb)

    # get by key
    ev = sp_event.get("123-ABC-723", redisdb)
    print (ev)

    # greedy search
    print ("greedy search Nippon Budokan") 
    for ev in sp_event.search_greedy(redisdb,
                                ('disabled_access', False),
                                ('medal_event', False),
                                ('venue', "Nippon Budokan")):

        print(ev)

    # facet search
    print ("facet search Nippon Budokan")
    for ev in facet_srch.search(redisdb,
                            ('disabled_access', False),
                            ('medal_event', False),
                            ('venue', "Nippon Budokan")):
        print(ev)

    # hash facet search
    print ("hash facet search Olympic Statidum")
    for ev in hash_facet_srch.search(redisdb,
                            ('disabled_access', True),
                            ('medal_event', False),
                            ('venue', "Olympic Stadium")):
        print(ev)

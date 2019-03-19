'''
Customer model
'''
import redis
import os
import json
import keynamehelper
import logging

class customer(object):
    def __init__(self, cid, cname):
        self.id = cid
        self.name = cname

    def __repr__(self):
        return json.dumps(self.__dict__)

    @classmethod
    def load(cls, json_obj):
        return cls(json_obj['id'],
                   json_obj['customer_name'])

    def put(self, redisdb):
        key = keynamehelper.create_key_name("customer", self.id)
        redisdb.set(key, repr(self))

    @classmethod
    def get(self, redisdb, *keys):
        results = []
        for k in keys:
            key = keynamehelper.create_key_name("customer", k)
            for tk in redisdb.scan_iter(key):
                cs = redisdb.get(tk)
                if cs:
                    cs = json.loads(cs.decode('UTF-8'))
                    results.append(customer(cs['id'], cs['name']))
                
        return results   

if __name__ == '__main__':
    # Redis setup
    DB_HOST = 'localhost'
    DB_PORT = 6379
    DB_NO = 0

    redisdb = redis.StrictRedis(
                    host=DB_HOST,
                    port=DB_PORT,
                    db=DB_NO)

    customers = [{'id': "bill", 'customer_name': "bill smith"},
             {'id': "mary", 'customer_name': "mary jane"},
             {'id': "jamie", 'customer_name': "jamie north"},
             {'id': "joan", 'customer_name': 'joan west'},
             {'id': "fred", 'customer_name': "fred smith"},
             {'id': "amy", 'customer_name': 'amy south'},
             {'id': "jim", 'customer_name': 'jim somebody'}
            ]

    for c in customers:
        cobj = customer(c['id'], c['customer_name'])
        cobj.put(redisdb)

    for r in customer.get(redisdb, "*"):
        print (r)

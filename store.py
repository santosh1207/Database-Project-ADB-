from pymongo import *

class Store(object):
    """
        A lightweight wrapper around the pymongo API
    """
    def __init__(self, db_name='yelp_db', coll_name='yelp'):
        self.client = MongoClient()
        self.db = self.client[db_name]
        self.coll = self.db[coll_name]
        self.colls = {}
        
    def init_collection(self, coll_name):
        """ Creates a new collection if not already created """
        if coll_name not in self.colls:
            self.colls[coll_name] = self.db[coll_name] # New collection created
    
    def insert_one_into(self, coll_name, obj):
        if coll_name in self.colls:
            return self.colls[coll_name].insert_one(obj)
        return None
    
    def insert_many_into(self, coll_name, objs):
        if coll_name in self.colls:
            return self.colls[coll_name].insert_many(objs)
        return None

    def insert_one(self, obj):
        """ Inserts a single object into the mongodb """
        return self.coll.insert_one(obj)

    def insert_many(self, objs):
        """ Inserts many objects simultaneously into the database """
        result = self.coll.insert_many(objs)
        return result.inserted_ids

    def query_one(self, q):
        return self.coll.find_one(q)

    def map_reduce(self, mapper, reducer, result_name="results"):
        """
            Performs the map_reduce operation on the collection and returns the result
        """
        result = self.coll.map_reduce(mapper, reducer, result_name)
        return result
        # for row in result.find():
            # print row

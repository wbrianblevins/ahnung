
import sys
import json
import pymongo

class MongoUtils(object):
    """ Global MongoDB related utilities.
    """


    def __init__(self):
        """ Constructor.
        """


    def getMongoClient(self, connectURI):

        mClient = pymongo.MongoClient(connectURI)
        return mClient






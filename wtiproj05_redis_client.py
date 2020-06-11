import ast
import json
import pickle

import redis


class Queue:

    def __init__(self):
        self.r = redis.Redis(port=6381)

    def cleardb(self):
        self.r.flushall()

    def addProfile(self, data):
        userID = data['userID']
        del data['userID']
        self.r.rpush(userID, json.dumps(data))
        return "dodano"

    def getProfile(self, id):
        value = self.r.lrange(id, 0, -1)
        return value[0].decode('utf-8')

    def getAllProfiles(self):
        listOfProfiles = {}
        for key in self.r.keys():
            profile = self.getProfile(key)
            newKey = key.decode('utf-8')
            listOfProfiles[newKey] = profile
        return listOfProfiles

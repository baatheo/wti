import json

import pandas as panda

import wtiproj03_ETL
from wtiproj05_redis_client import Queue


class api:
    q = Queue()
    genres = []
    merged_table = panda.DataFrame()
    fill = []
    averageRating = []

    def addProfile(self, data):
        return self.q.addProfile(data)

    def getAllProfiles(self):
        return self.q.getAllProfiles()

    def getProfile(self, id):
        return self.q.getProfile(id)

    def addProfiles(self):
        listOfIds = []
        for index, row in self.merged_table.iterrows():
            if not row['userID'] in listOfIds:
                listOfIds.append(row['userID'])
        for id in listOfIds:
            profile = self.average_user(id)
            profile['userID'] = id
            self.q.addProfile(profile)

    def post(self, data):
        newData = {}
        for key in data:
            if key == 'rating':
                #dla ulatwienia odejmujemy odrazu w tym miejscu srednia danego typu filmu
                newData[key] = str(float(data[key]) - float(self.average()['genre-'+data['genre']]))
            else:
                newData[key] = data[key]

        self.merged_table = self.merged_table.append(newData, ignore_index=True)
        fill, self.averageRating = wtiproj03_ETL.averageRating(self.merged_table)
        return "Dodany rekord"

    def getAll(self):
        self.genres, self.merged_table = wtiproj03_ETL.lab4JoinTables()
        self.merged_table = self.merged_table.fillna(0)
        fill, self.averageRating = wtiproj03_ETL.averageRating(self.merged_table)
        data = self.merged_table
        return data

    def get(self):
        data = self.merged_table.sample(n=1)
        return data

    def delete(self):
        self.merged_table = self.merged_table.iloc[0:0]
        return self.merged

    def average_user(self, userID):
        return wtiproj03_ETL.averageRatingOfUser(userID, self.merged_table)

    def average(self):
        return self.averageRating


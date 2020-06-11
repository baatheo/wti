import wtiproj06_cassandra_client as cc
from wtiproj03_ETL import lab4JoinTables, averageRatingOfUser

KEYSPACE = "ratings_keyspace"
USER_TABLE = "user_rated_movies"
AVG_RATINGS = "avg_genre_ratings_for_user"


class ApiLogic:
    def __init__(self):
        self.cass = cc.Cass()
        self.cass.delete_table(KEYSPACE, USER_TABLE)
        self.cass.delete_table(KEYSPACE, AVG_RATINGS)
        self.cass.create_user_rated_movies_table()
        self.cass.create_average_user_rating_table()
        self.genres, self.merged_table = lab4JoinTables()
        self.addAll()

    def addAll(self):
        for index, i in self.merged_table.iterrows():
            self.cass.push_data_user_rated_table(i['userID'], i['movieID'], i['rating'], i['genre'])

        listOfIds = []
        for index, row in self.merged_table.iterrows():
            if not row['userID'] in listOfIds:
                listOfIds.append(row['userID'])
        for id in listOfIds:
            profile = averageRatingOfUser(id, self.merged_table)
            dataOfProfile = []
            for key in profile:
                if not key == 'userID':
                    dataOfProfile.append(profile[key])
            self.cass.push_data_average_rating_table(id, dataOfProfile)

    def getAllProfiles(self):
        data = self.cass.get_data_table(KEYSPACE, AVG_RATINGS)
        return data

    def getAllRatings(self):
        data = self.cass.get_data_table(KEYSPACE, USER_TABLE)
        return data

    def postRating(self, data):
        self.cass.push_data_user_rated_table(data['userID'], data['movieID'], data['rating'], data['genre'])
        return "dodano"

    def getProfile(self, id):
        data = self.cass.get_data_table_byId(KEYSPACE, AVG_RATINGS, id)
        return data

    def clearAll(self):
        self.cass.delete_table(KEYSPACE, USER_TABLE)
        self.cass.delete_table(KEYSPACE, AVG_RATINGS)
        return "wyczyszczono"
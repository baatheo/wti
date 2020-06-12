import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np


class ElasticClient:
    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)

    # ------ Simple operations ------
    def index_documents(self):
        df = pd.read_csv('/home/vagrant/PycharmProjects/wtiproj03/user_ratedmovies.dat',
                         delimiter='\t').loc[:, ['userID', 'movieID', 'rating']]
        means = df.groupby(['userID'], as_index=False, sort=False).mean().loc[:, ['userID', 'rating']].rename(
            columns={'rating': 'ratingMean'})
        df = pd.merge(df, means, on='userID', how="left", sort=False)
        df['ratingNormal'] = df['rating'] - df['ratingMean']
        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']] \
            .rename(columns={'ratingNormal': 'rating'}) \
            .pivot_table(index='userID', columns='movieID', values='rating') \
            .fillna(0)
        print("Indexing users...")
        index_users = [{
            "_index": "users",
            "_type": "user",
            "_id": index,
            "_source": {
                'ratings': row[row > 0].sort_values(ascending=False).index.values.tolist()
            }
        }
            for index, row in ratings.iterrows()]

        helpers.bulk(self.es, index_users)
        print("Done")
        print("Indexing movies...")
        index_movies = [{
            "_index": "movies",
            "_type": "movie",
            "_id": column,
            "_source": {
                "whoRated": ratings[column][ratings[column] > 0].sort_values(ascending=False).index.values.tolist()
            }
        } for column in ratings]

        helpers.bulk(self.es, index_movies)
        print("Done")

    def get_movies_liked_by_user(self, user_id, index='users'):
        user_id = int(user_id)
        return self.es.get(index=index, doc_type="user", id=user_id)["_source"]

    def get_users_that_like_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]

    def collabUserFilter(self, userID):
        q = {
            "query": {
                "match": {
                    "_id": str(userID)
                }
            }
        }

        val = self.es.search(index='users', body=q, filter_path=['hits.hits._source'])['hits']['hits']
        movieId = val[0]['_source']['ratings']
        newUsersQuery = {
            "query": {
                "bool": {
                    "must_not": {
                        "term": {
                            "_id": str(userID)
                        }
                    },
                    "filter": {
                        "terms": {"ratings": movieId}
                    }
                }
            }
        }
        val = self.es.search(index='users', body=newUsersQuery, filter_path=['hits.hits._source'], size=10000)
        all_hits = val["hits"]["hits"]

        collab_movie_ids = []
        for hit in all_hits:
            for movieid in hit['_source']['ratings']:
                collab_movie_ids.append(int(movieid))

        return list(np.unique(collab_movie_ids))

    def collabMovieFilter(self, movieId):
        query = {
            "query": {
                "match": {
                    "_id": str(movieId)
                }
            }
        }

        val = self.es.search(index='movies', body=query, filter_path=['hits.hits._source'])['hits']['hits']
        movies = val[0]['_source']['whoRated']
        newMovieQuery = {
            "query": {
                "bool": {
                    "must_not": {
                        "term": {
                            "_id": str(movieId)
                        }
                    },
                    "filter": {
                        "terms": {"whoRated": movies}
                    }
                }
            }
        }
        val = self.es.search(index='movies', body=newMovieQuery, filter_path=['hits.hits._source'], size=10000)
        all_hits = val["hits"]["hits"]
        collab_user_ids = []
        for hit in all_hits:
            for user_id in hit['_source']['whoRated']:
                collab_user_ids.append(int(user_id))

        return list(np.unique(collab_user_ids))

    def addUser(self, userId, ratings):
        if not ratings:
            content = {
                'ratings': []
            }
            self.es.index(index='users', doc_type='user', id=int(userId), body=content)
        else:
            content = {
                'ratings': ratings
            }
            self.es.index(index='users', doc_type='user', id=int(userId), body=content)
            for item in ratings:
                query = {
                    "query": {
                        "match": {
                            "_id": int(item)
                        }
                    }
                }
                val = self.es.search(index='movies', body=query, filter_path=['hits.hits._source'])
                if val == {}:
                    content = {
                        'whoRated': [int(userId)]
                    }

                    self.es.index(index='movies', doc_type='movie', id=int(item), body=content)
                else:
                    t = self.es.get(index='movies', doc_type='movie', id=item)['_source']['whoRated']
                    t.append(userId)
                    newRatings = list(np.unique(t))
                    request = {
                        'doc': {
                            'whoRated': newRatings
                        }
                    }
                    self.es.update(index='movies', doc_type='movie', id=int(item), body=request)

    def addMovie(self, movieId, userId):
        if not userId:
            content = {
                'whoRated': []
            }
            self.es.index(index='movies', doc_type='movie', id=int(movieId), body=content)
        else:
            content = {
                'whoRated': userId
            }
            self.es.index(index='movies', doc_type='movie', id=int(movieId), body=content)
            for item in userId:
                query = {
                    "query": {
                        "match": {
                            "_id": int(item)
                        }
                    }
                }
                val = self.es.search(index='users', body=query, filter_path=['hits.hits._source'])

                if val == {}:
                    content = {
                        'ratings': [int(movieId)]
                    }

                    self.es.index(index='users', doc_type='user', id=int(item), body=content)
                else:
                    t = self.es.get(index='users', doc_type='user', id=item)['_source']['ratings']
                    t.append(movieId)
                    newRatings = list(np.unique(t))
                    request = {
                        'doc': {
                            'ratings': newRatings
                        }
                    }
                    self.es.update(index='users', doc_type='user', id=int(item), body=request)

    def deleteMovie(self, movieID):
        movieID = int(movieID)
        m_doc = self.es.get(index="movies", doc_type="movie", id=movieID)["_source"]["whoRated"]
        self.delUser(movieID, m_doc)
        self.es.delete(index="movies", doc_type="movie", id=int(movieID))
        print("deleted movie")

    def upUser(self, userID, ratings):
        userID = int(userID)
        ratings = list(set(ratings))
        u_doc = self.es.get(index="users", doc_type="user", id=int(userID))
        old_u_doc = u_doc["_source"]["ratings"]

        movies_to_add = np.setdiff1d(ratings, old_u_doc)
        movies_to_remove = np.setdiff1d(old_u_doc, ratings)
        self.delMovie(userID, movies_to_remove)
        for m_add in movies_to_add:
            q = {
                "query": {
                    "match": {
                        "_id": int(m_add)
                    }
                }
            }
            s = self.es.search(index='users', body=q, filter_path=['hits.hits._source'])
            if s == {}:
                self.addMovie(movieId=m_add, userId=[userID])

            m_doc = self.es.get(index="movies", doc_type="movie", id=int(m_add))
            movies = m_doc["_source"]["whoRated"]
            movies.append(userID)
            movies = list(set(movies))
            q = {
                "doc": {
                    "whoRated": movies
                }
            }
            self.es.update(index="movies", doc_type="movie", id=int(m_add), body=q)
        b = {
            "doc": {
                "ratings": ratings
            }
        }
        self.es.update(index="users", doc_type="user", id=int(userID), body=b)

    def upMovie(self, movieID, whoRated):
        movieID = int(movieID)
        whoRated = list(set(whoRated))
        uDoc = self.es.get(index="movies", doc_type="movie", id=int(movieID))
        oldUDoc = uDoc["_source"]["whoRated"]

        usersToAdd = np.setdiff1d(whoRated, oldUDoc)
        usersToRemove = np.setdiff1d(oldUDoc, whoRated)

        self.delUser(movieID, usersToRemove)
        for user in usersToAdd:
            u_doc = self.es.get(index="users", doc_type="user", id=int(user))
            users = u_doc["_source"]["ratings"]
            users.append(movieID)
            users = list(set(users))
            q = {
                "doc": {
                    "ratings": users
                }
            }
            self.es.update(index="users", doc_type="user", id=int(user), body=q)

        b = {
            "doc": {
                "ratings": whoRated
            }
        }
        self.es.update(index="movies", doc_type="movie", id=int(movieID), body=b)

    def deleteUser(self, userID):
        userID = int(userID)
        tmp = self.get_movies_liked_by_user(userID)['ratings']
        self.delMovie(userID, tmp)
        self.es.delete(index='users', doc_type='user', id=int(userID))
        print("User deleted")

    def delUser(self, movieId, usersToDelete):
        for user in usersToDelete:
            u_doc = self.es.get(index="users", doc_type="user", id=int(user))
            users = u_doc["_source"]["ratings"]
            users.remove(movieId)
            users = list(set(users))
            q = {
                "doc": {
                    "ratings": users
                }
            }
            self.es.update(index="users", doc_type="user", id=int(user), body=q)

    def delMovie(self, userID, moviesToRemove):
        for m_rem in moviesToRemove:
            m_doc = self.es.get(index="movies", doc_type="movie", id=int(m_rem))
            movies = m_doc["_source"]["whoRated"]
            movies.remove(userID)
            movies = list(set(movies))
            q = {
                "doc": {
                    "whoRated": movies
                }
            }
            self.es.update(index="movies", doc_type="movie", id=int(m_rem), body=q)

    def bulkUserUpdate(self, data):
        for item in data:
            self.upUser(userID=item["user_id"], ratings=item["liked_movies"])

    def bulkMovieUpdate(self, data):
        for item in data:
            self.upMovie(movieID=item["movie_id"], whoRated=item["users_who_liked_movie"])


if __name__ == "__main__":
    ec = ElasticClient()
    print("Add movie 12345")
    ec.addMovie(12345, [])
    print("Who rated 12345:", ec.get_users_that_like_movie(12345))
    print("Add user 111111")
    ec.addUser(111111, [12345])
    print("Ratings for 111111:", ec.get_movies_liked_by_user(111111))
    print("Upload user")
    ec.upUser(111111, [12345])
    print("Ratings for 111111:", ec.get_movies_liked_by_user(111111))
    print("Delete movie 12345")
    ec.deleteMovie(12345)
    print("Ratings for 111111:", ec.get_movies_liked_by_user(111111))
    print("Delete user 111111")
    ec.deleteUser(111111)


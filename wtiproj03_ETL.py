import pandas as panda
import numpy as np


def lab4JoinTables():
    df = panda.read_csv('/home/vagrant/PycharmProjects/wtiproj03/user_ratedmovies.dat', sep=" ",
                        nrows=2000, delimiter="\t",
                        usecols=["userID", "movieID", "rating"])
    df1 = panda.read_csv('/home/vagrant/PycharmProjects/wtiproj03/movie_genres.dat', sep=" ",
                         nrows=2000, delimiter="\t",
                         usecols=["movieID", "genre"])

    merged_table = panda.merge(df, df1, on='movieID')
    genres = []
    for i in range(df1.shape[0]):
        genreType = df1.iloc[i]['genre']
        if 'genre-' + genreType not in genres:
            genres.append('genre-' + genreType)

    genres = sorted(genres)
    return genres, merged_table


def dataFrameToDict():
    genres, merged_table = lab4JoinTables()
    return merged_table.to_dict(orient='records')


def dictToDataFrame():
    return panda.DataFrame.from_dict(dataFrameToDict())


def averageRatingOfUser(id, merged_table):
    list1 = {}
    genres, fill = lab4JoinTables()
    for genre in genres:
        list1[genre] = [0]

    for index, i in merged_table.iterrows():
        if i['userID'] == id and list1['genre-' + i['genre']] == [0]:
            list1['genre-' + i['genre']] = []
    for index, i in merged_table.iterrows():
        if i['userID'] == id:
            list1['genre-' + i['genre']].append(i['rating'])

    listOfAverageForUser = {}
    listOfAverageForUser['userID'] = id
    for key, value in list1.items():
        tempValue = np.array(value).astype(np.float)
        listOfAverageForUser[key] = round(np.nanmean(tempValue),2)

    return listOfAverageForUser


def averageRating(merged_table):
    newDataFrame = panda.DataFrame()
    genres, fill = lab4JoinTables()
    genresWithRating = {}
    for genre in genres:
        genresWithRating[genre] = []
    for index, i in merged_table.iterrows():
        genresWithRating['genre-' + i['genre']].append(float(i['rating']))

    genresWithAverageRating = {}
    for genre in genres:
        if not genresWithRating[genre]:
            genresWithRating[genre].append(0)
        genresWithAverageRating[genre] = round(np.nanmean(genresWithRating[genre]),2)

    for index, i in merged_table.iterrows():
        newDataFrame = newDataFrame.append(i, ignore_index=True)
        newDataFrame.at[index, 'rating'] = round(float(i['rating'])-float(genresWithAverageRating['genre-'+i['genre']]),2)

    #print(genresWithAverageRating)
    #print(merged_table.iloc[0])

    return newDataFrame, genresWithAverageRating


def allTypes():
    return ['genre-Action', 'genre-Adventure', 'genre-Animation',
            'genre-Children', 'genre-Comedy', 'genre-Crime', 'genre-Documentary', 'genre-Drama',
            'genre-Fantasy', 'genre-Film-Noir', 'genre-Horror', 'genre-IMAX', 'genre-Musical',
            'genre-Mystery', 'genre-Romance', 'genre-Sci-Fi', 'genre-Short', 'genre-Thriller',
            'genre-War', 'genre-Western']


def joinTables():
    df = panda.read_csv('/home/vagrant/PycharmProjects/wtiproj03/user_ratedmovies.dat', sep=" ",
                        header=None, nrows=100, delimiter="\t",
                        names=['userID', 'movieID', 'rating', 'date_day', 'date_month', 'date_year', 'date_hour',
                               'date_minute', 'date_second'])
    df1 = panda.read_csv('/home/vagrant/PycharmProjects/wtiproj03/movie_genres.dat', sep=" ",
                         header=None, nrows=100, delimiter="\t", names=['movieID', 'genre'])

    result = panda.merge(df, df1, on='movieID', how='inner')
    my_dict = {'userID': 0, 'movieID': 0, 'rating': 0.0, 'genre-Action': 0, 'genre-Adventure': 0, 'genre-Animation': 0,
               'genre-Children': 0, 'genre-Comedy': 0, 'genre-Crime': 0, 'genre-Documentary': 0, 'genre-Drama': 0,
               'genre-Fantasy': 0, 'genre-Film-Noir': 0, 'genre-Horror': 0, 'genre-IMAX': 0, 'genre-Musical': 0,
               'genre-Mystery': 0, 'genre-Romance': 0, 'genre-Sci-Fi': 0, 'genre-Short': 0, 'genre-Thriller': 0,
               'genre-War': 0, 'genre-Western': 0}
    df2 = panda.DataFrame(columns=['userID', 'movieID', 'rating', 'genre-Action', 'genre-Adventure', 'genre-Animation',
                                   'genre-Children', 'genre-Comedy', 'genre-Crime', 'genre-Documentary', 'genre-Drama',
                                   'genre-Fantasy', 'genre-Film-Noir', 'genre-Horror', 'genre-IMAX', 'genre-Musical',
                                   'genre-Mystery', 'genre-Romance', 'genre-Sci-Fi', 'genre-Short', 'genre-Thriller',
                                   'genre-War', 'genre-Western'])
    for index, row in result.iterrows():
        if index == 0:
            lastUser = row['userID']
            lastMovie = row['movieID']
        if row['userID'] == lastUser and row['movieID'] == lastMovie:
            my_dict['userID'] = row['userID']
            my_dict['movieID'] = row['movieID']
            my_dict['rating'] = row['rating']
            genre = row['genre']
            my_dict['genre-' + genre] = 1
            lastUser = row['userID']
            lastMovie = row['movieID']
        else:
            df2 = df2.append(my_dict, ignore_index=True)
            my_dict['userID'] = row['userID']
            my_dict['movieID'] = row['movieID']
            my_dict['rating'] = row['rating']
            my_dict['genre-Action'] = 0
            my_dict['genre-Adventure'] = 0
            my_dict['genre-Animation'] = 0
            my_dict['genre-Children'] = 0
            my_dict['genre-Comedy'] = 0
            my_dict['genre-Crime'] = 0
            my_dict['genre-Documentary'] = 0
            my_dict['genre-Drama'] = 0
            my_dict['genre-Fantasy'] = 0
            my_dict['genre-Film-Noir'] = 0
            my_dict['genre-Horror'] = 0
            my_dict['genre-IMAX'] = 0
            my_dict['genre-Musical'] = 0
            my_dict['genre-Mystery'] = 0
            my_dict['genre-Romance'] = 0
            my_dict['genre-Sci-Fi'] = 0
            my_dict['genre-Short'] = 0
            my_dict['genre-Thriller'] = 0
            my_dict['genre-War'] = 0
            my_dict['genre-Western'] = 0
            genre = row['genre']
            my_dict['genre-' + genre] = 1
            lastUser = row['userID']
            lastMovie = row['movieID']

    return df2

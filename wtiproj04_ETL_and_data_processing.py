import numpy as np
import pandas as panda
from wtiproj03_ETL import averageRatingOfUser, averageRating, lab4JoinTables


def getProfile(id):
    genre, merged_table = lab4JoinTables()
    usersRating = averageRatingOfUser(id, merged_table)
    fill, avgRating = averageRating(merged_table)

    for key in usersRating:
        usersRating[key] -= avgRating[key]
    print(usersRating)
    for key in avgRating:
        if not key in usersRating:
            usersRating[key] = 0
    print(usersRating)

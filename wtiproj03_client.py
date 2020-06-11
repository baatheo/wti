import json

from wtiproj03_ETL import joinTables, lab4JoinTables, dictToDataFrame, dataFrameToDict, averageRating, \
    averageRatingOfUser
from wtiproj04_ETL_and_data_processing import getProfile


def main():
    # result = joinTables()
    # for index, row in result.iterrows():
    #     print(row.to_dict())

    getProfile(75)


if __name__ == "__main__":
    main()
import json
from flask import Flask, request, make_response, jsonify


import wtiproj06_api_logic

api = Flask(__name__)
api.config['JSON_SORT_KEYS'] = False

data = wtiproj06_api_logic.ApiLogic()


@api.route('/rating/add', methods=['POST'])
def postProfile():
    requestData = request.form
    return make_response(jsonify(data.postRating(requestData)))


@api.route('/profile/all', methods=['GET'])
def getAllProfile():
    values = list(data.getAllProfiles())
    return make_response(jsonify(values), 200)


@api.route('/ratings', methods=['GET', 'DELETE'])
def getRating():
    if request.method == 'GET':
        return make_response(jsonify(list(data.getAllRatings())), 200)
    if request.method == 'DELETE':
        return make_response(data.clearAll(), 200)


@api.route('/avg-genre-ratings/all-users', methods=['GET'])
def getAllUsersRatings():
    tempData = data.averageRating()
    return make_response(tempData, 200)


@api.route('/profile/<userID>', methods=['GET'])
def getUserRating(userID):
    return make_response(jsonify(list(data.getProfile(userID))), 200)


if __name__ == '__main__':
    api.run(host='127.0.0.1', port=9123)

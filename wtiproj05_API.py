import json

import wtiproj05_api_logic
from flask import Flask, request, make_response, jsonify

api = Flask(__name__)
api.config['JSON_SORT_KEYS'] = False

data = wtiproj05_api_logic.api()


@api.route('/profile/add', methods=['POST'])
def postProfile():
    requestData = request.form
    return make_response(jsonify(data.addProfile(requestData)))


@api.route('/profile/<id>', methods=['GET'])
def getProfile(id):
    return make_response(jsonify(data.getProfile(id)), 200)


@api.route('/profile/postAll', methods=['GET'])
def getProfiles():
    return make_response(jsonify(data.addProfiles()), 200)


@api.route('/profile/all', methods=['GET'])
def getAllProfile():
    values = data.getAllProfiles()
    print(type(values))
    return make_response(values, 200)


@api.route('/rating', methods=['POST'])
def postRating():
    requestData = request.form
    return make_response(jsonify(data.post(requestData)), 200)


@api.route('/ratings', methods=['GET', 'DELETE'])
def getRating():
    if request.method == 'GET':
        return make_response(data.getAll().to_json(orient='records'), 200)
    if request.method == 'DELETE':
        return make_response(data.delete().to_json(orient='records'), 200)


@api.route('/avg-genre-ratings/all-users', methods=['GET'])
def getAllUsersRatings():
    tempData = data.average()
    return make_response(json.dumps(tempData), 200)


@api.route('/avg-genre-ratings/user/<userID>', methods=['GET'])
def getUserRating(userID):
    return make_response(json.dumps(data.average_user(userID)), 200)


if __name__ == '__main__':
    api.run(host='127.0.0.1', port=9123)

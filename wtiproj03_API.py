import json
import random
from flask import request, jsonify, make_response, Flask
from wtiproj03_ETL import joinTables, allTypes
api = Flask(__name__)
api.config['JSON_SORT_KEYS'] = False
listOfGenreTypes = allTypes()
result = joinTables()
result = result.fillna(0)


@api.route('/rating', methods=['POST'])
def addOne():
    value = request.form
    global result
    result = result.append(value, ignore_index=True)
    return make_response("Dodano rekord", 200)


@api.route('/ratings', methods=['GET', 'DELETE'])
def ratings():
    global result
    if request.method == 'GET':
        if result.empty:
            return jsonify('empty')
        else:
            jsonfiles = json.loads(result.to_json(orient='records'))
            return make_response(jsonify(jsonfiles), 200)
    if request.method == 'DELETE':
        result = result.iloc[0:0]
        if result.empty:
            return make_response("usunieto", 200)


@api.route('/avg−genre−ratings/<id>', methods=['GET'])
def getUser(id):
    if result.empty:
        return jsonify('empty')
    else:
        my_dict = {}
        for c in listOfGenreTypes:
            my_dict[c] = random.uniform(0, 5)
        my_dict['userID'] = id
        return jsonify(my_dict)


@api.route('/avg-genre-ratings/all-users', methods=['GET'])
def allUsers():
    if result.empty:
        return jsonify('empty')
    else:
        my_dict = {}
        for c in listOfGenreTypes:
            my_dict[c] = random.uniform(0, 5)
        return jsonify(my_dict)


if __name__ == '__main__':
    api.run()
from flask import Flask, jsonify, abort, request
from wtiproj07_extended_elasticsearch_client import ElasticClient
import json
import numpy as np
app = Flask(__name__)
es = ElasticClient()


class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32,
                              np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


# ------ Simple operations ------
@app.route("/user/document/<id>", methods=["GET"])
def get_user(id):
    try:
        index = request.args.get('index', default='users')
        result = es.get_movies_liked_by_user(id, index)
        return jsonify(result)
    except:
        abort(404)


@app.route("/movie/document/<id>", methods=["GET"])
def get_movie(id):
    try:
        index = request.args.get('index', default='movies')
        result = es.get_users_that_like_movie(id, index)
        return jsonify(result)
    except:
        abort(404)


# ------ Preselection ------
@app.route("/user/preselection/<int:userid>", methods=["GET"])
def use_preselection(userid):
    try:
        result = es.collabUserFilter(int(userid))
        result = {
            "moviesFound": result
        }
        return json.dumps(result, cls=NumpyEncoder)
    except Exception as e:
        print(e)
        abort(404)


@app.route("/movie/preselection/<int:movieid>", methods=["GET"])
def movies_preselection(movieid):
    try:
        result = es.collabMovieFilter(int(movieid))
        result = {
            "usersFound": result
        }
        return json.dumps(result, cls=NumpyEncoder)
    except Exception as e:
        print(e)
        abort(404)


# ------ Add/Update/Delete ------
@app.route("/user/document/<user_id>", methods=["PUT"])
def add_user_document(user_id):
    try:
        movies_liked_by_user = request.json
        es.addUser(user_id, movies_liked_by_user)
        return "Ok", 200
    except:
        abort(400)


@app.route("/movie/document/<movie_id>", methods=["PUT"])
def add_movie_document(movie_id):
    try:
        users_who_like_movie = request.json
        es.addMovie(movie_id, users_who_like_movie)
        return "Ok", 200
    except:
        abort(400)


@app.route("/user/document/<user_id>", methods=["POST"])
def update_user_document(user_id):
    try:
        movies_liked_by_user = request.json
        es.upUser(user_id, movies_liked_by_user)
        return "Ok", 200
    except:
        abort(400)


@app.route("/movie/document/<movie_id>", methods=["POST"])
def update_movie_document(movie_id):
    try:
        users_who_like_movie = request.json
        es.upMovie(movie_id, users_who_like_movie)
        return "Ok", 200
    except:
        abort(400)


@app.route("/user/document/<user_id>", methods=["DELETE"])
def delete_user_document(user_id):
    try:
        es.deleteUser(user_id)
        return "Ok", 200
    except Exception as e:
        print(e)
        abort(400)


@app.route("/movie/document/<movie_id>", methods=["DELETE"])
def delete_movie_document(movie_id):
    try:
        es.deleteMovie(movie_id)
        return "Ok", 200
    except Exception as e:
        print(e)
        abort(400)


@app.route("/user/bulk", methods=["POST"])
def bulk_update_users():
    body = request.json
    es.bulkUserUpdate(body)
    return 'Ok', 200


@app.route("/movie/bulk", methods=["POST"])
def bulk_update_movies():
    body = request.json
    es.bulkMovieUpdate(body)
    return 'Ok', 200


if __name__ == '__main__':
    es.index_documents()
    app.run()


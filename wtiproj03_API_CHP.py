import json
import random

import cherrypy
from flask import jsonify, make_response

from wtiproj03_ETL import allTypes, joinTables

listOfGenreTypes = allTypes()
result = joinTables()
result = result.fillna(0)


@cherrypy.expose
@cherrypy.tools.json.out()
class rating(object):
    @cherrypy.tools.accept(media='text/plain')
    def POST(self, **data):
        global df
        print(data)
        df = df.append(data, ignore_index=True)
        return df.to_dict(orient='records')


@cherrypy.expose
@cherrypy.tools.json_out()
class ratings(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        if result.empty:
            return jsonify('empty')
        else:
            jsonfiles = json.loads(result.to_json(orient='records'))
            return make_response(jsonify(jsonfiles), 200)

    def DELETE(self):
        global result
        result = result.iloc[0:0]
        if result.empty:
            return make_response("usunieto", 200)


@cherrypy.expose
@cherrypy.tools.json_out()
class average(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        dict = {}
        for c in listOfGenreTypes:
            dict[c] = random.uniform(0, 5)
        return dict


@cherrypy.expose
@cherrypy.tools.json_out()
class average2(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self, user):
        dict = {}
        for c in listOfGenreTypes:
            dict[c] = random.uniform(0, 5)
        dict['userID'] = user
        return dict


if __name__ == '__main__':
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.sessions.on': True,
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'text/plain')],
        }
    }
    cherrypy.tree.mount(rating(), '/rating', conf)
    cherrypy.tree.mount(ratings(), '/ratings', conf)
    cherrypy.tree.mount(average(), '/avg-genre-ratings/all-users', conf)
    cherrypy.tree.mount(average2(), '/avg-genre-ratings/', conf)
    cherrypy.engine.start()
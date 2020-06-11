import cherrypy

from wtiproj06_API import api


cherrypy.tree.graft(api.wsgi_app, '/')
cherrypy.config.update({'server.socket_host': '127.0.0.1',
                        'server.socket_port': 9898,
                        'server.thread_pool': 50,
                        'engine.autoreload.on': False,
                        })

if __name__ == '__main__':
    cherrypy.engine.start()

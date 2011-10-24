import brukva
import tornado.httpserver
import tornado.web
import tornado.websocket
import tornado.ioloop
import logging


logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger('app')

c = brukva.Client(yield_mode=True)
c.connect()

start = 0
count = 0

def on_set(res):
    global count
    print 'on_set'
    count += 1

#    if count >=2:
#        tornado.ioloop.IOLoop.instance().stop()


c.set('bar', 'Lorem ipsum #2', on_set)
c.set('zar', 'Lorem ipsum #3', on_set)

class BrukvaHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @brukva.adisp.process
    def get(self):
        foo, foo2 = yield [c.get('bar'), c.get('zar')]
        self.set_header('Content-Type', 'text/plain')
        self.write(foo)
        self.write(foo2)
        print foo, foo2
        self.finish()
#        tornado.ioloop.IOLoop.instance().stop()


application = tornado.web.Application([
    (r'/', BrukvaHandler),
], debug=True
)

if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(9000)
    instance = tornado.ioloop.IOLoop.instance()
    instance.start()


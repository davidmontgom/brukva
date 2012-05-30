import logging
import brukva
import tornado.httpserver
import tornado.web
import tornado.websocket
import tornado.ioloop
import redis

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

r = redis.Redis(REDIS_HOST, REDIS_PORT, db=9)
c = brukva.Client(REDIS_HOST, REDIS_PORT, yield_mode=True, pool_size=10)
c.connect()
c.select(9)

r.set('foo', 'bar')
r.set('foo2', 'bar2')


class BrukvaHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @brukva.adisp.process
    def get(self):
        res = yield [c.get(key) for key in ['foo', 'foo2'] * 5]
        self.set_header('Content-Type', 'text/plain')
        for r_ in res:
            self.write(r_)
        self.finish()


class RedisHandler(tornado.web.RequestHandler):
    def get(self):
        res = [r.get(key) for key in ['foo', 'foo2'] * 5]
        self.set_header('Content-Type', 'text/plain')
        for r_ in res:
            self.write(r_)
        self.finish()


class HelloHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header('Content-Type', 'text/plain')
        for i in xrange(10):
            self.write('Hello world!')
        self.finish()


application = tornado.web.Application([
    (r'/brukva', BrukvaHandler),
    (r'/redis', RedisHandler),
    (r'/hello', HelloHandler),
],debug=True)


if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    tornado.ioloop.IOLoop.instance().start()

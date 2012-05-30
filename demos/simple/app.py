import brukva
import tornado.httpserver
import tornado.web
import tornado.websocket
import tornado.ioloop
from brukva import adisp
import logging


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
formatter = logging.Formatter('%(msecs)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s')
handler = logging.FileHandler('logs.txt')
handler.setFormatter(formatter)
logger.handlers[0].setFormatter(formatter)
logger.addHandler(handler)

c = brukva.Client(yield_mode=True, reconnect_retries=2, reconnect_timeout=0.2, pool_size=4)
c.connect()


def on_set(result):
    logging.debug("set result: %s" % result)


c.set('foo', 'Lorem ipsum #1', on_set)
c.set('bar', 'Lorem ipsum #2', on_set)
c.set('zar', 'Lorem ipsum #3', on_set)


class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @adisp.process
    def get(self):
        try:
            foo = yield c.get('foo')
            bar = yield c.async.get('bar')
            zar = yield c.async.get('zar')
            self.set_header('Content-Type', 'text/html')
            self.write('foo='+foo)
            self.write('bar='+bar)
            self.write('zar='+zar)
            self.finish()
        except Exception:
            logger.exception('exception caught in handler')
            self.write('exception happened')
            self.finish()


application = tornado.web.Application([
    (r'/', MainHandler),
], debug=True)

if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    tornado.ioloop.IOLoop.instance().start()

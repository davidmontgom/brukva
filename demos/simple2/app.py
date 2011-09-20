from time import sleep, time
from timeit import timeit
import brukva
import tornado.httpserver
import tornado.web
import tornado.websocket
import tornado.ioloop
from brukva import adisp
import logging


logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger('app')

c = brukva.Client()
c.connect()

start = 0
count = 0
finish_count = 4000
def on_set(result):
    global count
#    log.debug("set result: %s" % result)
    count += 1
    if count >=finish_count:
        print time()-start
        tornado.ioloop.IOLoop.instance().stop()




#c.set('foo', 'Lorem ipsum #1', on_set)
#c.set('bar', 'Lorem ipsum #2', on_set)
#c.set('zar', 'Lorem ipsum #3', on_set)

def startf():
    global start
    start = time()
    for i in range(finish_count):
#        c.hgetall('ee98f536-349b-4c37-b168-ac83c188e2be_task_fb', on_set)
        c.hget('ee98f536-349b-4c37-b168-ac83c188e2be_task_fb', 'state', on_set)


application = tornado.web.Application([
    (r'/', tornado.web.RequestHandler),
])


if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(9000)
    instance = tornado.ioloop.IOLoop.instance()
    instance.add_callback(startf)

#    import cProfile, pstats
#    cProfile.run('instance.start()', 'iostats')
#    p = pstats.Stats('iostats')
#    p.sort_stats('time').print_stats(20)

#    instance.start()


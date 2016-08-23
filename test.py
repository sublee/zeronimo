# -*- coding: utf-8 -*-
import gc
import itertools
import os
import signal
import time
import warnings

import gevent
from gevent import joinall, spawn
from gevent.pool import Group, Pool
from psutil import Process
import pytest
import zmq.green as zmq

from conftest import (
    Application, link_sockets, rand_str, run_device, running, sync_pubsub)
import zeronimo
from zeronimo.core import Background, uuid4_bytes
from zeronimo.exceptions import Rejected
from zeronimo.helpers import eintr_retry_zmq
import zeronimo.messaging


warnings.simplefilter('always')


def require_libzmq(version_info):
    return pytest.mark.skipif('zmq.zmq_version_info() < %r' % (version_info,))


def get_results(results):
    return [result.get() for result in results]


def find_objects(cls):
    gc.collect()
    return [o for o in gc.get_objects() if isinstance(o, cls)]


def test_running():
    class NullBG(Background):
        def __call__(self):
            gevent.sleep(0.1)
    bg = NullBG()
    assert not bg.is_running()
    bg.start()
    assert bg.is_running()
    bg.wait()
    assert not bg.is_running()


socket_types = [zmq.PAIR, zmq.PULL, zmq.SUB, zmq.ROUTER, zmq.DEALER]
if zmq.zmq_version_info() >= (3,):
    socket_types.extend([zmq.XPUB, zmq.XSUB])


@pytest.mark.parametrize('socket_type', socket_types)
def test_recv_detects_closing(socket, socket_type):
    sock = socket(socket_type)
    gevent.spawn_later(0.5, sock.close)
    with pytest.raises(zmq.ZMQError):
        sock.recv()


def test_messaging(socket, addr, topic):
    push = socket(zmq.PUSH)
    pull = socket(zmq.PULL)
    link_sockets(addr, push, [pull])
    for t in [None, topic]:
        zeronimo.messaging.send(push, 1, prefix=t)
        assert zeronimo.messaging.recv(pull) == (t, 1)
        zeronimo.messaging.send(push, 'doctor', prefix=t)
        assert zeronimo.messaging.recv(pull) == (t, 'doctor')
        zeronimo.messaging.send(push, {'doctor': 'who'}, prefix=t)
        assert zeronimo.messaging.recv(pull) == (t, {'doctor': 'who'})
        zeronimo.messaging.send(push, ['doctor', 'who'], prefix=t)
        assert zeronimo.messaging.recv(pull) == (t, ['doctor', 'who'])
        zeronimo.messaging.send(push, Exception, prefix=t)
        assert zeronimo.messaging.recv(pull) == (t, Exception)
        zeronimo.messaging.send(push, Exception('Allons-y'), prefix=t)
        assert isinstance(zeronimo.messaging.recv(pull)[1], Exception)


def test_from_socket(socket, addr, reply_sockets):
    # sockets
    worker_sock = socket(zmq.PULL)
    worker_sock.bind(addr)
    push = socket(zmq.PUSH)
    push.connect(addr)
    reply_sock, (collector_sock, reply_topic) = reply_sockets()
    # logic
    app = Application()
    worker = zeronimo.Worker(app, [worker_sock], reply_sock)
    collector = zeronimo.Collector(collector_sock, reply_topic)
    customer = zeronimo.Customer(push, collector)
    with running([worker, collector]):
        # test
        result = customer.call('zeronimo')
        assert result.get() == 'zeronimo'


def test_socket_type_error(socket):
    # customer
    with pytest.raises(ValueError):
        zeronimo.Customer(socket(zmq.SUB))
    with pytest.raises(ValueError):
        zeronimo.Customer(socket(zmq.REQ))
    with pytest.raises(ValueError):
        zeronimo.Customer(socket(zmq.REP))
    with pytest.raises(ValueError):
        zeronimo.Customer(socket(zmq.ROUTER))
    with pytest.raises(ValueError):
        zeronimo.Customer(socket(zmq.PULL))
    # worker
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [socket(zmq.PUB)])
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [socket(zmq.REQ)])
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [socket(zmq.REP)])
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [socket(zmq.DEALER)])
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [socket(zmq.PUSH)])
    # TODO: reply_socket
    # collector
    with pytest.raises(ValueError):
        zeronimo.Collector(socket(zmq.PUB), 'x')
    with pytest.raises(ValueError):
        zeronimo.Collector(socket(zmq.REQ), 'x')
    with pytest.raises(ValueError):
        zeronimo.Collector(socket(zmq.REP), 'x')
    with pytest.raises(ValueError):
        zeronimo.Collector(socket(zmq.PUSH), 'x')


@require_libzmq((3,))
def test_xpub_xsub_type_error(socket):
    with pytest.raises(ValueError):
        zeronimo.Customer(socket(zmq.XSUB))
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [socket(zmq.XPUB)])


@require_libzmq((4, 0, 1))
def test_stream_type_error(socket):
    # zmq.STREAM is available from zmq-4.0.1
    with pytest.raises(ValueError):
        zeronimo.Customer(socket(zmq.STREAM))
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [socket(zmq.STREAM)])
    with pytest.raises(ValueError):
        zeronimo.Collector(socket(zmq.STREAM), 'x')


def test_fixtures(worker, push, pub, collector, addr1, addr2):
    assert isinstance(worker, zeronimo.Worker)
    assert len(worker.worker_sockets) == 2
    assert push.type == zmq.PUSH
    assert pub.type == zmq.PUB
    assert isinstance(collector, zeronimo.Collector)
    assert addr1 != addr2
    assert worker.is_running()
    assert collector.is_running()


@pytest.mark.flaky(reruns=3)
def test_nowait(worker, push):
    customer = zeronimo.Customer(push)
    assert worker.app.counter['zeronimo'] == 0
    for x in xrange(5):
        assert customer.call('zeronimo') is None
    assert worker.app.counter['zeronimo'] == 0
    gevent.sleep(0.01)
    assert worker.app.counter['zeronimo'] == 5


@pytest.mark.flaky(reruns=3)
def test_fanout_nowait(worker, worker2, worker3, worker4, worker5, pub, topic):
    fanout = zeronimo.Fanout(pub)
    assert worker.app.counter['zeronimo'] == 0
    assert fanout.emit(topic, 'zeronimo') is None
    assert worker.app.counter['zeronimo'] == 0
    gevent.sleep(0.01)
    assert worker.app.counter['zeronimo'] == 5


def test_return(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    assert customer.call('zeronimo').get() == 'zeronimo'
    assert customer.call('add', 100, 200).get() == 300
    assert customer.call('add', '100', '200').get() == '100200'


def test_yield(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    assert ' '.join(customer.call('rycbar123').get()) == \
           'run, you clever boy; and remember.'
    assert list(customer.call('dont_yield').get()) == []


def test_raise(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    with pytest.raises(ZeroDivisionError) as exc_info:
        customer.call('zero_div').get()
    assert issubclass(exc_info.type, zeronimo.RemoteException)
    g = customer.call('rycbar123_and_zero_div').get()
    assert next(g) == 'run,'
    assert next(g) == 'you'
    assert next(g) == 'clever'
    assert next(g) == 'boy;'
    assert next(g) == 'and'
    assert next(g) == 'remember.'
    with pytest.raises(ZeroDivisionError):
        next(g)


def test_iterator(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    assert isinstance(customer.call('xrange', 1, 100, 10).get(), xrange)
    assert \
        list(customer.call('iter_xrange', 1, 100, 10).get()) == \
        range(1, 100, 10)
    assert \
        set(customer.call('iter_dict_view', 1, 100, 10).get()) == \
        set(range(1, 100, 10))


def test_fanout_return(worker1, worker2, collector, pub, topic):
    fanout = zeronimo.Fanout(pub, collector)
    assert \
        get_results(fanout.emit(topic, 'zeronimo')) == \
        ['zeronimo', 'zeronimo']
    assert get_results(fanout.emit(topic, 'add', 10, 20)) == [30, 30]
    assert \
        get_results(fanout.emit(topic, 'add', '10', '20')) == \
        ['1020', '1020']


def test_fanout_yield(worker1, worker2, collector, pub, topic):
    fanout = zeronimo.Fanout(pub, collector)
    for result in fanout.emit(topic, 'rycbar123'):
        g = result.get()
        assert next(g) == 'run,'
        assert next(g) == 'you'
        assert next(g) == 'clever'
        assert next(g) == 'boy;'
        assert next(g) == 'and'
        assert next(g) == 'remember.'


def test_fanout_raise(worker1, worker2, collector, pub, topic):
    fanout = zeronimo.Fanout(pub, collector)
    with pytest.raises(ZeroDivisionError):
        get_results(fanout.emit(topic, 'zero_div'))


def test_2to1(worker, collector, push1, push2):
    customer1 = zeronimo.Customer(push1, collector)
    customer2 = zeronimo.Customer(push2, collector)
    def test(customer):
        assert customer.call('add', 1, 1).get() == 2
        assert len(list(customer.call('rycbar123').get())) == 6
        with pytest.raises(ZeroDivisionError):
            customer.call('zero_div').get()
    joinall([spawn(test, customer1), spawn(test, customer2)], raise_error=True)


def test_1to2(worker1, worker2, collector, push):
    customer = zeronimo.Customer(push, collector)
    result1 = customer.call('add', 1, 1)
    result2 = customer.call('add', 2, 2)
    assert result1.get() == 2
    assert result2.get() == 4
    assert result1.worker_info != result2.worker_info


def test_slow(worker, collector, push):
    print 1
    customer = zeronimo.Customer(push, collector)
    print 2
    with pytest.raises(gevent.Timeout):
        print 3
        with gevent.Timeout(0.1):
            print 4
            customer.call('sleep', 0.3).get()
            print 5
    print 6
    t = time.time()
    print 7
    assert customer.call('sleep', 0.1).get() == 0.1
    print 8
    assert time.time() - t >= 0.1
    print 9


def test_reject(worker1, worker2, collector, push, pub, topic):
    customer = zeronimo.Customer(push, collector)
    fanout = zeronimo.Fanout(pub, collector)
    def count_workers(iteration=10):
        worker_infos = set()
        for x in xrange(iteration):
            worker_infos.add(tuple(customer.call('zeronimo').worker_info))
        return len(worker_infos)
    # count accepted workers.
    assert count_workers() == 2
    # worker1 uses a greenlet pool sized by 1.
    worker1.greenlet_group = Pool(1)
    # emit long task.
    how_slow = zeronimo.Fanout.timeout * 4
    assert len(fanout.emit(topic, 'sleep', how_slow)) == 2
    assert len(fanout.emit(topic, 'sleep', how_slow)) == 1
    assert count_workers() == 1
    # wait for long task done.
    worker1.greenlet_group.wait_available()
    assert count_workers() == 2


# def test_reject_on_exception(worker1, worker2, collector, push):
#     # Workers wrap own object.
#     worker1.app = Application()
#     worker2.app = Application()
#     # Any worker accepts.
#     customer = zeronimo.Customer(push, collector)
#     assert customer.call('f_under_reject_on_exception').get() == 'zeronimo'
#     # worker1 will reject.
#     worker1.app.f = worker1.app.zero_div
#     # But worker2 still accepts.
#     assert customer.call('f_under_reject_on_exception').get() == 'zeronimo'
#     # worker2 will also reject.
#     worker2.app.f = worker2.app.zero_div
#     # All workers reject.
#     customer.timeout = 0.1
#     with pytest.raises(Rejected):
#         customer.call('f_under_reject_on_exception')


def test_manual_ack(worker1, worker2, collector, push):
    # Workers wrap own object.
    worker1.app = Application()
    worker2.app = Application()
    errors = []
    worker1.exception_handler = lambda w, e: errors.append(e)
    # Any worker accepts.
    customer = zeronimo.Customer(push, collector)
    assert customer.call('maybe_reject', 1, 2).get() == 3
    # worker1 will reject.
    worker1.app.maybe_reject.reject = True
    # But worker2 still accepts.
    assert customer.call('maybe_reject', 3, 4).get() == 7
    assert not errors
    # worker2 will also reject.
    worker2.app.maybe_reject.reject = True
    # All workers reject.
    customer.timeout = 0.1
    with pytest.raises(Rejected):
        customer.call('maybe_reject', 5, 6)
    # A generator marked as manual_rpc=True.
    worker1.app.iter_maybe_reject.reject = True
    gen = customer.call('iter_maybe_reject', 7, 8).get()
    assert next(gen) == 7
    assert next(gen) == 8
    assert worker2.app.iter_maybe_reject.final_state == (True, 7, 8)
    # All generators marked as manual_rpc=True.
    worker2.app.iter_maybe_reject.reject = True
    with pytest.raises(Rejected):
        customer.call('iter_maybe_reject', 9, 10)
    assert worker1.app.iter_maybe_reject.final_state is None
    assert worker2.app.iter_maybe_reject.final_state is None
    assert not errors


def test_max_retries(worker, collector, push):
    def start_accepting():
        worker.greenlet_group = Group()
    def stop_accepting():
        worker.greenlet_group = Pool(0)
    customer = zeronimo.Customer(push, collector)
    # don't retry.
    customer.max_retries = 0
    stop_accepting()
    g = gevent.spawn_later(1, start_accepting)
    with pytest.raises(Rejected):
        customer.call('zeronimo')
    g.join()
    # do retry.
    customer.max_retries = 1000
    stop_accepting()
    g = gevent.spawn_later(0.01, start_accepting)
    assert customer.call('zeronimo').get() == 'zeronimo'
    g.join()


def test_subscription(worker1, worker2, collector, pub, topic):
    fanout = zeronimo.Fanout(pub, collector)
    sub1 = [sock for sock in worker1.worker_sockets if sock.type == zmq.SUB][0]
    sub2 = [sock for sock in worker2.worker_sockets if sock.type == zmq.SUB][0]
    sub1.set(zmq.UNSUBSCRIBE, topic)
    assert len(fanout.emit(topic, 'zeronimo')) == 1
    sub2.set(zmq.UNSUBSCRIBE, topic)
    assert fanout.emit(topic, 'zeronimo') == []
    sub1.set(zmq.SUBSCRIBE, topic)
    # sync_pubsub will disturb the worker. so the worker should be stopped
    # during sync_pubsub works.
    worker1.stop()
    sync_pubsub(pub, [sub1], topic)
    worker1.start()
    assert len(fanout.emit(topic, 'zeronimo')) == 1
    sub2.set(zmq.SUBSCRIBE, topic)
    # same reason
    worker1.stop()
    worker2.stop()
    sync_pubsub(pub, [sub2], topic)
    worker1.start()
    worker2.start()
    assert len(fanout.emit(topic, 'zeronimo')) == 2


def test_device(collector, worker_pub, socket,
                topic, addr1, addr2, addr3, addr4):
    # customer  |-----| forwarder |---> | worker
    #           | <----| streamer |-----|
    try:
        # run streamer
        streamer_in_addr, streamer_out_addr = addr1, addr2
        forwarder_in_addr, forwarder_out_addr = addr3, addr4
        streamer = spawn(run_device, socket(zmq.PULL), socket(zmq.PUSH),
                         streamer_in_addr, streamer_out_addr)
        streamer.join(0)
        # run forwarder
        sub = socket(zmq.SUB)
        sub.set(zmq.SUBSCRIBE, '')
        forwarder = spawn(run_device, sub, socket(zmq.PUB),
                          forwarder_in_addr, forwarder_out_addr)
        forwarder.join(0)
        # connect to the devices
        worker_pull1 = socket(zmq.PULL)
        worker_pull2 = socket(zmq.PULL)
        worker_pull1.connect(streamer_out_addr)
        worker_pull2.connect(streamer_out_addr)
        worker_sub1 = socket(zmq.SUB)
        worker_sub2 = socket(zmq.SUB)
        worker_sub1.set(zmq.SUBSCRIBE, topic)
        worker_sub2.set(zmq.SUBSCRIBE, topic)
        worker_sub1.connect(forwarder_out_addr)
        worker_sub2.connect(forwarder_out_addr)
        push = socket(zmq.PUSH)
        push.connect(streamer_in_addr)
        pub = socket(zmq.PUB)
        pub.connect(forwarder_in_addr)
        sync_pubsub(pub, [worker_sub1, worker_sub2], topic)
        # make and start workers
        app = Application()
        worker1 = zeronimo.Worker(app, [worker_pull1, worker_sub1], worker_pub)
        worker2 = zeronimo.Worker(app, [worker_pull2, worker_sub2], worker_pub)
        worker1.start()
        worker2.start()
        # zeronimo!
        customer = zeronimo.Customer(push, collector)
        fanout = zeronimo.Fanout(pub, collector)
        assert customer.call('zeronimo').get() == 'zeronimo'
        assert \
            get_results(fanout.emit(topic, 'zeronimo')) == \
            ['zeronimo', 'zeronimo']
    finally:
        try:
            streamer.kill()
            forwarder.kill()
            worker1.stop()
            worker2.stop()
        except UnboundLocalError:
            pass


@require_libzmq((3,))
def test_proxied_fanout(collector, worker_pub, socket, topic, addr1, addr2):
    # customer  |----| forwarder with XPUB/XSUB |---> | worker
    # collector | <-----------------------------------|
    # run forwarder
    forwarder_in_addr, forwarder_out_addr = addr1, addr2
    forwarder = spawn(run_device, socket(zmq.XSUB), socket(zmq.XPUB),
                      forwarder_in_addr, forwarder_out_addr)
    try:
        forwarder.join(0)
        # connect to the devices
        worker_sub1 = socket(zmq.SUB)
        worker_sub2 = socket(zmq.SUB)
        worker_sub1.set(zmq.SUBSCRIBE, topic)
        worker_sub2.set(zmq.SUBSCRIBE, topic)
        worker_sub1.connect(forwarder_out_addr)
        worker_sub2.connect(forwarder_out_addr)
        pub = socket(zmq.PUB)
        pub.connect(forwarder_in_addr)
        sync_pubsub(pub, [worker_sub1, worker_sub2], topic)
        # make and start workers
        app = Application()
        worker1 = zeronimo.Worker(app, [worker_sub1], worker_pub)
        worker2 = zeronimo.Worker(app, [worker_sub2], worker_pub)
        fanout = zeronimo.Fanout(pub, collector)
        with running([worker1, worker2, collector]):
            # zeronimo!
            results = fanout.emit(topic, 'zeronimo')
            assert get_results(results) == ['zeronimo', 'zeronimo']
    finally:
        forwarder.kill()


@require_libzmq((3,))
def test_proxied_collector(worker, push, socket, addr1, addr2):
    # customer  |-------------------> | worker
    # collector | <---| forwarder |---|
    try:
        forwarder = spawn(run_device, socket(zmq.XSUB), socket(zmq.XPUB),
                          addr1, addr2)
        forwarder.join(0)
        reply_topic = rand_str()
        collector_sock = socket(zmq.SUB)
        collector_sock.set(zmq.SUBSCRIBE, reply_topic)
        collector_sock.connect(addr2)
        worker.reply_socket.connect(addr1)
        sync_pubsub(worker.reply_socket, [collector_sock], reply_topic)
        collector = zeronimo.Collector(collector_sock, reply_topic)
        customer = zeronimo.Customer(push, collector)
        assert customer.call('zeronimo').get() == 'zeronimo'
    finally:
        try:
            forwarder.kill()
            collector.stop()
        except UnboundLocalError:
            pass


def test_2nd_start(worker, collector):
    assert worker.is_running()
    worker.stop()
    assert not worker.is_running()
    worker.start()
    assert worker.is_running()
    assert collector.is_running()
    collector.stop()
    assert not collector.is_running()
    collector.start()
    assert collector.is_running()


def test_concurrent_collector(worker, collector, push, pub, topic):
    customer = zeronimo.Customer(push, collector)
    fanout = zeronimo.Fanout(pub, collector)
    done = []
    def do_test():
        assert customer.call('zeronimo').get() == 'zeronimo'
        assert get_results(fanout.emit(topic, 'zeronimo')) == ['zeronimo']
        done.append(True)
    times = 5
    joinall([spawn(do_test) for x in xrange(times)], raise_error=True)
    assert len(done) == times


def test_stopped_collector(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    collector.stop()
    assert not collector.is_running()
    result = customer.call('zeronimo')
    assert result.get() == 'zeronimo'
    assert collector.is_running()
    collector.stop()


def test_undelivered(collector, push, pub, topic):
    customer = zeronimo.Customer(push, collector)
    fanout = zeronimo.Fanout(pub, collector)
    customer_nowait = zeronimo.Customer(push)
    with gevent.Timeout(1):
        with pytest.raises(zeronimo.Undelivered):
            customer.call('zeronimo')
        assert fanout.emit(topic, 'zeronimo') == []
        assert customer_nowait.call('zeronimo') is None


def test_pgm_connect(socket, fanout_addr):
    if 'pgm' not in fanout_addr[:4]:
        pytest.skip()
    sub = socket(zmq.SUB)
    sub.set(zmq.SUBSCRIBE, '')
    sub.connect(fanout_addr)
    worker = zeronimo.Worker(None, [sub])
    worker.start()
    pub1 = socket(zmq.PUB)
    pub1.connect(fanout_addr)
    pub2 = socket(zmq.PUB)
    pub2.connect(fanout_addr)  # in zmq-3.2, pgm-connect sends an empty message
    try:
        worker.wait(0.1)
    finally:
        worker.stop()


@pytest.mark.flaky(reruns=3)
def test_malformed_message(worker, push):
    push.send('')  # EOFError
    push.send('Zeronimo!')  # UnpicklingError
    push.send('c__main__\nNOTEXIST\np0\n.')  # AttributeError
    push.send_multipart(['a', 'b', 'c', 'd'])  # Too many message parts
    with warnings.catch_warnings(record=True) as w:
        gevent.sleep(0.1)
    assert len(w) == 4
    assert all(w_.category is zeronimo.MalformedMessage for w_ in w)
    w1, w2, w3, w4 = w[0].message, w[1].message, w[2].message, w[3].message
    assert 'EOFError' in str(w1)
    assert 'UnpicklingError' in str(w2)
    assert 'AttributeError' in str(w3)
    assert 'ValueError' in str(w4)
    assert str(w1).endswith(repr(['']))
    assert str(w2).endswith(repr(['Zeronimo!']))
    assert str(w3).endswith(repr(['c__main__\nNOTEXIST\np0\n.']))
    assert str(w4).startswith('<ValueError: too many message parts>')
    assert str(w4).endswith(repr(['a', 'b', 'c', 'd']))


@pytest.mark.flaky(reruns=3)
def test_queue_leaking_on_fanout(worker, collector, pub, topic):
    from gevent.queue import Queue
    fanout = zeronimo.Fanout(pub, collector)
    num_queues1 = len(find_objects(Queue))
    g = gevent.spawn(fanout.emit, topic, 'zeronimo')
    g.join(0)
    num_queues2 = len(find_objects(Queue))
    assert num_queues1 < num_queues2
    results = g.get()
    num_queues3 = len(find_objects(Queue))
    assert num_queues1 <= num_queues3
    gevent.sleep(0.5)
    num_queues4 = len(find_objects(Queue))
    assert num_queues3 == num_queues4
    get_results(results)
    num_queues5 = len(find_objects(Queue))
    assert num_queues1 == num_queues5


def test_result_broken(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    result = customer.call('sleep', 0.1)
    collector.socket.close()
    with pytest.raises(zeronimo.TaskClosed):
        result.get()


def _test_close_task(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    result = customer.call('sleep_multiple', 0.1, 10)
    g = result.get()
    assert next(g) == 0
    assert next(g) == 1
    result.close()
    with pytest.raises(zeronimo.TaskClosed):
        next(g)


def _test_duplex(socket, addr, left_type, right_type):
    left = socket(left_type)
    right = socket(right_type)
    left.bind(addr)
    right.connect(addr)
    worker = zeronimo.Worker(Application(), [left])
    collector = zeronimo.Collector(right)
    customer = zeronimo.Customer(right, collector)
    with running([worker, collector]):
        assert customer.call('zeronimo').get() == 'zeronimo'
        assert ' '.join(customer.call('rycbar123').get()) == \
               'run, you clever boy; and remember.'
        with pytest.raises(ZeroDivisionError):
            customer.call('zero_div').get()


def test_pair(socket, addr):
    _test_duplex(socket, addr, zmq.PAIR, zmq.PAIR)


def test_router_dealer(socket, addr):
    _test_duplex(socket, addr, zmq.ROUTER, zmq.DEALER)


def test_pair_with_collector(socket, addr, reply_sockets):
    # Failed at 0.3.0.
    left = socket(zmq.PAIR)
    right = socket(zmq.PAIR)
    left.bind(addr)
    right.connect(addr)
    reply_sock, (collector_sock, reply_topic) = reply_sockets()
    worker = zeronimo.Worker(Application(), [left], reply_sock)
    sync_pubsub(reply_sock, [collector_sock], reply_topic)
    collector = zeronimo.Collector(collector_sock, reply_topic)
    customer = zeronimo.Customer(right, collector)
    with running([worker, collector]):
        assert customer.call('zeronimo').get() == 'zeronimo'


@require_libzmq((3,))
def test_direct_xpub_xsub(socket, addr, reply_sockets):
    worker_sock = socket(zmq.XSUB)
    worker_sock.bind(addr)
    reply_sock, (collector_sock, reply_topic) = reply_sockets()
    xpub = socket(zmq.XPUB)
    xpub.connect(addr)
    # logic
    app = Application()
    worker = zeronimo.Worker(app, [worker_sock], reply_sock)
    collector = zeronimo.Collector(collector_sock, reply_topic)
    fanout = zeronimo.Fanout(xpub, collector)
    with running([worker, collector]):
        assert fanout.emit('', 'zeronimo') == []
        # subscribe ''
        worker_sock.send('\x01')
        assert xpub.recv() == '\x01'
        assert get_results(fanout.emit('', 'zeronimo')) == ['zeronimo']
        # unsubscribe ''
        worker_sock.send('\x00')
        assert xpub.recv() == '\x00'
        assert fanout.emit('', 'zeronimo') == []
        # subscribe 'zeronimo'
        worker_sock.send('\x01zeronimo')
        assert xpub.recv() == '\x01zeronimo'
        assert get_results(fanout.emit('zeronimo', 'zeronimo')) == ['zeronimo']


def test_mixture(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    assert customer.call('is_remote').get()
    assert not worker.app.is_remote()


def test_marshal_message(socket, addr, reply_sockets):
    import marshal
    pack = marshal.dumps
    unpack = marshal.loads
    # sockets
    worker_sock = socket(zmq.PULL)
    worker_sock.bind(addr)
    reply_sock, (collector_sock, reply_topic) = reply_sockets()
    customer_sock = socket(zmq.PUSH)
    customer_sock.connect(addr)
    # logic
    app = Application()
    worker = zeronimo.Worker(app, [worker_sock], reply_sock,
                             pack=pack, unpack=unpack)
    collector = zeronimo.Collector(collector_sock, reply_topic, unpack=unpack)
    customer = zeronimo.Customer(customer_sock, collector, pack=pack)
    with running([worker, collector]):
        assert customer.call('zeronimo').get() == 'zeronimo'


def test_exception_handler(worker, collector, push, capsys):
    customer = zeronimo.Customer(push, collector)
    # without exception handler
    with pytest.raises(ZeroDivisionError):
        customer.call('zero_div').get()
    out, err = capsys.readouterr()
    assert 'ZeroDivisionError:' in err
    # with exception handler
    exceptions = []
    def exception_handler(worker, exc_info):
        exceptions.append(exc_info[0])
    worker.exception_handler = exception_handler
    with pytest.raises(ZeroDivisionError):
        customer.call('zero_div').get()
    out, err = capsys.readouterr()
    assert 'ZeroDivisionError:' not in err
    assert len(exceptions) == 1
    assert exceptions[0] is ZeroDivisionError


def test_malformed_message_handler(worker, push, capsys):
    messages = []
    def malformed_message_handler(worker, exc_info, message_parts):
        messages.append(message_parts)
        raise exc_info[0], exc_info[1], exc_info[2]
    worker.malformed_message_handler = malformed_message_handler
    push.send('Zeronimo!')
    worker.wait()
    out, err = capsys.readouterr()
    assert not worker.is_running()
    assert 'UnpicklingError' in err
    assert ['Zeronimo!'] in messages


class ExampleException(BaseException):

    errno = None
    initialized = False

    def __init__(self, errno):
        self.errno = errno
        self.initialized = True

    def __getstate__(self):
        return self.errno

    def __setstate__(self, errno):
        self.errno = errno


class ExampleExceptionRaiser(object):

    def throw(self, errno):
        raise ExampleException(errno)


def test_exception_state(socket, addr, reply_sockets):
    # sockets
    worker_sock = socket(zmq.PULL)
    worker_sock.bind(addr)
    push = socket(zmq.PUSH)
    push.connect(addr)
    reply_sock, (collector_sock, reply_topic) = reply_sockets()
    # logic
    app = ExampleExceptionRaiser()
    worker = zeronimo.Worker(app, [worker_sock], reply_sock)
    collector = zeronimo.Collector(collector_sock, reply_topic)
    customer = zeronimo.Customer(push, collector)
    with running([worker, collector]):
        result = customer.call('throw', 2007)
        try:
            result.get()
        except BaseException as exc:
            assert isinstance(exc, zeronimo.RemoteException)
            assert isinstance(exc, ExampleException)
            assert exc.errno == 2007
            assert not exc.initialized
        else:
            assert False


@pytest.mark.parametrize('itimer, signo', [
    (signal.ITIMER_REAL, signal.SIGALRM),
    (signal.ITIMER_VIRTUAL, signal.SIGVTALRM),
    (signal.ITIMER_PROF, signal.SIGPROF),
])
def test_eintr_retry_zmq(itimer, signo, socket, addr):
    push = socket(zmq.PUSH)
    pull = socket(zmq.PULL)
    link_sockets(addr, push, [pull])
    interrupted_frames = []
    def handler(signo, frame):
        interrupted_frames.append(frame)
    prev_handler = signal.signal(signo, handler)
    prev_itimer = signal.setitimer(itimer, 0.001, 0.001)
    for x in itertools.count():
        eintr_retry_zmq(push.send, str(x))
        assert eintr_retry_zmq(pull.recv) == str(x)
        if len(interrupted_frames) > 100:
            break
    signal.setitimer(itimer, *prev_itimer)
    signal.signal(signo, prev_handler)


def test_many_calls(request, monkeypatch):
    worker = zeronimo.Worker(Application(), [])
    call = ('zeronimo', (), {}, uuid4_bytes(), None)
    msg = zeronimo.messaging.PACK(call)
    class fake_socket(object):
        type = zmq.PULL
        def recv_multipart(self, *args, **kwargs):
            return [msg]
    class infinite_events(object):
        def __iter__(self):
            return self
        def __next__(self):
            return fake_socket(), zmq.POLLIN
        next = __next__
    monkeypatch.setattr(zmq.Poller, 'poll', lambda *a, **k: infinite_events())
    # Emit many calls.
    signal.alarm(10)
    worker.start()
    request.addfinalizer(worker.stop)
    gevent.sleep(1)
    assert worker.app.counter['zeronimo'] > 0


def test_silent_stop(worker):
    worker.stop()
    assert not worker.is_running()
    with pytest.raises(RuntimeError):
        worker.stop()
    worker.stop(silent=True)


def test_close(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    worker.close()
    collector.close()
    customer.close()
    assert not worker.is_running()
    assert not collector.is_running()
    assert all(s.closed for s in worker.worker_sockets)
    assert collector.socket.closed
    assert customer.socket.closed


def test_no_param_conflict(worker, push, pub, collector, topic):
    customer = zeronimo.Customer(push, collector)
    fanout = zeronimo.Fanout(pub, collector)
    r = customer.call('kwargs', name='Zeronimo')
    assert r.get() == {'name': 'Zeronimo'}
    for r in fanout.emit(topic, 'kwargs', name='Zeronimo', topic='sublee'):
        assert r.get() == {'name': 'Zeronimo', 'topic': 'sublee'}


def test_only_workers_bind(socket, addr1, addr2):
    pub1, pub2 = socket(zmq.PUB), socket(zmq.PUB)
    sub1, sub2 = socket(zmq.SUB), socket(zmq.SUB)
    pub1.bind(addr1)
    sub1.bind(addr2)
    pub2.connect(addr2)
    sub2.connect(addr1)
    topic1, topic2 = rand_str(), rand_str()
    sub1.set(zmq.SUBSCRIBE, topic1)
    sub2.set(zmq.SUBSCRIBE, topic2)
    gevent.sleep(0.1)
    worker = zeronimo.Worker(Application(), [sub1], pub1)
    collector = zeronimo.Collector(sub2, topic=topic2)
    fanout = zeronimo.Fanout(pub2, collector)
    with running([worker, collector]):
        took = False
        for r in fanout.emit(topic1, 'zeronimo'):
            assert r.get() == 'zeronimo'
            took = True
        assert took


@require_libzmq((3,))
def test_xpub_sub(socket, addr, reply_sockets, topic):
    worker_sub = socket(zmq.SUB)
    worker_sub.set(zmq.SUBSCRIBE, topic)
    worker_sub.bind(addr)
    customer_xpub = socket(zmq.XPUB)
    customer_xpub.connect(addr)
    assert customer_xpub.recv() == '\x01%s' % topic
    reply_sock, (collector_sock, reply_topic) = reply_sockets()
    worker = zeronimo.Worker(Application(), [worker_sub], reply_sock)
    collector = zeronimo.Collector(collector_sock, reply_topic)
    fanout = zeronimo.Fanout(customer_xpub, collector)
    with running([worker, collector]):
        took = False
        for r in fanout.emit(topic, 'zeronimo'):
            assert r.get() == 'zeronimo'
            took = True
        assert took


left_right_types = [(zmq.PAIR, zmq.PAIR),
                    (zmq.PULL, zmq.PUSH),
                    (zmq.ROUTER, zmq.DEALER)]
if zmq.zmq_version_info() >= (4,):
    # XSUB before zmq-4 didn't allow arbitrary messages to send.
    left_right_types.append((zmq.XPUB, zmq.XSUB))


@pytest.mark.parametrize('left_type, right_type', left_right_types)
def test_collector_without_topic(socket, addr, worker, push,
                                 left_type, right_type):
    left = socket(left_type)
    right = socket(right_type)
    right.bind(addr)
    left.connect(addr)
    worker.reply_socket = right
    collector = zeronimo.Collector(left)  # topic is not required.
    customer = zeronimo.Customer(push, collector)
    with running([collector]):
        assert customer.call('zeronimo').get() == 'zeronimo'


def test_drop_if(worker, collector, pub, topic):
    fanout = zeronimo.Fanout(pub, collector, drop_if=lambda x: x != topic)
    results = list(fanout.emit(topic, 'zero_div'))
    assert results
    with pytest.raises(ZeroDivisionError):
        results[0].get()
    assert not list(fanout.emit(topic[::-1], 'zero_div'))


# catch leaks


@pytest.mark.trylast
def test_no_worker_leak():
    assert not find_objects(zeronimo.Worker)


@pytest.mark.trylast
def test_no_emitter_leak():
    assert not find_objects(zeronimo.Customer)
    assert not find_objects(zeronimo.Fanout)


@pytest.mark.trylast
def test_no_collector_leak():
    assert not find_objects(zeronimo.Collector)


@pytest.mark.trylast
def test_no_call_leak():
    assert not find_objects(zeronimo.messaging.Call)


@pytest.mark.trylast
def test_no_reply_leak():
    assert not find_objects(zeronimo.messaging.Reply)


proc = Process(os.getpid())
collect_conns = lambda: set(
    conn for conn in proc.connections()
    if conn.status not in ('CLOSE_WAIT', 'NONE')
)
# Mark initial connections.  They are not leacked connections.
initial_conns = collect_conns()


@pytest.mark.trylast
def test_no_socket_leak():
    for x in xrange(3):
        conns = collect_conns() - initial_conns
        if not conns:
            break
        gevent.sleep(0.1)
    else:
        pytest.fail('{0} connections leacked:\n{1}'.format(
            len(conns), '\n'.join('- %r' % (c,) for c in conns)
        ))

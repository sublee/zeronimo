# -*- coding: utf-8 -*-
import gc
import itertools
import os
import signal
import time
import warnings

import gevent
from gevent import joinall, spawn
from gevent.pool import Pool
from psutil import Process
import pytest
import zmq.green as zmq

from conftest import Application, link_sockets, rand_str, running, sync_pubsub
import zeronimo
from zeronimo.core import Background, uuid4_bytes
from zeronimo.exceptions import Rejected
from zeronimo.helpers import eintr_retry_zmq, socket_type_name
import zeronimo.messaging


warnings.simplefilter('always')
zmq_version_info = zmq.zmq_version_info()


def require_libzmq(version_info):
    args = version_info + ('x',) * (3 - len(version_info))
    reason = 'at least zmq-%s.%s.%s required' % args
    return pytest.mark.skipif(zmq_version_info < version_info, reason=reason)


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


@pytest.mark.parametrize('socket_type', [
    zmq.PAIR, zmq.PULL, zmq.SUB, zmq.ROUTER, zmq.DEALER] +
    ([zmq.XPUB, zmq.XSUB] if zmq_version_info >= (3,) else []),
    ids=socket_type_name)
def test_recv_detects_closing(socket, socket_type):
    sock = socket(socket_type)
    gevent.spawn_later(0.5, sock.close)
    with pytest.raises(zmq.ZMQError):
        sock.recv()


def test_messaging(socket, addr, topic):
    push = socket(zmq.PUSH)
    pull = socket(zmq.PULL)
    link_sockets(addr, push, [pull])
    for t in ['', topic]:
        zeronimo.messaging.send(push, ['doctor'], 'who', (t,))
        assert zeronimo.messaging.recv(pull) == (['doctor'], 'who', [t])
    with pytest.raises(TypeError):
        zeronimo.messaging.send(push, 1)


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


def test_fixtures(worker, push, pub, collector, addr1, addr2):
    assert isinstance(worker, zeronimo.Worker)
    assert len(worker.sockets) == 2
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
    customer = zeronimo.Customer(push, collector)
    r = customer.call('sleep', 0.3)
    with pytest.raises(gevent.Timeout), gevent.Timeout(0.1):
        r.get()
    t = time.time()
    assert customer.call('sleep', 0.1).get() == 0.1
    assert time.time() - t >= 0.1
    # If the task is remaining in the worker, zmq-2.1.4 crashes.
    r.get()


def test_reject(worker1, worker2, collector, push, pub, topic, monkeypatch):
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
    worker1.stop()
    worker1.start()
    # emit long task.
    how_slow = zeronimo.Fanout.timeout * 4
    assert len(fanout.emit(topic, 'sleep', how_slow)) == 2
    assert len(fanout.emit(topic, 'sleep', how_slow)) == 1
    assert count_workers() == 1
    # wait for long task done.
    worker1.greenlet_group.wait_available()
    assert count_workers() == 2


def test_reject_if(worker, collector, pub, topic):
    for s in worker.sockets:
        if s.type == zmq.SUB:
            s.set(zmq.SUBSCRIBE, b'')
    worker.reject_if = lambda call, topics: topics[-1] != topic
    unpacked = []
    def unpack(x):
        unpacked.append(x)
        return zeronimo.messaging.UNPACK(x)
    worker.unpack = unpack
    fanout = zeronimo.Fanout(pub, collector)
    assert get_results(fanout.emit(topic, 'zeronimo')) == ['zeronimo']
    assert len(unpacked) == 1
    assert get_results(fanout.emit(topic[::-1], 'zeronimo')) == []
    assert len(unpacked) == 1


def test_error_on_reject_if(worker, collector, pub, topic):
    worker.reject_if = lambda call, topics: 0 / 0
    fanout = zeronimo.Fanout(pub, collector)
    with warnings.catch_warnings(record=True) as w:
        fanout.emit(topic, 'zeronimo')
        gevent.sleep(0.1)
    assert worker.is_running()
    assert len(w) == 1
    assert 'ZeroDivisionError' in str(w[0].message)


def test_max_retries(worker, collector, push, monkeypatch):
    def stop_accepting():
        monkeypatch.setattr(worker.greenlet_group, 'full', lambda: True)
    customer = zeronimo.Customer(push, collector)
    # don't retry.
    customer.max_retries = 0
    stop_accepting()
    g = gevent.spawn_later(1, monkeypatch.undo)
    with pytest.raises(Rejected):
        customer.call('zeronimo')
    g.join()
    # do retry.
    customer.max_retries = 1000
    stop_accepting()
    g = gevent.spawn_later(0.01, monkeypatch.undo)
    assert customer.call('zeronimo').get() == 'zeronimo'
    g.join()


def test_subscription(worker1, worker2, collector, pub, topic):
    fanout = zeronimo.Fanout(pub, collector)
    sub1 = [sock for sock in worker1.sockets if sock.type == zmq.SUB][0]
    sub2 = [sock for sock in worker2.sockets if sock.type == zmq.SUB][0]
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
    sync_pubsub(pub, [sub1, sub2], topic)
    worker1.start()
    worker2.start()
    assert len(fanout.emit(topic, 'zeronimo')) == 2


def test_device(collector, worker_pub, socket, device,
                topic, addr1, addr2, addr3, addr4, fin):
    # customer  |-----| forwarder |---> | worker
    #           | <----| streamer |-----|
    # run streamer
    streamer_in_addr, streamer_out_addr = addr1, addr2
    forwarder_in_addr, forwarder_out_addr = addr3, addr4
    device(socket(zmq.PULL), socket(zmq.PUSH),
           streamer_in_addr, streamer_out_addr)
    # run forwarder
    sub = socket(zmq.SUB)
    sub.set(zmq.SUBSCRIBE, '')
    device(sub, socket(zmq.PUB), forwarder_in_addr, forwarder_out_addr)
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
    fin(worker1.stop)
    fin(worker2.stop)
    # zeronimo!
    customer = zeronimo.Customer(push, collector)
    fanout = zeronimo.Fanout(pub, collector)
    assert customer.call('zeronimo').get() == 'zeronimo'
    assert \
        get_results(fanout.emit(topic, 'zeronimo')) == \
        ['zeronimo', 'zeronimo']


@require_libzmq((3,))
def test_proxied_fanout(collector, worker_pub, socket, device,
                        topic, addr1, addr2):
    # customer  |----| forwarder with XPUB/XSUB |---> | worker
    # collector | <-----------------------------------|
    # run forwarder
    forwarder_in_addr, forwarder_out_addr = addr1, addr2
    device(socket(zmq.XSUB), socket(zmq.XPUB),
           forwarder_in_addr, forwarder_out_addr)
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


@require_libzmq((3,))
def test_proxied_collector(worker, push, socket, device, addr1, addr2, fin):
    # customer  |-------------------> | worker
    # collector | <---| forwarder |---|
    device(socket(zmq.XSUB), socket(zmq.XPUB), addr1, addr2)
    reply_topic = rand_str()
    collector_sock = socket(zmq.SUB)
    collector_sock.set(zmq.SUBSCRIBE, reply_topic)
    collector_sock.connect(addr2)
    worker.reply_socket.connect(addr1)
    sync_pubsub(worker.reply_socket, [collector_sock], reply_topic)
    collector = zeronimo.Collector(collector_sock, reply_topic)
    customer = zeronimo.Customer(push, collector)
    assert customer.call('zeronimo').get() == 'zeronimo'


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
    expectations = []
    expect = expectations.append
    expect('EOFError: no seam after topics')
    push.send('')
    expect('EOFError: no seam after topics')
    push.send_multipart(['a', 'b', 'c', 'd'])
    expect('EOFError: neither header nor payload')
    push.send_multipart([zeronimo.messaging.SEAM])
    expect('TypeError')
    push.send_multipart([zeronimo.messaging.SEAM, 'x'])
    expect('TypeError')
    push.send_multipart([zeronimo.messaging.SEAM, 'x', 'y'])
    expect('TypeError')
    push.send_multipart([zeronimo.messaging.SEAM, 'x', 'y', 'z'])
    expect('AttributeError')
    push.send_multipart([zeronimo.messaging.SEAM, 'x', 'y', 'z', 'w'])
    expect('AttributeError')
    push.send_multipart([zeronimo.messaging.SEAM, 'x', 'y', 'z',
                         'c__main__\nNOTEXIST\np0\n.'])
    expect('UnpicklingError')
    push.send_multipart([zeronimo.messaging.SEAM, 'zeronimo', 'y', 'z', 'w'])
    with warnings.catch_warnings(record=True) as w:
        gevent.sleep(0.1)
    assert len(w) == len(expectations)
    assert all(w_.category is zeronimo.MalformedMessage for w_ in w)
    for err, warn in zip(expectations, w):
        assert err in str(warn.message)


def test_malformed_message_handler(worker, push, capsys):
    messages = []
    def malformed_message_handler(worker, exc_info, msgs):
        messages.append(msgs)
        raise exc_info[0], exc_info[1], exc_info[2]
    worker.malformed_message_handler = malformed_message_handler
    push.send('Zeronimo!')
    worker.wait()
    out, err = capsys.readouterr()
    assert not worker.is_running()
    assert 'EOFError: no seam after topics' in err
    assert ['Zeronimo!'] in messages


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


def test_close_task(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    result = customer.call('sleep_multiple', 0.1, 10)
    g = result.get()
    assert next(g) == 0
    assert next(g) == 1
    result.close()
    with pytest.raises(zeronimo.TaskClosed):
        next(g)
    if zmq_version_info == (2, 1, 4):
        worker.join()
        gevent.sleep(0.1)


def _test_duplex(socket, worker_socket, customer_and_collector_socket):
    worker = zeronimo.Worker(Application(), [worker_socket])
    collector = zeronimo.Collector(customer_and_collector_socket)
    customer = zeronimo.Customer(customer_and_collector_socket, collector)
    with running([worker, collector]):
        assert customer.call('zeronimo').get() == 'zeronimo'
        assert ' '.join(customer.call('rycbar123').get()) == \
               'run, you clever boy; and remember.'
        with pytest.raises(ZeroDivisionError):
            customer.call('zero_div').get()


def test_pair(socket, addr):
    ws = socket(zmq.PAIR)
    cs = socket(zmq.PAIR)
    ws.bind(addr)
    cs.connect(addr)
    _test_duplex(socket, ws, cs)


def test_router_dealer(socket, addr):
    ws = socket(zmq.ROUTER)
    cs = socket(zmq.DEALER)
    ws.bind(addr)
    cs.connect(addr)
    _test_duplex(socket, ws, cs)


def test_proxied_router_dealer(socket, addr1, addr2, device):
    s1 = socket(zmq.ROUTER)
    s2 = socket(zmq.DEALER)
    s3 = socket(zmq.ROUTER)
    s4 = socket(zmq.DEALER)
    s1.bind(addr1)
    s2.connect(addr1)
    s3.bind(addr2)
    s4.connect(addr2)
    device(s2, s3)
    _test_duplex(socket, s1, s4)


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
    paylaod = zeronimo.messaging.PACK(((), {}))
    parts = [zeronimo.messaging.SEAM, 'zeronimo', uuid4_bytes(), '', paylaod]
    class fake_socket(object):
        type = zmq.PULL
        x = 0
        def recv(self, *args, **kwargs):
            x = self.x
            self.x = (self.x + 1) % len(parts)
            return parts[x]
        def recv_multipart(self, *args, **kwargs):
            x = self.x % len(parts)
            self.x = 0
            return parts[x:]
        def getsockopt(self, *args, **kwargs):
            return self.x != len(parts) - 1
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
    assert all(s.closed for s in worker.sockets)
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


@pytest.mark.parametrize('left_type, right_type', [
    (zmq.PAIR, zmq.PAIR), (zmq.PULL, zmq.PUSH), (zmq.ROUTER, zmq.DEALER)] +
    # XSUB before zmq-4 didn't allow arbitrary messages to send.
    ([(zmq.XPUB, zmq.XSUB)] if zmq_version_info >= (4,) else []),
    ids=socket_type_name)
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
    packed = []
    def pack(x):
        packed.append(x)
        return zeronimo.messaging.PACK(x)
    # Without drop_if.
    fanout_without_drop_if = zeronimo.Fanout(pub, collector, pack=pack)
    assert list(fanout_without_drop_if.emit(topic, 'zeronimo'))
    assert len(packed) == 1
    assert not list(fanout_without_drop_if.emit(topic[::-1], 'zeronimo'))
    assert len(packed) == 2
    # With drop_if.
    fanout = zeronimo.Fanout(pub, collector, pack=pack,
                             drop_if=lambda x: x != topic)
    assert get_results(fanout.emit(topic, 'zeronimo')) == ['zeronimo']
    assert len(packed) == 3
    assert get_results(fanout.emit(topic[::-1], 'zeronimo')) == []
    assert len(packed) == 3  # not increased.


@require_libzmq((3,))
def test_drop_not_subscribed_topic(socket, worker, collector, addr, topic):
    xpub = socket(zmq.XPUB)
    xpub.bind(addr)
    sub = [s for s in worker.sockets if s.type == zmq.SUB][0]
    sub.connect(addr)
    msg = xpub.recv()
    sync_pubsub(xpub, [sub], topic)
    assert msg.startswith(b'\x01')
    found_topic = msg[1:]
    assert found_topic == topic
    packed = []
    def pack(x):
        packed.append(x)
        return zeronimo.messaging.PACK(x)
    fanout = zeronimo.Fanout(xpub, collector, pack=pack,
                             drop_if=lambda x: x != found_topic)
    assert get_results(fanout.emit(topic[::-1], 'zeronimo')) == []
    assert len(packed) == 0
    assert get_results(fanout.emit(topic, 'zeronimo')) == ['zeronimo']
    assert len(packed) == 1


def test_timeout(worker, push, collector):
    worker.stop()
    t = time.time()
    with pytest.raises(zeronimo.WorkerNotFound):
        zeronimo.Customer(push, collector, timeout=0.1).call('zeronimo').get()
    assert 0.1 <= time.time() - t <= 0.2
    t = time.time()
    with pytest.raises(zeronimo.WorkerNotFound):
        zeronimo.Customer(push, collector, timeout=0.5).call('zeronimo').get()
    assert 0.5 <= time.time() - t <= 0.6


def test_max_retries_option(push):
    assert zeronimo.Customer(push).max_retries is None
    assert zeronimo.Customer(push, max_retries=999).max_retries == 999


@pytest.mark.parametrize('left_type, right_type', [
    (zmq.PAIR, zmq.PAIR), (zmq.PUSH, zmq.PULL)] +
    # XSUB before zmq-4 didn't allow arbitrary messages to send.
    ([(zmq.XSUB, zmq.XPUB)] if zmq_version_info >= (4,) else []),
    ids=socket_type_name)
def test_fanout_by_other_types(left_type, right_type, socket, addr1, addr2):
    fanout_sock = socket(left_type)
    worker_sock = socket(right_type)
    worker_sock.bind(addr1)
    fanout_sock.connect(addr1)
    worker_pub, collector_sub = socket(zmq.PUB), socket(zmq.SUB)
    worker_pub.bind(addr2)
    collector_sub.connect(addr2)
    collector_sub.set(zmq.SUBSCRIBE, 'xxx')
    sync_pubsub(worker_pub, [collector_sub], 'xxx')
    worker = zeronimo.Worker(Application(), [worker_sock], worker_pub)
    collector = zeronimo.Collector(collector_sub, 'xxx')
    fanout = zeronimo.Fanout(fanout_sock, collector)
    with running([worker]):
        assert get_results(fanout.emit('anything', 'zeronimo')) == ['zeronimo']


def test_worker_releases_call(worker, push, collector):
    customer = zeronimo.Customer(push, collector)
    customer.call('zeronimo').get()
    assert not find_objects(zeronimo.messaging.Call)


def test_unicode(worker, push, collector):
    customer = zeronimo.Customer(push, collector)
    assert customer.call(u'zeronimo').get() == 'zeronimo'


def test_hints(worker, pub, collector, topic):
    fanout = zeronimo.Fanout(pub, collector)
    r = fanout.emit(topic, ['hello', 'world'], 'zeronimo')
    assert get_results(r) == ['zeronimo']
    worker.reject_if = lambda call, topics: 'reject' in call.hints
    r = fanout.emit(topic, ['hello', 'world'], 'zeronimo')
    assert get_results(r) == ['zeronimo']
    r = fanout.emit(topic, ['reject'], 'zeronimo')
    assert get_results(r) == []
    r = fanout.emit(topic, ['foo', 'reject', 'bar'], 'zeronimo')
    assert get_results(r) == []
    r = fanout.emit(topic, ['hello', 'world'], 'hints')
    assert get_results(r) == [('hello', 'world')]


def test_raw(worker, push, pub, collector, topic):
    customer = zeronimo.Customer(push, collector)
    fanout = zeronimo.Fanout(pub, collector)
    payload = zeronimo.messaging.PACK(((), {'hello': 'world'}))
    assert customer.call_raw('kwargs', payload).get() == {'hello': 'world'}
    assert \
        get_results(fanout.emit_raw(topic, 'kwargs', payload)) == \
        [{'hello': 'world'}]
    with pytest.raises(TypeError):
        customer.call_raw('kwargs')
    with pytest.raises(TypeError):
        customer.call_raw('kwargs', payload=payload)
    with pytest.raises(TypeError):
        customer.call_raw('kwargs', payload, payload)


def test_specific_reject_if(worker, push, collector):
    customer = zeronimo.Customer(push, collector, timeout=0.1)
    assert customer.call('reject_by_hints').get() == 'accepted'
    assert customer.call('reject_by_hints_staticmethod').get() == 'accepted'
    assert customer.call('reject_by_hints_classmethod').get() == 'accepted'
    unpack_called = []
    def unpack(*args, **kwargs):
        unpack_called.append(True)
        return zeronimo.messaging.UNPACK(*args, **kwargs)
    worker.unpack = unpack
    assert not unpack_called
    with pytest.raises(Rejected):
        customer.call(['reject'], 'reject_by_hints').wait()
    assert not unpack_called
    with pytest.raises(Rejected):
        customer.call(['reject'], 'reject_by_hints_staticmethod').wait()
    assert not unpack_called
    with pytest.raises(Rejected):
        customer.call(['reject'], 'reject_by_hints_classmethod').wait()
    assert not unpack_called
    with pytest.raises(Rejected):
        customer.call('reject_by_odd_even').wait()
    assert unpack_called


def test_require_rpc_specs(worker, push, collector):
    worker.require_rpc_specs = True
    customer = zeronimo.Customer(push, collector, timeout=0.1)
    with pytest.raises(Rejected):
        customer.call('add', 0, 42).wait()
    assert customer.call('zeronimo').get() == 'zeronimo'
    with pytest.raises(Rejected):
        customer.call('_zeronimo').wait()


def test_trace(worker, push, collector):
    # Log traced events.
    traced = []
    def trace(method, args):
        traced.append((method,) + args)
    collector.trace = trace
    customer = zeronimo.Customer(push, collector, timeout=0.1)
    customer.call('zeronimo').get()
    assert len(traced) == 3
    call_id = traced[0][1]
    assert traced[0] == (0, call_id, customer, 'zeronimo', (), {})
    assert traced[1][:2] == (zeronimo.messaging.ACCEPT, call_id)
    reply_id = traced[1][2]
    assert traced[1][3] == worker.info
    assert traced[2] == (zeronimo.messaging.RETURN,
                         call_id, reply_id, 'zeronimo')
    # Trace RPC execution time.
    def trace_time(mem, method, args):
        if method == 0:
            mem.called_at = time.time()
        elif method == zeronimo.messaging.RETURN:
            mem.returned_at = time.time()
    mem = type('', (), {})()
    collector.trace = lambda *x: trace_time(mem, *x)
    customer.call('sleep', 0.1).get()
    assert 0.1 <= mem.returned_at - mem.called_at < 0.2


def test_raise_remote_exception(worker, push, collector):
    customer = zeronimo.Customer(push, collector, timeout=0.1)
    try:
        customer.call('raise_remote_value_error', 'zeronimo').get()
    except BaseException as exc:
        pass
    else:
        assert False, 'not raised'
    assert isinstance(exc, ValueError)
    assert isinstance(exc, zeronimo.RemoteException)
    assert exc.args[0] == 'zeronimo'


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

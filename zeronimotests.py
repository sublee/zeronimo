# -*- coding: utf-8 -*-
import time
import warnings
import weakref

import gevent
from gevent import joinall, spawn
import pytest
import zmq.green as zmq

from conftest import (
    Application, link_sockets, run_device, running, sync_pubsub)
import zeronimo


warnings.simplefilter('always')


def test_running():
    from zeronimo.components import Component
    class NullRunner(Component):
        def run(self):
            gevent.sleep(0.1)
    runner = NullRunner()
    assert not runner.is_running()
    runner.start()
    assert runner.is_running()
    runner.wait()
    assert not runner.is_running()


def test_messaging(ctx, addr, topic):
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    link_sockets(addr, push, [pull])
    for t in [None, topic]:
        zeronimo.send(push, 1, topic=t)
        assert zeronimo.recv(pull) == 1
        zeronimo.send(push, 'doctor', topic=t)
        assert zeronimo.recv(pull) == 'doctor'
        zeronimo.send(push, {'doctor': 'who'}, topic=t)
        assert zeronimo.recv(pull) == {'doctor': 'who'}
        zeronimo.send(push, ['doctor', 'who'], topic=t)
        assert zeronimo.recv(pull) == ['doctor', 'who']
        zeronimo.send(push, Exception, topic=t)
        assert zeronimo.recv(pull) == Exception
        zeronimo.send(push, Exception('Allons-y'), topic=t)
        assert isinstance(zeronimo.recv(pull), Exception)


def test_from_socket(ctx, addr1, addr2):
    # sockets
    worker_sock = ctx.socket(zmq.PULL)
    worker_sock.bind(addr1)
    collector_sock = ctx.socket(zmq.PULL)
    collector_sock.bind(addr2)
    push = ctx.socket(zmq.PUSH)
    push.connect(addr1)
    # components
    app = Application()
    worker = zeronimo.Worker(app, [worker_sock])
    collector = zeronimo.Collector(collector_sock, addr2)
    customer = zeronimo.Customer(push, collector)
    with running([worker], sockets=[worker_sock, collector_sock, push]):
        # test
        assert customer.zeronimo() == 'zeronimo'


def test_socket_type_error(ctx):
    # customer
    with pytest.raises(ValueError):
        zeronimo.Customer(ctx.socket(zmq.SUB))
    with pytest.raises(ValueError):
        zeronimo.Customer(ctx.socket(zmq.REQ))
    with pytest.raises(ValueError):
        zeronimo.Customer(ctx.socket(zmq.REP))
    with pytest.raises(ValueError):
        zeronimo.Customer(ctx.socket(zmq.DEALER))
    with pytest.raises(ValueError):
        zeronimo.Customer(ctx.socket(zmq.ROUTER))
    with pytest.raises(ValueError):
        zeronimo.Customer(ctx.socket(zmq.PULL))
    # worker
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [ctx.socket(zmq.PUB)])
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [ctx.socket(zmq.REQ)])
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [ctx.socket(zmq.REP)])
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [ctx.socket(zmq.DEALER)])
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [ctx.socket(zmq.ROUTER)])
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [ctx.socket(zmq.PUSH)])
    # collector
    with pytest.raises(ValueError):
        zeronimo.Collector(ctx.socket(zmq.PUB), 'x')
    with pytest.raises(ValueError):
        zeronimo.Collector(ctx.socket(zmq.SUB), 'x')
    with pytest.raises(ValueError):
        zeronimo.Collector(ctx.socket(zmq.REQ), 'x')
    with pytest.raises(ValueError):
        zeronimo.Collector(ctx.socket(zmq.REP), 'x')
    with pytest.raises(ValueError):
        zeronimo.Collector(ctx.socket(zmq.DEALER), 'x')
    with pytest.raises(ValueError):
        zeronimo.Collector(ctx.socket(zmq.ROUTER), 'x')
    with pytest.raises(ValueError):
        zeronimo.Collector(ctx.socket(zmq.PUSH), 'x')


@pytest.mark.skipif('zmq.zmq_version_info() < (3,)')
def test_xpubsub_type_error(ctx):
    # XPUB/XSUB is available from libzmq-3
    with pytest.raises(ValueError):
        zeronimo.Customer(ctx.socket(zmq.XSUB))
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [ctx.socket(zmq.XPUB)])
    with pytest.raises(ValueError):
        zeronimo.Collector(ctx.socket(zmq.XSUB), 'x')


@pytest.mark.skipif('zmq.zmq_version_info() < (4, 0, 1)')
def test_stream_type_error(ctx):
    # zmq.STREAM is available from libzmq-4.0.1
    with pytest.raises(ValueError):
        zeronimo.Customer(ctx.socket(zmq.STREAM))
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [ctx.socket(zmq.STREAM)])
    with pytest.raises(ValueError):
        zeronimo.Collector(ctx.socket(zmq.STREAM), 'x')


def test_fixtures(worker, push, pub, collector, addr1, addr2, ctx):
    assert isinstance(worker, zeronimo.Worker)
    assert len(worker.sockets) == 2
    assert push.type == zmq.PUSH
    assert pub.type == zmq.PUB
    assert isinstance(collector, zeronimo.Collector)
    assert addr1 != addr2
    assert isinstance(ctx, zmq.Context)
    assert worker.is_running()
    assert collector.is_running()


def test_nowait(worker, push):
    customer = zeronimo.Customer(push)
    assert worker.obj.counter['zeronimo'] == 0
    for x in xrange(5):
        assert customer.zeronimo() is None
    assert worker.obj.counter['zeronimo'] == 0
    gevent.sleep(0.01)
    assert worker.obj.counter['zeronimo'] == 5


def test_fanout_nowait(worker, worker2, worker3, worker4, worker5, pub, topic):
    customer = zeronimo.Customer(pub)
    assert worker.obj.counter['zeronimo'] == 0
    assert customer[topic].zeronimo() is None
    assert worker.obj.counter['zeronimo'] == 0
    gevent.sleep(0.01)
    assert worker.obj.counter['zeronimo'] == 5


def test_return(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    assert customer.zeronimo() == 'zeronimo'
    assert customer.add(100, 200) == 300
    assert customer.add('100', '200') == '100200'


def test_yield(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    assert ' '.join(customer.rycbar123()) == \
           'run, you clever boy; and remember.'
    assert list(customer.dont_yield()) == []


def test_raise(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    with pytest.raises(ZeroDivisionError):
        customer.zero_div()
    g = customer.rycbar123_and_zero_div()
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
    assert list(customer.xrange(1, 100, 10)) == range(1, 100, 10)
    assert set(customer.dict_view(1, 100, 10)) == set(range(1, 100, 10))


def test_fanout_return(worker1, worker2, collector, pub, topic):
    customer = zeronimo.Customer(pub, collector)
    assert customer[topic].zeronimo() == ['zeronimo', 'zeronimo']
    assert customer[topic].add(100, 200) == [300, 300]
    assert customer[topic].add('100', '200') == ['100200', '100200']


def test_fanout_yield(worker1, worker2, collector, pub, topic):
    customer = zeronimo.Customer(pub, collector)
    for g in customer[topic].rycbar123():
        assert next(g) == 'run,'
        assert next(g) == 'you'
        assert next(g) == 'clever'
        assert next(g) == 'boy;'
        assert next(g) == 'and'
        assert next(g) == 'remember.'


def test_fanout_raise(worker1, worker2, collector, pub, topic):
    customer = zeronimo.Customer(pub, collector)
    with pytest.raises(ZeroDivisionError):
        customer[topic].zero_div()


def test_2to1(worker, collector, push1, push2):
    customer1 = zeronimo.Customer(push1, collector)
    customer2 = zeronimo.Customer(push2, collector)
    def test(customer):
        assert customer.add(1, 1) == 2
        assert len(list(customer.rycbar123())) == 6
        with pytest.raises(ZeroDivisionError):
            customer.zero_div()
    joinall([spawn(test, customer1), spawn(test, customer2)], raise_error=True)


def test_1to2(worker1, worker2, task_collector, push):
    customer = zeronimo.Customer(push, task_collector)
    task1 = customer.add(1, 1)
    task2 = customer.add(2, 2)
    assert task1() == 2
    assert task2() == 4
    assert task1.worker_info != task2.worker_info


def test_slow(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    with pytest.raises(gevent.Timeout):
        with gevent.Timeout(0.1):
            customer.sleep(0.3)
        t = time.time()
        assert customer.sleep(0.1) == 0.1
        assert time.time() - t >= 0.1


def test_reject(worker1, worker2, task_collector, push, pub, topic):
    customer = zeronimo.Customer(push, task_collector)
    fanout_customer = zeronimo.Customer(pub, task_collector)[topic]
    assert worker1.accepting
    assert worker2.accepting
    assert len(fanout_customer.zeronimo()) == 2
    worker2.reject_all()
    assert not worker2.accepting
    assert customer.zeronimo().worker_info == worker1.info
    assert customer.zeronimo().worker_info == worker1.info
    assert len(fanout_customer.zeronimo()) == 1
    worker1.reject_all()
    assert not worker1.accepting
    with pytest.raises(zeronimo.WorkerNotFound):
        customer.zeronimo()
    assert fanout_customer.zeronimo() == []
    worker1.accept_all()
    worker2.accept_all()
    assert worker1.accepting
    assert worker2.accepting
    assert len(fanout_customer.zeronimo()) == 2


def test_subscription(worker1, worker2, collector, pub, topic):
    fanout_customer = zeronimo.Customer(pub, collector)[topic]
    sub1 = [sock for sock in worker1.sockets if sock.socket_type == zmq.SUB][0]
    sub2 = [sock for sock in worker2.sockets if sock.socket_type == zmq.SUB][0]
    sub1.set(zmq.UNSUBSCRIBE, topic)
    assert len(fanout_customer.zeronimo()) == 1
    sub2.set(zmq.UNSUBSCRIBE, topic)
    assert fanout_customer.zeronimo() == []
    worker1.stop()
    sub1.set(zmq.SUBSCRIBE, topic)
    sync_pubsub(pub, [sub1], topic)
    worker1.start()
    assert len(fanout_customer.zeronimo()) == 1
    worker1.stop()
    worker2.stop()
    sub2.set(zmq.SUBSCRIBE, topic)
    sync_pubsub(pub, [sub2], topic)
    worker1.start()
    worker2.start()
    assert len(fanout_customer.zeronimo()) == 2


def test_device(ctx, collector, topic, addr1, addr2, addr3, addr4):
    # customer  |-----| forwarder |---> | worker
    # collector | <---| streamer |------|
    try:
        # run streamer
        streamer_in_addr, streamer_out_addr = addr1, addr2
        forwarder_in_addr, forwarder_out_addr = addr3, addr4
        streamer = spawn(
            run_device, ctx.socket(zmq.PULL), ctx.socket(zmq.PUSH),
            streamer_in_addr, streamer_out_addr)
        streamer.join(0)
        # run forwarder
        sub = ctx.socket(zmq.SUB)
        sub.set(zmq.SUBSCRIBE, '')
        forwarder = spawn(
            run_device, sub, ctx.socket(zmq.PUB),
            forwarder_in_addr, forwarder_out_addr)
        forwarder.join(0)
        # connect to the devices
        worker_pull1 = ctx.socket(zmq.PULL)
        worker_pull2 = ctx.socket(zmq.PULL)
        worker_pull1.connect(streamer_out_addr)
        worker_pull2.connect(streamer_out_addr)
        worker_sub1 = ctx.socket(zmq.SUB)
        worker_sub2 = ctx.socket(zmq.SUB)
        worker_sub1.set(zmq.SUBSCRIBE, topic)
        worker_sub2.set(zmq.SUBSCRIBE, topic)
        worker_sub1.connect(forwarder_out_addr)
        worker_sub2.connect(forwarder_out_addr)
        push = ctx.socket(zmq.PUSH)
        push.connect(streamer_in_addr)
        pub = ctx.socket(zmq.PUB)
        pub.connect(forwarder_in_addr)
        sync_pubsub(pub, [worker_sub1, worker_sub2], topic)
        # make and start workers
        app = Application()
        worker1 = zeronimo.Worker(app, [worker_pull1, worker_sub1])
        worker2 = zeronimo.Worker(app, [worker_pull2, worker_sub2])
        worker1.start()
        worker2.start()
        # zeronimo!
        customer = zeronimo.Customer(push, collector)
        fanout_customer = zeronimo.Customer(pub, collector)[topic]
        assert customer.zeronimo() == 'zeronimo'
        assert fanout_customer.zeronimo() == ['zeronimo', 'zeronimo']
    finally:
        try:
            streamer.kill()
            forwarder.kill()
            push.close()
            pub.close()
            worker1.stop()
            worker2.stop()
        except UnboundLocalError:
            pass


@pytest.mark.skipif('zmq.zmq_version_info() < (3,)')
def test_x_forwarder(ctx, collector, topic, addr1, addr2):
    # XPUB/XSUB is available from libzmq-3
    # customer  |----| forwarder with XPUB/XSUB |---> | worker
    # collector | <-----------------------------------|
    # run forwarder
    forwarder_in_addr, forwarder_out_addr = addr1, addr2
    forwarder = spawn(
        run_device, ctx.socket(zmq.XSUB), ctx.socket(zmq.XPUB),
        forwarder_in_addr, forwarder_out_addr)
    try:
        forwarder.join(0)
        # connect to the devices
        worker_sub1 = ctx.socket(zmq.SUB)
        worker_sub2 = ctx.socket(zmq.SUB)
        worker_sub1.set(zmq.SUBSCRIBE, topic)
        worker_sub2.set(zmq.SUBSCRIBE, topic)
        worker_sub1.connect(forwarder_out_addr)
        worker_sub2.connect(forwarder_out_addr)
        pub = ctx.socket(zmq.PUB)
        pub.connect(forwarder_in_addr)
        sync_pubsub(pub, [worker_sub1, worker_sub2], topic)
        # make and start workers
        app = Application()
        worker1 = zeronimo.Worker(app, [worker_sub1])
        worker2 = zeronimo.Worker(app, [worker_sub2])
        with running([worker1, worker2], sockets=[pub]):
            # zeronimo!
            fanout_customer = zeronimo.Customer(pub, collector)[topic]
            assert fanout_customer.zeronimo() == ['zeronimo', 'zeronimo']
    finally:
        forwarder.kill()


def test_proxied_collector(ctx, worker, push, addr1, addr2):
    # customer  |-------------------> | worker
    # collector | <---| streamer |----|
    try:
        streamer = spawn(
            run_device, ctx.socket(zmq.PULL), ctx.socket(zmq.PUSH),
            addr1, addr2)
        streamer.join(0)
        collector_sock = ctx.socket(zmq.PULL)
        collector_sock.connect(addr2)
        collector = zeronimo.Collector(collector_sock, addr1)
        customer = zeronimo.Customer(push, collector)
        assert customer.zeronimo() == 'zeronimo'
    finally:
        try:
            streamer.kill()
            collector.stop()
            collector_sock.close()
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
    fanout_customer = zeronimo.Customer(pub, collector)[topic]
    done = []
    def do_test():
        assert customer.zeronimo() == 'zeronimo'
        assert fanout_customer.zeronimo() == ['zeronimo']
        done.append(True)
    times = 5
    joinall([spawn(do_test) for x in xrange(times)], raise_error=True)
    assert len(done) == times


def test_stopped_collector(worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    collector.stop()
    assert not collector.is_running()
    assert customer.zeronimo() == 'zeronimo'
    assert collector.is_running()
    collector.stop()


def test_unreachable(worker, collector, push, pub, topic):
    customer = zeronimo.Customer(push, collector)
    fanout_customer = zeronimo.Customer(pub, collector)[topic]
    customer_nowait = zeronimo.Customer(push)
    worker.stop()
    for sock in worker.sockets:
        sock.close()
    with gevent.Timeout(1):
        with pytest.raises(zeronimo.WorkerNotFound):
            customer.zeronimo()
        assert fanout_customer.zeronimo() == []
        assert customer_nowait.zeronimo() is None


def test_expected_exception(capsys, worker, collector, push):
    customer = zeronimo.Customer(push, collector)
    with pytest.raises(ZeroDivisionError):
        customer.zero_div()
    out, err = capsys.readouterr()
    assert 'Traceback (most recent call last)' in err
    assert 'ZeroDivisionError' in err
    with pytest.raises(ZeroDivisionError):
        customer.ignore_exc(ZeroDivisionError, ValueError)
    out, err = capsys.readouterr()
    assert 'Traceback (most recent call last)' in err
    assert 'ZeroDivisionError' in err
    with pytest.raises(ZeroDivisionError):
        customer.ignore_exc(ZeroDivisionError, ZeroDivisionError)
    out, err = capsys.readouterr()
    assert not err
    with pytest.raises(ZeroDivisionError):
        customer.ignore_exc(ZeroDivisionError, (ArithmeticError, ValueError))
    out, err = capsys.readouterr()
    assert not err


def test_pgm_connect(ctx, fanout_addr):
    if 'pgm' not in fanout_addr[:4]:
        pytest.skip()
    sub = ctx.socket(zmq.SUB)
    sub.set(zmq.SUBSCRIBE, '')
    sub.connect(fanout_addr)
    worker = zeronimo.Worker(None, [sub])
    worker.start()
    pub1 = ctx.socket(zmq.PUB)
    pub1.connect(fanout_addr)
    pub2 = ctx.socket(zmq.PUB)
    pub2.connect(fanout_addr)  # in zmq-3.2, pgm-connect sends an empty message
    try:
        worker.wait(0.1)
    finally:
        worker.stop()
        sub.close()
        pub1.close()
        pub2.close()


def test_malformed_message(worker, push):
    push.send('Zeronimo!')  # indicates to ExtraData
    push.send('')  # indicates to UnpackValueError
    with warnings.catch_warnings(record=True) as w:
        gevent.sleep(0.1)
    assert len(w) == 2
    assert w[0].category is zeronimo.MalformedMessage
    assert w[1].category is zeronimo.MalformedMessage
    assert w[0].message.message == 'Zeronimo!'
    assert w[1].message.message == ''


def test_queue_leaking(worker, task_collector, push):
    customer = zeronimo.Customer(push, task_collector)
    task = customer.zeronimo()
    reply_queue_ref = weakref.ref(task.reply_queue)
    assert task_collector.reply_queues
    assert task_collector.missing_queues
    assert task() == 'zeronimo'
    assert not task_collector.reply_queues
    assert not task_collector.missing_queues
    del task
    assert reply_queue_ref() is None


def test_task_broken(worker, task_collector, push):
    customer = zeronimo.Customer(push, task_collector)
    task = customer.sleep(0.1)
    task_collector.socket.close()
    with pytest.raises(zeronimo.SocketClosed):
        task()


def test_pair(ctx, addr):
    left = ctx.socket(zmq.PAIR)
    right = ctx.socket(zmq.PAIR)
    left.bind(addr)
    right.connect(addr)
    worker = zeronimo.Worker(Application(), [left])
    with pytest.raises(ValueError):  # pair collector doesn't need an address
        zeronimo.Collector(right, addr)
    collector = zeronimo.Collector(right)
    customer = zeronimo.Customer(right, collector)
    with running([worker], sockets=[left, right]):
        assert customer.zeronimo() == 'zeronimo'
        assert ' '.join(customer.rycbar123()) == \
               'run, you clever boy; and remember.'
        with pytest.raises(ZeroDivisionError):
            customer.zero_div()


def test_initial_topic(pub):
    customer = zeronimo.Customer(pub, topic='zeronimo')
    assert customer._znm_topic == 'zeronimo'


@pytest.mark.skipif('zmq.zmq_version_info() < (3,)')
# XPUB/XSUB is available from libzmq-3
def test_direct_xpub_xsub(ctx, addr1, addr2):
    worker_sock = ctx.socket(zmq.XSUB)
    worker_sock.bind(addr1)
    collector_sock = ctx.socket(zmq.PULL)
    collector_sock.bind(addr2)
    xpub = ctx.socket(zmq.XPUB)
    xpub.connect(addr1)
    # components
    app = Application()
    worker = zeronimo.Worker(app, [worker_sock])
    collector = zeronimo.Collector(collector_sock, addr2)
    customer = zeronimo.Customer(xpub, collector)
    with running([worker], sockets=[worker_sock, collector_sock, xpub]):
        assert customer.zeronimo() == []
        # subscribe ''
        worker_sock.send('\x01')
        assert xpub.recv() == '\x01'
        assert customer.zeronimo() == ['zeronimo']
        # unsubscribe ''
        worker_sock.send('\x00')
        assert xpub.recv() == '\x00'
        assert customer.zeronimo() == []
        # subscribe 'zeronimo'
        worker_sock.send('\x01zeronimo')
        assert xpub.recv() == '\x01zeronimo'
        assert customer['zeronimo'].zeronimo() == ['zeronimo']


def test_marshal_message(ctx, addr1, addr2):
    import marshal
    pack = marshal.dumps
    unpack = marshal.loads
    # sockets
    worker_sock = ctx.socket(zmq.PULL)
    worker_sock.bind(addr1)
    customer_sock = ctx.socket(zmq.PUSH)
    customer_sock.connect(addr1)
    collector_sock = ctx.socket(zmq.PULL)
    collector_sock.bind(addr2)
    sockets = [worker_sock, collector_sock, customer_sock]
    # components
    app = Application()
    worker = zeronimo.Worker(app, [worker_sock], pack=pack, unpack=unpack)
    collector = zeronimo.Collector(collector_sock, addr2, unpack=unpack)
    customer = zeronimo.Customer(customer_sock, collector, pack=pack)
    with running([worker], sockets=sockets):
        assert customer.zeronimo() == 'zeronimo'

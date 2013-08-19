# -*- coding: utf-8 -*-
import time

import gevent
from gevent import joinall, spawn
import pytest
import zmq.green as zmq

from conftest import Application, link_sockets, run_device, sync_pubsub
import zeronimo


def test_running():
    from zeronimo.components import Runnable
    class NullRunner(Runnable):
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
    try:
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
        worker.start()
        collector = zeronimo.Collector(collector_sock, addr2)
        customer = zeronimo.Customer(push, collector)
        # test
        assert customer.zeronimo() == 'zeronimo'
    finally:
        try:
            collector.stop()
            collector_sock.close()
            push.close()
            worker.stop()
            worker_sock.close()
        except UnboundLocalError:
            pass


def test_socket_type_error(ctx):
    with pytest.raises(ValueError):
        zeronimo.Customer(ctx.socket(zmq.PAIR))
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [ctx.socket(zmq.PAIR)])
    with pytest.raises(ValueError):
        zeronimo.Collector(ctx.socket(zmq.PAIR), 'x')


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
    with pytest.raises(zeronimo.WorkerNotFound):
        fanout_customer.zeronimo()
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
    with pytest.raises(zeronimo.WorkerNotFound):
        fanout_customer.zeronimo()
    worker1.stop()
    sub1.set(zmq.SUBSCRIBE, topic)
    sync_pubsub(pub, [sub1], topic)
    worker1.start()
    assert len(fanout_customer.zeronimo()) == 1
    worker1.stop()
    worker2.stop()
    sub2.set(zmq.SUBSCRIBE, topic)
    sync_pubsub(pub, [sub1, sub2], topic)
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


@pytest.mark.xfail('zmq.zmq_version_info() < (3, 2)')  # zmq<3.2 should fail
def test_x_forwarder(ctx, collector, topic, addr1, addr2):
    # customer  |----| forwarder with XPUB/XSUB |---> | worker
    # collector | <-----------------------------------|
    try:
        # run forwarder
        forwarder_in_addr, forwarder_out_addr = addr1, addr2
        forwarder = spawn(
            run_device, ctx.socket(zmq.XSUB), ctx.socket(zmq.XPUB),
            forwarder_in_addr, forwarder_out_addr)
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
        worker1.start()
        worker2.start()
        # zeronimo!
        fanout_customer = zeronimo.Customer(pub, collector)[topic]
        assert fanout_customer.zeronimo() == ['zeronimo', 'zeronimo']
    finally:
        try:
            forwarder.kill()
            pub.close()
            worker1.stop()
            worker2.stop()
        except UnboundLocalError:
            pass


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
        with pytest.raises(zeronimo.WorkerNotFound):
            fanout_customer.zeronimo()
        assert customer_nowait.zeronimo() is None


def test_exc_logs(capsys, worker, collector, push):
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

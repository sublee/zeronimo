# -*- coding: utf-8 -*-
import functools
import os

import gevent
from gevent import joinall, spawn, Timeout
import pytest
import zmq.green as zmq

from conftest import (
    app, autowork, ctx, green, run_device, sync_pubsub,
    patch_worker_to_be_slow)
import zeronimo


def test_running():
    class MockRunner(zeronimo.Runner):
        def __init__(self, sleep=None):
            self.sleep = sleep
        def run(self, stopper):
            assert not stopper.is_set()
            assert self.is_running()
            if self.sleep is not None:
                gevent.sleep(self.sleep)
    mock_runner = MockRunner()
    assert not mock_runner.is_running()
    mock_runner.run()
    assert not mock_runner.is_running()
    sleeping_mock_runner = MockRunner(0.01)
    assert not sleeping_mock_runner.is_running()
    sleeping_mock_runner.start()
    assert sleeping_mock_runner.is_running()
    sleeping_mock_runner.join()
    assert not sleeping_mock_runner.is_running()
    spawn(sleeping_mock_runner.run)
    with pytest.raises(RuntimeError):
        sleeping_mock_runner.join()
    sleeping_mock_runner.wait()


@autowork
def test_basic_zeronimo():
    # prepare sockets
    prefix = 'test'
    worker_pull = ctx.socket(zmq.PULL)
    worker_sub = ctx.socket(zmq.SUB)
    customer_pull = ctx.socket(zmq.PULL)
    tunnel_push = ctx.socket(zmq.PUSH)
    tunnel_pub = ctx.socket(zmq.PUB)
    worker_pull.bind('inproc://worker')
    worker_sub.bind('inproc://worker-fanout')
    customer_pull.bind('inproc://customer')
    tunnel_push.connect('inproc://worker')
    tunnel_pub.connect('inproc://worker-fanout')
    worker_sub.set(zmq.SUBSCRIBE, prefix)
    sync_pubsub(tunnel_pub, [worker_sub], prefix)
    # prepare runners
    worker = zeronimo.Worker(app, [worker_pull, worker_sub])
    customer = zeronimo.Customer(customer_pull, 'inproc://customer')
    worker.start()
    customer.start()
    yield autowork.will_stop(worker)
    yield autowork.will_stop(customer)
    yield autowork.will_close(tunnel_push)
    yield autowork.will_close(tunnel_pub)
    # zeronimo!
    tunnel = customer.link([tunnel_push, tunnel_pub], 'test')
    assert tunnel.simple() == 'ok'
    assert tunnel(fanout=True).simple() == ['ok']


@autowork
def test_fixtures(customer, worker, tunnel_socks, prefix):
    tunnel = customer.link(tunnel_socks, prefix)
    assert tunnel.simple() == 'ok'
    assert tunnel(fanout=True).simple() == ['ok']


@autowork
def test_tunnel_context(customer, worker, tunnel_socks, prefix):
    assert not customer.tunnels
    with customer.link(tunnel_socks, prefix) as tunnel:
        assert tunnel.simple() == 'ok'
        assert tunnel(fanout=True).simple() == ['ok']


@autowork
def test_tunnel(customer, worker, tunnel_socks, prefix):
    assert len(customer.tunnels) == 0
    with customer.link(tunnel_socks, prefix) as tunnel:
        assert len(customer.tunnels) == 1
    assert len(customer.tunnels) == 0
    with customer.link(tunnel_socks, prefix) as tunnel1, \
         customer.link(tunnel_socks, prefix) as tunnel2:
        assert customer.is_running()
        assert len(customer.tunnels) == 2
    # should clean up
    assert not customer.is_running()
    assert len(customer.tunnels) == 0


@autowork
def test_return(customer, worker, tunnel_socks, prefix):
    with customer.link(tunnel_socks, prefix) as tunnel:
        assert tunnel.add(1, 1) == 'cutie'
        assert tunnel.add(2, 2) == 'cutie'
        assert tunnel.add(3, 3) == 'cutie'
        assert tunnel.add(4, 4) == 'cutie'
        assert tunnel.add(5, 5) == 'cutie'
        assert tunnel.add(6, 6) == 'xoxoxoxoxoxo cutie'
        assert tunnel.add(42, 12) == 54


@autowork
def test_yield(customer, worker, tunnel_socks, prefix):
    with customer.link(tunnel_socks, prefix) as tunnel:
        assert len(list(tunnel.jabberwocky())) == 4
        assert list(tunnel.xrange()) == [0, 1, 2, 3, 4]
        view = tunnel.dict_view()
        assert view.next() == 0
        assert view.next() == 1
        assert view.next() == 2
        assert view.next() == 3
        assert view.next() == 4
        with pytest.raises(StopIteration):
            view.next()
        assert list(tunnel.dont_yield()) == []


@autowork
def test_raise(customer, worker, tunnel_socks, prefix):
    with customer.link(tunnel_socks, prefix) as tunnel:
        with pytest.raises(ZeroDivisionError):
            tunnel.zero_div()
        rocket_launching = tunnel.launch_rocket()
        assert rocket_launching.next() == 3
        assert rocket_launching.next() == 2
        assert rocket_launching.next() == 1
        with pytest.raises(RuntimeError):
            rocket_launching.next()


@autowork
def test_2to1(customer1, customer2, worker, tunnel_socks, prefix):
    def test(tunnel):
        assert tunnel.add(1, 1) == 'cutie'
        assert len(list(tunnel.jabberwocky())) == 4
        with pytest.raises(ZeroDivisionError):
            tunnel.zero_div()
    with customer1.link(tunnel_socks, prefix) as tunnel1, \
         customer2.link(tunnel_socks, prefix) as tunnel2:
        joinall([spawn(test, tunnel1), spawn(test, tunnel2)])


@autowork
def test_1to2(customer, worker1, worker2, tunnel_socks, prefix):
    with customer.link(tunnel_socks, prefix, as_task=True) as tunnel:
        task1 = tunnel.add(1, 1)
        task2 = tunnel.add(2, 2)
        assert task1() == 'cutie'
        assert task2() == 'cutie'
        assert task1.worker_info != task2.worker_info


@autowork
def test_fanout(customer, worker1, worker2, tunnel_socks, prefix):
    with customer.link(tunnel_socks, prefix) as tunnel:
        assert list(tunnel.rycbar123()) == \
               'run, you clever boy; and remember.'.split()
        for rycbar123 in tunnel(fanout=True).rycbar123():
            assert next(rycbar123) == 'run,'
            assert next(rycbar123) == 'you'
            assert next(rycbar123) == 'clever'
            assert next(rycbar123) == 'boy;'
            assert next(rycbar123) == 'and'
            assert next(rycbar123) == 'remember.'
        with pytest.raises(ZeroDivisionError):
            tunnel(fanout=True).zero_div()
        failures = tunnel(fanout=True, as_task=True).zero_div()
        assert len(failures) == 2
        with pytest.raises(ZeroDivisionError):
            failures[0]()
        with pytest.raises(ZeroDivisionError):
            failures[1]()


@autowork
def test_slow(customer, worker, tunnel_socks, prefix):
    with customer.link(tunnel_socks, prefix) as tunnel:
        with pytest.raises(Timeout):
            with Timeout(0.1):
                tunnel.sleep()
        assert tunnel.sleep() == 'slept'


@autowork
def test_reject(customer, worker1, worker2, tunnel_socks, prefix):
    with customer.link(tunnel_socks, prefix) as tunnel:
        assert len(tunnel(fanout=True).simple()) == 2
        worker2.reject_all()
        assert tunnel(as_task=True).simple().worker_info == worker1.info
        assert tunnel(as_task=True).simple().worker_info == worker1.info
        assert len(tunnel(fanout=True).simple()) == 1
        worker1.reject_all()
        with pytest.raises(zeronimo.WorkerNotFound):
            assert tunnel(fanout=True).simple()
        worker1.accept_all()
        worker2.accept_all()
        assert len(tunnel(fanout=True).simple()) == 2


@autowork
def test_subscription(customer, worker1, worker2, tunnel_socks, prefix):
    sub1 = [sock for sock in worker1.sockets if sock.socket_type == zmq.SUB][0]
    sub2 = [sock for sock in worker2.sockets if sock.socket_type == zmq.SUB][0]
    with customer.link(tunnel_socks, prefix) as tunnel:
        assert len(tunnel(fanout=True).simple()) == 2
        sub1.set(zmq.UNSUBSCRIBE, prefix)
        assert len(tunnel(fanout=True).simple()) == 1
        sub2.set(zmq.UNSUBSCRIBE, prefix)
        with pytest.raises(zeronimo.WorkerNotFound):
            tunnel(fanout=True).simple()
        sub1.set(zmq.SUBSCRIBE, prefix)
        assert len(tunnel(fanout=True).simple()) == 1
        sub2.set(zmq.SUBSCRIBE, prefix)
        assert len(tunnel(fanout=True).simple()) == 2


@autowork
def test_offbeat(customer, worker1, worker2, tunnel_socks, prefix):
    # worker2 sleeps 0.2 seconds before accepting
    patch_worker_to_be_slow(worker2, delay=0.02)
    with customer.link(tunnel_socks, prefix) as tunnel:
        tasks = tunnel(fanout=True, as_task=True).sleep_range(0.01, 5)
        assert len(tasks) == 1
        list(tasks[0]())
        assert len(tasks) == 2
        generous = tunnel(fanout=True, as_task=True, finding_timeout=0.05)
        assert len(generous.simple()) == 2


@autowork
def test_device(customer1, customer2, prefix, addr1, addr2, addr3, addr4):
    # run devices
    streamer_in_addr, streamer_out_addr = addr1, addr2
    forwarder_in_addr, forwarder_out_addr = addr3, addr4
    streamer = spawn(
        run_device, ctx.socket(zmq.PULL), ctx.socket(zmq.PUSH),
        streamer_in_addr, streamer_out_addr)
    sub = ctx.socket(zmq.SUB)
    sub.set(zmq.SUBSCRIBE, '')
    forwarder = spawn(
        run_device, sub, ctx.socket(zmq.PUB),
        forwarder_in_addr, forwarder_out_addr)
    streamer.join(0)
    forwarder.join(0)
    yield autowork.will(streamer.kill, block=True)
    yield autowork.will(forwarder.kill, block=True)
    # connect to the devices
    worker_pull1 = ctx.socket(zmq.PULL)
    worker_pull2 = ctx.socket(zmq.PULL)
    worker_sub1 = ctx.socket(zmq.SUB)
    worker_sub2 = ctx.socket(zmq.SUB)
    tunnel_push = ctx.socket(zmq.PUSH)
    tunnel_pub = ctx.socket(zmq.PUB)
    worker_pull1.connect(streamer_out_addr)
    worker_pull2.connect(streamer_out_addr)
    worker_sub1.connect(forwarder_out_addr)
    worker_sub2.connect(forwarder_out_addr)
    tunnel_push.connect(streamer_in_addr)
    tunnel_pub.connect(forwarder_in_addr)
    worker_sub1.set(zmq.SUBSCRIBE, prefix)
    worker_sub2.set(zmq.SUBSCRIBE, prefix)
    sync_pubsub(tunnel_pub, [worker_sub1, worker_sub2], prefix)
    # make and start workers
    worker1 = zeronimo.Worker(app, [worker_pull1, worker_sub1])
    worker1.start()
    yield autowork.will_stop(worker1)
    worker2 = zeronimo.Worker(app, [worker_pull2, worker_sub2])
    worker2.start()
    yield autowork.will_stop(worker2)
    # zeronimo!
    with customer1.link([tunnel_push, tunnel_pub], prefix) as tunnel1, \
         customer2.link([tunnel_push, tunnel_pub], prefix) as tunnel2:
        assert tunnel1.simple() == 'ok'
        assert tunnel2.simple() == 'ok'
        assert tunnel1(fanout=True).simple() == ['ok', 'ok']
        assert tunnel2(fanout=True).simple() == ['ok', 'ok']


@pytest.mark.xfail('zmq.zmq_version_info() < (3, 2)')
@autowork
def test_forwarder(customer1, customer2, prefix, addr1, addr2):
    # run devices
    forwarder_in_addr, forwarder_out_addr = addr1, addr2
    forwarder = spawn(
        run_device, ctx.socket(zmq.XSUB), ctx.socket(zmq.XPUB),
        forwarder_in_addr, forwarder_out_addr)
    forwarder.join(0)
    yield autowork.will(forwarder.kill, block=True)
    # connect to the devices
    worker_sub1 = ctx.socket(zmq.SUB)
    worker_sub2 = ctx.socket(zmq.SUB)
    tunnel_pub = ctx.socket(zmq.PUB)
    worker_sub1.connect(forwarder_out_addr)
    worker_sub2.connect(forwarder_out_addr)
    tunnel_pub.connect(forwarder_in_addr)
    worker_sub1.set(zmq.SUBSCRIBE, prefix)
    worker_sub2.set(zmq.SUBSCRIBE, prefix)
    sync_pubsub(tunnel_pub, [worker_sub1, worker_sub2], prefix)
    # make and start workers
    worker1 = zeronimo.Worker(app, [worker_sub1])
    worker1.start()
    yield autowork.will_stop(worker1)
    worker2 = zeronimo.Worker(app, [worker_sub2])
    worker2.start()
    yield autowork.will_stop(worker2)
    # zeronimo!
    with customer1.link([tunnel_pub], prefix) as tunnel1, \
         customer2.link([tunnel_pub], prefix) as tunnel2:
        assert tunnel1(fanout=True).simple() == ['ok', 'ok']
        assert tunnel2(fanout=True).simple() == ['ok', 'ok']


@autowork
def test_simple(addr1, addr2):
    # sockets
    worker_sock = ctx.socket(zmq.PULL)
    worker_sock.bind(addr1)
    customer_sock = ctx.socket(zmq.PULL)
    customer_sock.bind(addr2)
    tunnel_sock = ctx.socket(zmq.PUSH)
    tunnel_sock.connect(addr1)
    yield autowork.will_close(worker_sock)
    yield autowork.will_close(customer_sock)
    yield autowork.will_close(tunnel_sock)
    # run
    worker = zeronimo.Worker(app, [worker_sock])
    worker.start()
    yield autowork.will_stop(worker)
    customer = zeronimo.Customer(customer_sock, addr2)
    with customer.link([tunnel_sock]) as tunnel:
        assert tunnel.simple() == 'ok'
        with pytest.raises(KeyError):
            tunnel(fanout=True).simple()


@autowork
def test_2nd_start(customer, worker):
    assert worker.is_running()
    worker.stop()
    assert not worker.is_running()
    worker.start()
    assert worker.is_running()
    assert customer.is_running()
    customer.stop()
    assert not customer.is_running()
    customer.start()
    assert customer.is_running()


@autowork
def test_concurrent_tunnels(customer, worker, tunnel_socks, prefix):
    done = []
    def test_tunnel():
        with customer.link(tunnel_socks, prefix) as tunnel:
            assert customer.is_running()
            assert tunnel.simple() == 'ok'
            assert tunnel(fanout=True).simple() == ['ok']
        done.append(True)
    customer.stop()  # stop customer's auto-running
    assert not customer.is_running()
    times = 5
    joinall([spawn(test_tunnel) for x in xrange(times)])
    assert len(done) == times
    assert not customer.is_running()


@autowork
def test_proxied_customer(worker, tunnel_socks, prefix, addr1, addr2):
    streamer = spawn(
        run_device, ctx.socket(zmq.PULL), ctx.socket(zmq.PUSH), addr1, addr2)
    streamer.join(0)
    yield autowork.will(streamer.kill, block=True)
    customer_sock = ctx.socket(zmq.PULL)
    customer_sock.connect(addr2)
    yield autowork.will_close(customer_sock)
    customer = zeronimo.Customer(customer_sock, addr1)
    with customer.link(tunnel_socks, prefix) as tunnel:
        assert tunnel.simple() == 'ok'


@autowork
def test_tunnel_without_customer(worker, tunnel_socks, prefix):
    tunnel = zeronimo.Tunnel(None, tunnel_socks, prefix)
    tunnel(wait=False).simple()
    tunnel(wait=False, fanout=True).simple()
    with pytest.raises(ValueError):
        tunnel.simple()
    with pytest.raises(ValueError):
        tunnel(fanout=True).simple()


@autowork
def test_finding_timeout(customer, worker, tunnel_socks, prefix):
    patch_worker_to_be_slow(worker, delay=0.05)
    with customer.link(tunnel_socks, prefix) as tunnel:
        with pytest.raises(zeronimo.WorkerNotFound):
            tunnel(finding_timeout=0).simple()
        with pytest.raises(zeronimo.WorkerNotFound):
            tunnel(fanout=True, finding_timeout=0).simple()
        with pytest.raises(zeronimo.WorkerNotFound):
            tunnel(finding_timeout=0.01).simple()
        with pytest.raises(zeronimo.WorkerNotFound):
            tunnel(fanout=True, finding_timeout=0.01).simple()
        assert tunnel(finding_timeout=0.1).simple() == 'ok'
        assert tunnel(fanout=True, finding_timeout=0.1).simple() == ['ok']


def test_socket_type_error():
    with pytest.raises(ValueError):
        zeronimo.Customer(ctx.socket(zmq.PAIR), 'x')
    with pytest.raises(ValueError):
        zeronimo.Worker(None, [ctx.socket(zmq.PAIR)])
    customer = zeronimo.Customer(ctx.socket(zmq.PULL), 'x')
    with pytest.raises(ValueError):
        tunnel = customer.link([ctx.socket(zmq.PULL)])

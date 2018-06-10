from __future__ import print_function, division

import abc

from public import Process, ClientProtocol as CP
from collections import defaultdict as ddict
from itertools import count
from Queue import Queue

try:
    from typing import Tuple
except ImportError:
    pass


class MyProcess(Process):
    _store = {}

    def __init__(self, pid):
        Process.__init__(self, pid)
        self.process_count = None
        self.majority = None
        self.ignore_time = None
        self.promises = ddict(lambda: 1)
        self.accepts = ddict(lambda: 1)
        self.accepts_vals = ddict()
        self.value = None
        self.requests = Queue()
        self.prev_accepted_value = (None, None)

    def on_setup(self, process_count):
        # print('setup')
        self.process_count = process_count
        self.majority = process_count // 2 + 1
        MyProcess._store = {}

    def on_tick(self, ctx):
        # print('tick', ctx.time)
        pass

    def broadcast(self, ctx, message, itself=False):
        pid = self.pid
        for i in range(1, self.process_count):
            if itself or i != pid:
                # # print(self.process_count, i, self.pid)
                ctx.send(i, message)
        return pid, message

    def on_receive(self, ctx, sender, message):
        # print('recv', self.majority, self.pid, ctx.time, sender, dict(self.promises), dict(self.accepts), message)
        if message[CP.METHOD] == 'get':
            if self.value is not None:
                ctx.send(sender, {
                    CP.ID: message[CP.ID], CP.VALUE: self.value})
                return
            # print('put', ('get', sender, message[CP.ID], None))
            self.requests.put(('get', sender, message[CP.ID], None))

        if message[CP.METHOD] == 'set':
            req_id = message[CP.ID]
            if self.value is not None:
                ctx.send(sender, {
                    CP.ID: req_id, CP.VALUE: self.value, CP.FLAG: False})
                return
            time = ctx.time
            sender, message = self.broadcast(ctx, {
                CP.METHOD: 'prepare',
                CP.VALUE: message[CP.VALUE],
                'IDp': time,
            })
            # print('put', ('set', sender, req_id, None))
            self.requests.put(('set', sender, req_id, time))

        if message[CP.METHOD] == 'prepare':
            res = self.prepare(ctx, message)
            if res is None:
                return
            sender, message = res

        if message[CP.METHOD] == 'promise':
            res = self.promise(ctx, message)
            if res is None:
                return
            sender, message = res

        if message[CP.METHOD] == 'accept-request':
            res = self.accept_request(ctx, message)
            if res is None:
                return
            sender, message = res

        if message[CP.METHOD] == 'accept':
            self.accept(ctx, message)

    def prepare(self, ctx, message):
        if self.ignore_time is not None and self.ignore_time > ctx.time:
            return
        time = message['IDp']
        if self.ignore_time is None:  # TODO: in future
            self.ignore_time = time
        return self.broadcast(ctx, {
            CP.METHOD: 'promise',
            'IDp': time,
            CP.VALUE: message[CP.VALUE],
            'accepted': self.prev_accepted_value,
        })

    def promise(self, ctx, message):
        time = message['IDp']
        value = message[CP.VALUE]
        promised = self.promises[time]  # TODO: ATOMIC here ???
        accepted_id, accepted_value = message['accepted']
        self.promises[time] += 1

        if accepted_id:
            self.accepts_vals[accepted_id] = accepted_value

        if promised != self.majority - 1:  # TODO really != majority
            return

        a_time = max(self.accepts_vals.keys() +
                     [-float('inf')])  # message['accepted']
        a_value = self.accepts_vals.get(a_time, None)
        # # print('kek', a_time, a_value)

        # if a_time > time:  # TODO: really > ?
        #     value = a_value  # TODO: check
        #     time = a_time
        if a_value is not None:
            value = a_value
            # time = a_time

        return self.broadcast(ctx, {
            CP.METHOD: 'accept-request',
            'IDp': time,
            CP.VALUE: value,
        })

    def accept_request(self, ctx, message):
        time = message['IDp']
        value = message[CP.VALUE]
        if self.ignore_time is not None and self.ignore_time > time:
            return
        self.accepts_vals[time] = value

        return self.broadcast(ctx, {
            CP.METHOD: 'accept',
            'IDp': time,
            CP.VALUE: value,
        })

    def accept(self, ctx, message):
        time = message['IDp']
        accepted = self.accepts[time]
        value = message[CP.VALUE]

        if (self.prev_accepted_value is None
            or self.prev_accepted_value[0] < time):
            self.prev_accepted_value = (time, value)
        self.value = value

        self.accepts[time] += 1
        if accepted != self.majority - 1:  # TODO really != majority
            return

        # print(1)

        while not self.requests.empty():
            method, sender, req_id, IDp = self.requests.get()
            # print('finish', method, sender, req_id, IDp, self.pid, value)
            if method == 'get':
                ctx.send(0, {
                    CP.ID: req_id, CP.VALUE: value})
            elif method == 'set':
                ctx.send(0, {
                    CP.ID: req_id, CP.VALUE: value, CP.FLAG: time == IDp})

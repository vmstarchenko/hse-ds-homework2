import json
import logging
import random
from collections import deque

from public import Context, Process

try:
    from typing import List, Dict, Tuple
except ImportError:
    pass


class Environment(object):
    processes = None  # type: List[Process]
    channels = None  # type: Dict[Tuple[int, int], deque]
    time = None  # type: int

    class BoundContext(Context):
        def __init__(self, env, pid):
            self._env = env
            self._pid = pid

        @property
        def time(self):
            assert self._env is not None, "context was destroyed"
            return self._env.time

        def send(self, recepient, message):
            assert self._env is not None, "context was destroyed"
            self._env._step_send_to_channel(self._pid, recepient, message)

        def destroy(self):
            self._env = self._pid = None

    def __init__(self):
        self.processes = []
        self.dead_processes = []
        self.channels = {}
        self.time = -1

    def spawn_process(self, cls, *args, **kwargs):
        pid = len(self.processes)
        instance = cls(pid, *args, **kwargs)
        self.processes.append(instance)
        logging.debug("spawned process %r (pid=%d)", instance, pid)
        return instance

    def setup(self):
        process_count = len(self.processes)
        for i in range(process_count):
            for j in range(process_count):
                if i == j: # TODO: CHECK THIS
                    continue
                self.channels[(i, j)] = deque()
        channel_count = len(self.channels)
        logging.debug("created %d channels for %d processes", channel_count, process_count)
        for process in self.processes:
            process.on_setup(process_count)

    def _get_pid(self, process):
        if isinstance(process, Process):
            return process.pid
        elif isinstance(process, int) and (0 <= process < len(self.processes)):
            return process
        else:
            raise ValueError("value %r is neither a process nor a pid" % process)

    def kill_process(self, process):
        process = self._get_pid(process)
        self.dead_processes.append(process)

    def _step_tick(self, process):
        self.time += 1
        tick_time = self.time
        logging.debug("t=%-5d  pid=%-2d  ->on_tick", self.time, process)
        ctx = Environment.BoundContext(self, process)
        self.processes[process].on_tick(ctx)
        ctx.destroy()
        logging.debug("t=%-5d  pid=%-2d  <-on_tick  # entered at t=%d", self.time, process, tick_time)

    def _step_receive_from_channel(self, sender, recepient):
        self.time += 1
        receive_time = self.time
        payload, send_time = self.channels[(sender, recepient)].popleft()
        logging.debug("t=%-5d  pid=%-2d  ->on_receive(from_pid=%d, payload=%s)  # sent at t=%d",
                      self.time, recepient, sender, payload, send_time)
        message = json.loads(payload)
        ctx = Environment.BoundContext(self, recepient)
        self.processes[recepient].on_receive(ctx, sender, message)
        ctx.destroy()
        logging.debug("t=%-5d  pid=%-2d  <-on_receive  # entered at t=%d",
                      self.time, recepient, receive_time)

    def _step_send_to_channel(self, sender, recepient, message):
        self.time += 1
        payload = json.dumps(message)
        logging.debug("t=%-5d  pid=%-2d  send(to_pid=%d, payload=%s)",
                      self.time, sender, recepient, payload)
        self.channels[(sender, recepient)].append((payload, self.time))

    def step_by_ticking_process(self, process):
        process = self._get_pid(process)
        if process in self.dead_processes:
            return
        self._step_tick(process)

    def step_by_delivering_messages(self, process, direction="both"):
        process = self._get_pid(process)
        if process in self.dead_processes:
            return
        for channel, queue in self.channels.iteritems():
            sender, recepient = channel
            should_receive = False
            if process == recepient:
                should_receive |= (direction == "incoming" or direction == "both")
            if process == sender:
                should_receive |= (direction == "outcoming" or direction == "both")
            if not should_receive:
                continue
            while len(queue) > 0:
                self._step_receive_from_channel(sender, recepient)

    def step_randomly(self):
        def is_active_channel(k):
            if len(self.channels[k]) == 0:
                return False
            if k[0] in self.dead_processes:
                return False
            if k[1] in self.dead_processes:
                return False
            return True

        active_channels = filter(is_active_channel, self.channels)
        if len(active_channels) == 0:
            logging.debug("t=%-5d [no active channels]", self.time)
            next_action = 0
        else:
            next_action = random.randint(0, 1)
        if next_action == 1:
            channel = random.choice(active_channels)
            self._step_receive_from_channel(*channel)
        if next_action == 0:
            while True:
                process = random.randint(0, len(self.processes) - 1)
                if process not in self.dead_processes:
                    break
            self._step_tick(process)

import abc

from public import Process, ClientProtocol

try:
    from typing import Tuple
except ImportError:
    pass


class KeyValueStoreProcess(Process):
    def on_setup(self, process_count):
        pass

    def on_tick(self, ctx):
        pass

    def on_receive(self, ctx, sender, message):
        CP = ClientProtocol
        if message[CP.METHOD] == "get":
            value = self.handle_get(message[CP.KEY])
            ctx.send(sender, {CP.ID: message[CP.ID], CP.VALUE: value})
        if message[CP.METHOD] == "set":
            value, flag = self.handle_set(message[CP.KEY], message[CP.VALUE])
            ctx.send(sender, {CP.ID: message[CP.ID], CP.VALUE: value, CP.FLAG: flag})

    @abc.abstractmethod
    def handle_get(self, key):
        # type: (str) -> str
        pass

    @abc.abstractmethod
    def handle_set(self, key, value):
        # type: (str, str) -> Tuple[str, bool]
        pass


class LocalKeyValueStoreProcess(KeyValueStoreProcess):
    def __init__(self, pid):
        super(LocalKeyValueStoreProcess, self).__init__(pid)
        self._store = {}

    def handle_get(self, key):
        return self._store.get(key, None)

    def handle_set(self, key, value):
        if key in self._store:
            return self._store[key], False
        else:
            self._store[key] = value
            return value, True


class GlobalKeyValueStoreProcess(KeyValueStoreProcess):
    _store = {}

    def on_setup(self, process_count):
        GlobalKeyValueStoreProcess._store = {}

    def handle_get(self, key):
        return GlobalKeyValueStoreProcess._store.get(key, None)

    def handle_set(self, key, value):
        if key in GlobalKeyValueStoreProcess._store:
            return GlobalKeyValueStoreProcess._store[key], False
        else:
            GlobalKeyValueStoreProcess._store[key] = value
            return value, True

import abc


class Context(object):
    @abc.abstractproperty
    def time(self):
        # type: () -> int
        pass

    @abc.abstractmethod
    def send(self, recepient, message):
        # type: (int, object) -> None
        pass


class Process(object):
    def __init__(self, pid):
        self._pid = pid

    @property
    def pid(self):
        # type: () -> int
        return self._pid

    @abc.abstractmethod
    def on_setup(self, process_count):
        # type: (int) -> None
        pass

    @abc.abstractmethod
    def on_tick(self, ctx):
        # type: (Context) -> None
        pass

    @abc.abstractmethod
    def on_receive(self, ctx, sender, message):
        # type: (Context, int, object) -> None
        pass


class Future(object):
    def __init__(self):
        self._value = None
        self._callbacks = None

    @property
    def has_value(self):
        return self._value is not None

    def subscribe(self, fn):
        if self.has_value:
            fn(self._value)
        else:
            if self._callbacks is None:
                self._callbacks = []
            self._callbacks.append(fn)

    def get_value(self):
        assert self._value is not None
        return self._value

    def set_value(self, value):
        assert self._value is None
        assert value is not None
        self._value = value
        if self._callbacks:
            for fn in self._callbacks:
                fn(self._value)
        self._callbacks = None


class ClientProtocol(object):
    ID = "request_id"
    METHOD = "method"
    KEY = "key"
    VALUE = "value"
    FLAG = "flag"


class ClientProcess(Process):
    def __init__(self, pid):
        super(ClientProcess, self).__init__(pid)
        self._request_id = 0
        self._active_requests = {}
        self._pending_requests = []

    def on_setup(self, process_count):
        pass

    def on_tick(self, ctx):
        for process, request, future in self._pending_requests:
            ctx.send(process, request)
            self._active_requests[request[ClientProtocol.ID]] = future
        self._pending_requests = []

    def on_receive(self, ctx, sender, message):
        assert isinstance(message, dict)
        request_id = message.pop(ClientProtocol.ID)
        future = self._active_requests.pop(request_id)
        future.set_value(message)

    def call(self, process, method, **kwargs):
        # type: (int, str, str) -> Future
        assert isinstance(process, int)
        assert isinstance(method, str)
        request = {ClientProtocol.ID: self._request_id, ClientProtocol.METHOD: method}
        request.update(kwargs)
        future = Future()
        self._request_id += 1
        self._pending_requests.append((process, request, future))
        return future

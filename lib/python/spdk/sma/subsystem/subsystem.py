import multiprocessing
from ..proto import sma_pb2


class SubsystemException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message


class Subsystem:
    def __init__(self, name, client):
        self._client = client
        self.name = name

    def create_device(self, request):
        raise NotImplementedError()

    def remove_device(self, request):
        raise NotImplementedError()

    def connect_controller(self, request):
        raise NotImplementedError()

    def disconnect_controller(self, request):
        raise NotImplementedError()

    def attach_volume(self, request):
        raise NotImplementedError()

    def detach_volume(self, request):
        raise NotImplementedError()

    def owns_device(self, id):
        raise NotImplementedError()

    def owns_controller(self, id):
        raise NotImplementedError()

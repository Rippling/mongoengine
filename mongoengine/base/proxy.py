import weakref

from lazy_object_proxy.slots import Proxy


class DocumentProxy(Proxy):
    id = None
    collection = None
    wrapped = None
    _instance = None
    def __init__(self, wrapped, id, collection, instance=None, **kwargs):
        super(DocumentProxy, self).__init__(wrapped)
        self.id = id
        self.collection = collection
        if instance:
            self._instance = weakref.proxy(instance)
        for attr, val in kwargs.items():
            object.__setattr__(self, attr, val)

    def __call__(self, *args, **kwargs):
        # Hack as callable(lazy_object_proxy.Proxy) return True
        return self.__wrapped__

    def __eq__(self, other):
        if type(other) is DocumentProxy or hasattr(other, 'id'):
            return self.id == other.id
        return self.__wrapped__ == other

    def __ne__(self, other):
        return not self.__eq__(other)

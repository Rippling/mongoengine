import lazy_object_proxy


class DocumentProxy(lazy_object_proxy.Proxy):
    id = None
    wrapped = None
    def __init__(self, wrapped, id):
        super(DocumentProxy, self).__init__(wrapped)
        self.id = id

    def __call__(self, *args, **kwargs):
        # Hack as callable(lazy_object_proxy.Proxy) return True
        return self.__wrapped__

    def __eq__(self, other):
        if type(other) is DocumentProxy or hasattr(other, 'id'):
            return self.id == other.id
        return self.__wrapped__ == other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __nonzero__(self):
        return True
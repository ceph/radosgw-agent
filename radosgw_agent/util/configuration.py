

class Configuration(dict):
    """
    An immutable dictionary where values set for the first time are allowed and
    existing keys raise a TypeError exception
    """

    def pop(self, key, default=None):
        self._default_error()

    def popitem(self):
        self._default_error()

    def update(self):
        self._default_error()

    def clear(self):
        self._default_error()

    def _default_error(self):
        msg = 'config object does not allow key changes'
        raise TypeError(msg)

    def __setitem__(self, key, value):
        try:
            self[key]
        except KeyError:
            self[key] = value
        else:
            self._default_error()

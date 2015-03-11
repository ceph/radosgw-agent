

class Configuration(object):
    """
    An immutable dictionary where values set for the first time are allowed and
    existing keys raise a TypeError exception
    """

    def __init__(self):
        self._dict = {}

    def __str__(self):
        return self._dict

    def pop(self, key, default=None):
        self._default_error()

    def popitem(self):
        self._default_error()

    def update(self):
        self._default_error()

    def clear(self):
        self._default_error()

    def get(self, key, default=None):
        return self._dict.get(key, default)

    def _default_error(self):
        msg = 'config object does not allow key changes'
        raise TypeError(msg)

    def __setitem__(self, key, value):
        try:
            self._dict[key]
        except KeyError:
            self._dict[key] = value
        else:
            self._default_error()

    def __getitem__(self, key):
        return self._dict[key]

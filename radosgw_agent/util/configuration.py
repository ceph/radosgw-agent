

class Configuration(object):
    """
    An immutable dictionary where values set for the first time are allowed and
    existing keys raise a TypeError exception

    Even though there is effort made into making it immutable, a consumer can
    force its way through by accessing the private `dict` method that contains
    the values, although it defeats the purpose it is exposed in that way in
    the event that something needs to change.

    All normal methods and operations should be supported.
    """

    def __init__(self, seed=None):
        if seed and isinstance(seed, dict):
            self._dict = seed
        else:
            self._dict = {}

    def __str__(self):
        return str(self._dict)

    def pop(self, key, default=None):
        try:
            self._dict[key]
        except KeyError:
            raise
        else:
            self._default_error()

    def popitem(self):
        self._default_error()

    def update(self):
        self._default_error()

    def clear(self):
        self._default_error()

    def values(self):
        return self._dict.values()

    def keys(self):
        return self._dict.keys()

    def items(self):
        return self._dict.items()

    def get(self, key, default=None):
        return self._dict.get(key, default)

    def _default_error(self):
        msg = 'config object does not allow key changes'
        raise TypeError(msg)

    def __setitem__(self, key, value):
        try:
            self._dict[key]
        except KeyError:
            if isinstance(value, dict):
                self._dict[key] = Configuration(value)
            else:
                self._dict[key] = value
        else:
            self._default_error()

    def __getitem__(self, key):
        return self._dict[key]

    def __contains__(self, key):
        try:
            self._dict[key]
            return True
        except KeyError:
            return False

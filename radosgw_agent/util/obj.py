

def to_dict(_object, **extra_keys):
    """
    A utility to convert an object with attributes to a dictionary with the
    optional feature of slapping extra_keys. Because extra_keys can be
    optionally set, it is assumed that any keys that clash will get
    overwritten.

    Private methods (anything that starts with `_`) are ignored.
    """
    dictified_obj = {}
    for k, v in _object.__dict__.items():
        if not k.startswith('_'):
            # get key
            value = extra_keys.pop(k, v)
            dictified_obj[k] = value
    if extra_keys:
        for k, v in extra_keys.items():
            dictified_obj[k] = v

    return dictified_obj

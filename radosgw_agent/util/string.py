
def concatenate(*a, **kw):
    """
    helper function to concatenate all arguments with added (optional)
    newlines
    """
    newline = kw.get('newline', False)
    string = ''
    for item in a:
        if newline:
            string += item + '\n'
        else:
            string += item
    return string

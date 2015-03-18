from radosgw_agent.util import obj


class Empty(object):

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class TestToDict(object):

    def test_underscores_are_ignored(self):
        fake = Empty(a=1, _b=2)
        result = obj.to_dict(fake)
        assert result.get('_b') is None
        assert result.get('a') == 1

    def test_overrides_are_respected(self):
        fake = Empty(a=1, b=2)
        result = obj.to_dict(fake, b=3)
        assert result.get('b') == 3

    def test_overrides_dont_mess_up_other_keys(self):
        fake = Empty(a=1, b=2)
        result = obj.to_dict(fake, b=3)
        assert result.get('a') == 1

    def test_extra_keys_are_set(self):
        result = obj.to_dict(Empty(), a=1, b=2)
        assert result['a'] == 1
        assert result['b'] == 2


class TestKeysToAttribute(object):

    def test_replace_dashes(self):
        dictionary = {'dashed-word': 1}
        result = obj.to_obj(dictionary)
        assert result.dashed_word == 1

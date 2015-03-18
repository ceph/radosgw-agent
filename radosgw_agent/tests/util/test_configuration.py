import pytest
from radosgw_agent.util import configuration


@pytest.fixture
def conf():
    return configuration.Configuration()


class TestConfiguration(object):

    def test_set_new_keys(self, conf):
        conf['key'] = 1
        assert conf['key'] == 1

    def test_not_allowed_to_change_value(self, conf):
        conf['key'] = 1
        with pytest.raises(TypeError):
            conf['key'] = 2

    def test_not_allowed_to_pop_existing_key(self, conf):
        conf['key'] = 1
        with pytest.raises(TypeError):
            conf.pop('key')

    def test_keyerror_when_popping(self, conf):
        with pytest.raises(KeyError):
            conf.pop('key')

    def test_adding_nested_values(self, conf):
        conf['key'] = {}
        conf['key']['bar'] = 1
        assert conf['key']['bar'] == 1

    def test_modifiying_nested_values_fails(self, conf):
        conf['key'] = {}
        conf['key']['bar'] = 1
        with pytest.raises(TypeError):
            conf['key']['bar'] = 2

    def test_initial_dict_seeding(self):
        my_dict = {'a': 1}
        conf = configuration.Configuration(my_dict)
        assert conf['a'] == 1

    def test_initial_dict_seeding_doesnt_allow_updates(self):
        my_dict = {'a': 1}
        conf = configuration.Configuration(my_dict)
        with pytest.raises(TypeError):
            conf['a'] = 2

    def test_assign_a_new_key_to_a_dict(self, conf):
        my_dict = {'a': 1}
        conf['args'] = my_dict
        assert conf['args']['a'] == 1

    def test_contains_element(self, conf):
        exists = False
        try:
            if 'key' in conf:
                exists = True
        except KeyError:
            assert False, "dict object should support 'contains' operations"
        assert exists is False

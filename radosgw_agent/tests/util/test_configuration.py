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

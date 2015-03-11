from radosgw_agent.util import configuration


class TestConfiguration(object):

    def test_set_new_keys(self):
        conf = configuration.Configuration()
        conf['key'] = 1
        assert conf['key'] == 1

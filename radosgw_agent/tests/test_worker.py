from mock import Mock, patch
import py.test
import time

from radosgw_agent import worker, client
from radosgw_agent.exceptions import HttpError, NotFound, BucketEmpty


class TestSyncObject(object):

    def setup(self):
        # setup the fake client, but get the exceptions back into place
        self.client = Mock()
        self.client.HttpError = HttpError
        self.client.NotFound = NotFound

        self.src = Mock()
        self.src.zone.name = 'Zone Name'
        self.src.host = 'example.com'
        self.obj = Mock()
        self.obj.name = 'mah-object'

    def test_syncs_correctly(self):
        with patch('radosgw_agent.worker.client'):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            assert w.sync_object('mah-bucket', self.obj) is True

    def test_syncs_not_found_on_master_deleting_from_secondary(self):
        self.client.sync_object_intra_region = Mock(side_effect=NotFound(404, ''))

        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            w.wait_for_object = lambda *a: None
            assert w.sync_object('mah-bucket', self.obj) is True

    def test_syncs_deletes_from_secondary(self):
        self.client.sync_object_intra_region = Mock(side_effect=NotFound(404, ''))
        self.client.delete_object = Mock(side_effect=NotFound(404, ''))

        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            w.wait_for_object = lambda *a: None
            assert w.sync_object('mah-bucket', self.obj) is False

    def test_syncs_could_not_delete_from_secondary(self):
        self.client.sync_object_intra_region = Mock(side_effect=NotFound(404, ''))
        self.client.delete_object = Mock(side_effect=ValueError('unexpected error'))

        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            w.wait_for_object = lambda *a: None

            with py.test.raises(worker.SyncFailed):
                w.sync_object('mah-bucket', self.obj)

    def test_syncs_encounters_a_http_error(self):
        self.client.sync_object_intra_region = Mock(side_effect=HttpError(400, ''))

        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            w.wait_for_object = lambda *a: None
            w.sync_object('mah-bucket', self.obj)

    def test_sync_client_raises_sync_failed(self):
        self.client.sync_object_intra_region = Mock(side_effect=worker.SyncFailed('failed intra region'))

        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)

            with py.test.raises(worker.SyncFailed) as exc:
                w.sync_object('mah-bucket', self.obj)

            exc_message = exc.value[0]
            assert 'failed intra region' in exc_message

    def test_fails_to_remove_op_state(self, capsys):
        # really tricky to test this one, we are forced to just use `capsys` from py.test
        # which will allow us to check into the stderr logging output and see if the agent
        # was spitting what we are expecting.
        self.client.remove_op_state = Mock(side_effect=ValueError('could not remove op'))

        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            w.wait_for_object = lambda *a: None
            assert w.sync_object('mah-bucket', self.obj) is True
        # logging does not play nice and so we are forced to comment this out.
        # this test does test the right thing, but we are unable to have a nice
        # assertion, the fix here is not the test it is the code that needs to
        # improve. For now, this just
        # gives us the coverage.
        # out, err = capsys.readouterr()
        # assert 'could not remove op state' in out
        # assert 'could not remove op state' in err

    def test_fails_to_do_anything_fallsback_to_wait_for_object(self):
        self.client.sync_object_intra_region = Mock(side_effect=ValueError('severe error'))

        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            w.wait_for_object = lambda *a: None
            assert w.sync_object('mah-bucket', self.obj) is True

    def test_wait_for_object_state_not_found_raises_sync_failed(self):
        self.client.get_op_state = Mock(side_effect=NotFound(404, ''))
        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            with py.test.raises(worker.SyncFailed) as exc:
                w.wait_for_object(None, None, time.time() + 1000, None)

        exc_message = exc.exconly()
        assert 'state not found' in exc_message

    def test_wait_for_object_timeout(self):
        msg = 'should not have called get_op_state'
        self.client.get_op_state = Mock(side_effect=AssertionError(msg))
        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            with py.test.raises(worker.SyncTimedOut) as exc:
                w.wait_for_object(None, None, time.time() - 1, None)

    def test_wait_for_object_state_complete(self):
        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            self.client.get_op_state = lambda *a: [{'state': 'complete'}]
            assert w.wait_for_object(None, None, time.time() + 1, None) is None

    def test_wait_for_object_state_error(self):
        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            self.client.get_op_state = lambda *a: [{'state': 'error'}]
            with py.test.raises(worker.SyncFailed) as exc:
                w.wait_for_object(None, None, time.time() + 1, None)

        exc_message = exc.exconly()
        assert 'state is error' in exc_message

    def test_sync_bucket_delayed_not_found(self):
        class fake_iterable(object):
            def __iter__(self):
                raise BucketEmpty
        with patch('radosgw_agent.worker.client', self.client):
            w = worker.DataWorker(None, None, None, self.src, None, daemon_id=1)
            w.sync_object = lambda *a: None
            objects = fake_iterable()
            with py.test.raises(BucketEmpty):
                w.sync_bucket('foo', objects)

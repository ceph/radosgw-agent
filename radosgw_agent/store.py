import logging
import os
import tempfile
import yaml

log = logging.getLogger(__name__)

UNIX_EPOCH = '1970-01-01 00:00:00'

class Simple(object):
    """
    Stores how far syncing got for the last successful full sync
    and each shard in a partial sync.

    Format is simple yaml structure, which is entirely re-written every
    time an entry is updated.
    """

    def __init__(self, filename):
        self.state_file = os.path.abspath(filename)
        self.dir = os.path.dirname(self.state_file)
        if os.path.exists(self.state_file):
            self._load_state()
        else:
            log.info('no state file found, creating %s', self.state_file)
            self._create_blank()

    def _load_state(self):
        with open(self.state_file) as f:
            self.state = yaml.safe_load(f)

    def _create_blank(self):
        log.debug('dir = %s', self.dir)
        if not os.path.exists(self.dir):
            os.path.makedirs(self.dir)
        self.state = {
            'last_full_sync': UNIX_EPOCH,
            'shards': {}}
        self._save_to_disk()

    def _save_to_disk(self):
        fd, tmp_name = tempfile.mkstemp(dir=self.dir)
        try:
            content = yaml.safe_dump(self.state)
            while content:
                written = os.write(fd, content)
                log.debug('write %d bytes, %d left', written, len(content))
                content = content[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.rename(tmp_name, self.state_file)
        # fsync dir too so rename is persistent
        fd = os.open(self.dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def save_to_disk(self):
        try:
            self._save_to_disk()
        except:
            log.exception('error saving state to disk - aborting')
            raise

    def update_shard_timestamp(self, shard_id, timestamp):
        self.state['shards'][shard_id] = timestamp

    def update_full_sync_timestamp(self, timestamp):
        self.state['last_full_sync'] = timestamp

    def last_full_sync(self):
        return self.state['last_full_sync']

    def last_shard_sync(self, shard_id):
        return self.state['shards'].get(shard_id, self.last_full_sync())

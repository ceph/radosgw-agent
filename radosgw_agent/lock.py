import logging
import threading
import time

from radosgw_agent import client
from radosgw_agent.util import get_dev_logger

log = logging.getLogger(__name__)
dev_log = get_dev_logger(__name__)


class LockBroken(Exception):
    pass


class LockRenewFailed(LockBroken):
    pass


class LockExpired(LockBroken):
    pass


class Lock(threading.Thread):
    """A lock on a shard log that automatically refreshes itself.

    It may be used to lock different shards throughout its lifetime.
    To lock a new shard, call aquire() with the shard_num desired.

    To release the lock, call release_and_clear(). This will raise an
    exception if the lock ever failed to be acquired in the timeout
    period.
    """

    def __init__(self, conn, type_, locker_id, timeout, zone_id):
        super(Lock, self).__init__()
        self.conn = conn
        self.type = type_
        self.timeout = timeout
        self.lock = threading.Lock()
        self.locker_id = locker_id
        self.zone_id = zone_id
        self.shard_num = None
        self.last_locked = None
        self.failed = False

    def set_shard(self, shard_num):
        dev_log.debug('set_shard to %d', shard_num)
        with self.lock:
            assert self.shard_num is None, \
                'attempted to acquire new lock without releasing old one'
            self.failed = False
            self.last_locked = None
            self.shard_num = shard_num

    def unset_shard(self):
        dev_log.debug('unset shard')
        with self.lock:
            self.shard_num = None

    def acquire(self):
        """Renew an existing lock, or acquire a new one.

        The old lock must have already been released if shard_num is specified.
        client.NotFound may be raised if the log contains no entries.
        """
        dev_log.debug('acquire lock')
        with self.lock:
            self._acquire()

    def _acquire(self):
        # same as aqcuire() but assumes self.lock is held
        now = time.time()
        client.lock_shard(self.conn, self.type, self.shard_num,
                          self.zone_id, self.timeout, self.locker_id)
        self.last_locked = now

    def release_and_clear(self):
        """Release the lock currently being held.

        Prevent it from being automatically renewed, and check if there
        were any errors renewing the current lock or if it expired.
        If the lock was not sustained, raise LockAcquireFailed or LockExpired.
        """
        dev_log.debug('release and clear lock')
        with self.lock:
            shard_num = self.shard_num
            self.shard_num = None
            diff = time.time() - self.last_locked
            if diff > self.timeout:
                msg = 'lock was not renewed in over %0.2f seconds' % diff
                raise LockExpired(msg)
            if self.failed:
                raise LockRenewFailed()
            try:
                client.unlock_shard(self.conn, self.type, shard_num,
                                    self.zone_id, self.locker_id)
            except client.HttpError as e:
                log.warn('failed to unlock shard %d in zone %s: %s',
                         shard_num, self.zone_id, e)
            self.last_locked = None

    def run(self):
        while True:
            with self.lock:
                if self.shard_num is not None:
                    try:
                        self._acquire()
                    except client.HttpError as e:
                        log.error('locking shard %d in zone %s failed: %s',
                                  self.shard_num, self.zone_id, e)
                        self.failed = True
            time.sleep(0.5 * self.timeout)

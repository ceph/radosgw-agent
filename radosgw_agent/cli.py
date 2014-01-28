from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import argparse
import contextlib
import logging
import logging.handlers
import yaml
import sys

from radosgw_agent import client
from radosgw_agent import sync

def check_positive_int(string):
    value = int(string)
    if value < 1:
        msg = '%r is not a positive integer' % string
        raise argparse.ArgumentTypeError(msg)
    return value

def check_endpoint(endpoint):
    try:
        return client.parse_endpoint(endpoint)
    except client.InvalidProtocol as e:
        raise argparse.ArgumentTypeError(str(e))
    except client.InvalidHost as e:
        raise argparse.ArgumentTypeError(str(e))

def parse_args():
    conf_parser = argparse.ArgumentParser(add_help=False)
    conf_parser.add_argument(
        '-c', '--conf',
        type=file,
        help='configuration file'
        )
    args, remaining = conf_parser.parse_known_args()
    defaults = dict(
        sync_scope='incremental',
        log_lock_time=20,
        )
    if args.conf is not None:
        with contextlib.closing(args.conf):
            config = yaml.safe_load_all(args.conf)
            for new in config:
                defaults.update(new)

    parser = argparse.ArgumentParser(
        parents=[conf_parser],
        description='Synchronize radosgw installations',
        )
    parser.set_defaults(**defaults)
    verbosity = parser.add_mutually_exclusive_group(required=False)
    verbosity.add_argument(
        '-v', '--verbose',
        action='store_true', dest='verbose',
        help='be more verbose',
        )
    verbosity.add_argument(
        '-q', '--quiet',
        action='store_true', dest='quiet',
        help='be less verbose',
        )
    parser.add_argument(
        '--src-access-key',
        required='src_access_key' not in defaults,
        help='access key for source zone system user',
        )
    parser.add_argument(
        '--src-secret-key',
        required='src_secret_key' not in defaults,
        help='secret key for source zone system user',
        )
    parser.add_argument(
        '--dest-access-key',
        required='dest_access_key' not in defaults,
        help='access key for destination zone system user',
        )
    parser.add_argument(
        '--dest-secret-key',
        required='dest_secret_key' not in defaults,
        help='secret key for destination zone system user',
        )
    parser.add_argument(
        'destination',
        type=check_endpoint,
        nargs=None if 'destination' not in defaults else '?',
        help='radosgw endpoint to which to sync '
        '(e.g. http://zone2.example.org:8080)',
        )
    src_options = parser.add_mutually_exclusive_group(required=False)
    src_options.add_argument(
        '--source',
        type=check_endpoint,
        help='radosgw endpoint from which to sync '
        '(e.g. http://zone1.example.org:8080)',
        )
    src_options.add_argument(
        '--src-zone',
        help='radosgw zone from which to sync',
        )
    parser.add_argument(
        '--metadata-only',
        action='store_true',
        help='sync bucket and user metadata, but not bucket contents',
        )
    parser.add_argument(
        '--num-workers',
        default=1,
        type=check_positive_int,
        help='number of items to sync at once',
        )
    parser.add_argument(
        '--sync-scope',
        choices=['full', 'incremental'],
        default='incremental',
        help='synchronize everything (for a new region) or only things that '
             'have changed since the last run',
        )
    parser.add_argument(
        '--lock-timeout',
        type=check_positive_int,
        default=60,
        help='timeout in seconds after which a log segment lock will expire if '
             'not refreshed',
        )
    parser.add_argument(
        '--log-file',
        help='where to store log output',
        )
    parser.add_argument(
        '--max-entries',
        type=check_positive_int,
        default=1000,
        help='maximum number of log entries to process at once during '
        'continuous sync',
        )
    parser.add_argument(
        '--incremental-sync-delay',
        type=check_positive_int,
        default=30,
        help='seconds to wait between syncs',
        )
    parser.add_argument(
        '--object-sync-timeout',
        type=check_positive_int,
        default=60 * 60 * 60,
        help='seconds to wait for an individual object to sync before '
        'assuming failure',
        )
    parser.add_argument(
        '--prepare-error-delay',
        type=check_positive_int,
        default=10,
        help='seconds to wait before retrying when preparing '
        'an incremental sync fails',
        )
    parser.add_argument(
        '--rgw-data-log-window',
        type=check_positive_int,
        default=30,
        help='period until a data log entry is valid - '
        'must match radosgw configuration',
        )
    parser.add_argument(
        '--test-server-host',
        # host to run a simple http server for testing the sync agent on,
        help=argparse.SUPPRESS,
        )
    parser.add_argument(
        '--test-server-port',
        # port to run a simple http server for testing the sync agent on,
        type=check_positive_int,
        default=8080,
        help=argparse.SUPPRESS,
        )
    return parser.parse_args(remaining)

class TestHandler(BaseHTTPRequestHandler):
    """HTTP handler for testing radosgw-agent.

    This should never be used outside of testing.
    """
    num_workers = None
    lock_timeout = None
    max_entries = None
    rgw_data_log_window = 30
    src = None
    dest = None

    def do_POST(self):
        log = logging.getLogger(__name__)
        status = 200
        resp = ''
        sync_cls = None
        if self.path.startswith('/metadata/full'):
            sync_cls = sync.MetaSyncerFull
        elif self.path.startswith('/metadata/incremental'):
            sync_cls = sync.MetaSyncerInc
        elif self.path.startswith('/data/full'):
            sync_cls = sync.DataSyncerFull
        elif self.path.startswith('/data/incremental'):
            sync_cls = sync.DataSyncerInc
        else:
            log.warn('invalid request, ignoring')
            status = 400
            resp = 'bad path'

        try:
            if sync_cls is not None:
                syncer = sync_cls(TestHandler.src, TestHandler.dest,
                                  TestHandler.max_entries,
                                  rgw_data_log_window=TestHandler.rgw_data_log_window,
                                  object_sync_timeout=TestHandler.object_sync_timeout)
                syncer.prepare()
                syncer.sync(
                    TestHandler.num_workers,
                    TestHandler.lock_timeout,
                    )
        except Exception as e:
            log.exception('error during sync')
            status = 500
            resp = str(e)

        self.log_request(status, len(resp))
        if status >= 400:
            self.send_error(status, resp)
        else:
            self.send_response(status)
            self.end_headers()

def main():
    args = parse_args()
    log = logging.getLogger()
    log_level = logging.INFO
    lib_log_level = logging.WARN
    if args.verbose:
        log_level = logging.DEBUG
        lib_log_level = logging.DEBUG
    elif args.quiet:
        log_level = logging.WARN
    logging.basicConfig(level=log_level)
    logging.getLogger('boto').setLevel(lib_log_level)
    logging.getLogger('requests').setLevel(lib_log_level)

    if args.log_file is not None:
        handler = logging.handlers.WatchedFileHandler(
            filename=args.log_file,
            )
        formatter = logging.Formatter(
            fmt='%(asctime)s.%(msecs)03d %(process)d:%(levelname)s:%(name)s:%(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S',
            )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)

    dest = args.destination
    dest.access_key = args.dest_access_key
    dest.secret_key = args.dest_secret_key
    src = args.source or client.Endpoint(None, None, None)
    if args.src_zone:
        src.zone = args.src_zone
    dest_conn = client.connection(dest)

    try:
        region_map = client.get_region_map(dest_conn)
    except Exception:
        log.exception('Could not retrieve region map from destination')
        sys.exit(1)

    try:
        client.configure_endpoints(region_map, dest, src, args.metadata_only)
    except client.ClientException as e:
        log.error(e)
        sys.exit(1)

    src.access_key = args.src_access_key
    src.secret_key = args.src_secret_key

    if args.test_server_host:
        log.warn('TEST MODE - do not run unless you are testing this program')
        TestHandler.src = src
        TestHandler.dest = dest
        TestHandler.num_workers = args.num_workers
        TestHandler.lock_timeout = args.lock_timeout
        TestHandler.max_entries = args.max_entries
        TestHandler.rgw_data_log_window = args.rgw_data_log_window
        TestHandler.object_sync_timeout = args.object_sync_timeout
        server = HTTPServer((args.test_server_host, args.test_server_port),
                            TestHandler)
        server.serve_forever()
        sys.exit()

    if args.sync_scope == 'full':
        meta_cls = sync.MetaSyncerFull
        data_cls = sync.DataSyncerFull
    else:
        meta_cls = sync.MetaSyncerInc
        data_cls = sync.DataSyncerInc

    meta_syncer = meta_cls(src, dest, args.max_entries)
    data_syncer = data_cls(src, dest, args.max_entries,
                           rgw_data_log_window=args.rgw_data_log_window,
                           object_sync_timeout=args.object_sync_timeout)

    # fetch logs first since data logs need to wait before becoming usable
    # due to rgw's window of data log updates during which the bucket index
    # log may still be updated without the data log getting a new entry for
    # the bucket
    sync.prepare_sync(meta_syncer, args.prepare_error_delay)
    if not args.metadata_only:
        sync.prepare_sync(data_syncer, args.prepare_error_delay)

    if args.sync_scope == 'full':
        log.info('syncing all metadata')
        meta_syncer.sync(args.num_workers, args.lock_timeout)
        if not args.metadata_only:
            log.info('syncing all data')
            data_syncer.sync(args.num_workers, args.lock_timeout)
        log.info('Finished full sync. Check logs to see any issues that '
                 'incremental sync will retry.')
    else:
        sync.incremental_sync(meta_syncer, data_syncer,
                              args.num_workers,
                              args.lock_timeout,
                              args.incremental_sync_delay,
                              args.metadata_only,
                              args.prepare_error_delay)

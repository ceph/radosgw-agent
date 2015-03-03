from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import argparse
import contextlib
import logging
import logging.handlers
import os.path
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
    log_dir = '/var/log/ceph/radosgw-agent/'
    log_file = 'radosgw-agent.log'
    if args.conf is not None:
        log_file = os.path.basename(args.conf.name)
    defaults = dict(
        sync_scope='incremental',
        log_lock_time=20,
        log_file=os.path.join(log_dir, log_file),
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
        '--destination',
        type=check_endpoint,
        required='destination' not in defaults,
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
    return parser.parse_known_args(remaining)

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

class SyncToolCommand:

    def __init__(self):
        args, self.remaining = parse_args()

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

        self.dest = args.destination
        self.dest.access_key = args.dest_access_key
        self.dest.secret_key = args.dest_secret_key
        self.src = args.source or client.Endpoint(None, None, None)
        if args.src_zone:
            self.src.zone = args.src_zone
        self.dest_conn = client.connection(self.dest)

        try:
            self.region_map = client.get_region_map(self.dest_conn)
        except Exception:
            log.exception('Could not retrieve region map from destination: ' % self.dest)
            sys.exit(1)

        try:
            client.configure_endpoints(self.region_map, self.dest, self.src, args.metadata_only)
        except client.ClientException as e:
            log.error(e)
            sys.exit(1)

        self.src.access_key = args.src_access_key
        self.src.secret_key = args.src_secret_key


    def _parse(self):
        parser = argparse.ArgumentParser(
            description='radosgw sync tool',
            usage='''radosgw-sync <command> [<args>]

The commands are:
    data_sync               sync data of bucket / object
''')
        parser.add_argument('command', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        
        args = parser.parse_args(self.remaining[0:1])
        if not hasattr(self, args.command) or args.command[0] == '_':
            print 'Unrecognized command:', args.command
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        ret = getattr(self, args.command)
        return ret

    def data_sync(self):
        parser = argparse.ArgumentParser(
            description='Sync bucket / object',
            usage='radosgw-sync data_sync <bucket_name>/<key> [<args>]')
        parser.add_argument('source')
        args = parser.parse_args(self.remaining[1:])

        target = args.source.split('/', 1)

        assert len(target) > 0

        # TODO


def main():

    cmd = SyncToolCommand()._parse()
    cmd()


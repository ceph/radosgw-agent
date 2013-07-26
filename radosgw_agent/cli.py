from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import argparse
import contextlib
import logging
import time
import yaml

from radosgw_agent import client
from radosgw_agent import sync

def check_positive_int(string):
    value = int(string)
    if value < 1:
        msg = '%r is not a positive integer' % string
        raise argparse.ArgumentTypeError(msg)
    return value

def parse_args():
    conf_parser = argparse.ArgumentParser(add_help=False)
    conf_parser.add_argument(
        '-c', '--conf',
        type=file,
        help='configuration file'
        )
    args, remaining = conf_parser.parse_known_args()
    defaults = dict(
        sync_scope='partial',
        log_lock_time=10,
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
        '--src-host',
        required='src_host' not in defaults,
        help='hostname or ip address of source radosgw',
        )
    parser.add_argument(
        '--src-port',
        type=check_positive_int,
        help='port number for source radosgw'
        )
    parser.add_argument(
        '--src-zone',
        required='src_zone' not in defaults,
        help='availability zone hosted by the source'
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
        '--dest-host',
        required='dest_host' not in defaults,
        help='hostname or ip address of destination radosgw',
        )
    parser.add_argument(
        '--dest-port',
        type=check_positive_int,
        help='port number for destination radosgw'
        )
    parser.add_argument(
        '--dest-zone',
        required='dest_zone' not in defaults,
        help='availability zone hosted by the destination'
        )
    parser.add_argument(
        '--num-workers',
        default=1,
        type=check_positive_int,
        help='number of items to sync at once',
        )
    parser.add_argument(
        '--sync-scope',
        choices=['full', 'partial'],
        default='partial',
        help='synchronize everything (for a new region) or only things that '
             'have changed since the last run'
        )
    parser.add_argument(
        '--lock-timeout',
        type=check_positive_int,
        default=60,
        help='timeout in seconds after which a log segment lock will expire if '
             'not refreshed'
        )
    parser.add_argument(
        '--log-file',
        help='where to store log output',
        )
    parser.add_argument(
        '--daemon-id',
        required='daemon_id' not in defaults,
        help='name of this incarnation of the radosgw-agent, i.e. host1',
        )
    parser.add_argument(
        '--max-entries',
        type=check_positive_int,
        default=1000,
        help='maximum number of log entries to process at once during '
        'continuous sync',
        )
    parser.add_argument(
        '--partial-sync-delay',
        type=check_positive_int,
        default=20,
        help='seconds to wait between syncs',
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
    syncer = None
    num_workers = None
    lock_timeout = None
    max_entries = None

    def do_POST(self):
        log = logging.getLogger(__name__)
        status = 200
        resp = ''
        if self.path.startswith('/metadata/full'):
            try:
                TestHandler.syncer.sync_full(TestHandler.num_workers,
                                             TestHandler.lock_timeout)
            except Exception as e:
                log.exception('error doing full sync')
                status = 500
                resp = str(e)
        elif self.path.startswith('/metadata/partial'):
            try:
                TestHandler.syncer.sync_partial(TestHandler.num_workers,
                                                TestHandler.lock_timeout,
                                                TestHandler.max_entries)
            except Exception as e:
                log.exception('error doing partial sync')
                status = 500
                resp = str(e)
        else:
            log.warn('invalid request, ignoring')
            status = 400
            resp = 'bad path'

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
    requests_log_level = logging.WARN
    if args.verbose:
        log_level = logging.DEBUG
        requests_log_level = logging.DEBUG
    elif args.quiet:
        log_level = logging.WARN
    logging.basicConfig(level=log_level)
    logging.getLogger('requests').setLevel(requests_log_level)

    if args.log_file is not None:
        handler = logging.FileHandler(
            filename=args.log_file,
            )
        formatter = logging.Formatter(
            fmt='%(asctime)s.%(msecs)03d %(process)d:%(levelname)s:%(name)s:%(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S',
            )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)

    src = client.Endpoint(args.src_host, args.src_port, args.src_access_key,
                          args.src_secret_key, args.src_zone)
    dest = client.Endpoint(args.dest_host, args.dest_port, args.dest_access_key,
                           args.dest_secret_key, args.dest_zone)
    # TODO: check src and dest zone names and endpoints match the region map
    syncer = sync.Syncer('metadata', src, dest, args.daemon_id)

    if args.test_server_host:
        log.warn('TEST MODE - do not run unless you are testing this program')
        TestHandler.syncer = syncer
        TestHandler.num_workers = args.num_workers
        TestHandler.lock_timeout = args.lock_timeout
        TestHandler.max_entries = args.max_entries
        server = HTTPServer((args.test_server_host, args.test_server_port),
                            TestHandler)
        server.serve_forever()
    elif args.sync_scope == 'full':
        syncer.sync_full(args.num_workers, args.lock_timeout)
    else:
        while True:
            try:
                syncer.sync_partial(args.num_workers, args.lock_timeout,
                                    args.max_entries)
            except:
                log.exception('error doing partial sync, trying again later')
            log.debug('waiting %d seconds until next sync',
                      args.partial_sync_delay)
            time.sleep(args.partial_sync_delay)

import argparse
import contextlib
import logging
import os
import sys
import yaml

from radosgw_agent import client
from radosgw_agent import store
from radosgw_agent import sync

def parse_args():
    conf_parser = argparse.ArgumentParser(add_help=False)
    conf_parser.add_argument(
        '-c', '--conf',
        type=file,
        help='configuration file'
        )
    args, remaining = conf_parser.parse_known_args()
    defaults = dict(
        num_workers=1,
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
        type=int,
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
        type=int,
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
        type=int,
        default=60,
        help='timeout in seconds after which a log segment lock will expire if '
             'not refreshed'
        )
    parser.add_argument(
        '--data-file',
        help='file to store sync state in, defaults to /var/lib/ceph/radosgw-agent/$src-$dest.state'
        )
    parser.add_argument(
        '--log-file',
        help='where to store log output',
        )
    return parser.parse_args(remaining)

def main():
    args = parse_args()
    log = logging.getLogger()
    log_level = logging.INFO
    if args.verbose:
        log_level = logging.DEBUG
    elif args.quiet:
        log_level = logging.WARN
    logging.basicConfig(level=log_level)

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
    filename = args.data_file
    if filename is None:
        default_filename = '%s_to_%s.state' % (args.src_zone, args.dest_zone)
        filename = os.path.join('/var/lib/ceph/radosgw-agent',
                                default_filename)
    persistent_store = store.Simple(filename)
    syncer = sync.Syncer('metadata', src, dest, persistent_store)
    if args.sync_scope == 'full':
        syncer.sync_full(args.num_workers, args.lock_timeout)
    else:
        if persistent_store.last_full_sync() == store.UNIX_EPOCH:
            log.error('Full sync data not found - must run full sync before '
                      'partial sync can work')
            sys.exit(1)
        syncer.sync_partial(args.num_workers, args.lock_timeout)

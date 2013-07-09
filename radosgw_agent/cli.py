import argparse
import logging

from . import sync

LOG = logging.getLogger(__name__)

def parse_args(args, namespace):
    parser = argparse.ArgumentParser(
        description='Synchronize radosgw installations',
        )
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
        '--src-az', '--source-availability-zone',
        dest='src_zone',
        required=True,
        help='availability zone to sync from',
        )
    parser.add_argument(
        '--src-access-key',
        dest='src_akey',
        required=True,
        help='access key for source zone system user',
        )
    parser.add_argument(
        '--src-secret-key',
        dest='src_skey',
        required=True,
        help='secret key for source zone system user',
        )
    parser.add_argument(
        '--src-host',
        required=True,
        help='hostname or ip address of source radosgw',
        )
    parser.add_argument(
        '--src-port',
        type=int,
        default=None,
        help='port number for source radosgw'
        )
    parser.add_argument(
        '--dest-az', '--destination-availability-zone',
        dest='dest_zone',
        required=True,
        help='availability zone to sync to',
        )
    parser.add_argument(
        '--dest-access-key',
        dest='dest_akey',
        required=True,
        help='access key for destination zone system user',
        )
    parser.add_argument(
        '--dest-secret-key',
        dest='dest_skey',
        required=True,
        help='secret key for destination zone system user',
        )
    parser.add_argument(
        '--dest-host',
        required=True,
        help='hostname or ip address of destination radosgw',
        )
    parser.add_argument(
        '--dest-port',
        type=int,
        default=None,
        help='port number for destination radosgw'
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
        '--sync-type',
        choices=['data', 'metadata'],
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
    return parser.parse_args(args=args, namespace=namespace)

def main(args=None, namespace=None):
    args = parse_args(args, namespace)
    syncer = sync.Syncer(args.sync_type)
    syncer.sync_all(args.src_akey, args.src_skey, args.src_host, args.src_port,
                    args.src_zone, args.dest_akey, args.dest_skey,
                    args.dest_host, args.dest_port, args.dest_zone,
                    args.num_workers, args.lock_timeout)

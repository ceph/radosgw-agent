import logging
import sys
import os

# set an environ variable that tells us that we are really testing
os.environ['PYTEST'] = '1'

# this console logging configuration is basically just to be able to see output
# in tests, and this file gets executed by py.test when it runs, so we get that
# for free.

# Console Logger
sh = logging.StreamHandler()
sh.setLevel(logging.WARNING)

formatter = logging.Formatter(
    fmt='%(asctime)s.%(msecs)03d %(process)d:%(levelname)s:%(name)s:%(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    )
sh.setFormatter(formatter)


# because we're in a module already, __name__ is not the ancestor of
# the rest of the package; use the root as the logger for everyone
root_logger = logging.getLogger()

# allow all levels at root_logger, handlers control individual levels
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(sh)

console_loglevel = logging.DEBUG  # start at DEBUG for now

# Console Logger
sh.setLevel(console_loglevel)





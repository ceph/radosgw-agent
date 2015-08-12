Changelog
=========

1.2.4
-----
12-Aug-2015
* Fix invalid references for HttpError in lock.py
* Fix an issue where pinning of the mock library would make installation fail

1.2.3
-----
15-Jul-2015
* suppress override of config settings by argparse defaults
* properly detect ipv6 endpoints
* add Python 2.7 testing


1.2.2
-----
27-Apr-2015
* Improve terminal logging with better report to actua sync state
* Catch all exceptions to create better error reporting at the terminal
* If log location is not available fall back to current working directory
* Add a flag to indicate versioning support of endpoints
* support object versioning operations
* ensure logging is fully configured before any parsing to display errors
  regardless of failure
* set the version in ``__init__.py`` and display it when using help
* log all initial settings and flags of the agent when it starts

1.2.1
-----
* Parity in version release for DEB/RPMs to PyPI. Previous 1.2 release had
  fixes available only for the Python package.

1.2
---
* Improve usage for log (working better with logrotate)
* Fixes for racing threads when shard number changes
* Better logging of exceptions
* Retry sync when transient errors are returned by the gateway.
* Drops dependency on Python's ``request`` library (in favour of ``boto``)
* Better support of objects when they are not found.
* When there are buckets with no logs, process them as a full sync.
* Fix mishandling of reserved characters in URLs.

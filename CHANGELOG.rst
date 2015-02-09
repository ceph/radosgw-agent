Changelog
=========

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

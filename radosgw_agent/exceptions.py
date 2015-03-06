
class AgentError(Exception):
    """
    The actual base exception for the agent
    """


class ClientException(AgentError):
    """
    Historical base radosgw_agent client exception.
    """
    pass


class NetworkError(AgentError):
    pass


class RegionMapError(AgentError):

    def __init__(self, error):
        self.error = error

    def __str__(self):
        msg = 'Could not retrieve region map from destination: %s'
        return msg % self.error


class InvalidProtocol(ClientException):
    pass


class InvalidHost(ClientException):
    pass


class InvalidZone(ClientException):
    pass


class ZoneNotFound(ClientException):
    pass


class BucketEmpty(ClientException):
    pass


class HttpError(ClientException):
    def __init__(self, code, body):
        self.code = code
        self.str_code = str(code)
        self.body = body
        self.message = 'Http error code %s content %s' % (code, body)

    def __str__(self):
        return self.message


class NotFound(HttpError):
    pass


class SkipShard(Exception):
    pass


class SyncError(Exception):
    pass


class SyncTimedOut(SyncError):
    pass


class SyncFailed(SyncError):
    pass

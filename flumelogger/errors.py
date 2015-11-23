class ConfigurationError(Exception):
    """Raised when something is incorrectly configured."""


class ConnectionFailure(Exception):
    """Raised when a connection to the thrift source cannot be made os is lost."""


class ServerSelectionError(Exception):
    """Thrown when no thrift source is available for an operation."""


class OperationFailure(Exception):
    """Raised when a thrift operation fails."""

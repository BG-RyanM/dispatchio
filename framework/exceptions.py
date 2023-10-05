class RegistrationError(Exception):
    """Exception raised for errors in registering or deregistering listener."""


class DispatcherError(Exception):
    """Exception raised when dispatcher encounters error."""


class ListenerError(Exception):
    """Exception raised when message listener encounters error."""

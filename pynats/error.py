"""Exceptions that might get thrown"""


class NATSException(Exception):
    """General NATS protocol exception"""

    pass


class AuthException(NATSException):
    """Thrown when there is an authentication related problem"""

    pass

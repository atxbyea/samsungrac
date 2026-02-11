"""Exceptions for the Climate IP integration."""
from homeassistant.exceptions import HomeAssistantError

class CannotConnect(HomeAssistantError):
    """Error to indicate we cannot connect."""

class ConnectionRefused(CannotConnect):
    """Error to indicate the connection was refused."""

class InvalidHeaderError(CannotConnect):
    """Error to indicate the device sent malformed HTTP headers."""

class AuthError(HomeAssistantError):
    """Error to indicate there is an authentication problem."""

class CertNotFound(HomeAssistantError):
    """Error to indicate the certificate file is missing."""

class TokenAcquisitionError(HomeAssistantError):
    """Base custom exception for token acquisition errors."""

class AuthTurnedOffError(TokenAcquisitionError):
    """Raised when authentication fails because the device was turned off (ErrorCode 301)."""
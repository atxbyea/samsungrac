# custom_components/climate_ip/token_acquirer.py
"""Helper to acquire a token from older Samsung AC units in phases."""
import asyncio
import logging
import os
import re
import ssl
from typing import Any, Dict, Optional

from .exceptions import CannotConnect, TokenAcquisitionError, AuthTurnedOffError, CertNotFound
from .helpers import mask_sensitive_data

_LOGGER = logging.getLogger(__name__)

class SamsungTokenAcquirer:
    """Manages the phased token acquisition process."""

    def __init__(self, hass, ip_address: str, cert_path: Optional[str] = None):
        """Initialize the acquirer."""
        self._hass = hass
        self._ip_address = ip_address
        self._user_cert_path = cert_path  # Store the original user-provided path

        # Resolve the certificate path. If a path without a directory is provided,
        # assume it is relative to the integration's directory.
        if cert_path:
            if not os.path.dirname(cert_path):
                _LOGGER.debug("Certificate path '%s' appears to be a filename. Resolving relative to integration directory.", cert_path)
                self._resolved_cert_path = os.path.join(os.path.dirname(__file__), cert_path)
            else:
                # The path is absolute or contains directory components, use it as is.
                self._resolved_cert_path = cert_path
        else:
            self._resolved_cert_path = None
        _LOGGER.debug("Final resolved certificate path to be used for connection: %s", self._resolved_cert_path)

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def _connect(self) -> Optional[Dict[str, Any]]:
        """
        Establish a connection to the device by trying different certificate and cipher strategies.
        Returns a dictionary with the successful certificate path and verify mode.
        """
        cfg = self
        last_error = None

        # Define cipher suites to try
        cipher_configs = [ # Add new Cipher Suite C for broader compatibility
            ("HIGH:!DH:!aNULL:@SECLEVEL=0", "Cipher Suite A"),
            ("HIGH:!aNULL:!MD5:@SECLEVEL=0", "Cipher Suite B"),
            ("ALL:@SECLEVEL=0", "Cipher Suite C")
        ]

        # Define certificate strategies based on user input
        resolved_user_cert = self._resolved_cert_path
        default_cert_path = os.path.join(os.path.dirname(__file__), 'ac14k_m.pem')

        strategies = []
        if resolved_user_cert:
            # If a user certificate is provided, try strict verification first, then no verification.
            strategies.append({'cert': resolved_user_cert, 'name': 'User Cert (Strict Verify)', 'verify_mode': ssl.CERT_REQUIRED})
            strategies.append({'cert': resolved_user_cert, 'name': 'User Cert (No Verify)', 'verify_mode': ssl.CERT_NONE})
            # As a fallback, try with no certificate at all.
            strategies.append({'cert': None, 'name': 'No Certificate (Fallback)'})
        else:
            # If no user certificate, the only possible verification mode is CERT_NONE.
            strategies.append({'cert': None, 'name': 'No Certificate (Default)'})
            # As a fallback, try with the integration's default certificate.
            strategies.append({'cert': default_cert_path, 'name': 'Default Certificate (Fallback)'})

        # Build a list of all possible connection attempts
        all_attempts = [
            {
                'cert': strategy['cert'],
                # Default to CERT_NONE if verify_mode is not specified.
                'verify_mode': strategy.get('verify_mode', ssl.CERT_NONE),
                'cipher_config': cipher_config,
                'strategy_name': strategy['name']
            }
            for strategy in strategies
            for cipher_config in cipher_configs
        ]

        for attempt in all_attempts:
            cert_path = attempt['cert']
            ciphers, cipher_name = attempt['cipher_config']
            strategy_name = attempt['strategy_name']
            verify_mode = attempt['verify_mode']

            try:
                _LOGGER.debug("Attempting connection with Strategy: '%s', Cipher: '%s', Verify: %s", strategy_name, cipher_name, verify_mode)
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
                ssl_context.set_ciphers(ciphers)
                ssl_context.verify_mode = verify_mode
                ssl_context.check_hostname = False

                if cert_path:
                    _LOGGER.debug("Loading certificate: %s", os.path.basename(cert_path))
                    try:
                        # Use to_thread for the blocking file I/O
                        await asyncio.to_thread(ssl_context.load_verify_locations, cafile=cert_path)
                        await asyncio.to_thread(ssl_context.load_cert_chain, cert_path)
                    except (ssl.SSLError, FileNotFoundError) as e:
                        _LOGGER.error("Failed to load certificate '%s': %s", cert_path, e)
                        raise CertNotFound(f"Failed to load certificate file: {e}") from e

                conn_future = asyncio.open_connection(self._ip_address, 2878, ssl=ssl_context)
                self._reader, self._writer = await asyncio.wait_for(conn_future, timeout=15)
                
                _LOGGER.info("SSL connection successful with Strategy: '%s', Cipher: '%s'", strategy_name, cipher_name)
                
                # Connection successful, read initial handshake and return the working cert path
                try:
                    initial_data = await asyncio.wait_for(self._reader.read(4096), timeout=15.0)
                    _LOGGER.debug("Received initial handshake: %s", initial_data.decode('utf-8', 'ignore'))
                except asyncio.TimeoutError:
                    _LOGGER.warning("Did not receive an initial handshake, but connection is open. Continuing.")
                
                # --- Return Logic ---
                # If the successful certificate was the user-provided one, return the original user input.
                successful_config = {'cert': None, 'verify_mode': verify_mode}
                if cert_path == resolved_user_cert:
                    successful_config['cert'] = self._user_cert_path
                # If the successful certificate was the default one, return its filename.
                elif cert_path == default_cert_path:
                    successful_config['cert'] = os.path.basename(default_cert_path)
                
                _LOGGER.info(
                    "Successful connection config found: cert='%s', verify_mode=%s",
                    successful_config.get('cert'), successful_config.get('verify_mode')
                )
                return successful_config

            # --- START OF FIX: Explicitly catch TimeoutError and add a small delay ---
            except asyncio.TimeoutError as e:
                _LOGGER.debug("Connection attempt with '%s' / '%s' timed out after 15s. Trying next.", strategy_name, cipher_name)
                last_error = e
            except (ConnectionRefusedError, OSError) as e:
            # --- END OF FIX ---
                _LOGGER.debug("Connection attempt with '%s' / '%s' failed: %s. Trying next.", strategy_name, cipher_name, e)
                last_error = e
            except CertNotFound as e:
                _LOGGER.warning("Certificate error with strategy '%s': %s. Trying next.", strategy_name, e)
                # No need to sleep here, as this is a client-side configuration error.
                continue
            except Exception as e:
                _LOGGER.warning("Connection with '%s' / '%s' failed unexpectedly: %s. Trying next.", strategy_name, cipher_name, e)
                last_error = e

            # If an attempt fails with a connection error, close the connection and wait before the next try.
            await self.async_close()
            _LOGGER.debug("Waiting 1.5s before next connection attempt.")
            await asyncio.sleep(1.5)

        # If all attempts failed
        _LOGGER.error("All connection attempts failed. Last error: %s", last_error)
        raise CannotConnect(f"All connection attempts failed. Last error: {last_error}") from last_error

    async def async_initiate_pairing(self) -> Optional[Dict[str, Any]]:
        """
        Phase 1: Connects, puts the device in pairing mode, and returns the successful connection config.
        """
        _LOGGER.info("Initiating pairing for %s", self._ip_address)
        successful_config = await self._connect()
        
        if not self._writer:
            raise TokenAcquisitionError("Connection failed, writer not available.")

        request_msg = b'<Request Type="GetToken" />\r\n'
        _LOGGER.debug("Sending GetToken request: %s", request_msg.decode('utf-8').strip())
        self._writer.write(request_msg)
        await self._writer.drain()

        try:
            # --- START OF FIX: Add null check for self._reader ---
            if not self._reader:
                raise TokenAcquisitionError("Connection failed, reader not available.")
            # --- END OF FIX ---
            data = await asyncio.wait_for(self._reader.read(4096), timeout=15.0)
            decoded_data = data.decode('utf-8', 'ignore')
            _LOGGER.debug("Received response for GetToken: %s", decoded_data.strip())
            # --- START OF FIX ---
            # Some devices respond with 'InvalidateAccount' instead of 'Ready' during the
            # initial pairing. We should treat this as a successful initiation.
            if '<Response Type="GetToken" Status="Ready"/>' not in decoded_data and 'InvalidateAccount' not in decoded_data:
            # --- END OF FIX ---
                raise TokenAcquisitionError("Did not receive 'Ready' status from AC unit")
            _LOGGER.info("AC unit is 'Ready'. Pairing initiated successfully.")
            return successful_config # Return the successful config dict
        except asyncio.TimeoutError:
            raise TokenAcquisitionError("Timeout waiting for 'Ready' response")

    async def async_wait_for_token(self) -> str:
        """
        Phase 2: Waits for the user to press the power button and retrieves the token.
        """
        if not self._reader:
            raise TokenAcquisitionError("Connection not established. Run initiate_pairing first")
        
        _LOGGER.info("Now listening for the token...")
        try:
            data = await asyncio.wait_for(self._reader.read(4096), timeout=45.0)
            
            if not data:
                raise TokenAcquisitionError("Connection closed by device.")

            decoded_buffer = data.decode('utf-8', 'ignore')
            _LOGGER.debug("Received data after button press: %s", mask_sensitive_data(decoded_buffer))
            
            if 'Status="Fail"' in decoded_buffer and 'Type="Authenticate"' in decoded_buffer:
                error_code_match = re.search(r'ErrorCode="(\d+)"', decoded_buffer)
                error_code = error_code_match.group(1) if error_code_match else "Unknown"
                _LOGGER.error("Authentication failed with ErrorCode: %s", error_code)
                if error_code == "301":
                    raise AuthTurnedOffError("Authentication failed: The device was likely turned off instead of on (ErrorCode 301).")
                else:
                    raise TokenAcquisitionError(f"Authentication failed with ErrorCode {error_code}")

            token_match = re.search(r'Token="([a-fA-F0-9-]{36})"', decoded_buffer)
            if token_match:
                _LOGGER.info("Successfully acquired token.")
                return token_match.group(1)
            
            raise TokenAcquisitionError("Received unexpected data instead of a token")
        except asyncio.TimeoutError:
            raise TokenAcquisitionError("Token not received within the 45-second window")
        finally:
            await self.async_close()

    async def async_close(self) -> None:
        """Closes the connection."""
        if self._writer:
            _LOGGER.info("Closing connection.")
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except (ssl.SSLError, ConnectionResetError, asyncio.CancelledError):
                _LOGGER.debug("Ignoring non-critical error during connection close.")

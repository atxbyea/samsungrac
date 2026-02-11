# custom_components/climate_ip/connection_request_tls_auto.py
"""
Synchronous connection engine using requests with tolerance for mTLS renegotiation.

TARGET DEVICES:
- Samsung SmartThings HVAC (connection type: 'request_tls_auto')
- Samsung SmartThings DHW (connection type: 'request_tls_auto')

This engine creates a FRESH session for every request, which is inefficient but
necessary for some devices that do not support Keep-Alive correctly or require
frequent TLS renegotiation.

NOTE: Unlike the 'Legacy (requests)' engine, these SmartThings devices typically 
do NOT require the 'urllib3' monkey-patch for malformed headers, but this file 
inherits from ConnectionRequestBase which might apply it globally.
"""
# Monkey-patch urllib3 to be more tolerant of malformed headers from some AC units.
import urllib3.util.response as response_util
from urllib3.exceptions import HeaderParsingError
import urllib3.connection as connection_mod
import logging

_LOGGER_PATCH = logging.getLogger(__package__)

_original_assert = response_util.assert_header_parsing

def _tolerant_assert_header_parsing(headers):
    """A tolerant version of assert_header_parsing that logs instead of raising."""
    try:
        _original_assert(headers)
    except HeaderParsingError as e:
        _LOGGER_PATCH.debug(
            "Ignored HeaderParsingError: %s",
            e
        )

response_util.assert_header_parsing = _tolerant_assert_header_parsing
connection_mod.assert_header_parsing = _tolerant_assert_header_parsing
import copy
import concurrent.futures
import json
import logging
import os
import re
import ssl
import time
from typing import Any, Dict, Tuple

from requests.adapters import HTTPAdapter

from .connection import Connection, register_connection
from .exceptions import AuthError, CannotConnect
from .helpers import mask_sensitive_data
from .const import (
    CONF_CERT,
    CONFIG_DEVICE_CONNECTION_TEMPLATE,
    CONFIG_DEVICE_CONNECTION_PARAMS,
    CONFIG_DEVICE_CONNECTION,
    CONFIG_DEVICE_CONDITION_TEMPLATE,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)

CONNECTION_TYPE_REQUEST = "tls_auto"
CONNECTION_TYPE_REQUEST_PRINT = "request_tls_auto_print"

class SamsungHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1
        ssl_context.set_ciphers("ALL:@SECLEVEL=0")
        kwargs["ssl_context"] = ssl_context
        return super().init_poolmanager(*args, **kwargs)

def _mask_request_params(params: dict, log_prefix: str) -> dict:
    """Return a copy of request params with sensitive data masked for logging."""
    masked_params = copy.deepcopy(params)

    # List of sensitive keys to mask
    SENSITIVE_KEYS = ["token", "DeviceToken", "Authorization", "mac", "unique_id", "uuid", "DUID"]

    # 1. Mask headers
    headers = masked_params.get("headers")
    if isinstance(headers, dict):
        for key, value in headers.items():
            if key in SENSITIVE_KEYS and isinstance(value, str) and len(value) > 8:
                headers[key] = f"***{value[-6:]}"

    # 2. Mask JSON body
    json_payload = masked_params.get("json")
    if isinstance(json_payload, dict):
        for key, value in json_payload.items():
            if key in SENSITIVE_KEYS and isinstance(value, str) and len(value) > 8:
                json_payload[key] = f"***{value[-6:]}"

    # 3. Mask URL
    url = masked_params.get("url")
    if isinstance(url, str):
        # Regular expression to find UUIDs or long alphanumeric strings
        url = re.sub(r'([a-fA-F0-9]{8,})', lambda m: f"***{m.group(1)[-6:]}", url)
        masked_params["url"] = url

    return masked_params

class ConnectionRequestBase(Connection):
    def __init__(self, hass_config, _logger, insecure_ssl=False, timeout=30, retry_delay=1.0, debug=False):
        super(ConnectionRequestBase, self).__init__(hass_config, _logger)
        self._params: Dict[str, Any] = {"timeout": timeout}
        self._max_retries = 3
        self._embedded_command = None
        self._controller = None # Will be set by the property that creates this.
        logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
        self.update_configuration_from_hass(hass_config)
        self._condition_template = None
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._insecure_ssl = insecure_ssl
        self._retry_delay = retry_delay
        self._debug = debug

    def set_controller_ref(self, controller):
        """Allows the property to set a reference to the main controller."""
        self._controller = controller

    @property
    def log_prefix(self) -> str:
        """Get the log prefix from the controller for consistent logging."""
        if self._controller:
            return self._controller.log_prefix
        return "[NO_ID]"

    def update_auth_token(self, token: str):
        """Updates the Authorization header with a new token."""
        if self._params and "headers" in self._params:
            self._params["headers"]["authorization"] = f"Bearer {token}"
            _LOGGER.info("%s [Auth] Updated Authorization header with new token.", self.log_prefix)


    async def close(self):
        """Async wrapper for closing resources. No persistent session in tls_auto."""
        self._thread_pool.shutdown(wait=False)

    def __del__(self):
        self._thread_pool.shutdown(wait=False)

    @property
    def embedded_command(self):
        return self._embedded_command

    @property
    def condition_template(self):
        return self._condition_template

    def update_configuration_from_hass(self, hass_config):
        if hass_config is not None:
            cert_file = hass_config.get(CONF_CERT, None)
            if cert_file is not None:
                if cert_file.find("\\") == -1 and cert_file.find("/") == -1:
                    cert_file = os.path.join(os.path.dirname(__file__), cert_file)

            self._params[CONF_CERT] = cert_file # type: ignore

    def load_from_yaml(self, node, connection_base):
        from jinja2 import Template

        if connection_base:
            self._params.update(connection_base._params.copy())
            self._condition_template = connection_base._condition_template
            self._insecure_ssl = connection_base._insecure_ssl
            self._retry_delay = connection_base._retry_delay
            self._debug = connection_base._debug

        if node:
            self._params.update(node.get(CONFIG_DEVICE_CONNECTION_PARAMS, {}))
            self._insecure_ssl = node.get("insecure_ssl", self._insecure_ssl)
            self._params["timeout"] = node.get("timeout", self._params["timeout"])
            self._retry_delay = node.get("retry_delay", self._retry_delay)
            self._debug = node.get("debug", self._debug)
            if CONFIG_DEVICE_CONNECTION in node:
                self._embedded_command = self.create_updated(
                    node[CONFIG_DEVICE_CONNECTION]
                )
            if CONFIG_DEVICE_CONDITION_TEMPLATE in node:
                self._condition_template = Template(
                    node[CONFIG_DEVICE_CONDITION_TEMPLATE]
                )

        return True

    def check_execute_condition(self, device_state):
        do_execute = True
        if self.condition_template is not None:
            _LOGGER.debug("%s Evaluating execute condition for command.", self.log_prefix)
            try:
                rendered_condition = self.condition_template.render(
                    device_state=device_state
                )
                _LOGGER.debug("%s Execute condition result: %s", self.log_prefix, rendered_condition)
                do_execute = rendered_condition == "1"
            except Exception as e:
                _LOGGER.error(
                    "%s Error evaluating execute condition, executing command anyway. Error: %s", self.log_prefix, e, exc_info=True
                )
                do_execute = True

        return do_execute

    def execute_internal(self, template, value, device_state, device_id=None) -> Tuple[Any, bool, int]:
        import warnings
        import requests
        from urllib3.exceptions import InsecureRequestWarning

        params = self._params.copy()
        if template is not None:
            try:
                params.update(json.loads(template.render(value=value, device_id=device_id)))
            except Exception as exc:
                _LOGGER.error("%s Error rendering template or parsing JSON: %s", self.log_prefix, exc)
                raise ValueError(f"Template rendering failed: {exc}") from exc

        with warnings.catch_warnings():
            for attempt in range(self._max_retries):
                try:
                    warnings.filterwarnings("ignore", category=InsecureRequestWarning)
                    with requests.sessions.Session() as session:
                        if self._insecure_ssl:
                            session.mount("https://", SamsungHTTPAdapter())
                        
                        if self._debug:
                            _LOGGER.debug("%s Executing request (attempt %d/%d) with params: %s", self.log_prefix, attempt + 1, self._max_retries, mask_sensitive_data(params)) # lgtm [py/clear-text-logging-sensitive-data]

                        resp = session.request(**params)
                        resp.raise_for_status()
                        
                        _LOGGER.debug( # Use DEBUG for successful commands to avoid log spam
                            "%s Command successful with code: %s",
                            self.log_prefix, resp.status_code
                        )
                        
                        return (resp.json(), True, resp.status_code)

                except json.JSONDecodeError as e:
                    _LOGGER.warning("%s Failed to parse JSON response. Response text: %s", self.log_prefix, resp.text, exc_info=True)
                    raise ValueError("Failed to parse JSON response") from e

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code in (401, 403):
                        _LOGGER.debug("%s Authentication error: %s", self.log_prefix, e, exc_info=True)
                        raise AuthError(f"Authentication failed with status {e.response.status_code}") from e
                    else:
                        _LOGGER.error("%s HTTP error: %s", self.log_prefix, e, exc_info=True)
                        raise CannotConnect(f"HTTP error {e.response.status_code}") from e
                
                except requests.exceptions.Timeout as e:
                    if attempt < self._max_retries - 1:
                        _LOGGER.warning("%s Request timed out. Retrying in %s seconds...", self.log_prefix, self._retry_delay)
                        time.sleep(self._retry_delay)
                        continue
                    else:
                        _LOGGER.error("%s Request timed out after %d attempts: %s", self.log_prefix, self._max_retries, e, exc_info=True)
                        raise CannotConnect("Request timed out") from e

                except requests.exceptions.SSLError as e:
                    if "CERTIFICATE_VERIFY_FAILED" in str(e):
                        _LOGGER.error("%s SSL Certificate verification failed. Please check your configuration (is 'verify: True' set for a self-signed cert?). Error: %s", self.log_prefix, e)
                        raise CannotConnect("SSL verification failed. Set 'verify: False' or provide a valid CA.") from e
                    else:
                        _LOGGER.error("%s SSL error: %s", self.log_prefix, e, exc_info=False)
                        raise CannotConnect(f"SSL error: {e}") from e

                except requests.exceptions.ConnectionError as e:
                    # Downgrade to WARNING and remove traceback for expected connection refusals (e.g. device offline)
                    _LOGGER.warning("%s Connection error: %s", self.log_prefix, e) # removed exc_info=True
                    raise CannotConnect("Failed to establish a connection") from e

                except requests.exceptions.RequestException as e:
                    _LOGGER.error("%s Unhandled request exception: %s", self.log_prefix, e, exc_info=True)
                    raise CannotConnect(f"An unexpected network error occurred: {e}") from e
        # --- FIX: Fallback return to satisfy static analysis ---
        # This line ensures the function returns a tuple even if the loop finishes
        # without raising an exception (which shouldn't happen, but satisfies the linter).
        return (None, False, 0)

    def execute(self, template, value, device_state, device_id=None):
        """Synchronously executes the command. To be run in an executor."""
        is_poll_request = template and not value and not device_state
        if is_poll_request:
            _LOGGER.debug("%s Received poll request.", self.log_prefix)
        else:
            _LOGGER.debug("%s Received command request with value: %s", self.log_prefix, value)

        if self.embedded_command:
            _LOGGER.debug("%s Executing embedded command...", self.log_prefix)
            if hasattr(self.embedded_command, 'set_controller_ref'):
                self.embedded_command.set_controller_ref(self._controller)
            # Since this is now sync, we can't await. The embedded command must also be sync.
            self.embedded_command.execute(template, value, device_state, device_id)

        if not self.check_execute_condition(device_state):
            _LOGGER.debug("%s Execute condition not met, skipping command", self.log_prefix)
            return {}

        _LOGGER.debug("%s Executing command...", self.log_prefix)
        try:
            # Run the blocking execute_internal method in an executor
            j, ok, code = self.execute_internal(template, value, device_state, device_id)
            return j
        except Exception as e:
            raise e


@register_connection
class ConnectionRequestTlsAuto(ConnectionRequestBase):
    def __init__(self, hass_config, logger, insecure_ssl=False, timeout=30, retry_delay=1.0, debug=False):
        super(ConnectionRequestTlsAuto, self).__init__(hass_config, logger, insecure_ssl, timeout, retry_delay, debug)

    @staticmethod
    def match_type(type):
        return type == CONNECTION_TYPE_REQUEST or type == "request_tls_auto"

    def create_updated(self, node):
        c = ConnectionRequestTlsAuto(None, _LOGGER, self._insecure_ssl, self._params["timeout"], self._retry_delay, self._debug)
        c.load_from_yaml(node, self)
        return c

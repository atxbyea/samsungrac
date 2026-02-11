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
import asyncio
import copy
import concurrent.futures
import contextlib # Added for _borrow_session
import json
import logging
import requests # Added for requests.Session
import re
import os
import ssl
import time
from typing import Any, Dict, Optional, Tuple
from homeassistant.const import CONF_IP_ADDRESS, CONF_MAC, CONF_PORT, CONF_TOKEN
from requests.adapters import HTTPAdapter

from .connection import Connection, register_connection
from .exceptions import AuthError, CannotConnect, ConnectionRefused
from .helpers import mask_sensitive_data
from .const import (
    CONF_CERT,
    CONFIG_DEVICE_CONNECTION_TEMPLATE,
    CONFIG_DEVICE_CONNECTION_PARAMS,
    CONFIG_DEVICE_CONNECTION,
    CONFIG_DEVICE_CONDITION_TEMPLATE,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)

CONNECTION_TYPE_REQUEST = "request"
CONNECTION_TYPE_REQUEST_PRINT = "request_print"

REQUEST_MAX_RETRIES = 3
REQUEST_RETRY_DELAY = 1.0  # seconds

class SamsungHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
        ssl_context.check_hostname = False
        ssl_context.set_ciphers("ALL:@SECLEVEL=0")
        pool_kwargs["ssl_context"] = ssl_context
        
        # --- START OF FIX: Limit Pool Concurrency ---
        # Enforce single connection per session to prevent "leaks" (multiple concurrent connections)
        # resulting from parallel thread execution.
        # We ignore the 'connections' and 'maxsize' args passed by requests/adapter and enforce 1.
        forced_connections = 1
        forced_maxsize = 1
        forced_block = True
    
        # DEBUG: Trace new pool manager creation
        _LOGGER.debug(
            "[SamsungHTTPAdapter] Initializing new PoolManager. "
            "Original: connections=%s, maxsize=%s, block=%s. "
            "Forced: connections=%s, maxsize=%s, block=%s", 
            connections, maxsize, block,
            forced_connections, forced_maxsize, forced_block
        )
        
        return super().init_poolmanager(forced_connections, forced_maxsize, block=forced_block, **pool_kwargs)
        # --- END OF FIX ---

    # --- START OF FIX: Add logging to close ---
    def close(self):
        """Log when the adapter is closed."""
        _LOGGER.debug("[SamsungHTTPAdapter] Closing adapter and clearing pool.")
        super().close()
    # --- END OF FIX ---

    def cert_verify(self, conn, url, verify, cert):
        """
        Override default certification verification.
        We rely entirely on the custom SSLContext created in init_poolmanager.
        Blocking this method prevents 'requests' from overriding our context
        with default 'CERT_REQUIRED' logic or modifying the connection state,
        which solves both the SSLError and the Keep-Alive dropping issue.
        """
        pass



class ConnectionRequestBase(Connection):
    def __init__(self, hass_config, _logger, session=None):
        super(ConnectionRequestBase, self).__init__(hass_config, _logger)
        self._params: Dict[str, Any] = {"timeout": 30}
        self._embedded_command = None # An optional nested command.
        self._controller = None  # Will be set by the property that creates this.
        logging.getLogger("urllib3.connectionpool").setLevel(logging.DEBUG)
        self.update_configuration_from_hass(hass_config)
        self._condition_template = None
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        # --- START OF FIX: Persistent Session ---
        # Initialize a persistent session to support Keep-Alive.
        if session:
            self._session = session
        else:
            self._session = requests.sessions.Session()
            self._session.verify = False 
            self._session.mount("https://", SamsungHTTPAdapter())
        
        # --- START OF FIX: Read keep_alive setting ---
        self._keep_alive = hass_config.get("keep_alive", True) if hass_config else True
        # --- END OF FIX ---
        
        # Registry for child connections (embedded commands) to propagate session updates
        self._children = []
        self._parent = None # Reference to parent connection for upward propagation




    def set_controller_ref(self, controller):
        """Allows the property to set a reference to the main controller."""
        self._controller = controller

    @property
    def log_prefix(self) -> str:
        """Get the log prefix from the controller for consistent logging."""
        if self._controller:
            return self._controller.log_prefix
        return "[NO_ID]"

    def _update_session(self, session):
        """Updates the session and propagates it to all children."""
        self._session = session
        _LOGGER.debug("%s [Session Propagation] Updated session to ID: %s", self.log_prefix, id(session))
        
        # Propagate to children
        for child in self._children:
             child._update_session(session)

    def _update_session_from_reset(self, session):
        """Entry point for updates triggered by internal reset."""
        if self._parent:
             _LOGGER.debug("%s [Session Propagation] Delegating session update to parent.", self.log_prefix)
             self._parent._update_session(session)
        else:
             self._update_session(session)





    async def close(self):
        """Async wrapper for closing the connection resources."""
        _LOGGER.debug("%s [ConnectionRequest] Closing connection resources (Async)...", self.log_prefix)
        # Run the sync close in an executor to avoid blocking the loop
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._close_sync)

    def _close_sync(self):
        """Explicitly close the session and thread pool (Synchronous)."""
        _LOGGER.debug("%s [ConnectionRequest] _close_sync: Cleanup started.", self.log_prefix)
        
        if hasattr(self, "_session") and self._session:
            try:
                self._session.close()
                _LOGGER.debug("%s [ConnectionRequest] Session closed.", self.log_prefix)
            except Exception as e:
                _LOGGER.error("%s [ConnectionRequest] Error closing session: %s", self.log_prefix, e)
        
        if hasattr(self, "_thread_pool") and self._thread_pool:
            try:
                _LOGGER.debug("%s [ConnectionRequest] Shutting down thread pool...", self.log_prefix)
                self._thread_pool.shutdown(wait=False)
            except Exception as e:
                _LOGGER.error("%s [ConnectionRequest] Error shutting down thread pool: %s", self.log_prefix, e)

    def __del__(self):
        # Fallback cleanup
        if hasattr(self, "_thread_pool"):
            self._thread_pool.shutdown(wait=False)
        self._close_sync()

    @contextlib.contextmanager
    def _borrow_session(self):
        """Yields the persistent session without closing it on exit."""
        _LOGGER.debug("%s [Debug] Borrowing session ID: %s", self.log_prefix, id(self._session))
        if self._session and self._session.adapters:
             adapter = self._session.get_adapter("https://")
             _LOGGER.debug("%s [Debug] Session Adapter ID for https://: %s", self.log_prefix, id(adapter))
             if hasattr(adapter, 'poolmanager'):
                 _LOGGER.debug("%s [Debug] PoolManager ID: %s", self.log_prefix, id(adapter.poolmanager))
        
        yield self._session

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
            self._keep_alive = getattr(connection_base, "_keep_alive", True)
            self._parent = connection_base


        if node:
            self._params.update(node.get(CONFIG_DEVICE_CONNECTION_PARAMS, {}))
            if "keep_alive" in node:
                self._keep_alive = node["keep_alive"]
            
            if CONFIG_DEVICE_CONNECTION in node:
                self._embedded_command = self.create_updated(
                    node[CONFIG_DEVICE_CONNECTION]
                )
                if self._embedded_command:
                     self._children.append(self._embedded_command)
                     _LOGGER.debug("%s [Session Propagation] Registered child connection.", self.log_prefix)
            if CONFIG_DEVICE_CONDITION_TEMPLATE in node:
                self._condition_template = Template(
                    node[CONFIG_DEVICE_CONDITION_TEMPLATE]
                )

        return True

    def check_execute_condition(self, device_state):
        do_execute = True
        if self.condition_template is not None:
            _LOGGER.debug("%s Evaluating execute condition", self.log_prefix)
            try:
                rendered_condition = self.condition_template.render(
                    device_state=device_state
                )
                _LOGGER.debug("%s Execute condition result: %s", self.log_prefix, rendered_condition)
                do_execute = rendered_condition == "1"
            except:
                _LOGGER.error(
                    "%s Error evaluating execute condition, executing command anyway", self.log_prefix
                )
                do_execute = True

        return do_execute

    def execute_internal(self, template, value, device_state, device_id=None) -> (json, bool, int):
        """Internal synchronous method to execute the HTTP request with retries."""
        import warnings
        import urllib3
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        from typing import Tuple, Any, Optional

        # --- START OF FIX: StreamWrapper logic ---
        # The StreamWrapper logic from controller_yaml.py needs to be applied here
        # to ensure placeholders are replaced just before execution.
        def _stream_wrapper(data: str, token: Optional[str], ip_address: Optional[str], device_id: Optional[str]) -> str:
            """Replaces placeholders in the rendered template string."""
            if token is not None:
                data = data.replace("__CLIMATE_IP_TOKEN__", str(token))
            if ip_address is not None:
                data = data.replace("__CLIMATE_IP_HOST__", str(ip_address))
            if device_id is not None:
                data = data.replace("__DEVICE_ID__", str(device_id))
            return data

        params = self._params.copy()
        if template is not None:
            try:
                # Pass device_id to the template for use in URLs, etc.
                rendered_template = template.render(value=value, device_id=device_id)
                # --- START OF FIX: Add null check for self._controller ---
                token = self._controller.token if self._controller else None
                ip_address = self._controller.ip_address if self._controller else None
                final_template = _stream_wrapper(rendered_template, token, ip_address, device_id)
                # --- END OF FIX ---
                params.update(json.loads(final_template))
            except Exception as exc:
                _LOGGER.error("%s Error rendering template or parsing JSON: %s", self.log_prefix, exc)
                raise ValueError(f"Template rendering failed: {exc}") from exc

        # INCREASE DELAY: Give the device more time to recover between failures (e.g., 5 seconds)
        LOCAL_RETRY_DELAY = 5.0 

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=InsecureRequestWarning) # type: ignore
            
            # CRITICAL FIX: Use the persistent session (Keep-Alive)
            # We use a helper context manager to yield the session without closing it,
            # preserving the indentation of the block below.
            with self._borrow_session() as session:
                # The session is already holding the adapter and pool. 
                # We do NOT remount it here to safe-guard connection reuse.

                for attempt in range(REQUEST_MAX_RETRIES):
                    try:
                        _LOGGER.debug("%s Request (attempt %s/%s): %s", self.log_prefix, attempt + 1, REQUEST_MAX_RETRIES, mask_sensitive_data(params))
                        
                        # --- ADAPTIVE KEEP-ALIVE LOGIC ---
                        # If we previously detected stability issues (timeouts likely due to missing Content-Length),
                        # we strictly force 'Connection: close'.
                        # pivot: Use a fresh copy of headers to avoid mutating self._params via shallow reference.
                        if getattr(self, "_force_close_connection", False):
                             # Ensure we don't fail if headers key is missing or None
                             if "headers" not in params or params["headers"] is None:
                                 params["headers"] = {}
                             else:
                                 # Shallow copy the headers dict to detach from self._params
                                 params["headers"] = params["headers"].copy()
                             
                             params["headers"]["Connection"] = "close"

                        # --- OPTIMIZATION: Fast Fail on First Attempt ---
                        # If we are seemingly in "stable" mode (keep-alive) but the device hangs (no Content-Length),
                        # the default timeout (e.g. 30s) will cause the Coordinator to mark us 'unavailable'
                        # BEFORE we have a chance to retry with Connection: close.
                        # So, for the FIRST attempt only, if we are not forcing close, cap the timeout to 10s.
                        request_params = params.copy() # This is now safe as we handled headers above
                        
                        # --- START OF FIX: Remove 'verify' from params ---
                        # Rely on the Adapter's SSLContext. Passing verify=False explicitely
                        # might cause requests/urllib3 to bypass the pool or recreate the connection.
                        if 'verify' in request_params:
                            del request_params['verify']
                        # --- END OF FIX ---

                        current_timeout = request_params.get('timeout', 30)
                        if attempt == 0 and not getattr(self, "_force_close_connection", False):
                            if isinstance(current_timeout, (int, float)) and current_timeout > 12:
                                _LOGGER.debug("%s [Optimization] Capping timeout to 10s for first attempt to allow retry within window.", self.log_prefix)
                                request_params['timeout'] = 10.0
                        
                        resp = session.request(**request_params)
                        
                        # --- START OF FIX: HTTP Version Detection ---
                        # Dynamically adjust Keep-Alive support based on server response.
                        # resp.raw.version is an integer: 10 (HTTP/1.0) or 11 (HTTP/1.1)
                        if getattr(resp.raw, "version", 0) == 11:
                             if getattr(self, "_force_close_connection", False):
                                 _LOGGER.debug("%s [Optimization] Server speaks HTTP/1.1. Re-enabling Keep-Alive.", self.log_prefix)
                             self._force_close_connection = False
                        elif getattr(resp.raw, "version", 0) == 10:
                             if not getattr(self, "_force_close_connection", False):
                                 _LOGGER.debug("%s [Compatibility] Server speaks HTTP/1.0. Enforcing 'Connection: close'.", self.log_prefix)
                             self._force_close_connection = True
                        # --- END OF FIX ---
                        
                        # --- DEBUGGING: Log Raw Response on Error ---
                        if resp.status_code >= 400:
                            # Try to mask JSON response if possible
                            try:
                                json_body = resp.json()
                                log_body = json.dumps(mask_sensitive_data(json_body))
                            except:
                                log_body = resp.text

                            _LOGGER.debug(
                                "%s [Debug] HTTP %s Response Body: %s", 
                                self.log_prefix, resp.status_code, log_body
                            )
                        # --------------------------------------------

                        resp.raise_for_status()
                        
                        # Use DEBUG level for successful command execution to avoid log spam.
                        _LOGGER.debug(
                            "%s Command successful with code: %s",
                            self.log_prefix, resp.status_code
                        )
                        
                        if not resp.text or not resp.text.strip():
                            _LOGGER.debug("%s Response was empty, returning None to trigger a poll.", self.log_prefix)
                            return (None, True, resp.status_code)

                        try:
                            return (resp.json(), True, resp.status_code)
                        except (requests.exceptions.JSONDecodeError, json.JSONDecodeError):
                            # --- START OF FIX: Treat non-JSON success response as a trigger to poll ---
                            # If the response is successful (2xx) but not valid JSON (e.g., just "OK"),
                            # it's a successful command acknowledgment. We return None to trigger a refresh.
                            _LOGGER.debug("%s Response was not valid JSON (e.g., 'OK'). Returning None to trigger poll. Response: %s", self.log_prefix, resp.text.strip())
                            return (None, True, resp.status_code)
                            # --- END OF FIX ---

                    except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as e:
                        _LOGGER.warning("%s Parsing response json failed! Not retrying. Error: %s", self.log_prefix, e)
                        raise ValueError("Failed to parse JSON response") from e

                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code in (401, 403):
                            _LOGGER.error("%s Authentication error: %s. Not retrying", self.log_prefix, e)
                            raise AuthError(f"Authentication failed with status {e.response.status_code}") from e
                        # Retrying 500 errors with longer delay
                        elif 500 <= e.response.status_code < 600 and attempt < REQUEST_MAX_RETRIES - 1:
                            _LOGGER.warning("%s Server error (%s). Retrying in %s seconds", self.log_prefix, e.response.status_code, LOCAL_RETRY_DELAY)
                            time.sleep(LOCAL_RETRY_DELAY)
                            continue
                        else:
                            # Enhanced error logging
                            _LOGGER.error("%s HTTP error: %s. Body: %s. Not retrying", self.log_prefix, e, getattr(e.response, 'text', 'No Body'))
                            raise CannotConnect(f"HTTP error {e.response.status_code}") from e
                    
                    except requests.exceptions.ReadTimeout as e:
                         # --- ADAPTIVE RECOVERY ---
                         if not getattr(self, "_force_close_connection", False):
                            _LOGGER.warning(
                                "%s [Legacy] ReadTimeout detected (%s). "
                                "The device likely violates HTTP protocol (missing Content-Length). "
                                "Switching to 'Connection: close' mode for future attempts.", 
                                self.log_prefix, str(e)
                            )
                            self._force_close_connection = True
                            # Continue to next attempt, which will now use Connection: close
                            if attempt < REQUEST_MAX_RETRIES - 1:
                                 continue
                         
                         # If we were already in force close mode OR ran out of retries
                         if attempt < REQUEST_MAX_RETRIES - 1:
                            _LOGGER.warning("%s ReadTimeout error. Retrying in %s seconds", self.log_prefix, LOCAL_RETRY_DELAY)
                            time.sleep(LOCAL_RETRY_DELAY)
                            continue
                         else:
                            _LOGGER.error("%s Request timed out (ReadTimeout) after %s attempts: %s", self.log_prefix, REQUEST_MAX_RETRIES, e)
                            raise CannotConnect("Request timed out (ReadTimeout)") from e

                    except requests.exceptions.Timeout as e:
                        if attempt < REQUEST_MAX_RETRIES - 1:
                            _LOGGER.warning("%s Request timed out. Retrying in %s seconds", self.log_prefix, LOCAL_RETRY_DELAY)
                            time.sleep(LOCAL_RETRY_DELAY)
                            continue
                        else:
                            _LOGGER.error("%s Request timed out after %s attempts: %s", self.log_prefix, REQUEST_MAX_RETRIES, e)
                            raise CannotConnect("Request timed out") from e

                    except requests.exceptions.ConnectionError as e:
                        # --- ADAPTIVE RECOVERY for Connection Errors (e.g. RemoteDisconnected) ---
                        if not getattr(self, "_force_close_connection", False):
                            _LOGGER.warning(
                                "%s [Legacy] ConnectionError detected (%s). "
                                "Switching to 'Connection: close' mode for future attempts.", 
                                self.log_prefix, str(e)
                            )
                            self._force_close_connection = True
                            if attempt < REQUEST_MAX_RETRIES - 1:
                                 # Retry immediately without sleep
                                 continue

                        if attempt < REQUEST_MAX_RETRIES - 1:
                            _LOGGER.warning("%s Connection error. Retrying in %s seconds", self.log_prefix, LOCAL_RETRY_DELAY)
                            time.sleep(LOCAL_RETRY_DELAY)
                            continue
                        else:
                            # Recursively check the exception chain for the root cause.
                            def has_connection_refused(exc):
                                if isinstance(exc, ConnectionRefusedError):
                                    return True
                                if exc.__cause__:
                                    return has_connection_refused(exc.__cause__)
                                if exc.__context__:
                                    return has_connection_refused(exc.__context__)
                                return False

                            if has_connection_refused(e):
                                _LOGGER.debug("%s Connection refused after %s attempts. Device is likely offline or IP is incorrect.", self.log_prefix, REQUEST_MAX_RETRIES)
                                raise CannotConnect("Connection was refused by the device") from e
                            else:
                                _LOGGER.error("%s Connection error after %s attempts: %s", self.log_prefix, REQUEST_MAX_RETRIES, e, exc_info=True)
                                raise CannotConnect("Failed to establish a connection") from e

                    except requests.exceptions.RequestException as e:
                        _LOGGER.error("%s Unhandled request exception: %s. Not retrying", self.log_prefix, e, exc_info=True)
                        raise CannotConnect(f"An unexpected network error occurred: {e}") from e
        
        # Fallback return to satisfy static analysis
        return (None, False, 0)

    def execute(self, template, value, device_state, device_id=None):
        """Synchronously executes the command. To be run in an executor."""
        if self.embedded_command:
             # If we have an embedded command, this acts as a command wrapper, not a simple poll.
             pass

        # Determine if it's a polling request (no value). 
        # Device state might be present during updates, so we don't check for it being None.
        # Template might also be None if using defaults, so we rely primarily on value being None.
        is_poll_request = value is None
        
        if is_poll_request:
            _LOGGER.debug("%s Received poll request.", self.log_prefix)
        else:
            _LOGGER.debug("%s Received command request with value: %s", self.log_prefix, value)
        
        # --- START OF FIX: Periodic Reset Logic (Legacy) ---
        if is_poll_request and not self._keep_alive:
             _LOGGER.debug("%s [Legacy|Periodic Reset] Closing persistent session before poll.", self.log_prefix)
             self._close_sync()
             
             # Small delay for TCP cleanup (consistent with aiohttp engine)
             time.sleep(0.2)
             
             # Re-create the session
             new_session = requests.sessions.Session()
             new_session.verify = False 
             new_session.mount("https://", SamsungHTTPAdapter())
             
             # Reset adaptive flags to give connection reuse a chance in the new cycle
             self._force_close_connection = False
             
             # Propagate the new session to self and children (delegating to parent if exists)
             self._update_session_from_reset(new_session)
             
             _LOGGER.debug("%s [Legacy|Periodic Reset] New session created.", self.log_prefix)
        # --- END OF FIX ---
        
        if self.embedded_command:
            _LOGGER.debug("%s Executing embedded command...", self.log_prefix)
        
            if hasattr(self.embedded_command, 'set_controller_ref'):
                self.embedded_command.set_controller_ref(self._controller)
            # Pass device_id to the nested command.
            self.embedded_command.execute(template, value, device_state, device_id)

        if not self.check_execute_condition(device_state):
            _LOGGER.debug("%s Execute condition not met, skipping command", self.log_prefix)
            return {}

        try:
            # --- START: Timing measurement for sync execute ---
            start_time = time.perf_counter()
            # --- END: Timing measurement ---
            # Pass device_id to the internal execution method.
            j, ok, code = self.execute_internal(template, value, device_state, device_id)
            elapsed = time.perf_counter() - start_time
            _LOGGER.info("%s [REQUESTS] Execute completed in %.3f seconds (status code %s)", self.log_prefix, elapsed, code)
            # --- END: Timing measurement ---
            # The retry logic for server errors (5xx) is now inside execute_internal

            # --- FIX: PREVENT DOUBLE POLLING ---
            # If the command was successful but returned no data, strictly return empty dict.
            # We DO NOT trigger a refresh here anymore. The Coordinator handles the 
            # post-command refresh logic (Smart Polling or Delay).
            if j is None:
                _LOGGER.debug("%s Command returned no data (or was a simple 'OK'). Returning empty dict.", self.log_prefix)
                return {}
            # -----------------------------------

            return j
        except Exception as e:
            raise e


@register_connection
class ConnectionRequest(ConnectionRequestBase):
    def __init__(self, hass_config, _logger, session=None):
        super(ConnectionRequest, self).__init__(hass_config, _logger, session=session)

    @staticmethod
    def match_type(type):
        return type == CONNECTION_TYPE_REQUEST

    def create_updated(self, node):
        c = ConnectionRequest(None, _LOGGER, session=self._session)
        
        # --- START OF MODIFICATION: Topology Debugging ---
        _LOGGER.debug(
            "%s [Topology] create_updated: Creating child (ID=%s) from parent (ID=%s).",
            self.log_prefix, id(c), id(self)
        )
        # --- END OF MODIFICATION ---
        
        c.load_from_yaml(node, self)
        
        # --- START OF FIX: Child Propagation ---
        # Register this new instance as a child so it receives session updates
        self._children.append(c)
        _LOGGER.debug("%s [Session Propagation] Registered property connection via create_updated.", self.log_prefix)
        # --- END OF FIX ---
        
        return c


test_json = {
    "Devices": [
        {
            "Alarms": [
                {
                    "alarmType": "Device",
                    "code": "FilterAlarm",
                    "id": "0",
                    "triggeredTime": "2019-02-25T08:46:01",
                }
            ],
            "ConfigurationLink": {"href": "/devices/0/configuration"},
            "Diagnosis": {"diagnosisStart": "Ready"},
            "EnergyConsumption": {"saveLocation": "/files/usage.db"},
            "InformationLink": {"href": "/devices/0/information"},
            "Mode": {
                "modes": ["Auto"],
                "options": [
                    "Comode_Off",
                    "Sleep_0",
                    "Autoclean_Off",
                    "Spi_Off",
                    "FilterCleanAlarm_0",
                    "OutdoorTemp_63",
                    "CoolCapa_35",
                    "WarmCapa_40",
                    "UsagesDB_254",
                    "FilterTime_10000",
                    "OptionCode_54458",
                    "UpdateAllow_0",
                    "FilterAlarmTime_500",
                    "Function_15",
                    "Volume_100",
                ],
                "supportedModes": ["Cool", "Dry", "Wind", "Auto"],
            },
            "Operation": {"power": "Off"},
            "Temperatures": [
                {
                    "current": 22.0,
                    "desired": 25.0,
                    "id": "0",
                    "maximum": 30,
                    "minimum": 16,
                    "unit": "Celsius",
                }
            ],
            "Wind": {"direction": "Fix", "maxSpeedLevel": 4, "speedLevel": 0},
            "connected": True,
            "description": "TP6X_RAC_16K",
            "id": "0",
            "name": "RAC",
            "resources": [
                "Alarms",
                "Configuration",
                "Diagnosis",
                "EnergyConsumption",
                "Information",
                "Mode",
                "Operation",
                "Temperatures",
                "Wind",
            ],
            "type": "Air_Conditioner",
            "uuid": "00000000-0000-0000-0000-000000000000",
        }
    ]
}


@register_connection
class ConnectionRequestPrint(ConnectionRequestBase):
    def __init__(self, hass_config, _logger, session=None):
        super(ConnectionRequestPrint, self).__init__(hass_config, _logger, session=session)

    @staticmethod
    def match_type(type):
        return type == CONNECTION_TYPE_REQUEST_PRINT

    def create_updated(self, node):
        c = ConnectionRequestPrint(None, _LOGGER, session=self._session)
        c.load_from_yaml(node, self)
        return c

    async def execute(self, template, value, device_state, device_id=None):
        _LOGGER.debug(
            "%s ConnectionRequestPrint (dry-run), execute with params: %s, device_id: %s",
            self.log_prefix, self._params, device_id
        )
        return test_json

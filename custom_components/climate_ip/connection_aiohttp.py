# custom_components/climate_ip/connection_aiohttp.py
"""
Asynchronous connection engine for modern Samsung devices (port 8888) using aiohttp.
This engine implements HTTP Keep-Alive for low latency and correct mTLS.
"""
import asyncio
import copy
import json
import time
import ssl
from urllib.parse import urlparse
import logging
import os
from jinja2 import Template
from typing import Any, Dict, Optional, Tuple
from functools import partial

import aiohttp
from homeassistant.const import CONF_TOKEN

from .connection import Connection, register_connection
from .exceptions import AuthError, CannotConnect, InvalidHeaderError
from .const import CONF_CERT
from .helpers import mask_sensitive_data

_LOGGER = logging.getLogger(__name__)

CONNECTION_TYPE_AIOHTTP_8888 = "samsung_8888_aiohttp"

@register_connection
class ConnectionAiohttp8888(Connection):
    """
    An asynchronous connection handler for Samsung devices on port 8888.
    It uses aiohttp's ClientSession for persistent connections (Keep-Alive)
    and implements the correct mTLS (mutual-TLS) authentication.
    """

    def __init__(self, config: Dict[str, Any], logger: logging.Logger, hass: Any, session: aiohttp.ClientSession, ip_address: str):
        logger.debug("[aiohttp_init] Initializing ConnectionAiohttp8888. IP: %s", ip_address)
        super().__init__(config, logger)
        self._hass = hass
        self._controller = None # Initialize controller reference
        self._session = session
        self._ip_address = ip_address
        self._token = config.get(CONF_TOKEN)
        self._cert_path = self._resolve_cert_path(config.get(CONF_CERT))

        # --- START OF FIX: Reintroduce shared state with a Lock ---
        # Object to share initialization state across all copies.
        # The Lock prevents race conditions during the first initialization.
        # We also store 'local_session' here so it is shared across copies (commands).
        self._shared_state = {
            "initialized": False, 
            "lock": asyncio.Lock(), 
            "ssl_context": None,
            "local_session": None
        }
        # --- END OF FIX ---
        # --- Simplified Connection Logic ---
        # This will hold the Jinja2 template for this specific connection instance.
        self._connection_template: Optional[Template] = None

        self.condition_template: Optional[Template] = None
        self._embedded_command: Optional[ConnectionAiohttp8888] = None
        self._ssl_context: Optional[ssl.SSLContext] = None
        
        # --- START OF FIX: Local Session for Periodic Reset ---
        self._keep_alive = config.get("keep_alive", True)
        # self._local_session removed in favor of shared_state["local_session"]
        # --- END OF FIX ---
        
        # --- START OF FIX: Strict Serialization Lock ---
        # Initialize a lock to strictly serialize requests and force connection reuse.
        self._request_lock = asyncio.Lock()
        # --- END OF FIX ---

        if not self._token:
             _LOGGER.error("[aiohttp_init] aiohttp engine started without a token. This will fail.")
        
        # Check if cert is missing
        if not self._cert_path or not os.path.exists(self._cert_path):
            # Only error if we are NOT in insecure mode (SmartThings/Emulator uses insecure_ssl=True)
            if not config.get("insecure_ssl", False):
                _LOGGER.error(f"[aiohttp_init] Certificate file not found or invalid at {self._cert_path}")
            else:
                 _LOGGER.debug(f"[aiohttp_init] Certificate file not found at {self._cert_path}. This is expected for SmartThings/Emulator (insecure_ssl=True).")

    @property
    def log_prefix(self) -> str:
        """Generate a consistent log prefix."""
        if self._controller and self._controller.unique_id:
            return self._controller.log_prefix
        return f"[{self._ip_address or 'NO_IP'}]"

    def set_controller_ref(self, controller):
        """Allows the property to set a reference to the main controller."""
        _LOGGER.debug("%s [set_controller_ref] Setting controller reference for connection object.", self.log_prefix)
        self._controller = controller

    def _resolve_cert_path(self, cert_file: Optional[str]) -> Optional[str]:
        """Resolve the full path to the certificate file."""
        if not cert_file:
            return None
        if not os.path.dirname(cert_file):
            return os.path.join(os.path.dirname(__file__), cert_file)
        return cert_file

    async def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """
        Creates the correct SSL context.
        - If cert is present, sets up mTLS (Strict/Verify=None but loads cert).
        - If cert is missing:
             - If insecure_ssl=True (Emulator/SmartThings), sets up lenient context (Weak Ciphers, Verify=None).
             - If insecure_ssl=False (Cloud default), returns None (aiohttp default strict).
        """
        # Read insecure_ssl. It comes from 'config' passed to __init__.
        # Note: ConnectionRequestBase reads it as self._insecure_ssl
        insecure_ssl = self._config.get("insecure_ssl", False)

        has_cert = self._cert_path and os.path.exists(self._cert_path)

        if not has_cert and not insecure_ssl:
             # Standard Secure Cloud Connection
             _LOGGER.debug("%s [aiohttp] No cert and insecure_ssl=False. Using default aiohttp SSL context (Strict).", self.log_prefix)
             return None

        try:
            _LOGGER.debug("%s [aiohttp] Creating custom SSL context. Cert: %s, Insecure: %s", self.log_prefix, has_cert, insecure_ssl)
            
            # Use PROTOCOL_TLSv1 for maximum compatibility (as in requests)
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
            
            # If insecure_ssl or mTLS (self-signed), we don't verify hostname/chain
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

            # Replicate the 'requests' logic to allow ALL ciphers (fixes Handshake Failure)
            context.set_ciphers("ALL:@SECLEVEL=0")
            
            if has_cert:
                 _LOGGER.debug("%s [aiohttp] Loading mTLS cert from: %s", self.log_prefix, self._cert_path)
                 loop = asyncio.get_running_loop()
                 load_chain_func = partial(context.load_cert_chain, self._cert_path)
                 await loop.run_in_executor(None, load_chain_func)
            
            return context
        except Exception as e:
            _LOGGER.error("%s [aiohttp] Failed to create SSL context: %s.", self.log_prefix, e)
            return None

    @staticmethod
    def match_type(type_str: str) -> bool:
        return type_str == CONNECTION_TYPE_AIOHTTP_8888

    def load_from_yaml(self, node, connection_base) -> bool:
        if node and "keep_alive" in node:
            self._keep_alive = node["keep_alive"]
        return True

    def create_updated(self, yaml_node):
        """
        Creates a new connection instance with updated parameters from YAML.
        This is crucial for async operations where each 'value' can have its own
        connection_template OR static params.
        
        HACK: This also converts static 'params' blocks into a
        'connection_template' because the async_set_value in properties.py
        fails to check _params.
        """
        from .const import (
            CONFIG_DEVICE_CONNECTION_TEMPLATE,
            CONFIG_DEVICE_CONNECTION_PARAMS,
            CONFIG_DEVICE_CONNECTION,
            CONFIG_DEVICE_CONDITION_TEMPLATE,
        )

        # Create a shallow copy. This is important so that each value-specific
        # operation gets its own connection object with its own template.
        new_connection = copy.copy(self)

        # A shallow copy makes new_connection._params point to the *same*
        # dictionary as self._params. We need to break that link.
        new_connection._params = {} # Create a new, empty dict for this instance.

        # --- START OF FIX: Ensure controller reference is inherited ---
        # The controller reference is set on the base connection object.
        # When we create a copy for a specific value, it must inherit this reference.
        new_connection._controller = self._controller
        # --- END OF FIX ---
        # --- START OF FIX: Revert to a single shared state ---
        # All copies must point to the SAME shared state object
        # so that the connection initialization only happens once.
        new_connection._shared_state = self._shared_state
        # --- END OF FIX ---

        # --- START OF FIX: Move import to the top of the function ---
        from jinja2 import Template
        # --- END OF FIX ---

        # --- START OF FIX: Propagate keep_alive logic ---
        if yaml_node and "keep_alive" in yaml_node:
             new_connection._keep_alive = yaml_node["keep_alive"]
        # --- END OF FIX ---

        # If a connection_template is defined in the value-specific node,
        # compile it and store it in the new connection object.
        if yaml_node and CONFIG_DEVICE_CONNECTION_TEMPLATE in yaml_node:
            template_str = yaml_node[CONFIG_DEVICE_CONNECTION_TEMPLATE]
            #_LOGGER.debug("%s [create_updated] Found and creating value-specific template: %s", self.log_prefix, template_str)
            new_connection._connection_template = Template(template_str)
        
        # HACK: Convert static params to a connection_template string.
        elif yaml_node and CONFIG_DEVICE_CONNECTION_PARAMS in yaml_node:
            # This is a static command (like setting a mode)
            params = {**self._params, **yaml_node.get(CONFIG_DEVICE_CONNECTION_PARAMS, {})} # Inherit from parent
            new_connection._params.update(params) # Store params for good practice
            
            # --- START OF FIX (v2) ---
            # The template must contain all necessary keys. If a key (like 'url')
            # is missing in the value-specific params, fall back to the base connection's params.
            template_dict = {}
            if 'json' in params:
                template_dict['json'] = params['json']
            
            # Use the value-specific method/url if present, otherwise use the base one.
            # --- START OF FIX ---
            # Ensure the URL is just the path, not the full URL, to prevent duplication,
            # UNLESS it is an absolute URL (SmartThings), in which case we keep it.
            url_from_params = params.get('url', self._params.get('url'))
            if url_from_params:
                parsed = urlparse(url_from_params)
                if parsed.scheme and parsed.netloc:
                     # Absolute URL - Keep it entirely
                     template_dict['url'] = url_from_params
                else:
                     # Relative URL - Extract path only (for legacy 8888 behavior)
                     template_dict['url'] = parsed.path
            
            # --- START OF FIX ---
            # If the method is not in the current params (common for embedded commands),
            # explicitly fall back to the base connection's params (`self._params`).
            template_dict['method'] = params.get('method', self._params.get('method'))
            # --- END OF FIX ---

            if not template_dict.get('url'):
                _LOGGER.error("%s [create_updated] HACK FAILED: Could not determine 'url' from value-specific or base params.", self.log_prefix)
            
            template_str = json.dumps(template_dict)
            #_LOGGER.debug("%s [create_updated] HACK: Converting static params to template: %s", self.log_prefix, template_str)
            new_connection._connection_template = Template(template_str)
            # --- END OF FIX (v2) ---
        #else:
        #    _LOGGER.debug("%s [create_updated] No 'connection_template' or 'params' found in this YAML node.", self.log_prefix)

        # --- START: Replicate embedded command logic from connection_request.py ---
        # This MUST run AFTER the parent's params have been processed.
        if yaml_node and CONFIG_DEVICE_CONNECTION in yaml_node:
            #_LOGGER.debug("%s [create_updated] Found an embedded command. Creating it from parent with params: %s", self.log_prefix, new_connection._params)
            # The 'new_connection' object is now the fully configured parent.
            new_connection._embedded_command = new_connection.create_updated(yaml_node[CONFIG_DEVICE_CONNECTION])

            if CONFIG_DEVICE_CONDITION_TEMPLATE in yaml_node[CONFIG_DEVICE_CONNECTION]:
                condition_str = yaml_node[CONFIG_DEVICE_CONNECTION][CONFIG_DEVICE_CONDITION_TEMPLATE]
                #_LOGGER.debug("%s [create_updated] Found condition_template for embedded command: %s", self.log_prefix, condition_str)
                if new_connection._embedded_command:
                    new_connection._embedded_command.condition_template = Template(condition_str)
        # --- END: Replicate embedded command logic ---

        return new_connection

    @property
    def is_async_native(self) -> bool:
        return True
    
    async def _try_connection(self) -> Optional[str]:
        """
        Probes the connection (HTTPS mTLS ONLY)
        and memorizes it for future use.
        Returns response text if successful, None otherwise.
        """
        # --- START OF FIX: Use Lock for safe, one-time initialization ---
        if self._shared_state["initialized"]:
            return None

        async with self._shared_state["lock"]:
            # Double-check in case another task initialized it while we were waiting for the lock.
            if self._shared_state["initialized"]:
                return None

            current_token = self._token
            if self._controller:
                current_token = self._controller._config.get(CONF_TOKEN, self._token)
            probe_headers = {"Authorization": f"Bearer {current_token}"}

            # --- START OF FIX: Use the shared state's SSL context ---
            if not self._shared_state["ssl_context"]:
                self._shared_state["ssl_context"] = await self._create_ssl_context()

            # If create_ssl_context returned None (no cert), we might still want to proceed if this is a
            # SmartThings device which doesn't need mTLS.
            # However, for 8888 devices, no cert = failure.
            # We can detect this by checking if we have a token (SmartThings usually relies on token).
            
            ssl_ctx = self._shared_state["ssl_context"]
            if ssl_ctx is None:
                 # Logic for "insecure" / no-cert connection
                 ssl_ctx = False 

            if True: #Indent block 
            # --- END OF FIX ---
                try:
                    _LOGGER.debug("%s [aiohttp_probe] Probing connection...", self.log_prefix)
                    
                    # --- START OF FIX: Generalize Probe URL ---
                    probe_url = f"https://{self._ip_address}:8888/devices"
                    if self._params and self._params.get("url") and str(self._params.get("url")).startswith("http"):
                        probe_url = self._params.get("url")
                        _LOGGER.debug("%s [aiohttp_probe] Detected absolute URL, probing: %s", self.log_prefix, probe_url)
                    # --- END OF FIX ---

                    # Update timeout to be more granular
                    async with self._session.request("GET", probe_url, headers=probe_headers, ssl=self._shared_state["ssl_context"], timeout=aiohttp.ClientTimeout(total=10, sock_read=5)) as response:
                        if response.status in (200, 401, 403, 405): # Added 405 for Method Not Allowed (probing POST with GET)
                            _LOGGER.info("%s [aiohttp] Connection successful and memorized. Status: %s", self.log_prefix, response.status)
                            self._shared_state["initialized"] = True
                            
                            # --- START OF FIX: Optimization - Return text for reuse ---
                            if response.status == 200:
                                _LOGGER.debug("%s [aiohttp_probe] Reading response body...", self.log_prefix)
                                return await response.text()
                            return None
                            # --- END OF FIX ---
                        else:
                            raise Exception(f"Unexpected probe response: {response.status}")
                
                # --- START OF FIX: Handle offline device gracefully ---
                except aiohttp.ClientConnectorError as e:
                    # Log as warning (not error) because it's expected when AC is offline.
                    _LOGGER.warning("%s [aiohttp_probe] Device is unreachable (offline). Connection refused: %s", self.log_prefix, e)
                    self._shared_state["ssl_context"] = None # Reset to try again later
                    # Raise CannotConnect to let the coordinator know and retry later, but prevent the noisy traceback below.
                    raise CannotConnect(f"Device unreachable: {e}") from e
                # --- END OF FIX ---

                except Exception as e:
                    # --- START OF FIX: Detect malformed header error ---
                    if "Invalid header token" in str(e):
                        _LOGGER.error(
                            "%s [aiohttp_probe] Malformed header error detected! "
                            "The device does not comply with the HTTP standard. "
                            "Please switch to the 'Legacy (requests)' connection engine in the integration options.",
                            self.log_prefix
                        )
                        raise InvalidHeaderError("Malformed HTTP headers from device") from None
                    # --- END OF FIX ---
                    _LOGGER.warning("%s [aiohttp_probe] Initial probe with HTTPS (mTLS) failed: %s.", self.log_prefix, e, exc_info=True)
                    self._shared_state["ssl_context"] = None # Clear on failure to allow retries

            _LOGGER.error("%s [aiohttp_probe] HTTPS (mTLS) connection probe failed. The device is unreachable or the certificate/token is incorrect.", self.log_prefix)
            raise CannotConnect("Connection initialization failed (HTTPS)")
            raise CannotConnect("Connection initialization failed (HTTPS)")
        # --- END OF FIX ---

    async def _get_session(self) -> aiohttp.ClientSession:
        """
        Returns the appropriate aiohttp session.
        If keep_alive is True, returns the shared session.
        If keep_alive is False, returns a dedicated local session.
        """
        if self._keep_alive:
            return self._session
        
        local_session = self._shared_state.get("local_session")
        if local_session is None or local_session.closed:
             # Retrieve the shared SSL context (should be initialized by _try_connection)
             ssl_context = self._shared_state.get("ssl_context")
             
             # Create a dedicated session with the same timeout config as the global one
             # We use a custom connector with a long keepalive timeout to mimic the shared one
             # during the "active" phase (before we explicitly close it).
             # --- START OF FIX: Inject SSL Context into Connector to force reuse ---
             # Passing ssl=ssl_context ensures the connector treats connections as reusable
             # for this specific SSL configuration.
             # We also set limit=1 to enforce serial execution and prevent concurrent connections.
             connector = aiohttp.TCPConnector(keepalive_timeout=75, ssl=ssl_context, limit=1)
             # --- END OF FIX ---
             timeout = aiohttp.ClientTimeout(total=30, connect=10)
             local_session = aiohttp.ClientSession(connector=connector, timeout=timeout)
             self._shared_state["local_session"] = local_session
             _LOGGER.debug("%s [aiohttp] Created new local session (ID: %s) with connector (ID: %s) and fixed SSL Context.", self.log_prefix, id(local_session), id(connector))
        
        return self._shared_state["local_session"]

    async def _async_execute_request(
        self,
        method: str,
        url_path: Optional[str],
        data: Optional[str],
        headers: Optional[Dict[str, str]],
        _is_probe: bool = False, # Flag interno (ignorado en esta versión, pero mantenido por si acaso)
        _is_poll: bool = False,
    ) -> Tuple[str, Optional[Dict[str, str]]]:
        """
        Executes a command asynchronously using aiohttp.
        It uses the "memorized" connection logic (HTTPS only).
        """
        req_headers = headers.copy() if headers else {}
        
        current_token = self._token
        if self._controller:
            current_token = self._controller._config.get(CONF_TOKEN, self._token)

        if not current_token:
            _LOGGER.error("%s [aiohttp] No token available! The request will fail.", self.log_prefix)
            raise AuthError("Token not configured for the aiohttp engine")

        req_headers.setdefault("Authorization", f'Bearer {current_token}')
        req_headers.setdefault("Content-Type", "application/json")
        
        # --- ADAPTIVE KEEP-ALIVE LOGIC ---
        # If we previously detected stability issues (timeouts likely due to missing Content-Length),
        # we strictly force 'Connection: close'.
        if getattr(self, "_force_close_connection", False):
            req_headers["Connection"] = "close"

        # --- FIX: REMOVE FALLBACK LOGIC ---
        # We only use HTTPS (mTLS)
        # --- START OF FIX: Always use the shared SSL context ---
        ssl_context = self._shared_state.get("ssl_context")
        # --- START OF FIX: URL Handling Generalization ---
        # Detect if the path is actually an absolute URL (for SmartThings).
        if url_path and url_path.startswith("http"):
            base_url = "" # No base URL needed
            # Provide a default ssl_context (unverified) if one wasn't created via mTLS probe
            if not ssl_context:
                # Use _create_ssl_context to get the correct lenient context for insecure_ssl=True
                ssl_context = await self._create_ssl_context()
        else:
            base_url = f"https://{self._ip_address}:8888"
        # --- END OF FIX ---

        full_url = f"{base_url}{url_path}"

        try:
            # --- START OF FIX: Strict Serialization with Lock ---
            # We acquire a lock to ensure that requests are executed one by one.
            # This prevents aiohttp from opening multiple concurrent connections during bursts,
            # ensuring that the single persistent connection is reused.
            async with self._request_lock:
                # --- START: Add log for sent command ---
                _LOGGER.debug(
                    "%s [aiohttp] Sending request -> Method: %s, URL: %s, Payload: %s, Close Mode: %s",
                    self.log_prefix, method, full_url, mask_sensitive_data(data), getattr(self, "_force_close_connection", "False")
                )
                # --- START: Timing measurement for aiohttp execute ---
                start_time = time.perf_counter()
                # --- END: Add log for sent command ---
                # --- END: Add log for sent command ---
                
                # --- START OF FIX: Use _get_session() ---
                session = await self._get_session()
                # Debug logging to confirm session reuse
                _LOGGER.debug("%s [aiohttp] Using session ID: %s | SSL Context ID: %s", self.log_prefix, id(session), id(ssl_context))
                # --- END OF FIX ---

                async with session.request(
                    method,
                    url=full_url,
                    headers=req_headers,
                    data=data,
                    ssl=ssl_context,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                
                    response_text = await response.text()
                
                # --- START OF FIX: HTTP Version Detection ---
                # Check protocol version to decide on Keep-Alive
                if response.version.major == 1 and response.version.minor >= 1:
                     if getattr(self, "_force_close_connection", False):
                         _LOGGER.debug("%s [aiohttp] Server speaks HTTP/%s.%s. Re-enabling Keep-Alive.", self.log_prefix, response.version.major, response.version.minor)
                     self._force_close_connection = False
                else:
                     if not getattr(self, "_force_close_connection", False):
                         _LOGGER.debug("%s [aiohttp] Server speaks HTTP/%s.%s. Enforcing 'Connection: close'.", self.log_prefix, response.version.major, response.version.minor)
                     self._force_close_connection = True
                # --- END OF FIX ---
                
                if response.status != 200:
                     if response.status in (401, 403):
                        _LOGGER.error("%s [aiohttp] Authentication error (status %d). Token: %s...%s", self.log_prefix, response.status, current_token[:4], current_token[-4:])
                        raise AuthError(f"Authentication failed with status {response.status}. Check your token.")
                     
                     _LOGGER.error(
                         "%s [aiohttp] HTTP Error %s: %s", 
                         self.log_prefix, response.status, response_text
                    )
                     # In case of other errors, we might want to return the text and None or raise.
                     # The original logic raised exceptions for non-200.
                     # Let's align with the expectation that we return valid data or raise status.
                     response.raise_for_status()

                return response_text, dict(response.headers)

        except (asyncio.TimeoutError, aiohttp.ClientConnectorError, aiohttp.ClientError) as e:
            # --- ADAPTIVE RECOVERY ---
            # If we timed out and haven't forced close yet, it's highly likely the "missing Content-Length" issue.
            if not getattr(self, "_force_close_connection", False):
                _LOGGER.warning(
                    "%s [aiohttp] Timeout/Error detected (%s). "
                    "The device likely violates HTTP protocol (missing Content-Length). "
                    "Switching to 'Connection: close' mode for resilience.", 
                    self.log_prefix, str(e)
                )
                self._force_close_connection = True
                req_headers["Connection"] = "close"
                
                # RETRY IMMEDIATELY with the new header
                _LOGGER.debug("%s [aiohttp] Retrying request with 'Connection: close'...", self.log_prefix)
                try:
                    # --- START OF FIX: Use _get_session() ---
                    session = await self._get_session()
                    async with session.request(
                        method, 
                        full_url, 
                        data=data, 
                        headers=req_headers, 
                        ssl=ssl_context,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        response_text = await response.text()
                        return response_text, None
                except Exception as retry_exc:
                     _LOGGER.error("%s [aiohttp] Retry failed even with 'Connection: close': %s", self.log_prefix, retry_exc)
                     raise CannotConnect(f"Connection failed after retry: {retry_exc}") from retry_exc
            
            # If we were already forcing close, then it's a real network issue.
            _LOGGER.error("%s [aiohttp] Connection failed: %s", self.log_prefix, e)
            raise CannotConnect(f"Connection error: {e}") from e
        except Exception as e:
             _LOGGER.error("%s [aiohttp] Unexpected error: %s", self.log_prefix, e, exc_info=True)
             raise

    async def async_execute(
        self,
        method: str,
        url_path: Optional[str],
        data: Optional[str],
        headers: Optional[Dict[str, str]], # Main command's headers
        device_state: Optional[Dict[str, Any]] = None, # Pass device state for conditions
        _is_probe: bool = False,
        _is_poll: bool = False
    ) -> Tuple[str, Optional[Dict[str, str]]]:
        """
        Orchestrates the execution of commands, including embedded ones.
        """
        # --- START: Replicate and FIX embedded command logic ---
        # --- START OF FIX: Ensure initialization before any execution ---
        # Capture probe result to avoid double polling
        probe_response_text = await self._try_connection()
        # --- END OF FIX ---
        if self._embedded_command:
            _LOGGER.debug("%s [async_execute] Found embedded command.", self.log_prefix)
            try:
                # Check the condition before executing the embedded command
                if hasattr(self._embedded_command, 'check_execute_condition') and device_state:
                    if not self._embedded_command.check_execute_condition(device_state):
                        _LOGGER.debug("%s [async_execute] Embedded command condition not met. Skipping execution.", self.log_prefix)
                    else:
                        _LOGGER.debug("%s [async_execute] Embedded command condition met. Executing it before the main command.", self.log_prefix)
                        # The embedded command has its own template and params. We must render them.
                        embedded_template = getattr(self._embedded_command, '_connection_template', None)
                        if embedded_template:
                            # The embedded command might not need a 'value'
                            embedded_params_str = embedded_template.render()
                            embedded_params = json.loads(embedded_params_str)
                            embedded_data = json.dumps(embedded_params.get('json')) if 'json' in embedded_params else None

                            _LOGGER.debug("%s [async_execute] Executing embedded command with its own params: %s", self.log_prefix, mask_sensitive_data(embedded_params))
                            # Execute the embedded command by calling its own async_execute method.
                            # This ensures it is fully initialized and uses its own specific parameters.
                            await self._embedded_command.async_execute(
                                method=embedded_params.get('method'), url_path=embedded_params.get('url'),
                                data=embedded_data, headers=embedded_params.get('headers', headers),
                                device_state=device_state)
                            # --- END OF FIX ---
                        else:
                            _LOGGER.warning("%s [async_execute] Embedded command found but it has no connection_template.", self.log_prefix)
                else:
                    _LOGGER.warning("%s [async_execute] Embedded command found, but cannot check its condition (device_state is missing). Skipping.", self.log_prefix)

            except (CannotConnect, AuthError) as e:
                _LOGGER.warning(
                    "%s [async_execute] Embedded command failed due to connection error: %s", self.log_prefix, e
                )
                raise
            except Exception as e:
                _LOGGER.error("%s [async_execute] Embedded command failed: %s", self.log_prefix, e, exc_info=True)
                # Re-raise the exception to prevent the main command from executing, as the failure is critical.
                raise
        # --- END: Replicate and FIX embedded command logic ---

        # Now, execute the main command.
        if self.check_execute_condition(device_state):
             do_execute = True
        else:
             do_execute = False
        if not do_execute:
             _LOGGER.debug("%s [async_execute] Condition not met (template result false). Skipping execution.", self.log_prefix)
             return "{}", {}

        # --- START OF FIX: Periodic Reset Logic ---
        # If this is a poll and we are in "Periodic Reset" mode (keep_alive=False),
        # we explicitly close the LOCAL session before starting the new poll.
        if _is_poll and not self._keep_alive:
            # 1. Capture and clear shared state immediately to prevent race conditions
            # This ensures that if a command arrives during the close/sleep, it creates
            # a NEW session that stays valid (we don't overwrite it later).
            local_session = self._shared_state.get("local_session")
            if local_session:
                 self._shared_state["local_session"] = None

            # 2. Close the old session
            if local_session and not local_session.closed:
                _LOGGER.debug("%s [Periodic Reset] Closing local session (ID: %s) before poll.", self.log_prefix, id(local_session))
                await local_session.close()
                
                # 3. Wait for TCP cleanup
                # Aiohttp's close() is graceful but returns quickly. A small yield ensures
                # the FIN packet is processed by the OS/Server before we open a new one.
                await asyncio.sleep(0.2)
        # --- END OF FIX ---

        # --- START OF FIX: Optimization - Reuse probe response ---
        if probe_response_text and method == "GET" and url_path == "/devices":
             _LOGGER.debug("%s [async_execute] OPTIMIZATION: Reusing probe response for initial poll.", self.log_prefix)
             return probe_response_text, None
        # --- END OF FIX ---

        return await self._async_execute_request(method, url_path, data, headers, _is_probe=_is_probe, _is_poll=_is_poll)

    def check_execute_condition(self, device_state):
        """Replicates the condition check from connection_request.py for async."""
        do_execute = True
        if hasattr(self, 'condition_template') and self.condition_template is not None:
            _LOGGER.debug("%s Evaluating execute condition for a command.", self.log_prefix)
            try:
                # The template expects '1' for true.
                rendered_condition = self.condition_template.render(device_state=device_state)
                _LOGGER.debug("%s Execute condition result: %s", self.log_prefix, rendered_condition)
                do_execute = str(rendered_condition).strip() == "1"
            except Exception as e:
                _LOGGER.error("%s Error evaluating execute condition, executing command anyway. Error: %s", self.log_prefix, e, exc_info=True)
                do_execute = True
        return do_execute

    async def close(self):
        """
        Close the connection and release resources.
        This is called when the integration is unloaded or the connection method changes.
        """
        _LOGGER.debug("%s [aiohttp] Closing connection resources...", self.log_prefix)
        
        # 1. Close internal embedded command (if any)
        if self._embedded_command and hasattr(self._embedded_command, "close"):
             try:
                 await self._embedded_command.close()
             except Exception as e:
                 _LOGGER.warning("%s [aiohttp] Error closing embedded command: %s", self.log_prefix, e)

        # 2. Close the local session if it exists (for keep_alive=False)
        local_session = self._shared_state.get("local_session")
        if local_session and not local_session.closed:
            _LOGGER.debug("%s [aiohttp] Closing local session (ID: %s)...", self.log_prefix, id(local_session))
            try:
                await local_session.close()
            except Exception as e:
                _LOGGER.error("%s [aiohttp] Error closing local session: %s", self.log_prefix, e)
            finally:
                self._shared_state["local_session"] = None
        
        # 3. Reset shared state to allow clean re-initialization
        self._shared_state["initialized"] = False
        self._shared_state["ssl_context"] = None
        
        # Note: We do NOT close self._session here because it is a shared session 
        # managed by Home Assistant (or the dedicated one managed in __init__.py).
        # The dedicated session in __init__.py is closed explicitly in async_unload_entry.
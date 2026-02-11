import asyncio
import logging
import os
import re
import ssl
from typing import Callable, Dict, Any, Optional, Tuple, Coroutine

import xmltodict
from homeassistant.const import CONF_IP_ADDRESS, CONF_MAC, CONF_PORT, CONF_TOKEN

from .connection import Connection, register_connection
from .exceptions import AuthError, CannotConnect
from .properties import DeviceProperty, register_status_getter
from .const import (
    CONF_CERT,
    CONFIG_DEVICE_POWER_TEMPLATE,
    CONFIG_DEVICE_CONNECTION_PARAMS,
    CONFIG_DEVICE_CONNECTION_TEMPLATE,
)
from .helpers import mask_sensitive_data
from .const import (
    DEFAULT_CONF_CERT_FILE,
    PROTOCOL_2878_DPLUG,
    PROTOCOL_2878_DRC,
    PROTOCOL_2878_INVALIDATE,
    PROTOCOL_2878_DEVICE_STATE,
    PROTOCOL_2878_DEVICE_CONTROL,
    PROTOCOL_2878_STATUS_OK,
    PROTOCOL_2878_STATUS,
    PROTOCOL_2878_UPDATE,
    PROTOCOL_2878_RESPONSE,
    PROTOCOL_2878_ATTR,
    PROTOCOL_2878_ATTR_ID,
    PROTOCOL_2878_ATTR_VALUE,
    PROTOCOL_2878_POWER_ID,
    PROTOCOL_2878_VALUE_ON,
)

_LOGGER = logging.getLogger(__name__)

CONNECTION_TYPE_S2878 = "samsung_2878"
CONF_DUID = "duid"

INITIAL_RECONNECT_DELAY = 15
MAX_RECONNECT_DELAY = 300
RECONNECT_FACTOR = 2
COMMAND_TIMEOUT = 20.0
MAX_RECONNECT_RETRIES = 5

class connection_config:
    def __init__(self, host, port, token, cert, duid):
        self.host = host
        self.port = port
        self.token = token
        self.duid = duid
        self.cert = cert

@register_connection
class ConnectionSamsung2878(Connection):
    def __init__(self, hass_config, logger):
        super(ConnectionSamsung2878, self).__init__(hass_config, logger)
        self._params = {}
        self._connection_init_template = None
        self._cfg = connection_config(None, None, None, None, None)
        self._device_status = {}
        self._socket_timeout = 30.0
        self._controller = None

        self._reader = None
        self._writer = None
        self._read_task: Optional[asyncio.Task] = None # Task for reading from the socket
        self._lock = asyncio.Lock()  # To serialize execute() calls
        self._close_lock = asyncio.Lock() # To serialize _close_connection calls
        self._cmd_queue = asyncio.Queue()
        self._manager_task: Optional[asyncio.Task] = None # The main task that manages the persistent connection.
        self._update_callback: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None
        self._pending_future: Optional[asyncio.Future] = None # The future for the command currently being processed.
        self._reconnect_delay = INITIAL_RECONNECT_DELAY
        self._reconnect_retries = 0
        self._is_available = True # Used for stateful logging to report connection status changes.
        self._is_ready = asyncio.Event()  # Event to signal when connection is ready
        self._last_successful_config: Optional[Dict[str, Any]] = None
        self._ssl_context_cache: Dict[Tuple[Optional[str], str, int], ssl.SSLContext] = {}
        self._initial_connection_done = False # To prevent double poll at startup
        
        self.update_configuration_from_hass(hass_config)
        self._power_template = None

    def set_controller_ref(self, controller):
        self._controller = controller

    @property
    def log_prefix(self) -> str:
        if self._controller and self._controller.unique_id:
            return self._controller.log_prefix
        if self._cfg and self._cfg.duid:
            return f"[{self._cfg.duid[-6:]}]"
        return "[NO_ID]"

    def set_update_callback(self, callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        self._update_callback = callback

    def start_listening(self) -> None:
        if self._manager_task is None or self._manager_task.done():
            _LOGGER.info("%s Starting connection manager", self.log_prefix)
            self._reconnect_retries = 0
            self._manager_task = asyncio.create_task(self._connection_manager())

    async def stop_listening(self) -> None:
        if self._manager_task:
            _LOGGER.info("%s Stopping connection manager", self.log_prefix)
            self._manager_task.cancel()
            try:
                await self._manager_task
            except asyncio.CancelledError:
                pass
            self._manager_task = None
        await self._close_connection()

    def update_configuration_from_hass(self, hass_config):
        if hass_config is not None:
            # Clear the SSL context cache if configuration changes, as cert path might have changed.
            self._ssl_context_cache.clear()

            duid = None
            mac = hass_config.get(CONF_MAC)
            if mac:
                duid = re.sub(":", "", mac)

            cert_file = hass_config.get(CONF_CERT) or ""
            # If the certificate path does not contain a directory, assume it's in the integration's folder.
            # This makes it robust to HA installation changes.
            if cert_file and not os.path.dirname(cert_file):
                log_prefix = self.log_prefix
                if log_prefix == "[NO_ID]" and duid:
                     log_prefix = f"[{duid[-6:]}]"
                _LOGGER.debug("%s Resolving relative certificate path for 2878 connection: %s", log_prefix, cert_file)
                cert_file = os.path.join(os.path.dirname(__file__), cert_file)

            self._cfg = connection_config(
                host=hass_config.get(CONF_IP_ADDRESS),
                port=hass_config.get(CONF_PORT, 2878),
                token=hass_config.get(CONF_TOKEN),
                cert=cert_file,
                duid=duid,
            )
            # Ensure DUID and token are available for templates.
            self._params.update({
                CONF_DUID: self._cfg.duid,
                CONF_TOKEN: self._cfg.token,
                CONF_IP_ADDRESS: self._cfg.host,
            })
            
            # Load the preferred connection settings if they were saved during pairing
        self._last_successful_config = None
        if hass_config.get("preferred_connection"):
            self._last_successful_config = hass_config.get("preferred_connection").copy()
            # Resolve relative path in preferred config if needed, to match the runtime behavior
            pref_cert = self._last_successful_config.get('cert')
            if pref_cert and not os.path.dirname(pref_cert):
                 self._last_successful_config['cert'] = os.path.join(os.path.dirname(__file__), pref_cert)


    def load_from_yaml(self, node, connection_base):
        from jinja2 import Template
        if connection_base:
            self._params.update(connection_base._params.copy())
        if not node:
            return False
        
        params_node = node.get(CONFIG_DEVICE_CONNECTION_PARAMS, {})
        if CONFIG_DEVICE_CONNECTION_TEMPLATE in params_node:
            self._connection_init_template = Template(params_node[CONFIG_DEVICE_CONNECTION_TEMPLATE])
        elif not connection_base:
            _LOGGER.error("%s Missing 'connection_template' in YAML configuration", self.log_prefix)
            return False

        if CONFIG_DEVICE_POWER_TEMPLATE in params_node:
            self._power_template = Template(params_node[CONFIG_DEVICE_POWER_TEMPLATE])

        if not connection_base:
            # These are critical and should have been provided during setup.
            if not self._cfg.host: _LOGGER.error("%s Missing 'host' parameter", self.log_prefix); return False
            if not self._cfg.token: _LOGGER.error("%s Missing 'token' parameter", self.log_prefix); return False
            if not self._cfg.duid: _LOGGER.error("%s Missing 'mac' parameter", self.log_prefix); return False

        self._params.update(params_node)
        return True

    @staticmethod
    def match_type(type):
        return type == CONNECTION_TYPE_S2878

    def create_updated(self, node):
        self.load_from_yaml(node, self)
        return self

    async def _post_connect_status_request(self):
        """Queues a request for the full device status after a connection is established."""
        # Give the system a moment to be ready for a new command.
        try:
            await asyncio.sleep(1)
            
            command = f'<Request Type="{PROTOCOL_2878_DEVICE_STATE}" DUID="{self._cfg.duid}"></Request>\n'
            _LOGGER.debug("%s Queuing post-reconnection status request", self.log_prefix)
            
            future = asyncio.get_event_loop().create_future()
            await self._cmd_queue.put((command, future))
            
            # Wait for the command to be processed
            await asyncio.wait_for(future, timeout=COMMAND_TIMEOUT) # This raises TimeoutError if it takes too long
            _LOGGER.debug("%s Post-reconnection status request was processed successfully", self.log_prefix)
 
        except asyncio.TimeoutError:
            _LOGGER.warning("%s Post-reconnection status request timed out", self.log_prefix)
            # If the future that timed out is still the pending one, clear it to unblock the manager.
            if self._pending_future and self._pending_future == future:
                self._pending_future = None
        except Exception as e:
            _LOGGER.error("%s Failed to queue post-reconnection status request: %s", self.log_prefix, e, exc_info=True)

    async def _close_connection(self):
        # Use a lock to ensure we don't close the connection multiple times concurrently
        if self._close_lock.locked():
             _LOGGER.debug("%s Connection close already in progress, waiting...", self.log_prefix)

        async with self._close_lock:
            # Check if already closed to avoid redundant work and logs
            if self._writer is None and self._read_task is None:
                 _LOGGER.debug("%s Connection already closed, skipping.", self.log_prefix)
                 return

            self._is_ready.clear()
            
            # --- START OF FIX: Cancel pending read task to unblock manager ---
            if self._read_task and not self._read_task.done():
                _LOGGER.debug("%s Cancelling pending read task", self.log_prefix)
                self._read_task.cancel()
                try:
                    await self._read_task
                except asyncio.CancelledError:
                    pass
            self._read_task = None
            # --- END OF FIX ---

            if self._writer:
                _LOGGER.debug("%s Closing connection", self.log_prefix)
                try:
                    self._writer.close()
                    # --- START OF FIX: Add timeout to wait_closed ---
                    try:
                        await asyncio.wait_for(self._writer.wait_closed(), timeout=2.0)
                    except asyncio.TimeoutError:
                        _LOGGER.warning("%s Timeout waiting for connection close, forcing reset", self.log_prefix)
                    # --- END OF FIX ---
                except (ConnectionResetError, ssl.SSLError, asyncio.CancelledError, OSError) as e:
                    _LOGGER.warning("%s Ignoring error during connection close: %s", self.log_prefix, e)
            self._writer = self._reader = None

    async def _establish_connection_and_handshake(self):
        await self._close_connection()
        cfg = self._cfg
        initial_msg = None

        # Define the cipher suites to try, in order.
        cipher_configs = [
            ("HIGH:!DH:!aNULL:@SECLEVEL=0", "Cipher Suite A"),
            ("HIGH:!aNULL:!MD5:@SECLEVEL=0", "Cipher Suite B"),
            ("ALL:@SECLEVEL=0", "Cipher Suite C")
        ]

        # Define connection strategies based on user input.
        user_cert = cfg.cert
        default_cert_path = os.path.join(os.path.dirname(__file__), DEFAULT_CONF_CERT_FILE)
        
        strategies = []
        if user_cert:
            # If a user certificate is provided, try strict verification first, then no verification.
            strategies.append({'cert': user_cert, 'name': 'User Cert (Strict Verify)', 'verify_mode': ssl.CERT_REQUIRED})
            strategies.append({'cert': user_cert, 'name': 'User Cert (No Verify)', 'verify_mode': ssl.CERT_NONE})
            # As a fallback, try with no certificate at all.
            strategies.append({'cert': None, 'name': 'No Certificate (Fallback)'})
        else:
            # If no user certificate, the only possible verification mode is CERT_NONE.
            strategies.append({'cert': None, 'name': 'No Certificate (Default)'})
            # As a fallback, try with the integration's default certificate.
            strategies.append({'cert': default_cert_path, 'name': 'Default Certificate (Fallback)'})

        # Build a list of all possible connection attempts.
        all_attempts = [] # List of connection attempts
        for strategy in strategies:
            for cipher_config in cipher_configs:
                all_attempts.append({
                    'cert': strategy['cert'],
                    # Default to CERT_NONE if verify_mode is not specified.
                    'verify_mode': strategy.get('verify_mode', ssl.CERT_NONE),
                    'cipher_config': cipher_config,
                    'strategy_name': strategy['name']
                })

        # If we have a last known good configuration, prioritize it
        if self._last_successful_config:
            _LOGGER.debug("%s Prioritizing last successful config: %s", self.log_prefix, self._last_successful_config)
            
            preferred_cert = self._last_successful_config.get('cert')
            preferred_cipher = self._last_successful_config.get('cipher_name')
            preferred_verify = self._last_successful_config.get('verify_mode')

            # Find all attempts that match the criteria
            matching_attempts = []
            other_attempts = []

            for attempt in all_attempts:
                cert_match = attempt['cert'] == preferred_cert
                verify_match = attempt['verify_mode'] == preferred_verify
                
                # If cipher name is saved, we require it to match.
                # If it's NOT saved (legacy config), we ignore it and match only on cert/verify.
                cipher_match = True
                if preferred_cipher:
                    cipher_match = attempt['cipher_config'][1] == preferred_cipher
                
                if cert_match and verify_match and cipher_match:
                    matching_attempts.append(attempt)
                else:
                    other_attempts.append(attempt)
            
            # Reconstruct the list with matching attempts first
            if matching_attempts:
                _LOGGER.info("%s Optimizing reconnection: Restricting attempts to last known good configuration.", self.log_prefix)
                all_attempts = matching_attempts
                # We discard 'other_attempts' to avoid trying invalid configs (like No Cert)

        connection_successful = False
        last_error = None

        for attempt in all_attempts:
            cert_path = attempt['cert']
            ciphers, cipher_name = attempt['cipher_config']
            verify_mode = attempt['verify_mode']
            strategy_name = attempt['strategy_name']

            try:
                _LOGGER.debug("%s Attempting connection with Strategy: '%s', Cipher: '%s', Verify: %s", self.log_prefix, strategy_name, cipher_name, verify_mode)
                
                # --- SSL Context Caching Logic ---
                cache_key = (cert_path, ciphers, verify_mode) # The key must include all SSL parameters
                ssl_context = self._ssl_context_cache.get(cache_key)

                if not ssl_context:
                    _LOGGER.debug("%s Creating and caching new SSL context for key: %s", self.log_prefix, cache_key)
                    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
                    ssl_context.set_ciphers(ciphers)
                    ssl_context.verify_mode = verify_mode
                    ssl_context.check_hostname = False
                    if cert_path:
                        _LOGGER.debug("%s Loading certificate: %s", self.log_prefix, os.path.basename(cert_path))
                        await asyncio.to_thread(ssl_context.load_verify_locations, cafile=cert_path)
                        await asyncio.to_thread(ssl_context.load_cert_chain, cert_path)
                    self._ssl_context_cache[cache_key] = ssl_context

                conn_future = asyncio.open_connection(cfg.host, cfg.port, ssl=ssl_context)
                self._reader, self._writer = await asyncio.wait_for(conn_future, timeout=self._socket_timeout)

                # Log connection details on success
                ssl_object = self._writer.get_extra_info('ssl_object')
                if ssl_object:
                    cipher = ssl_object.cipher()
                    _LOGGER.info(
                        "%s SSL connection established. Protocol: %s, Cipher: %s, Verify: %s",
                        self.log_prefix, cipher[1], cipher[0], verify_mode
                    )

                # Memorize the successful configuration for future reconnections
                self._last_successful_config = {'cert': cert_path, 'cipher_name': cipher_name, 'verify_mode': verify_mode}
                connection_successful = True
                break  # Exit the loop on successful connection.

            except (ConnectionRefusedError, asyncio.TimeoutError, OSError, ssl.SSLError) as e:
                # This is an expected failure when the device is offline or the port is wrong.
                # Log at debug level to avoid spamming logs when the device is intentionally off.
                _LOGGER.debug("%s Connection attempt with '%s' / '%s' failed: %s. Trying next.", self.log_prefix, strategy_name, cipher_name, e)
                last_error = e
                await self._close_connection()
                continue
            except Exception as e:
                _LOGGER.warning("%s Connection with '%s' / '%s' failed unexpectedly: %s. Trying next.", self.log_prefix, strategy_name, cipher_name, e)
                last_error = e
                await self._close_connection()
                continue

        if not connection_successful:
            _LOGGER.debug("%s Could not connect to device (likely offline): %s", self.log_prefix, last_error)
            return False
        
        # If we are here, one of the attempts was successful at the socket level
        # Now, proceed with the application-level handshake
        if not initial_msg: # Read initial message if not already read during plain TCP check
            initial_msg = await self._read_full_response(timeout=self._socket_timeout)

        if initial_msg:
            # Attempt to parse the initial message to see if it's an update or response
            is_response, is_update, parsed_data = self._parse_and_update_state(initial_msg)
            if parsed_data:
                self._device_status.update(parsed_data)
                if is_update and self._update_callback:
                    _LOGGER.debug("%s Initial message was an update, calling update callback", self.log_prefix)
                    asyncio.create_task(self._update_callback(parsed_data))

        if not initial_msg or (PROTOCOL_2878_DPLUG not in initial_msg and PROTOCOL_2878_DRC not in initial_msg and PROTOCOL_2878_INVALIDATE not in initial_msg):
            _LOGGER.warning("%s Handshake failed: Did not receive expected initial message (DPLUG-1.6 or DRC-1.00 or InvalidateAccount). Got: %s", self.log_prefix, initial_msg)
            raise CannotConnect("Handshake failed: Did not receive expected initial message")
        
        if not self._connection_init_template:
            _LOGGER.error("%s Handshake failed: Connection initialization template is missing.", self.log_prefix)
            raise CannotConnect("Handshake failed: Connection initialization template is missing.")

        auth_command = self._connection_init_template.render(**self._params) + "\n"
        await self._write_data(auth_command)

        auth_response = await self._read_full_response(timeout=self._socket_timeout)
        if not auth_response or PROTOCOL_2878_STATUS_OK not in auth_response:
            # Only process the error response if it's not None
            if auth_response:
                if 'ErrorCode="301"' in auth_response:
                    _LOGGER.error("%s Authentication failed (ErrorCode 301). The device was likely turned off. Please ensure the device is ON before pairing", self.log_prefix)
                    raise AuthError("Authentication failed: Device was turned off (301)")
                
                error_code_match = re.search(r'ErrorCode="(\d+)"', auth_response)
                error_code = error_code_match.group(1) if error_code_match else "Unknown"
                _LOGGER.error("%s Authentication failed with ErrorCode %s. Got: %s", self.log_prefix, error_code, auth_response)
                raise AuthError("Authentication failed")
            raise AuthError("Authentication failed: No response from device")

        _LOGGER.info("%s Connection ready", self.log_prefix)
        self._reconnect_delay = INITIAL_RECONNECT_DELAY
        self._reconnect_retries = 0
        self._is_ready.set()  # Signal that we are ready for commands
        _LOGGER.debug("%s Connection is ready, _is_ready event set.", self.log_prefix)
        
        # Stateful logging: Log when connection is re-established
        if not self._is_available:
            _LOGGER.info("%s Connection re-established", self.log_prefix)
            self._is_available = True

        # Request a full status update only on reconnections, not on the very first connection.
        if self._initial_connection_done:
            asyncio.create_task(self._post_connect_status_request())
            # --- START OF FIX: Proactively refresh HA state after reconnection ---
            if self._controller and hasattr(self._controller, 'coordinator'):
                _LOGGER.info("%s Requesting immediate coordinator refresh after reconnection.", self.log_prefix)
                asyncio.create_task(self._controller.coordinator.async_request_refresh())
            # --- END OF FIX ---
        
        self._initial_connection_done = True
        
        return True

    async def _read_full_response(self, timeout=10.0) -> Optional[str]:
        if not self._reader or self._reader.at_eof():
            return None
        try:
            buffer = b""
            end_time = asyncio.get_event_loop().time() + timeout
            while True:
                remaining_time = end_time - asyncio.get_event_loop().time()
                if remaining_time <= 0:
                    raise asyncio.TimeoutError

                chunk = await asyncio.wait_for(self._reader.read(4096), timeout=remaining_time)
                if not chunk: 
                    await self._close_connection()
                    return buffer.decode("utf-8", errors='ignore') if buffer else None

                buffer += chunk
                decoded_buffer = buffer.decode("utf-8", errors='ignore').strip()
                if "</Response>" in decoded_buffer or "</Update>" in decoded_buffer or PROTOCOL_2878_DPLUG in decoded_buffer or decoded_buffer.endswith("/>"):
                    return decoded_buffer

        except (asyncio.TimeoutError, asyncio.IncompleteReadError) as e:
            _LOGGER.debug("%s No full response received in %s seconds", self.log_prefix, timeout, exc_info=True)
            return buffer.decode("utf-8", errors='ignore') if buffer else None
        except Exception as e:
            _LOGGER.error("%s Error during read: %s", self.log_prefix, e, exc_info=True)
            await self._close_connection()
            return None

    async def _write_data(self, data_str: str):
        if not self._writer or self._writer.is_closing():
            _LOGGER.error("%s Write failed: writer is not available", self.log_prefix)
            raise CannotConnect("Connection is not available for writing")
        try:
            self._writer.write(data_str.encode("utf-8"))
            await asyncio.wait_for(self._writer.drain(), timeout=5.0)
            return True
        except (asyncio.TimeoutError, OSError) as e:
            _LOGGER.error("%s Write failed: %s. Closing connection.", self.log_prefix, e)
            await self._close_connection()
            raise CannotConnect(f"Failed to write to connection: {e}") from e

    def _parse_and_update_state(self, response_xml: str) -> Tuple[bool, bool, Optional[Dict[str, Any]]]:
        if not response_xml:
            return False, False, None
        
        is_update = False
        is_response = False
        parsed_data = {}
        
        # Discard any non-XML prefix like "DPLUG-1.6\n" or "DRC-1.00\n"
        xml_start_index = response_xml.find("<?xml")
        if xml_start_index == -1:
            return False, False, None

        xml_candidate_content = response_xml[xml_start_index:]

        # Split by '<?xml' to handle multiple XML documents concatenated in the buffer.
        doc_parts = xml_candidate_content.split('<?xml')

        for doc_part in doc_parts:
            if not doc_part.strip():
                continue # Skip empty parts.

            # Reconstruct the full XML document string.
            # A valid XML fragment after '<?xml' should start with 'version="1.0"' or a root element tag.
            if doc_part.strip().startswith('version="1.0"') or doc_part.strip().startswith('<'):
                full_doc = '<?xml' + doc_part
            else:
                # This doc_part is not a valid XML fragment (e.g., "DPLUG-1.6" without "version="1.0"")
                # Log it and skip, do not attempt to parse as XML.
                _LOGGER.debug("%s Skipping non-XML fragment after '<?xml': %s", self.log_prefix, doc_part.strip())
                continue
            try:
                data = xmltodict.parse(full_doc)

                if PROTOCOL_2878_RESPONSE in data:
                    is_response = True
                    device_data = data[PROTOCOL_2878_RESPONSE].get(PROTOCOL_2878_DEVICE_STATE, {}).get('Device')
                elif PROTOCOL_2878_UPDATE in data:
                    is_update = True
                    device_data = data[PROTOCOL_2878_UPDATE].get(PROTOCOL_2878_STATUS)
                else:
                    continue

                if not device_data:
                    continue
                attrs = device_data.get(PROTOCOL_2878_ATTR, [])
                if not isinstance(attrs, list):
                    attrs = [attrs]
                
                # Ignore redundant 'Power On' push updates if the device was already known to be on.
                # This reduces unnecessary state updates in Home Assistant.
                if is_update and len(attrs) == 1 and attrs[0].get(PROTOCOL_2878_ATTR_ID) == PROTOCOL_2878_POWER_ID and attrs[0].get(PROTOCOL_2878_ATTR_VALUE) == PROTOCOL_2878_VALUE_ON:
                    if self._device_status.get(PROTOCOL_2878_POWER_ID) == PROTOCOL_2878_VALUE_ON:
                        _LOGGER.debug("%s Ignoring redundant 'Power On' push update", self.log_prefix)
                        return False, False, None

                for attr in attrs:
                    if PROTOCOL_2878_ATTR_ID in attr and PROTOCOL_2878_ATTR_VALUE in attr:
                        parsed_data[attr[PROTOCOL_2878_ATTR_ID]] = attr[PROTOCOL_2878_ATTR_VALUE]

            except Exception as e:
                _LOGGER.warning("%s Error parsing XML part: %s. Document: %s", self.log_prefix, e, full_doc)
        return is_response, is_update, parsed_data

    async def _process_command_queue(self, queue_task: asyncio.Task) -> None:
        """Process a command from the queue."""
        command, future = queue_task.result()
        self._pending_future = future
        # Store the command string on the future for debugging purposes using setattr.
        setattr(self._pending_future, '_command_debug', command)

        try:
            await self._write_data(command)
            # The future will be resolved when a corresponding response or update is received.
            _LOGGER.debug("%s Command written, now waiting for response to resolve future", self.log_prefix)
        except CannotConnect as e:
            if self._pending_future and not self._pending_future.done():
                self._pending_future.set_exception(e)
            self._pending_future = None

    async def _process_read_queue(self, buffer: bytes, done: set[asyncio.Task]) -> Optional[bytes]:
        """Process data received from the read task."""
        data = None
        is_cancelled = False
        try:
            data = self._read_task.result()
        except asyncio.CancelledError:
            # Task was cancelled (likely by _close_connection), treat as connection closed
            data = None
            is_cancelled = True
        except Exception as e:
             _LOGGER.warning("%s Read task failed: %s", self.log_prefix, e)
             data = None
             
        if not data: 
            if not is_cancelled:
                _LOGGER.debug("%s Connection closed by device (EOF)", self.log_prefix)
            else:
                _LOGGER.debug("%s Read task was cancelled", self.log_prefix)
                
            await self._close_connection()
            return None
        
        buffer += data
        # Process buffer to find full XML messages.
        while b"</Response>" in buffer or b"</Update>" in buffer or b"/>" in buffer:
            end_tag = b"</Response>" if b"</Response>" in buffer else (b"</Update>" if b"</Update>" in buffer else b"/>")
            end_index = buffer.find(end_tag) + len(end_tag)
            message = buffer[:end_index]
            buffer = buffer[end_index:]
            
            xml_data = message.decode("utf-8", errors='ignore')
            _LOGGER.debug("%s Received message: %s", self.log_prefix, xml_data.strip())
            is_response, is_update, parsed_data = self._parse_and_update_state(xml_data)

            # Update internal state for redundant 'Power On' logic.
            if parsed_data:
                self._device_status.update(parsed_data)
            
            # --- Command Resolution Logic ---
            # A command is considered complete if we receive:
            # 1. A direct 'DeviceControl Okay' response.
            # 2. Any other response or update that contains actual state data.

            is_control_okay = is_response and not parsed_data and PROTOCOL_2878_DEVICE_CONTROL in xml_data and PROTOCOL_2878_STATUS_OK in xml_data
            is_polling_response = is_response and PROTOCOL_2878_DEVICE_STATE in xml_data

            # Initialize should_resolve to False to prevent UnboundLocalError
            should_resolve = False

            # If a pending command exists, check if this message resolves it.
            if self._pending_future and not self._pending_future.done():
                # If the pending command was a poll, only a DeviceState response can resolve it.
                # If it was a control command, any data update or a specific 'Okay' can resolve it.
                command_debug = getattr(self._pending_future, '_command_debug', '')
                is_poll_command = PROTOCOL_2878_DEVICE_STATE in command_debug
                
                should_resolve = (is_poll_command and is_polling_response) or \
                                 (not is_poll_command and (is_control_okay or parsed_data))

            if should_resolve and self._pending_future:
                if is_control_okay:
                    _LOGGER.debug("%s 'DeviceControl Okay' received, resolving pending command future", self.log_prefix)
                else:
                    _LOGGER.debug("%s Response/Update with data received, resolving pending command future", self.log_prefix)
                try:
                    if not self._pending_future.done():
                        self._pending_future.set_result(True)
                    self._pending_future = None
                except asyncio.InvalidStateError:
                    pass  # Future was already resolved.
            if parsed_data and (is_response or is_update) and self._update_callback:
                _LOGGER.debug("%s Calling update callback with data: %s", self.log_prefix, parsed_data)
                asyncio.create_task(self._update_callback(parsed_data))
            elif is_control_okay:
                # This is just an acknowledgment. The device will send a separate <Update> push.
                # We don't need to do anything here except acknowledge
                # that the command was successful so the UI doesn't hang. The future is already resolved.
                _LOGGER.debug("%s 'DeviceControl Okay' ack received. Waiting for subsequent push update.", self.log_prefix)
        
        return buffer


    async def _handle_reconnection(self) -> bool:
        """Handle the reconnection process."""
        # Stateful logging: Log INFO only when the state changes from available to unavailable.
        if self._is_available:
            _LOGGER.info("%s Connection lost. Attempting to reconnect...", self.log_prefix)
            self._is_available = False
        else:
            _LOGGER.debug("%s Connection is down. Attempting to reconnect...", self.log_prefix)

        try:
            # If the handshake fails with a connection error, it returns False.
            # We must handle this case to prevent falling through and causing an AttributeError.
            if not await self._establish_connection_and_handshake():
                _LOGGER.debug("%s Handshake returned False. Proceeding to retry logic.", self.log_prefix)
                self._reconnect_retries += 1
                _LOGGER.warning("%s Connection failed. Retrying in %s seconds...", self.log_prefix, self._reconnect_delay)
                self._ssl_context_cache.clear() # Force fresh SSL context on retry
                await self._close_connection()
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * RECONNECT_FACTOR, MAX_RECONNECT_DELAY)
                return False
        except (CannotConnect, AuthError) as e:
            self._reconnect_retries += 1
            # If reconnection fails, fail any pending command
            if self._pending_future and not self._pending_future.done():
                self._pending_future.set_exception(CannotConnect(f"Connection lost and reconnect failed: {e}"))
                self._pending_future = None
            _LOGGER.warning("%s Connection failed. Retrying in %s seconds...", self.log_prefix, self._reconnect_delay)
            self._ssl_context_cache.clear() # Force fresh SSL context on retry
            await self._close_connection()
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * RECONNECT_FACTOR, MAX_RECONNECT_DELAY)
            return False
        
        return True

    async def _connection_manager(self):
        buffer = b""
        # read_task is now self._read_task
        queue_task = None

        # Add a small delay at startup to allow the initial poll to establish the first connection
        await asyncio.sleep(2)

        try:
            while True:
                try:
                    if not self._writer or self._writer.is_closing():
                        if not await self._handle_reconnection():
                            continue

                    # --- START OF FIX: Add null check for self._reader ---
                    if not self._reader:
                        _LOGGER.warning("%s Reader object is missing, forcing reconnection.", self.log_prefix)
                        await self._close_connection()
                        continue

                    # Define tasks to wait for.
                    # --- START OF FIX: Use self._read_task ---
                    if not self._read_task or self._read_task.done():
                         self._read_task = asyncio.create_task(self._reader.read(8192))
                    
                    tasks = [self._read_task]
                    # --- END OF FIX ---

                    if not self._pending_future:
                        queue_task = asyncio.create_task(self._cmd_queue.get())
                        tasks.append(queue_task)
                    else:
                        queue_task = None
                    
                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

                    # --- Process completed tasks ---
                    if queue_task and queue_task in done:
                        await self._process_command_queue(queue_task)

                    if self._read_task in done:
                        buffer = await self._process_read_queue(buffer, done)
                        if buffer is None: # Connection closed
                             continue
                    
                    # After processing all messages in the buffer, cancel any pending tasks
                    # to allow the next command to be processed.
                    # CRITICAL FIX: Do NOT cancel the read_task, as it needs to keep running!
                    for task in pending:
                        if task == self._read_task:
                            continue
                        task.cancel()

                except Exception as e:
                    _LOGGER.error("%s Unhandled exception in connection manager: %s", self.log_prefix, e, exc_info=True)
                    if self._pending_future and not self._pending_future.done():
                        self._pending_future.set_exception(e)
                    self._pending_future = None
                    await self._close_connection()
                    await asyncio.sleep(self._reconnect_delay)
        finally:
            _LOGGER.debug("%s Connection manager exiting, cleaning up", self.log_prefix)
            if self._read_task and not self._read_task.done():
                self._read_task.cancel()
            await self._close_connection()

    def execute(self, template: Any, v: Any, device_state: Dict[str, Any], device_id: Optional[str] = None) -> Dict[str, Any]:
        """Synchronous wrapper to execute a command. To be called from an executor."""
        # --- START OF FIX: Add null check for self._controller and self._controller.hass ---
        if not self._controller or not hasattr(self._controller, 'hass') or not self._controller.hass:
            _LOGGER.error("%s Cannot execute command synchronously: controller or HASS instance is not available.", self.log_prefix)
            raise RuntimeError("Controller or HASS instance not available for thread-safe execution.")
        # --- END OF FIX ---
        return asyncio.run_coroutine_threadsafe(
            self._async_execute_internal(template, v, device_state, device_id),
            self._controller.hass.loop
        ).result()

    async def _async_execute_internal(self, template: Any, v: Any, device_state: Dict[str, Any], device_id: Optional[str] = None) -> Dict[str, Any]:
        """The actual async implementation of the execute logic."""
        # Wait for the connection to be ready before proceeding.
        try:
            await asyncio.wait_for(self._is_ready.wait(), timeout=COMMAND_TIMEOUT)
        except asyncio.TimeoutError as e:
            _LOGGER.warning("%s Timed out waiting for connection to be ready (device is likely offline).", self.log_prefix)
            raise CannotConnect("Connection not ready") from e

        async with self._lock:
            is_polling_request = template and not v and not device_state
            # Use the provided device_id if available, otherwise fall back to the main DUID
            duid_to_use = device_id or self._cfg.duid

            if is_polling_request:
                command = f'<Request Type="{PROTOCOL_2878_DEVICE_STATE}" DUID="{duid_to_use}"></Request>\n'
            else:
                params = self._params.copy()
                params.update({"value": v, "device_state": device_state, "duid": duid_to_use})
                command = template.render(**params).strip() + "\n"

            _LOGGER.debug("%s Queuing command: %s", self.log_prefix, mask_sensitive_data(command.strip().replace('\n', '')))
            future = asyncio.get_event_loop().create_future()
            await self._cmd_queue.put((command, future))

            try:
                await asyncio.wait_for(future, timeout=COMMAND_TIMEOUT)
                _LOGGER.debug("%s Command executed successfully", self.log_prefix)
            except asyncio.TimeoutError as e:
                _LOGGER.warning("%s Command timed out: %s", self.log_prefix, mask_sensitive_data(command.strip().replace('\n', '')))
                
                # CRITICAL: If the command times out, we MUST clear the pending_future
                # so the manager can accept new commands and not get stuck.
                if self._pending_future and self._pending_future == future:
                    _LOGGER.debug("%s Command timed out. Clearing pending future to unblock manager.", self.log_prefix)
                    self._pending_future = None

                # CRITICAL FIX: Always force connection close on timeout.
                # If a command timed out (20s), the connection is effectively dead or hung.
                # We remove the 'is_polling_request' check to ensure we always recover.
                _LOGGER.debug("%s Command timed out. Forcing connection close to trigger reconnect.", self.log_prefix)
                asyncio.create_task(self._close_connection())
                
                raise CannotConnect("Command timed out") from e
            except Exception as e:
                _LOGGER.error("%s Command failed with exception: %s", self.log_prefix, e)
                raise

        return self._device_status

# climate_ip/connection_raw.py
import logging
import json
import os
import copy
import time
import inspect
from typing import Any, Dict, Tuple, Optional, cast
from urllib.parse import urlparse
from homeassistant.const import CONF_IP_ADDRESS
from .connection import Connection, register_connection
from .exceptions import CannotConnect, AuthError
from .const import CONF_CERT
from .protocol_8888 import (
    Samsung8888Client,
    ProtocolError,
    AuthError as LibAuthError,
    ConnectionError as LibConnError,
)

_LOGGER = logging.getLogger(__name__)

CONNECTION_TYPE_RAW_8888 = "samsung_8888_raw"

@register_connection
class ConnectionRaw8888(Connection):
    """Wrapper for the robust raw socket API with auto-negotiation."""

    @staticmethod
    def match_type(type_str):
        return type_str == CONNECTION_TYPE_RAW_8888

    def load_from_yaml(self, node, connection_base):
        """Load configuration from yaml node dictionary.
        This provides the specific configuration (like keep_alive)
        from the 'connection' section of the YAML file.
        """
        if node:
            # Update keep_alive if present in the specific connection YAML
            if "keep_alive" in node:
                self._keep_alive = node["keep_alive"]
                
            # Also load params if present, to be consistent
            if "params" in node:
                self._params.update(node["params"])

            return True
        return False

    def create_updated(self, yaml_node):
        """Create a new connection instance with updated parameters from YAML.
        Replicates the logic from ConnectionAiohttp8888 to handle
        connection_template and params conversion.
        """
        from .const import (
            CONFIG_DEVICE_CONNECTION_TEMPLATE,
            CONFIG_DEVICE_CONNECTION_PARAMS,
            CONFIG_DEVICE_CONNECTION,
            CONFIG_DEVICE_CONDITION_TEMPLATE,
        )

        # Shallow copy for value‑specific operations
        new_connection = copy.copy(self)
        new_connection._params = getattr(self, "_params", {}).copy()
        # Propagate controller reference to the new instance (crucial for shared client logic)
        new_connection._controller = getattr(self, "_controller", None)

        if yaml_node and "keep_alive" in yaml_node:
            new_connection._keep_alive = yaml_node["keep_alive"]

        # Compile connection_template if present
        if yaml_node and CONFIG_DEVICE_CONNECTION_TEMPLATE in yaml_node:
            template_str = yaml_node[CONFIG_DEVICE_CONNECTION_TEMPLATE]
            from jinja2 import Template
            new_connection._connection_template = Template(template_str)

        # Convert static params to a template (HACK for legacy configs)
        elif yaml_node and CONFIG_DEVICE_CONNECTION_PARAMS in yaml_node:
            params = {**getattr(self, "_params", {}), **yaml_node.get(CONFIG_DEVICE_CONNECTION_PARAMS, {})}
            new_connection._params = params
            template_dict = {}
            if "json" in params:
                template_dict["json"] = params["json"]
            url_from_params = params.get("url", getattr(self, "_params", {}).get("url"))
            if url_from_params:
                template_dict["url"] = urlparse(url_from_params).path
            template_dict["method"] = params.get("method", getattr(self, "_params", {}).get("method"))
            if not template_dict.get("url"):
                # Use self.log_prefix for consistency, even if it's not fully initialized yet.
                _LOGGER.error(
                    "%s Could not determine 'url' from params during template hack.",
                    self.log_prefix,
                )
            template_str = json.dumps(template_dict)
            from jinja2 import Template
            new_connection._connection_template = Template(template_str)

        # Embedded commands handling
        if yaml_node and CONFIG_DEVICE_CONNECTION in yaml_node:
            new_connection._embedded_command = new_connection.create_updated(
                yaml_node[CONFIG_DEVICE_CONNECTION]
            )
            if CONFIG_DEVICE_CONDITION_TEMPLATE in yaml_node[CONFIG_DEVICE_CONNECTION]:
                condition_str = yaml_node[CONFIG_DEVICE_CONNECTION][CONFIG_DEVICE_CONDITION_TEMPLATE]
                from jinja2 import Template
                if new_connection._embedded_command:
                    new_connection._embedded_command.condition_template = Template(condition_str)

        return new_connection

    def __init__(self, config: Dict[str, Any], logger: logging.Logger, hass: Any, session: Any, ip_address: Optional[str]):
        super().__init__(config, logger)
        self._host: Optional[str] = ip_address or cast(Optional[str], config.get(CONF_IP_ADDRESS))
        cert_file = config.get(CONF_CERT)
        if cert_file and not os.path.dirname(cert_file):
            cert_file = os.path.join(os.path.dirname(__file__), cert_file)
        self._cert = cert_file
        self._controller = None # Initialize controller reference
        self._client: Optional[Samsung8888Client] = None
        self._client: Optional[Samsung8888Client] = None
        self._params = {}
        self._connection_template = None
        # Public attribute used to store Jinja2 templates for execution conditions.
        # Initialize here so static analyzers and type checkers recognize it.
        self.condition_template: Optional[Any] = None
        self._embedded_command = None
        self._keep_alive = config.get("keep_alive", True)

    def set_controller_ref(self, controller):
        """Allows the property to set a reference to the main controller."""
        self._controller = controller
        # Propagate to embedded command if it exists
        if self._embedded_command and hasattr(self._embedded_command, "set_controller_ref"):
            self._embedded_command.set_controller_ref(controller)

    async def async_get_client(self) -> Samsung8888Client:
        # --- Shared Client Logic ---
        # If we have a controller reference, try to use a shared client stored on it.
        # This prevents multiple connections (sockets) for the same device.
        if self._controller:
            if not hasattr(self._controller, "_shared_raw_client"):
                self._controller._shared_raw_client = None
            
            if self._controller._shared_raw_client is None:
                if not self._host:
                    raise CannotConnect("Host/IP address not provided for RAW connection")
                
                # --- START OF FIX: Dynamic Port Detection ---
                port = 8888 # Default for legacy devices
                
                # Try to extract from current params URL (if absolute)
                url = self._params.get("url")
                if url:
                    parsed = urlparse(url)
                    if parsed.port:
                        port = parsed.port
                    elif parsed.scheme == "https":
                        port = 443
                    elif parsed.scheme == "http":
                        port = 80
                
                _LOGGER.debug("%s [Shared] Initializing NEW shared client. Controller ID: %s, Port: %s", self.log_prefix, id(self._controller), port)
                self._controller._shared_raw_client = Samsung8888Client(
                    self._host, port, self._cert, log_prefix=self.log_prefix
                )
            else:
                _LOGGER.debug("%s [Shared] Reusing EXISTING shared client. Controller ID: %s", self.log_prefix, id(self._controller))
            
            return self._controller._shared_raw_client
        
        # Fallback for standalone usage (no controller)
        if self._client is None:
            _LOGGER.debug("%s [Standalone] Controller is None! Initializing local client.", self.log_prefix)
            if not self._host:
                raise CannotConnect("Host/IP address not provided for RAW connection")
            
            # --- START OF FIX: Dynamic Port Detection (Standalone) ---
            port = 8888
            url = self._params.get("url")
            if url:
                parsed = urlparse(url)
                if parsed.port:
                    port = parsed.port
                elif parsed.scheme == "https":
                    port = 443
                elif parsed.scheme == "http":
                     port = 80

            self._client = Samsung8888Client(
                self._host, port, self._cert, log_prefix=self.log_prefix
            )
        return self._client



    @property
    def log_prefix(self) -> str:
        """Generate a consistent log prefix."""
        if self._controller and self._controller.unique_id:
            return self._controller.log_prefix
        # Fallback if controller is not set yet
        return f"[{self._host or 'NO_IP'}]"

    @property
    def is_async_native(self) -> bool:
        return True

    async def async_execute(
        self,
        method,
        url,
        data,
        headers,
        device_state=None,
        _is_probe=False,
        _is_poll=False,
    ) -> Tuple[Optional[str], Optional[Dict]]:
        """Execute a command (including embedded commands) over raw sockets."""
        # --- Embedded command handling ---
        if self._embedded_command:
            _LOGGER.debug("%s [async_execute] Found embedded command.", self.log_prefix)
            try:
                if hasattr(self._embedded_command, "check_execute_condition") and device_state is not None:
                    if not self._embedded_command.check_execute_condition(device_state):
                        _LOGGER.debug(
                            "%s [async_execute] Embedded command condition not met. Skipping execution.",
                            self.log_prefix,
                        )
                    else:
                        _LOGGER.debug("%s [async_execute] Embedded command condition met. Executing it before the main command.", self.log_prefix)
                        embedded_template = getattr(self._embedded_command, "_connection_template", None)
                        if embedded_template:
                            embedded_params_str = embedded_template.render()
                            embedded_params = json.loads(embedded_params_str)
                            embedded_data = json.dumps(embedded_params.get("json")) if "json" in embedded_params else None
                            _LOGGER.debug(
                                "%s [async_execute] Executing embedded command with its own params: %s",
                                self.log_prefix, embedded_params
                            )
                            res = cast(
                                Any,
                                self._embedded_command.async_execute(
                                    method=embedded_params.get("method"),
                                    url=embedded_params.get("url"),
                                    data=embedded_data,
                                    headers=embedded_params.get("headers", headers),
                                    device_state=device_state,
                                ),
                            )
                            # Support both sync and async implementations of embedded command execution.
                            if inspect.isawaitable(res):
                                await res
                        else:
                            _LOGGER.warning("%s [async_execute] Embedded command found but it has no connection_template.", self.log_prefix)
                else:
                    _LOGGER.warning("%s [async_execute] Embedded command found, but cannot check its condition (device_state missing).", self.log_prefix)
            except (CannotConnect, AuthError) as e:
                _LOGGER.warning(
                    "%s [async_execute] Embedded command failed due to connection error: %s", self.log_prefix, e
                )
                raise
            except Exception as e:
                _LOGGER.error(
                    "%s [async_execute] Embedded command failed: %s", self.log_prefix, e
                )
                raise
        # --- Timing measurement for main request ---
        start_time = time.perf_counter()

        # --- Periodic Connection Reset Logic ---
        # If this is a poll and we are in "Periodic Reset" mode (keep_alive=False in config),
        # we explicitly close the connection before starting the new poll.
        # If this is a poll and we are in "Periodic Reset" mode (keep_alive=False in config),
        # we explicitly close the connection before starting the new poll.
        if _is_poll and not self._keep_alive:
             client_to_close = None
             if self._controller and hasattr(self._controller, "_shared_raw_client"):
                 client_to_close = self._controller._shared_raw_client
                 self._controller._shared_raw_client = None # Clear shared ref
             elif self._client:
                 client_to_close = self._client
                 self._client = None
            
             if client_to_close:
                 _LOGGER.debug("%s [Periodic Reset] Closing connection before poll.", self.log_prefix)
                 await client_to_close.close()
        # ---------------------------------------

        _LOGGER.debug("%s [async_execute] Executing main command with data: %s (is_poll=%s, keep_alive=%s)", self.log_prefix, data, _is_poll, self._keep_alive)
        path = urlparse(url).path
        body = json.loads(data) if data else None

        # --- START OF FIX: Inject Authorization Token ---
        from homeassistant.const import CONF_TOKEN
        
        req_headers = headers.copy() if headers else {}
        
        # Get token from config or controller (mimics aiohttp logic)
        current_token = self._config.get(CONF_TOKEN)
        if self._controller:
            current_token = self._controller._config.get(CONF_TOKEN, current_token)

        if not current_token:
            _LOGGER.error("%s [RAW] No token available! The request will fail.", self.log_prefix)
            raise AuthError("Token not configured for the raw engine")

        req_headers.setdefault("Authorization", f"Bearer {current_token}")
        req_headers.setdefault("Content-Type", "application/json")
        # --- END OF FIX ---

        client = await self.async_get_client()
        try:
            resp, err = await client.request(method, path, body, req_headers)
            if err:
                _LOGGER.debug("%s API Error: %s", self.log_prefix, err)
                return None, None
            elapsed = time.perf_counter() - start_time
            _LOGGER.debug("%s [RAW] Request completed in %.3f seconds", self.log_prefix, elapsed)
            return resp, None
        except LibAuthError:
            raise AuthError("Invalid token")
        except LibConnError as e:
            _LOGGER.debug("%s Connection error (%s)", self.log_prefix, e)
            await client.close()
            if _is_probe:
                return None, None
            raise CannotConnect(f"Connection failure: {e}")
        except Exception as e:
            raise CannotConnect(f"Unexpected error: {e}")

        # Fallback return to satisfy type checkers and handle unexpected flows;
        # return a tuple of (resp, error) both set to None to indicate no response.
        return None, None

    def check_execute_condition(self, device_state):
        """Replicates the condition check from connection_request.py for async."""
        do_execute = True
        if hasattr(self, "condition_template") and self.condition_template is not None:
            _LOGGER.debug("%s Evaluating execute condition for a command.", self.log_prefix)
            try:
                rendered_condition = self.condition_template.render(device_state=device_state)
                _LOGGER.debug("%s Execute condition result: %s", self.log_prefix, rendered_condition)
                do_execute = str(rendered_condition).strip() == "1"
            except Exception as e:
                _LOGGER.error(
                    "%s Error evaluating execute condition, executing command anyway. Error: %s",
                    self.log_prefix,
                    e,
                    exc_info=True,
                )
                do_execute = True
        return do_execute

    async def close(self):
        """
        Close the connection and release resources.
        This is called when the integration is unloaded or the connection method changes.
        """
        _LOGGER.debug("%s [RAW] Closing connection resources...", self.log_prefix)
        
        # 1. Close internal embedded command (if any)
        if self._embedded_command and hasattr(self._embedded_command, "close"):
             try:
                 await self._embedded_command.close()
             except Exception as e:
                 _LOGGER.warning("%s [RAW] Error closing embedded command: %s", self.log_prefix, e)

        # 2. Close the local client if it exists
        if self._client:
             _LOGGER.debug("%s [RAW] Closing local client...", self.log_prefix)
             try:
                 await self._client.close()
             except Exception as e:
                 _LOGGER.error("%s [RAW] Error closing local client: %s", self.log_prefix, e)
             finally:
                 self._client = None
             
        # 3. Close the shared client if it exists and we have a controller ref
        if self._controller and hasattr(self._controller, "_shared_raw_client"):
            shared_client = self._controller._shared_raw_client
            if shared_client:
                _LOGGER.debug("%s [RAW] Closing shared client...", self.log_prefix)
                try:
                    await shared_client.close()
                except Exception as e:
                    _LOGGER.error("%s [RAW] Error closing shared client: %s", self.log_prefix, e)
                finally:
                    self._controller._shared_raw_client = None

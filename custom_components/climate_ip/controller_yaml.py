import aiofiles
import asyncio
import time

from homeassistant.components.climate import ClimateEntityFeature, HVACMode, ATTR_HVAC_MODE
import copy
import re
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import homeassistant.helpers.config_validation as cv
import homeassistant.helpers.entity_component
from homeassistant.helpers import config_entry_oauth2_flow
import voluptuous as vol
import yaml
from homeassistant.const import (
    ATTR_ENTITY_ID,
    ATTR_NAME,
    ATTR_TEMPERATURE,
    CONF_MAC,
    CONF_IP_ADDRESS,
    CONF_TEMPERATURE_UNIT,
    CONF_TOKEN,
    STATE_ON,
    STATE_UNKNOWN,
    UnitOfTemperature,
)
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.util.dt import now
from homeassistant.helpers.config_validation import PLATFORM_SCHEMA
from requests.exceptions import RequestException
from homeassistant.helpers.update_coordinator import UpdateFailed

from .connection import CLIMATE_IP_CONNECTIONS
from .controller import ATTR_POWER, ClimateController, register_controller
from .properties import DeviceProperty, create_property, create_status_getter
from .state import ClimateIPDeviceState
# FIX: Import the missing constant
from .const import (
    CONF_DEVICE_TYPE,
    DEVICE_TYPE_8888_GROUP,
    DEVICE_TYPE_AIOHTTP_SUPPORTED,
    DEVICE_TYPE_MIM_H03,
    DEVICE_TYPE_SAMSUNG_2878,
    # --- START OF MODIFICATION (Milestone 4) ---
    CONF_CONN_METHOD,
    CONN_METHOD_AIOHTTP,
    CONN_METHOD_REQUESTS,
    CONN_METHOD_RAW,
    # --- END OF MODIFICATION (Milestone 4) ---
    # --- END OF MODIFICATION (Milestone 4) ---
    DOMAIN, # Import DOMAIN
)
from .exceptions import CannotConnect, AuthError
from .helpers import stream_wrapper, get_value_by_path, mask_sensitive_data
from .const import (
    CONF_CONFIG_FILE,
    CONF_DEVICE_ID,
    CONFIG_DEVICE,
    CONFIG_DEVICE_ATTRIBUTES,
    CONFIG_DEVICE_CONNECTION,
    CONFIG_DEVICE_OPERATIONS,
    CONFIG_DEVICE_SENSORS,
    CONFIG_DEVICE_POLL,
    CONFIG_DEVICE_STATUS,
    CONFIG_DEVICE_CONNECTION_TYPE,
)

_LOGGER = logging.getLogger(__name__)

CONST_CONTROLLER_TYPE = "yaml"
CONST_MAX_GET_STATUS_RETRIES = 4

# Class-level cache to store the raw content of YAML files.
_YAML_FILE_CACHE: Dict[str, str] = {}

def clear_yaml_cache():
    """Clears the YAML file content cache to allow reloading from disk."""
    if _YAML_FILE_CACHE:
        _LOGGER.info("Clearing YAML file cache to force re-read on reload.")
        _YAML_FILE_CACHE.clear()


@register_controller
class YamlController(ClimateController):
    """
    YAML-based controller, refactored to support asynchronous operations.
    """

    def __init__(self, config: Dict[str, Any], logger: logging.Logger) -> None:
        super().__init__(config, logger)
        self.hass = config.get("hass")
        self._session = config.get("session")
        
        self._config = config
        self._yaml = config.get(CONF_CONFIG_FILE)
        self._ip_address = config.get(CONF_IP_ADDRESS, None)
        self._device_id = config.get(CONF_DEVICE_ID, None) 
        self._token = config.get(CONF_TOKEN, None)
        self._unique_id = config.get("unique_id") or config.get(CONF_MAC) or self._ip_address
        self._id_discovered = self._device_id is not None

        self._operations = {}
        self._operations_list = []
        self._properties = {}
        self._properties_list = []

        # Optimization: Cache for device keys extracted from templates.
        self._prop_template_key_cache: Dict[str, Optional[str]] = {}

        # --- ADD SENSOR STORAGE ---
        self._sensors: Dict[str, "DeviceProperty"] = {}
        self._sensors_list: List[str] = []
        # --- END OF ADDITION ---
        self._name = CONST_CONTROLLER_TYPE
        self._attributes = {"controller": self.id}
        self._state_getter = None
        self._debug = config.get("debug", False)
        self._temp_unit = UnitOfTemperature.CELSIUS
        self._service_schema_map = {vol.Optional(ATTR_ENTITY_ID): cv.comp_entity_ids}
        self._last_device_state = None
        
        self._raw_yaml_config = None
        # Context-aware cache for parsed YAML. Keyed by device_id.
        self._parsed_yaml_cache: Dict[Optional[str], Dict] = {}

        self.coordinator = None # Reference to the coordinator
        self._is_fully_initialized = False
        # Flag to signal that the fan modes list has changed and a UI flicker is needed.
        self._fan_modes_list_changed_pending_flicker: bool = False

        self._last_state_fetch_time = 0
        self._cached_device_state = None
        self.discovered_devices = None
        # Pre-compile regex for performance. This is safe and state-independent.
        self._device_state_key_regex = re.compile(r"device_state[\[\.](['\"]?)([A-Za-z0-9_]+)\1")

        self._poll = None
        self._consecutive_connection_errors = 0
        self._pending_updates: Dict[str, Tuple[Any, float]] = {}

    async def _refresh_smartthings_token(self) -> Optional[str]:
        """
        Refreshes the SmartThings access token using the official integration's
        OAuth2 session helper. This handles expiration checks, clock skew (20s),
        and token refreshing automatically.
        """
        try:
            # 1. Find the SmartThings config entry
            entries = self.hass.config_entries.async_entries("smartthings")
            if not entries:
                _LOGGER.debug("%s [Auth] No Official SmartThings config entries found.", self.log_prefix)
                return None
            
            # Use the first entry found
            entry = entries[0]
            
            # 2. Get the OAuth2 implementation and create a session
            implementation = await config_entry_oauth2_flow.async_get_config_entry_implementation(
                self.hass, entry
            )
            session = config_entry_oauth2_flow.OAuth2Session(self.hass, entry, implementation)

            # 3. Force validation (refresh if needed)
            # async_ensure_token_valid() checks expiration + clock skew
            await session.async_ensure_token_valid()
            
            # 4. Return the (potentially new) access token
            token = session.token.get("access_token")
            # Log masked token for debugging
            masked = f"***{token[-6:]}" if token and len(token) > 6 else "None"
            _LOGGER.debug("%s [Auth] OAuth2 session token validated. Token: %s", self.log_prefix, masked)
            
            return token

        except Exception as e:
            _LOGGER.error("%s [Auth] Error refreshing SmartThings token via OAuth2: %s", self.log_prefix, e)
            return None

    def _update_all_connections_token(self, new_token: str):
        """Iterates through all properties and updates their connection token."""
        _LOGGER.debug("%s [Auth] Propagating new token to all connections.", self.log_prefix)
        # Collect all properties
        all_props = [self._state_getter] if self._state_getter else []
        all_props.extend(self._operations.values())
        all_props.extend(self._sensors.values())

        # Use a set to update unique connection objects (avoid duplicate updates)
        updated_connections = set()

        for prop in all_props:
            if prop:
                conn = prop.get_connection(None)
                if conn and conn not in updated_connections:
                    if hasattr(conn, "update_auth_token"):
                        conn.update_auth_token(new_token)
                        updated_connections.add(conn)
        _LOGGER.debug("%s [Auth] Updated token for %d unique connection objects.", self.log_prefix, len(updated_connections))

    def _mask_sensitive_data(self, data: Any) -> Any:
        """Mask sensitive data in the device state for logging."""
        if isinstance(data, dict):
            masked = data.copy()
            if "uuid" in masked and isinstance(masked["uuid"], str) and len(masked["uuid"]) > 6:
                masked["uuid"] = "***" + masked["uuid"][-6:]
            
            # Recursively mask children
            for key, value in masked.items():
                if isinstance(value, (dict, list)):
                    masked[key] = self._mask_sensitive_data(value)
            return masked
        elif isinstance(data, list):
            return [self._mask_sensitive_data(item) for item in data]
        return data

    @property
    def is_fully_initialized(self) -> bool:
        """Return True if the controller has completed its initialization."""
        return self._is_fully_initialized

    @property
    def log_prefix(self) -> str:
        return f"[{self.unique_id[-6:]}]" if self.unique_id and len(self.unique_id) >= 6 else f"[{self.name or 'NO_ID'}]"

    @property
    def unique_id(self) -> Optional[str]:
        return self._unique_id

    @property
    def device_id(self) -> Optional[str]:
        return self._device_id

    @property
    def token(self) -> Optional[str]:
        """Return the device token."""
        return self._token

    @property
    def ip_address(self) -> Optional[str]:
        """Return the device IP address."""
        return self._ip_address

    async def _finish_initialization(self):
        """
        Completes the controller's initialization after the device_id has been discovered.
        This involves loading the final operational and attribute properties from the YAML.
        """
        if self._is_fully_initialized or not self._raw_yaml_config:
            return
    
        _LOGGER.debug("%s Finalizing initialization with discovered device_id: %s", self.log_prefix, self._device_id)
        
        # Use the context-aware cache
        if self._device_id in self._parsed_yaml_cache:
            _LOGGER.debug("%s [Cache] Using cached YAML for device_id: %s", self.log_prefix, self._device_id)
            yaml_device = self._parsed_yaml_cache[self._device_id]
        else:
            _LOGGER.debug("%s [Cache] Parsing and caching YAML for device_id: %s", self.log_prefix, self._device_id)
            final_yaml_str = stream_wrapper(
                self._raw_yaml_config, self._token, self._ip_address, self._device_id
            )
            yaml_device = yaml.safe_load(final_yaml_str)
            self._parsed_yaml_cache[self._device_id] = yaml_device

        ac = yaml_device.get(CONFIG_DEVICE, {}) if yaml_device else {}

        nodes = ac.get(CONFIG_DEVICE_OPERATIONS, {})
        for op_key in nodes.keys():
            _LOGGER.debug("%s [ObjTrace] Creating property '%s'. Connection ID: %s, Prefix: %s", self.log_prefix, op_key, id(self._connection), self._connection.log_prefix)
            op = create_property(op_key, nodes[op_key], self._connection, self, self._state_getter)
            if op is not None:
                self._operations[op.id] = op
                self._service_schema_map[vol.Optional(op.id)] = op.config_validation_type

        nodes = ac.get(CONFIG_DEVICE_ATTRIBUTES, {})
        _LOGGER.debug("%s Loading %d attributes...", self.log_prefix, len(nodes))
        for key in nodes.keys():
            _LOGGER.debug("%s Processing attribute '%s' from YAML", self.log_prefix, key)
            prop = create_property(key, nodes[key], self._connection, self, self._state_getter)
            if prop is not None:
                self._properties[prop.id] = prop
                # --- INICIO DE LA MODIFICACIÓN ---
                # Add support for static 'unit_of_measurement' in attributes, same as in sensors.
                has_setter = hasattr(prop, 'set_unit_of_measurement')
                has_unit_key = 'unit_of_measurement' in nodes[key]
                _LOGGER.debug("%s Attribute '%s': has_setter=%s, has_unit_key=%s", self.log_prefix, key, has_setter, has_unit_key)

                if has_setter and has_unit_key:
                    unit_value = nodes[key]['unit_of_measurement']
                    _LOGGER.debug("%s Setting static unit for attribute '%s' to '%s'", self.log_prefix, key, unit_value)
                    prop.set_unit_of_measurement(unit_value)
                else:
                    if has_setter and not has_unit_key:
                        _LOGGER.debug("%s Attribute '%s' supports units, but 'unit_of_measurement' not found in its YAML config.", self.log_prefix, key)
                # --- END OF MODIFICATION ---
        
        # --- ADD THIS BLOCK TO LOAD SENSORS ---
        node_sensors = ac.get(CONFIG_DEVICE_SENSORS, {})
        _LOGGER.debug("%s Loading %d sensors", self.log_prefix, len(node_sensors))
        for name in node_sensors.keys():
            prop = create_property(
                name,
                node_sensors[name],
                self._connection,
                self,
                self._state_getter,
            )
            if prop:
                self._sensors[name] = prop
                self._sensors_list.append(name)
        # --- END OF ADDITION ---
        
        self._operations_list = list(self._operations.keys())
        self._properties_list = list(self._properties.keys())
        self._is_fully_initialized = True
        _LOGGER.debug("%s Controller is now fully initialized.", self.log_prefix)


    async def initialize(self) -> bool:
        """
        Performs the initial loading of the YAML configuration file and sets up the base connection.
        """
        file = self._yaml
        if file is not None and file.find("\\") == -1 and file.find("/") == -1:
            file = os.path.join(os.path.dirname(__file__), file)
        _LOGGER.debug("%s Loading configuration file: %s", self.log_prefix, file)

        if file is None:
            _LOGGER.error("%s No configuration file specified. Aborting initialization.", self.log_prefix)
            return False

        # --- START: Improved YAML caching logic ---
        if file in _YAML_FILE_CACHE:
            _LOGGER.debug("%s [Cache] Using cached YAML file content for: %s", self.log_prefix, file)
            self._raw_yaml_config = _YAML_FILE_CACHE[file]
        else:
            try:
                async with aiofiles.open(file, "r", encoding="utf-8") as stream:
                    self._raw_yaml_config = await stream.read()
                    _YAML_FILE_CACHE[file] = self._raw_yaml_config
                    _LOGGER.debug("%s [Cache] YAML file loaded and cached: %s", self.log_prefix, file)
            except Exception as exc:
                _LOGGER.error("%s Error loading YAML configuration %s: %s", self.log_prefix, file, exc, exc_info=True)
                return False
        # --- END: Improved YAML caching logic ---

        # --- START OF FIX: Ensure raw_yaml_config is not None before proceeding ---
        if not self._raw_yaml_config:
            _LOGGER.error("%s YAML configuration is empty or could not be read.", self.log_prefix)
            return False
        # --- END OF FIX ---
        # El parseo inicial usa el device_id actual (que puede ser None) como clave de cache.
        partial_render_str = stream_wrapper(self._raw_yaml_config, self._token, self._ip_address, self._device_id)
        yaml_device = yaml.safe_load(partial_render_str)
        self._parsed_yaml_cache[self._device_id] = yaml_device

        if CONFIG_DEVICE not in yaml_device:
            _LOGGER.error("%s Configuration file '%s' is missing the 'device' root key", self.log_prefix, file)
            return False

        ac = yaml_device.get(CONFIG_DEVICE, {}) if yaml_device else {}
        
        # --- START OF MODIFICATION (Milestone 4) ---
        # Start with the connection node from YAML as a base.
        connection_node = ac.get(CONFIG_DEVICE_CONNECTION, {}).copy()
        device_type = self._config.get(CONF_DEVICE_TYPE)
        
        # --- START OF FIX: Determine connection engine BEFORE creating the connection object ---
        if device_type == DEVICE_TYPE_SAMSUNG_2878:
            _LOGGER.info("%s Using 'samsung_2878' connection engine", self.log_prefix)
            connection_node[CONFIG_DEVICE_CONNECTION_TYPE] = "samsung_2878" # Hard-coded for this device type
        elif device_type in DEVICE_TYPE_AIOHTTP_SUPPORTED:
            # Read the selected connection method from the config entry's options.
            conn_method = self._config.get(CONF_CONN_METHOD, CONN_METHOD_REQUESTS) # Default
            if self.hass and self.unique_id:
                entry = self.hass.config_entries.async_get_entry(self.unique_id)
                if entry and entry.options:
                    conn_method = entry.options.get(CONF_CONN_METHOD, conn_method)

            if conn_method == CONN_METHOD_AIOHTTP:
                _LOGGER.info("%s Using 'Modern (aiohttp)' connection engine (from options)", self.log_prefix)
                connection_node[CONFIG_DEVICE_CONNECTION_TYPE] = "samsung_8888_aiohttp"
            elif conn_method == CONN_METHOD_RAW:
                _LOGGER.info("%s Using 'Robust (raw socket)' connection engine (from options)", self.log_prefix)
                connection_node[CONFIG_DEVICE_CONNECTION_TYPE] = "samsung_8888_raw"
            else:
                _LOGGER.info("%s Using 'Legacy (requests)' connection engine (from options)", self.log_prefix)
                # For 'requests', we use the connection type defined in the base YAML (e.g., 'request' or 'tls_auto')
                connection_node[CONFIG_DEVICE_CONNECTION_TYPE] = ac.get(CONFIG_DEVICE_CONNECTION, {}).get(CONFIG_DEVICE_CONNECTION_TYPE, "request")
        # --- END OF FIX ---

        # This call will now create the correct connection type based on the logic above
        # --- START OF MODIFICATION (Milestone 3) ---
        _LOGGER.debug(
            "%s Calling create_connection. Session is valid: %s, HASS is valid: %s",
            self.log_prefix, self._session is not None, self.hass is not None
        )
        _LOGGER.debug(
            "%s Connection node being passed to create_connection: %s",
            self.log_prefix, mask_sensitive_data(connection_node)
        )

        # --- START: Logic moved from connection.py ---
        key = self.unique_id
        if not key:
            _LOGGER.error("%s Cannot create a unique connection without a unique_id", self.log_prefix)
            return False

        # Access global state from hass.data
        if not self.hass:
             _LOGGER.error("%s Cannot access hass.data: hass object is missing", self.log_prefix)
             return False
             
        domain_data = self.hass.data.get(DOMAIN, {})
        connections_store = domain_data.get("connections")
        connections_lock = domain_data.get("lock")

        if connections_store is None or connections_lock is None:
             _LOGGER.warning("%s Connection store or lock not initialized in hass.data. Attempting to recover...", self.log_prefix)
             
             # Fallback initialization
             if DOMAIN not in self.hass.data:
                 self.hass.data[DOMAIN] = {}
                 domain_data = self.hass.data[DOMAIN]
             
             if "connections" not in domain_data:
                 domain_data["connections"] = {}
             if "lock" not in domain_data:
                 domain_data["lock"] = asyncio.Lock()
                 
             connections_store = domain_data["connections"]
             connections_lock = domain_data["lock"]
             
             _LOGGER.info("%s Successfully initialized missing connection store/lock structure.", self.log_prefix)

        async with connections_lock:
            if key in connections_store:
                _LOGGER.debug("%s [Cache] Returning existing connection object for %s", self.log_prefix, key)
                self._connection = connections_store[key]
            else:
                _LOGGER.debug("%s Creating new connection object for %s", self.log_prefix, key)
                conn_type_str = connection_node.get(CONFIG_DEVICE_CONNECTION_TYPE)
                # Move import here to avoid blocking calls at startup
                if conn_type_str == "samsung_8888_aiohttp":
                    from .connection_aiohttp import ConnectionAiohttp8888
                elif conn_type_str == "samsung_8888_raw":
                    from .connection_raw import ConnectionRaw8888
                for conn_class in CLIMATE_IP_CONNECTIONS:
                    if conn_class.match_type(conn_type_str):
                        _LOGGER.debug("%s Found matching connection class '%s' for type '%s'", self.log_prefix, conn_class.__name__, conn_type_str)
                        if conn_class.__name__ == "ConnectionAiohttp8888":
                            # Merge global config with YAML connection node to ensure insecure_ssl and others are passed
                            merged_config = {**self._config, **connection_node}
                            self._connection = conn_class(merged_config, _LOGGER, self.hass, self._session, self._ip_address)
                        elif conn_class.__name__ == "ConnectionRaw8888":
                             self._connection = conn_class(self._config, _LOGGER, self.hass, self._session, self._ip_address)
                        else:
                            self._connection = conn_class(self._config, _LOGGER)
                        
                        if self._connection.load_from_yaml(connection_node, None):
                            connections_store[key] = self._connection
                            break
                if not self._connection:
                     _LOGGER.error("%s No matching connection class found for type '%s'", self.log_prefix, conn_type_str)
        # --- END: Logic moved from connection.py ---

        if self._connection is None:
            _LOGGER.error("%s Could not create connection object", self.log_prefix)
            return False
        
        # --- START OF MODIFICATION: Add logging ---
        _LOGGER.debug("%s Connection object created successfully. Type: %s", self.log_prefix, type(self._connection).__name__)
        # --- END OF MODIFICATION ---

        self._state_getter = create_status_getter(
            "state", ac.get(CONFIG_DEVICE_STATUS, {}), self._connection, self
        )
        if self._state_getter is None:
            _LOGGER.error("%s Missing 'status' configuration node in '%s'", self.log_prefix, file)
            return False
        
        self._name = ac.get(ATTR_NAME, CONST_CONTROLLER_TYPE)
        
        poll_config = str(ac.get(CONFIG_DEVICE_POLL, "")).lower()
        if poll_config == "true":
            self._poll = True
        elif poll_config == "false":
            self._poll = False
        else:
            self._poll = None # Let the coordinator decide
        
        return True

    async def async_get_status(self) -> Optional[Dict[str, Any]]:
        """
        Fetches the device state. Uses a Short-Term Cache (TTL 2s) to avoid
        double polling when the Coordinator requests a refresh immediately after
        a Smart Poll update.
        """
        # --- SHORT-TERM CACHE LOGIC ---
        now_ts = time.time()
        if self._cached_device_state and (now_ts - self._last_state_fetch_time < 2.0):
             _LOGGER.debug(
                 "%s [Cache] Returning cached device state (TTL < 2s) to prevent double polling.", 
                 self.log_prefix
             )
             return self._cached_device_state.copy()
        # ------------------------------

        _LOGGER.debug("%s Polling device for state. Connection ID: %s, Prefix: %s", self.log_prefix, id(self._connection), self._connection.log_prefix)

        # Directly call async_update_state to ensure the most recent state is always fetched when requested.
        device_state = await self.async_update_state()
        return device_state.copy() if device_state else None

    async def async_update_state(self) -> Optional[Dict[str, Any]]: # This is the main polling function
        """
        Fetches the full device state from the physical device using the state_getter.
        This method also handles the one-time discovery of sub-devices.
        """
        # --- START OF FIX: Add null check for self._state_getter ---
        if not self._state_getter:
            raise UpdateFailed("State getter is not initialized, cannot update state.")
        # --- END OF FIX ---

        try:
            full_device_state = await self._state_getter.async_update_state(None, self._debug)
            
            # --- CONNECTION ERROR COUNTER RESET ---
            if self._consecutive_connection_errors > 0:
                _LOGGER.info(
                    "%s Connection recovered after %d failure(s). Counter reset.", 
                    self.log_prefix, 
                    self._consecutive_connection_errors
                )
            self._consecutive_connection_errors = 0
            # --------------------------------------

        except AuthError:
            _LOGGER.info("%s [Auth] Authentication failed (401). Refreshing token via OAuth2Session...", self.log_prefix)
            new_token = await self._refresh_smartthings_token()
            
            if new_token and new_token != self._token:
                _LOGGER.info("%s [Auth] Automatically retrieved new Access Token from SmartThings integration.", self.log_prefix)
                self._token = new_token
                self._update_all_connections_token(new_token)
                
                # --- START OF MODIFICATION: Persist token to Config Entry ---
                if self.coordinator and self.coordinator.entry:
                    entry = self.coordinator.entry
                    # Update config entry data with new token
                    new_data = dict(entry.data)
                    new_data[CONF_TOKEN] = new_token
                    self.hass.config_entries.async_update_entry(entry, data=new_data)
                    _LOGGER.info("%s [Auth] Persisted new SmartThings token to Config Entry.", self.log_prefix)
                # --- END OF MODIFICATION ---

                # Retry the update with the new token
                try:
                    full_device_state = await self._state_getter.async_update_state(None, self._debug)
                    # Reset counter on successful retry
                    self._consecutive_connection_errors = 0
                except Exception as retry_exc:
                     raise UpdateFailed(f"Retry after token refresh failed: {retry_exc}") from retry_exc
            else:
                _LOGGER.info("%s [Auth] Token refresh failed. SmartThings integration may not be installed or configured.", self.log_prefix)
                # Raise ConfigEntryAuthFailed to trigger the Reconfiguration flow in Home Assistant
                raise ConfigEntryAuthFailed(
                    "Authentication failed. Please install and configure the official SmartThings integration to provide a valid token."
                )

        except (RequestException, CannotConnect) as e:
            # --- TRANSIENT ERROR SUPPRESSION ---
            self._consecutive_connection_errors += 1
            
            if self._consecutive_connection_errors < 2 and self._cached_device_state:
                _LOGGER.debug(
                    "%s Connection failed (%d/2). Using cached state to prevent unavailability. Error: %s", 
                    self.log_prefix, 
                    self._consecutive_connection_errors,
                    e
                )
                return self._cached_device_state
            
            # If we reached here, it's either the 2nd failure OR we have no cache.
            # We raise the exception to mark the entity as unavailable.
            raise UpdateFailed(f"Unrecoverable error communicating with device: {e}") from e
            # -----------------------------------

        if full_device_state is None:
            if self._cached_device_state:
                 _LOGGER.debug("%s Failed to get latest state (API Error), using cached state to prevent unavailability.", self.log_prefix)
                 return self._cached_device_state
            
            raise UpdateFailed("Failed to get device state: No data received and no cache available")
        
        # --- CACHE UPDATE ---
        # We successfully fetched data, so we update the cache and timestamp here.
        # This allows async_get_status to utilize this data if called immediately after.
        self._cached_device_state = full_device_state
        self._last_state_fetch_time = time.time()
        # --------------------

        # --- One-time device discovery and initialization logic ---
        if not self._is_fully_initialized:
            try:
                device_type = self._config.get(CONF_DEVICE_TYPE)
                # Use the cached YAML for the current context (device_id is likely None here)
                id_map = self._parsed_yaml_cache.get(self._device_id, {}).get(CONFIG_DEVICE, {}).get("identifiers")
                
                if id_map:
                    _LOGGER.debug("%s 'identifiers' map found, running discovery", self.log_prefix)
                    self.discovered_devices = get_value_by_path(full_device_state, id_map.get("path_to_devices", []))
                    
                    if self.discovered_devices:
                        device_to_discover = None
                        
                        if device_type == DEVICE_TYPE_MIM_H03: # Special logic for MIM-H03
                            # For MIM-H03, find the coordinator unit (the one without a "Mode")
                            device_to_discover = next((d for d in self.discovered_devices if d and "Mode" not in d), None)
                            if not device_to_discover and self.discovered_devices:
                                # Fallback for older configs or different structures
                                device_to_discover = self.discovered_devices[0]
                        else: # Default for SAMSUNG_8888 and others
                            device_to_discover = self.discovered_devices[0] if self.discovered_devices else None
                        
                        if device_to_discover:
                            discovered_id = get_value_by_path(device_to_discover, id_map.get("id", []))
                            if discovered_id is not None:
                                self._device_id = str(discovered_id)
                            _LOGGER.info("%s Discovered device with id=%s", self.log_prefix, self._device_id)

                # Now that _device_id is potentially assigned, finish initialization
                await self._finish_initialization()

            except Exception as e:
                _LOGGER.error("%s Error during initial device discovery: %s", self.log_prefix, e, exc_info=True)
                # Do not return here, allow property update to proceed with what we have

        await self.async_update_properties_from_state(full_device_state)
        return self._state_getter.value

    @staticmethod
    def match_type(type: str) -> bool:
        return str(type).lower() == CONST_CONTROLLER_TYPE

    @property
    def name(self) -> str:
        return self._name

    @property
    def debug(self) -> bool:
        return self._debug
    
    @property
    def poll(self) -> Optional[bool]:
        """Return the polling state from the YAML configuration."""
        _LOGGER.debug("%s Poll property accessed, returning: %s", self.log_prefix, self._poll)
        return self._poll

    async def async_update_properties_from_state(self, full_device_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Updates all properties from a given device state dictionary.
        If no state is provided, it reconstructs it from the current Home Assistant state.
        """
        if not self._is_fully_initialized:
            return {}

        # FIX: Check if coordinator exists before accessing its data.
        # --- START OF FIX: Add null check for self.coordinator ---
        current_hass_state = self.coordinator.data if self.coordinator else None
        # --- END OF FIX ---
        if not current_hass_state:
            _LOGGER.debug("%s Coordinator data is not available (normal during setup or first poll)", self.log_prefix)

        if full_device_state is None:
            _LOGGER.debug("%s [UpdateProps] No state provided, rebuilding from HASS for merge", self.log_prefix)
            if not current_hass_state:
                _LOGGER.error("%s [UpdateProps] Cannot rebuild state from HASS: coordinator data is null. Aborting update", self.log_prefix)
                return {} # ABORT
            full_device_state = await self._build_device_state_from_hass(current_hass_state)
        else:
            _LOGGER.debug("%s [UpdateProps] Using provided state (poll/prediction)", self.log_prefix)
        
        if full_device_state is None:
            _LOGGER.error("%s [UpdateProps] full_device_state is None, cannot update properties. Aborting", self.log_prefix)
            return {}

        device_to_process = full_device_state
        
        # --- START: Sub-device selection logic ---
        try:
            # Use the cached YAML for the current context
            id_map = self._parsed_yaml_cache.get(self._device_id, {}).get(CONFIG_DEVICE, {}).get("identifiers")
            
            if id_map:
                _LOGGER.debug("%s 'identifiers' map found. Searching in path: %s", self.log_prefix, id_map.get("path_to_devices", []))
                devices_list = get_value_by_path(full_device_state, id_map.get("path_to_devices", []))
                
                if devices_list:
                    _LOGGER.debug("%s Found %d devices in state. Selecting correct one.", self.log_prefix, len(devices_list))
                    
                    # Simplified logic: Take the first valid device
                    found_device = devices_list[0] if devices_list[0] else None
                    # TODO: This should be improved to select the device based on self._device_id
                    if found_device:
                        _LOGGER.debug("%s Success. 'device_to_process' is now the sub-device", self.log_prefix)
                        device_to_process = found_device
                    else:
                        _LOGGER.warning("%s 'devices_list' exists but the first element is empty or null", self.log_prefix)
                else:
                    _LOGGER.warning("%s 'identifiers' exists but the path '%s' did not return a list", self.log_prefix, id_map.get("path_to_devices", []))
            else:
                _LOGGER.debug("%s No 'identifiers' map. Using full state (normal for 2878)", self.log_prefix)

        except Exception as e:
            _LOGGER.error("%s Error during sub-device selection: %s. Using full state.", self.log_prefix, e, exc_info=True)
            device_to_process = full_device_state
        # --- END: Sub-device selection logic ---

        # --- START OF FIX: Store unwrapped state ---
        self._last_device_state = device_to_process
        # --- END OF FIX ---

        corrections = {}
        all_properties = list(self._operations.values()) + list(self._properties.values()) + list(self._sensors.values())
        _LOGGER.debug("%s Updating %d properties using device_state: %s", self.log_prefix, len(all_properties), str(mask_sensitive_data(device_to_process)))
        
        for prop in all_properties:
            try:
                # --- START OF FIX: Pending Updates Logic ---
                # Check if we have a pending optimistic update for this property.
                # If so, and it's recent (< 5 seconds), we skip the update from the (potentially stale) device state
                # and enforce the optimistic value. This prevents the "revert" effect in the UI.
                if prop.id in self._pending_updates:
                    pending_val, timestamp = self._pending_updates[prop.id]
                    if time.time() - timestamp < 5.0:
                        _LOGGER.debug(
                            "%s [UpdateProps] Skipping update for '%s' due to pending optimistic value: %s", 
                            self.log_prefix, prop.name, pending_val
                        )
                        # We must convert the pending value (which might be a float) to the internal format if needed,
                        # but usually op._value stores the HA representation.
                        # However, pending_val comes from async_set_property -> new_value.
                        # If new_value is what we sent, it should be compatible with op._value.
                        prop._value = pending_val 
                        continue
                    else:
                        # Expired, remove it
                        del self._pending_updates[prop.id]
                # --- END OF FIX ---

                await prop.async_update_state(device_to_process, self._debug)
            except Exception as e:
                _LOGGER.error("%s FAILED to update property '%s'. Error: %s", self.log_prefix, prop.name, e, exc_info=True)
        
        for prop in all_properties:
            if hasattr(prop, 'set_device_state_for_values'):
                prop.set_device_state_for_values(device_to_process)

        _LOGGER.debug("%s Checking for post-update state inconsistencies", self.log_prefix)
        for op_name, op in self._operations.items():
            if hasattr(op, 'values') and op.value is not None and op.value != STATE_UNKNOWN:
                if op.value not in op.values:
                    new_value = op.values[0] if op.values else STATE_UNKNOWN
                    _LOGGER.info(
                        "%s State auto-correction for '%s'. Value '%s' is no longer valid in %s. Setting to '%s'. Triggering UI flicker",
                        self.log_prefix, op.name, op.value, op.values, new_value,
                    )
                    op._value = new_value
                    corrections[op.id] = new_value

                    # If this auto-correction is for a property with a feature flag (like fan_mode),
                    # ensure the pending flicker flag is set so the coordinator can handle it on the next update.
                    if hasattr(op, '_feature_flag') and op._feature_flag == ClimateEntityFeature.FAN_MODE:
                        self._fan_modes_list_changed_pending_flicker = True

        self._rebuild_attributes()
        return corrections
    def _rebuild_attributes(self) -> None:
        """Rebuilds the _attributes dictionary from all properties."""
        self._attributes = {ATTR_NAME: self.name}
        all_properties = list(self._operations.values()) + list(self._properties.values())
        for prop in all_properties:
            self._attributes.update(prop.state_attributes)
        self._attributes["last_sync"] = now().strftime("%Y-%m-%d %H:%M:%S")

    async def _build_device_state_from_hass(self, hass_state: ClimateIPDeviceState) -> Optional[Dict[str, Any]]:
        """
        Converts a Home Assistant state object (ClimateIPDeviceState) back into a
        raw device state dictionary, similar to what self._state_getter.value would contain.
        This is essentially the inverse of async_update_properties_from_state.

        *** This function reads from the provided hass_state. ***
        """
        if not self._is_fully_initialized:
            _LOGGER.warning("%s Cannot convert HASS state to device state: controller not fully initialized", self.log_prefix)
            return None

        # --- START OF FIX: Add null check for self._state_getter ---
        if not self._state_getter:
            _LOGGER.warning("%s [HASS->DEV] Cannot build device state: state_getter is not initialized.", self.log_prefix)
            return None
        last_real_state = self._state_getter.value
        # --- END OF FIX ---
        if not last_real_state:
            _LOGGER.warning("%s [HASS->DEV] No previous real device state available to use as a template", self.log_prefix)
            return {}

        reconstructed_state = copy.deepcopy(last_real_state)
        _LOGGER.debug("%s [HASS->DEV] Rebuilding from real state template (deepcopied)", self.log_prefix)

        all_props = list(self._operations.values()) + list(self._properties.values())

        for op in all_props:
            # --- START OF FIX: Prioritize internal value over HASS state ---
            # When merging or predicting, the internal property value (op.value) might be more
            # up-to-date than the HASS state, which could be stale.
            if op.value is not None and op.value != STATE_UNKNOWN: # Check internal property value first
                hass_value = op.value
            else:
                hass_value = getattr(hass_state, op.id, None)
            # --- END OF FIX ---

            hass_value = getattr(hass_state, op.id, None)
            _LOGGER.debug("%s [HASS->DEV] Prop: '%s', HASS Value: %s (type: %s)", self.log_prefix, op.id, hass_value, type(hass_value).__name__)
        
            if hass_value is not None:                
                # This conversion is what matters for building the device state
                device_value = op.convert_hass_to_dev(hass_value)
                # --- START OF LOGGING ---
                _LOGGER.debug("%s [HASS->DEV] Prop: '%s', Device Value: %s (type: %s)", self.log_prefix, op.id, device_value, type(device_value).__name__)
                # --- END OF LOGGING ---
                # Optimization: Use cached device key from template.
                device_key = self._get_cached_device_key_from_prop(op) # Get the key like 'AC_FUN_OPMODE'
                # --- START OF FIX: Ensure key exists before writing ---
                # Check if the key exists in the reconstructed state dictionary.
                if device_key and device_key in reconstructed_state:
                    reconstructed_state[device_key] = device_value
                # --- END OF FIX ---
                
        # --- START OF LOGGING ---
        _LOGGER.debug("%s [HASS->DEV] Final reconstructed state: %s", self.log_prefix, str(reconstructed_state)[:200] + "...")
        return reconstructed_state
        # --- END OF LOGGING ---

    async def _build_device_state_from_props(self) -> Optional[Dict[str, Any]]: # Used for prediction
        """
        Builds a raw device state dictionary (template) using the current
        *internal* values of the properties (operations + attributes).
        This is the core of the optimistic update prediction.
        """
        # --- START OF FIX: Final robust state reconstruction ---
        # --- START OF FIX: Add null check for self._state_getter ---
        if not self._state_getter:
            _LOGGER.warning("%s [PROP->DEV] Cannot build device state: state_getter is not initialized.", self.log_prefix)
            return None
        last_real_state = self._state_getter.value
        # --- END OF FIX ---
        if not last_real_state:
            _LOGGER.warning("%s [PROP->DEV] No previous real device state available to use as a template", self.log_prefix)
            return {}

        reconstructed_state = copy.deepcopy(last_real_state)
        _LOGGER.debug("%s [PROP->DEV] Building future state from template: %s", self.log_prefix, str(reconstructed_state)[:200] + "...")

        # Now, iterate through all properties and inject their *current* internal values
        # into the reconstructed state. This ensures the new value (e.g., new temperature)
        # is present for the next step of the prediction.
        all_props = list(self._operations.values()) + list(self._properties.values())
        for op in all_props:
            device_key = self._get_cached_device_key_from_prop(op)
            if device_key and device_key in reconstructed_state and op.value is not None:
                reconstructed_state[device_key] = op.convert_hass_to_dev(op.value)

        return reconstructed_state

    async def async_merge_device_state(self, new_data: Dict[str, Any], is_response: bool, is_update: bool):
        """
        Combines a partial state update (from a push or a response) with the current state,
        rebuilding the full state from Home Assistant to ensure consistency.
        """
        if not new_data:
            _LOGGER.debug("%s async_merge_device_state called with no new data", self.log_prefix)
            return

        _LOGGER.debug("%s Merging partial state update (from push/response): %s", self.log_prefix, new_data)

        # --- START OF FIX: Add null check for self.coordinator ---
        current_hass_state = self.coordinator.data if self.coordinator else None
        # --- END OF FIX ---
        if not current_hass_state:
            _LOGGER.warning("%s Cannot merge state, coordinator data is not available", self.log_prefix)
            # --- START OF FIX: Add null check for self._state_getter ---
            if not self._state_getter:
                _LOGGER.error("%s Cannot get a base state for merging: state_getter is not initialized.", self.log_prefix)
                return
            base_state = self._state_getter.value  # Fallback to last known state
            # --- END OF FIX ---
        else:
            base_state = await self._build_device_state_from_hass(current_hass_state)
            _LOGGER.debug("%s Base state reconstructed from HASS: %s", self.log_prefix, base_state)     

        if base_state is None:
            _LOGGER.error("%s Could not get a base state for merging. Aborting.", self.log_prefix)
            return

        base_state.update(new_data) # Overwrite base state with new data
        _LOGGER.debug("%s Resulting merged state: %s", self.log_prefix, base_state)
        
        # --- START OF FIX: Add null check for self._state_getter ---
        if self._state_getter:
            self._state_getter._value = base_state
        else:
            _LOGGER.error("%s Cannot store merged state: state_getter is not initialized.", self.log_prefix)
            return
        # --- END OF FIX ---
        await self.async_update_properties_from_state(base_state)

    def _get_cached_device_key_from_prop(self, prop: Any) -> Optional[str]:
        """
        Gets the device state key from a property's template, using a cache
        to avoid repeated regex searches on the same template string.
        """
        prop_id = prop.id
        if prop_id in self._prop_template_key_cache:
            return self._prop_template_key_cache[prop_id]

        # Key not in cache, so we calculate and store it.
        key = self._get_device_key_from_template(prop.status_template)
        self._prop_template_key_cache[prop_id] = key
        _LOGGER.debug("%s [Cache] Stored template key for '%s' -> '%s'", self.log_prefix, prop_id, key)
        return key

    def _get_device_key_from_template(self, template_obj: Any) -> Optional[str]:
        """
        Extracts the *primary* device state key (e.g., 'AC_FUN_OPMODE') 
        from a Home Assistant Template object.
        """
        if not template_obj:
            return None
        
        # Use .template attribute which holds the raw string.
        if hasattr(template_obj, 'template'):
            template_string = template_obj.template
        else:
            # Fallback for non-template objects, though less common.
            template_string = str(template_obj) 

        if not template_string:
            return None

        match = self._device_state_key_regex.search(template_string)
        if match:
            return match.group(2)
        
        # If no match, it's likely a complex template. This is not an error,
        # so we log at debug level instead of warning to keep logs clean.
        if any(keyword in template_string for keyword in ['if', 'else', 'for']):
            _LOGGER.debug("%s [Regex] Template is complex, cannot auto-extract key. This is normal for templates with logic.", self.log_prefix)
        else:
            _LOGGER.debug("%s [Regex] Could not extract 'device_state' key from template: %s", self.log_prefix, template_string)
        return None

    async def async_predict_and_correct_state(self, current_hass_state: ClimateIPDeviceState, property_name: str, new_value: Any) -> Tuple[ClimateEntityFeature, Dict[str, Any]]:
        """
        Predicts the device state after a change, performs corrections,
        and triggers feature flags if necessary.
        It now uses the current state from Home Assistant (via the coordinator) as the baseline.
        """
        _LOGGER.debug("%s [Predict] Starting prediction based on coordinator state: %s", self.log_prefix, str(current_hass_state))
        # --- START OF FIX: Add null check for self._state_getter ---
        if not self._state_getter:
            _LOGGER.warning("%s [Predict] Cannot predict state: state_getter is not initialized.", self.log_prefix)
            return ClimateEntityFeature(0), {}
        last_real_state = self._state_getter.value
        # --- END OF FIX ---

        if not self._is_fully_initialized:
            _LOGGER.info("%s [Predict] Cannot predict state: controller not fully initialized", self.log_prefix)
            return ClimateEntityFeature(0), {}
            
        if not last_real_state:
            _LOGGER.info("%s [Predict] Cannot predict state: last real state is unavailable", self.log_prefix)
            return ClimateEntityFeature(0), {}
        
        corrections = {}

        _LOGGER.debug("%s [Predict] Simulating state change: %s -> %s", self.log_prefix, property_name, new_value)

        
        original_values = {
            op_name: (op.value.value if isinstance(op.value, HVACMode) else op.value)
            for op_name, op in self._operations.items()
        }

        _LOGGER.debug("%s [Predict] Syncing internal properties to current HASS state before prediction", self.log_prefix)
        for op in self._operations.values():
            if hasattr(current_hass_state, op.id):
                op._value = getattr(current_hass_state, op.id)
        for prop in self._properties.values():
            if hasattr(current_hass_state, prop.id):
                prop._value = getattr(current_hass_state, prop.id)


        prop_to_change = self._operations.get(property_name)
        if not prop_to_change:
            _LOGGER.warning("%s [Predict] Property '%s' not found for prediction", self.log_prefix, property_name)
            return ClimateEntityFeature(0), {}
        
        # --- START OF LOGGING ---
        _LOGGER.debug("%s [Predict] New value for '%s' is %s (type: %s)", self.log_prefix, property_name, new_value, type(new_value).__name__)
        # --- END OF LOGGING ---
        
        prop_to_change._value = new_value
        _LOGGER.debug("%s [Predict] Applied change to internal property '%s'", self.log_prefix, property_name)

        future_state = await self._build_device_state_from_props()
        if not future_state:
            _LOGGER.error("%s [Predict] Failed to reconstruct future state from internal properties", self.log_prefix)
            return ClimateEntityFeature(0), {}

        try:
            device_value = prop_to_change.convert_hass_to_dev(new_value)
            
            # Optimization: Use cached device key from template
            device_key = self._get_cached_device_key_from_prop(prop_to_change)
            
            if device_key:
                if device_key in future_state:
                    _LOGGER.debug("%s [Predict] Auto-injecting '%s' into key '%s' (from cached template key)", self.log_prefix, device_value, device_key)
                    future_state[device_key] = device_value
                else:
                    _LOGGER.warning("%s [Predict] Auto-key '%s' found for '%s', but key not in state. Falling back to manual logic.", self.log_prefix, device_key, property_name)
                    device_key = None # Invalidate key to trigger manual logic
            else:
                _LOGGER.debug("%s [Predict] Auto-key failed for '%s' (template complex?). Using manual logic", self.log_prefix, property_name)

            if device_key is None:
                if property_name == ATTR_TEMPERATURE:
                    _LOGGER.debug("%s [Predict] Manual-injecting temperature: %s", self.log_prefix, new_value)
                    # --- START OF FIX: Handle nested device structure ---
                    # The 'Temperatures' key is inside the first item of the 'Devices' list.
                    # We need to navigate to it correctly.
                    device_list = future_state.get('Devices')
                    if isinstance(device_list, list) and len(device_list) > 0:
                        device_obj = device_list[0]
                        if isinstance(device_obj, dict) and 'Temperatures' in device_obj:
                            device_obj['Temperatures'][0]['desired'] = new_value
                        else:
                            _LOGGER.warning("%s [Predict] 'Temperatures' key missing inside 'Devices' list.", self.log_prefix)
                    # --- START OF FIX: Fallback for 2878 devices ---
                    elif 'AC_FUN_TEMPSET' in future_state:
                        _LOGGER.debug("%s [Predict] Manual-injecting for 2878-style device into AC_FUN_TEMPSET", self.log_prefix)
                        future_state['AC_FUN_TEMPSET'] = str(new_value)
                    else:
                        _LOGGER.warning("%s [Predict] Manual prediction failed. Neither 'Devices' nor 'AC_FUN_TEMPSET' found.", self.log_prefix)
                    # --- END OF FIX ---
                    # --- END OF FIX ---

                elif property_name == ATTR_HVAC_MODE:
                    if 'AC_FUN_OPMODE' in future_state:
                        _LOGGER.debug("%s [Predict] Manual-injecting hvac_mode: %s into AC_FUN_OPMODE", self.log_prefix, device_value)
                        future_state['AC_FUN_OPMODE'] = device_value
                    else:
                        _LOGGER.debug("%s [Predict] Manual hvac_mode prediction failed, 'AC_FUN_OPMODE' key missing", self.log_prefix)

                else:
                    _LOGGER.debug("%s [Predict] Auto-key failed and no manual logic exists for '%s'. Prediction may be inaccurate", self.log_prefix, property_name)
        
        except Exception as e:
            _LOGGER.error("%s [Predict] Error while writing prediction: %s", self.log_prefix, e)

        _LOGGER.debug("%s [Predict] Updating properties with simulated future state: %s", self.log_prefix, str(future_state)[:200] + "...")
        corrections.update(await self.async_update_properties_from_state(future_state))
        
        return ClimateEntityFeature(0), corrections

    async def async_set_property(self, property_name, new_value, device_id: Optional[str] = None):
        """
        Asynchronously sets a property on the device.
        """
        if not self._is_fully_initialized:
            _LOGGER.error("%s Cannot set property '%s': controller not fully initialized", self.log_prefix, property_name)
            return False

        op = self._operations.get(property_name)
        if op:
            try:
                # --- START OF FIX: Register Pending Update ---
                # Store the value and current time to prevent immediate overwrite by stale polls.
                self._pending_updates[property_name] = (new_value, time.time())
                _LOGGER.debug("%s Registered pending update for '%s': %s", self.log_prefix, property_name, new_value)
                # --- END OF FIX ---

                return await op.async_set_value(new_value, self._device_id)
            except (RequestException, CannotConnect) as e:
                raise UpdateFailed(f"Failed to set property '{property_name}': {e}") from e
            
        _LOGGER.error("%s Failed to set property '%s': property not found", self.log_prefix, property_name)
        return False

    def get_property(self, property_name):
        value = None
        if property_name in self._operations:
            value = self._operations[property_name].value
        elif property_name in self._properties:
            value = self._properties[property_name].value
        elif property_name in self._sensors:
            value = self._sensors[property_name].value
        else:
            value = self._attributes.get(property_name)

        if value == STATE_UNKNOWN:
            value = None
        
        return value

    def get_property_object(self, property_name: str) -> Optional[Any]:
        """Returns the actual property object, not just its value."""
        if property_name in self._operations:
            return self._operations[property_name]
        if property_name in self._properties:
            return self._properties[property_name]
        if property_name in self._sensors:  # <-- ADD THIS LINE
            return self._sensors[property_name]  # <-- ADD THIS LINE
        _LOGGER.debug("%s Property object '%s' not found", self.log_prefix, property_name)
        return None

    def get_property_all_values(self, property_name: str) -> Optional[List[str]]:
        """Returns the complete, unfiltered list of values for a property."""
        prop = self.get_property_object(property_name)
        if prop and hasattr(prop, 'all_values'):
            return prop.all_values
        _LOGGER.debug("%s Property '%s' does not have an 'all_values' attribute", self.log_prefix, property_name)
        return None

    @property
    def state_attributes(self):
        return self._attributes

    @property
    def temperature_unit(self):
        return self._temp_unit

    @property
    def service_schema_map(self):
        return self._service_schema_map

    @property
    def operations(self):
        return self._operations_list

    @property
    def attributes(self) -> list:
        return self._properties_list

    # --- ADD THIS NEW PROPERTY ---
    @property
    def sensors(self) -> List["DeviceProperty"]:
        """Return a list of all defined sensor property objects."""
        # Return the actual property objects, not just the names
        return [self._sensors[name] for name in self._sensors_list if name in self._sensors]
    
    @property
    def device_state(self) -> Dict[str, Any]:
        """
        Return the current *unwrapped* device state.
        This provides access to the sub-device state (e.g. {'Mode': ...}) 
        which is used by properties and sensors, rather than the raw connection state
        (e.g. {'Devices': ...}).
        """
        if self._last_device_state:
             return self._last_device_state
        
        # Fallback to raw state if no unwrapped state is available yet
        if self._state_getter:
             return self._state_getter.value
        
        return {}
    # --- END OF ADDITION ---


PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_CONFIG_FILE): cv.string,
        vol.Optional(CONF_IP_ADDRESS): cv.string,
        vol.Optional(CONF_TOKEN): cv.string,
        vol.Optional(CONF_DEVICE_ID): cv.string,
        vol.Optional(CONF_TEMPERATURE_UNIT, default=UnitOfTemperature.CELSIUS): cv.string,
    }
)
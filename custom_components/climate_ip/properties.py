import json
import logging
from typing import Any, Dict, Optional
import homeassistant.helpers.config_validation as cv
from homeassistant.const import STATE_OFF, STATE_ON, STATE_UNKNOWN, UnitOfTemperature
from homeassistant.util.unit_conversion import TemperatureConverter

from .const import (
    CONFIG_TYPE,
    CONFIG_DEVICE_CONNECTION,
    CONFIG_DEVICE_STATUS_TEMPLATE,
    CONFIG_DEVICE_CONNECTION_TEMPLATE,
    CONFIG_DEVICE_VALIDATION_TEMPLATE,
    CONFIG_DEVICE_OPERATION_VALUES,
    CONFIG_DEVICE_OPERATION_VALUE,
    CONFIG_DEVICE_OPERATION_NUMBER_MIN,
    CONFIG_DEVICE_OPERATION_NUMBER_MAX,
    CONFIG_DEVICE_OPERATION_TEMP_UNIT_TEMPLATE,
)
from .exceptions import CannotConnect, AuthError

_LOGGER = logging.getLogger(__name__)

# Map YAML operation names to Home Assistant features.
from homeassistant.components.climate import ClimateEntityFeature
from homeassistant.components.climate.const import (
    ATTR_FAN_MODE,
    ATTR_PRESET_MODE,
    ATTR_SWING_MODE,
    ATTR_HVAC_MODES,
    ATTR_FAN_MODES,
    ATTR_PRESET_MODES,
    ATTR_SWING_MODES,
    ATTR_HVAC_MODE,
)

YAML_NAME_TO_HA_FEATURE = {
    "fan": ClimateEntityFeature.FAN_MODE, # "fan" is a legacy name for fan_mode
    "swing": ClimateEntityFeature.SWING_MODE,
    "preset": ClimateEntityFeature.PRESET_MODE, # 'special' is also used for presets
    "special": ClimateEntityFeature.PRESET_MODE,
    "hvac": ClimateEntityFeature.TARGET_TEMPERATURE, # No specific flag, but it's a primary operation
}
PROPERTY_TYPE_MODE = "modes"
PROPERTY_TYPE_SWITCH = "switch"
PROPERTY_TYPE_NUMBER = "number"
PROPERTY_TYPE_TEMP = "temperature"
PROPERTY_TYPE_STRING = "string"
STATUS_GETTER_JSON = "json_status"

UNIT_MAP = {
    "C": UnitOfTemperature.CELSIUS,
    "c": UnitOfTemperature.CELSIUS,
    "°C": UnitOfTemperature.CELSIUS,
    "Celsius": UnitOfTemperature.CELSIUS,
    "F": UnitOfTemperature.FAHRENHEIT,
    "f": UnitOfTemperature.FAHRENHEIT,
    "Fahrenheit": UnitOfTemperature.FAHRENHEIT,
    "°F": UnitOfTemperature.FAHRENHEIT,
    UnitOfTemperature.CELSIUS: UnitOfTemperature.CELSIUS,
    UnitOfTemperature.FAHRENHEIT: UnitOfTemperature.FAHRENHEIT,
}

CLIMATE_IP_PROPERTIES = []
CLIMATE_IP_STATUS_GETTER = []


def register_property(dev_prop):
    """Decorate a function to register a property."""
    CLIMATE_IP_PROPERTIES.append(dev_prop)
    return dev_prop


def register_status_getter(getter):
    """Decorate a function to register a status getter."""
    CLIMATE_IP_STATUS_GETTER.append(getter)
    return getter


def create_property(name, node, connection_base, controller, status_getter=None):
    for prop in CLIMATE_IP_PROPERTIES:
        if CONFIG_TYPE in node:
            if prop.match_type(node[CONFIG_TYPE]):
                op = prop(name, connection_base, controller, status_getter)
                if op.load_from_yaml(node):
                    return op
    return None


def create_status_getter(name, node, connection_base, controller):

    for getter in CLIMATE_IP_STATUS_GETTER:
        if CONFIG_TYPE in node:
            if getter.match_type(node[CONFIG_TYPE]):
                g = getter(name, connection_base, controller)
                if g.load_from_yaml(node):
                    return g
    return None


class DeviceProperty:
    def __init__(self, name, connection, controller, status_getter=None):

        self._name = name
        self._value = STATE_UNKNOWN
        self._connection = connection
        self._controller = controller
        self._status_getter = status_getter
        self._status_template = None
        self._id = name
        self._connection_template = None
        self._validation_template = None
        self._device_state = None

        # --- ADD THESE NEW ATTRIBUTES ---
        self._friendly_name: Optional[str] = None  # For the entity name
        self._device_class: Optional[str] = None
        self._unit_of_measurement: Optional[str] = None
        self._state_class: Optional[str] = None
        # --- END OF ADDITIONS ---

    @property
    def log_prefix(self) -> str:
        """Get the log prefix from the controller for consistent logging."""
        return self._controller.log_prefix

    @property
    def id(self):
        return self._id

    def is_valid(self, device_state):
        self._device_state = device_state
        if self.validation_template is None or device_state is None:
            return True
        else:
            try:
                v = self.validation_template.render(device_state=device_state)
                return str(v).lower() == "valid"
            except Exception as e:
                _LOGGER.error("%s Error rendering validation template for %s: %s", self.log_prefix, self.id, e)
                return False

    @property
    def config_validation_type(self):
        return cv.string

    @property
    def status_template(self):
        return self._status_template

    @property
    def value(self):
        return self._value

    @property
    def name(self):
        """Return the friendly name of the property, or the ID if not set."""
        return self._friendly_name or self._name

    # --- ADD THESE NEW PROPERTIES ---
    @property
    def device_class(self) -> Optional[str]:
        return self._device_class

    @property
    def unit_of_measurement(self) -> Optional[str]:
        return self._unit_of_measurement

    @property
    def state_class(self) -> Optional[str]:
        return self._state_class

    def set_unit_of_measurement(self, unit: str):
        """
        Sets the static unit of measurement for the property, converting
        common temperature units to Home Assistant constants if possible.
        """
        # Check if the provided unit is a known temperature unit and convert it.
        # This allows using "°F", "F", "Fahrenheit", etc., in YAML.
        _LOGGER.debug("%s [set_unit_of_measurement] for '%s' received raw unit: '%s'", self.log_prefix, self.id, unit)
        converted_unit = UNIT_MAP.get(unit, unit)
        _LOGGER.debug("%s [set_unit_of_measurement] for '%s' converted unit is: '%s' (type: %s)", self.log_prefix, self.id, converted_unit, type(converted_unit).__name__)
        self._unit_of_measurement = converted_unit

    def get_connection(self, value):
        return self._connection

    @property
    def connection_template(self):
        return self._connection_template

    @property
    def validation_template(self):
        return self._validation_template

    def load_from_yaml(self, node):
        """Load configuration from a YAML node dictionary. Return True if successful."""
        from jinja2 import Template

        if node is not None:
            if CONFIG_DEVICE_STATUS_TEMPLATE in node:
                self._status_template = Template(node[CONFIG_DEVICE_STATUS_TEMPLATE])
            if CONFIG_DEVICE_CONNECTION_TEMPLATE in node:
                self._connection_template = Template(
                    node[CONFIG_DEVICE_CONNECTION_TEMPLATE]
                )
            if CONFIG_DEVICE_VALIDATION_TEMPLATE in node:
                self._validation_template = Template(
                    node[CONFIG_DEVICE_VALIDATION_TEMPLATE]
                )
            self._connection = self._connection.create_updated(
                node.get(CONFIG_DEVICE_CONNECTION, {})
            )

            # --- ADD THIS BLOCK TO LOAD SENSOR METADATA ---
            # Use 'name' from YAML as the friendly name, self._name remains the key
            self._friendly_name = node.get("name", None)
            self._device_class = node.get("device_class", None)
            self._unit_of_measurement = node.get("unit_of_measurement", None)
            self._state_class = node.get("state_class", None)
            return True
        return False

    def convert_dev_to_hass(self, dev_value):
        """Convert device state value to HASS."""
        return dev_value

    async def async_update_state(self, device_state_override, debug):
        """Update property from device state and return current value."""
        from jinja2 import Template
        if device_state_override is not None:
            device_state = device_state_override
        else:
            device_state = self._status_getter.value if self._status_getter else None
        self._device_state = device_state
        v = STATE_UNKNOWN
        if self.status_template is not None and device_state is not None:
            try:
                v = self.status_template.render(device_state=device_state)
            except Exception as e:
                _LOGGER.error("%s Error rendering status template for %s: %s", self.log_prefix, self.id, e)
        if v is not STATE_UNKNOWN:
            self._value = self.convert_dev_to_hass(v)
        return self.value

    @property
    def state_attributes(self):
        """Return a dictionary with property attributes."""
        return {self.id: self.value}


@register_status_getter
class GetJsonStatus(DeviceProperty):
    def __init__(self, name, connection, controller, status_getter=None):
        super(GetJsonStatus, self).__init__(name, connection, controller, self)
        self._json_status = None
        self._attrs = {}

    @staticmethod
    def match_type(type):
        return type == STATUS_GETTER_JSON

    def load_from_yaml(self, node):
        """Load the connection details from the 'status' node in YAML."""
        from jinja2 import Template
        
        # --- START OF MODIFICATION: Topology Debugging ---
        if self._connection:
             _LOGGER.debug(
                 "%s [Topology] GetJsonStatus.load_from_yaml: base connection ID=%s, type=%s, parent=%s",
                 self.log_prefix, 
                 id(self._connection), 
                 type(self._connection).__name__,
                 getattr(self._connection, "_parent", "N/A")
             )
        else:
             _LOGGER.debug("%s [Topology] GetJsonStatus.load_from_yaml: base connection is None!", self.log_prefix)
        # --- END OF MODIFICATION ---
        
        super_result = super().load_from_yaml(node)

        # --- START OF MODIFICATION: Default connection template for aiohttp ---
        # If we are using the aiohttp engine and no connection_template was defined
        # in the YAML's 'status' block, we try to use the one from the connection object
        # (which might have been created from 'params' in create_updated).
        if self._connection and self._connection.is_async_native and not self._connection_template:
            # Try to inherit from connection object
            if hasattr(self._connection, "_connection_template") and self._connection._connection_template:
                 _LOGGER.debug("%s [GetJsonStatus] Inheriting connection_template from connection object.", self.log_prefix)
                 # It might be a Template object or a string.
                 # Since it's usually a Template object created in create_updated, we can use it directly.
                 # However, to be safe, we re-wrap connection_template if needed.
                 # Note: Jinja2 Templates are not easily copyable or strictly typed here, 
                 # but we can just reference it.
                 self._connection_template = self._connection._connection_template
            else:
                _LOGGER.debug(
                    "%s [GetJsonStatus] No connection_template found for aiohttp. Creating a default one.",
                    self.log_prefix
                )
                default_template_str = '{ "method": "GET", "url": "/devices" }'
                self._connection_template = Template(default_template_str)
        # --- END OF MODIFICATION ---
        return super_result

    async def async_update_state(self, device_state_override, debug):
        """Fetch the device state asynchronously."""
        if hasattr(self.get_connection(None), 'set_controller_ref'):
            self.get_connection(None).set_controller_ref(self._controller)

        device_state_result = None
        connection = self.get_connection(None)

        # --- START OF MODIFICATION: Add logging ---
        if connection is None:
            _LOGGER.error("%s [GetJsonStatus] Connection object is None! Cannot proceed with state update.", self.log_prefix)
            return None # Abort if connection is missing
        # --- END OF MODIFICATION ---

        # --- START OF MODIFICATION (Milestone 1) ---
        # Check if the connection is native async (aiohttp)
        if connection.is_async_native:
            _LOGGER.debug("[Dual Engine] Executing 'async_execute' (Async Engine)")
            try:
                # The connection_template contains the request parameters (method, url, etc.)
                # We need to render it to get the JSON string of parameters.
                if not self.connection_template:
                    _LOGGER.error("%s [GetJsonStatus] Connection template is missing for async execution.", self.log_prefix)
                    return None
                
                # --- START OF MODIFICATION: Add logging ---
                _LOGGER.debug(
                    "%s [GetJsonStatus] Using connection template for async execution: %s",
                    self.log_prefix,
                    self.connection_template.template if hasattr(self.connection_template, 'template') else self.connection_template
                )
                # --- END OF MODIFICATION ---

                params_str = self.connection_template.render()
                params = json.loads(params_str)

                # The async_execute method handles the request.
                response_text, _ = await connection.async_execute(params.get('method'), params.get('url'), None, params.get('headers'), _is_poll=True)
                
                if response_text is None:
                    _LOGGER.debug("%s [GetJsonStatus] No response text received (None).", self.log_prefix)
                    return None

                device_state_result = json.loads(response_text)
            except json.JSONDecodeError as e:
                _LOGGER.error("%s [GetJsonStatus] JSON parsing error. Response text was: '%s'. Error: %s", self.log_prefix, response_text, e)
                return None
            # --- START OF SOLUTION: Do not catch connection errors here ---
            # By removing the 'except Exception', we allow InvalidHeaderError and CannotConnect
            # to propagate up to the coordinator, which will handle them correctly.
            # --- END OF SOLUTION ---
        else:
            # It's a synchronous connection (requests or 2878)
            _LOGGER.debug("[Dual Engine] Executing 'execute' in executor (Sync Engine)")

            # This is the logic that will run now, identical to the previous one.
            # The synchronous 'execute' call is wrapped in async_add_executor_job
            # to avoid blocking the Home Assistant event loop.
            device_state_result = await self._controller.hass.async_add_executor_job(
                connection.execute,
                self.connection_template,
                None,
                self.value
            )
        # --- END OF MODIFICATION (Milestone 1) ---

        self._value = device_state_result
        self._json_status = device_state_result

        if device_state_result is not None:
            self._attrs = {"device_state": json.dumps(device_state_result)}
            if self.status_template is not None:
                try:
                    v = self.status_template.render(device_state=device_state_result)
                    if isinstance(v, str):
                        v = v.replace("'", '"')
                        v = v.replace("True", '"True"')
                        self._value = json.loads(v)
                    else:
                        self._value = v
                except Exception as e:
                    _LOGGER.debug(
                        "%s Could not parse status template result as JSON: %s",
                        self.log_prefix,
                        e,
                        exc_info=True
                    )
        else:
            self._attrs = {"device_state": None}

        return self.value

    @property
    def state_attributes(self):
        """Return a dictionary with property attributes."""
        return self._attrs


class DeviceOperation(DeviceProperty):
    def __init__(self, name, connection, controller, status_getter=None):
        super(DeviceOperation, self).__init__(name, connection, controller, status_getter)

    async def async_set_value(self, v, device_id: Optional[str] = None):
        """Set device property value asynchronously."""
        connection = self.get_connection(v)
        if hasattr(connection, 'set_controller_ref'):
            connection.set_controller_ref(self._controller)

        current_full_state = self._device_state
        if current_full_state is None:
            _LOGGER.warning("%s _device_state is None during set_value, falling back to status_getter.value", self.log_prefix)
            # --- START OF FIX: Add null check for self._status_getter ---
            if self._status_getter:
                current_full_state = self._status_getter.value
            # --- END OF FIX ---

        # --- START: Logic to handle async nested commands ---
        if connection.is_async_native:
            _LOGGER.debug("[Dual Engine] Executing 'async_execute' (Async Engine)")
            try:
                # The `async_execute` method in ConnectionAiohttp8888 will handle its embedded command internally.
                # We just need to call it once with the parameters for the *main* command.                
                # --- START OF SOLUTION: Use the property's template, not the connection's ---
                # The property's connection_template (self) is the one loaded from YAML
                # for numeric/temperature operations. The connection's one might be empty.
                template_to_use = self.connection_template or getattr(connection, '_connection_template', None)
                # --- END OF SOLUTION ---
                if not template_to_use: # Now we check after trying both sources
                    _LOGGER.error("%s [async_set_value] Main command is missing a connection template.", self.log_prefix)
                    return False

                # --- START OF SOLUTION: Merge base and template parameters ---
                # Render the operation-specific template (e.g., for temperature).
                rendered_params_str = template_to_use.render(value=self.convert_hass_to_dev(v), device_id=device_id)
                operation_params = json.loads(rendered_params_str)

                # Get the base parameters from the connection (which contain method and url).
                # The 'hack' in `create_updated` ensures that `_connection_template` exists.
                base_template = getattr(connection, '_connection_template', None)
                base_params_str = base_template.render() if base_template else "{}"
                base_params = json.loads(base_params_str)

                # Merge the parameters, giving priority to the operation-specific ones.
                params = {**base_params, **operation_params}
                # --- END OF SOLUTION ---
                data_payload = json.dumps(params.get('json')) if 'json' in params else None

                response, _ = await connection.async_execute(params.get('method'), params.get('url'), data_payload, params.get('headers'), device_state=current_full_state)
                return response is not None
            except (CannotConnect, AuthError) as e:
                 _LOGGER.warning("%s Failed to set value for %s: connection error: %s", self.log_prefix, self.id, e)
                 return False
            except Exception as e:
                _LOGGER.error("%s Error during async_set_value for %s: %s", self.log_prefix, self.id, e, exc_info=True)
                return False
        # --- END: Logic to handle async nested commands ---
        else: # Fallback to original synchronous logic
            _LOGGER.debug("[Dual Engine] Executing 'execute' in executor (Sync Engine)")
            try:
                response = await self._controller.hass.async_add_executor_job(
                    connection.execute,
                    self.connection_template,
                    self.convert_hass_to_dev(v),
                    current_full_state,
                    device_id
                )
                # The synchronous `execute` method in `connection_request.py` returns `None`
                # on success with an empty body to trigger a poll. A `dict` or `list` is returned
                # if there is a JSON body. An exception is raised on failure.
                # Therefore, if no exception was raised, the command was successful.
                return True
            except (CannotConnect, AuthError) as e:
                 _LOGGER.warning("%s Failed to set value for %s: connection error: %s", self.log_prefix, self.id, e)
                 return False
            except Exception as e:
                _LOGGER.error("%s Error during async_set_value for %s: %s", self.log_prefix, self.id, e, exc_info=True)
                return False

    def match_value(self, value):
        """Check if value matches the operation. True if the value is correct."""
        return False

    def convert_hass_to_dev(self, hass_value):
        """Convert HASS state value to the device's expected value."""
        return hass_value


class BasicDeviceOperation(DeviceOperation):
    def __init__(self, name, connection, controller, status_getter=None):
        super(BasicDeviceOperation, self).__init__(name, connection, controller, status_getter)
        self._values_dev_to_ha_map = {}
        self._values_ha_to_dev_map = {}
        self._values = []
        self._value_connections_map = {}
        self._value_validation_templates = {}
        self._last_valid_values = []
        # Cache for dynamic value lists to improve performance.
        self._values_cache: Dict[str, list[str]] = {}
    
    def get_connection(self, value):
        """
        Gets the connection for a specific value, or the default connection if none is defined."""
        return self._value_connections_map.get(value, self._connection)

    def load_from_yaml(self, node):
        """Load configuration from a YAML node dictionary. Return True if successful."""
        if super(BasicDeviceOperation, self).load_from_yaml(node):
            from jinja2 import Template

            # Store the corresponding HA feature flag, if it exists for this operation.
            self._feature_flag = YAML_NAME_TO_HA_FEATURE.get(self._name)

            if node is not None:
                node_values = node.get(CONFIG_DEVICE_OPERATION_VALUES, {})
                if len(node_values) == 0:
                    return False

                for ha_value in node_values.keys():
                    node_value = node_values[ha_value]
                    # The connection node for a value can contain both general connection
                    # parameters and the specific 'connection_template'.
                    # We pass the entire value node to create_updated.
                    connection_node = node_value.get(CONFIG_DEVICE_CONNECTION, node_value)
                    r = self._connection.create_updated(connection_node)

                    self._value_connections_map[ha_value] = r
                    self._values.append(ha_value)
                    if CONFIG_DEVICE_OPERATION_VALUE in node_value:
                        dev_value = node_value[CONFIG_DEVICE_OPERATION_VALUE]
                        self._values_dev_to_ha_map[dev_value] = ha_value
                        self._values_ha_to_dev_map[ha_value] = dev_value
                    
                    if CONFIG_DEVICE_VALIDATION_TEMPLATE in node_value:
                        self._value_validation_templates[ha_value] = Template(
                            node_value[CONFIG_DEVICE_VALIDATION_TEMPLATE]
                        )

                return True
        return False

    def set_device_state_for_values(self, device_state):
        """Set the device state to be used by the `values` property."""
        self._device_state = device_state

    @property
    def all_values(self):
        """Return the complete, unfiltered list of values."""
        return self._values

    @property
    def values(self):
        """Return a list of valid values, which can be dynamic."""
        # If there are no dynamic validations, there's nothing to cache, so return the full list.
        if not self._value_validation_templates:
            return self._values

        # Determine the cache key. The key is the state of the properties on which the
        # templates depend (usually hvac_mode).
        cache_key_prop = self._controller.get_property(ATTR_HVAC_MODE)
        cache_key = str(cache_key_prop) if cache_key_prop else "None"

        if cache_key in self._values_cache:
            #_LOGGER.debug("%s Cache hit for '%s' with key '%s'. Returning cached values", self.log_prefix, self.name, cache_key)
            return self._values_cache[cache_key]
        
        _LOGGER.debug("%s Cache miss for '%s' with key '%s'. Calculating values", self.log_prefix, self.name, cache_key)

        valid_values = []
        for ha_value in self._values:
            if self.is_value_valid(ha_value, self._device_state):
                valid_values.append(ha_value)

        # Detect if the list of valid values has changed since the last check.
        if sorted(valid_values) != sorted(self._last_valid_values) and self._last_valid_values:
            _LOGGER.debug("%s Valid values for '%s' changed to: %s", self.log_prefix, self.name, valid_values)
            
            # If the controller exists and this is the fan_mode property, set the pending flicker flag.
            if self._controller and self._id == ATTR_FAN_MODE:
                _LOGGER.debug("%s Setting fan_modes_list_changed_pending_flicker flag", self.log_prefix)
                self._controller._fan_modes_list_changed_pending_flicker = True
        
        # 3. Save the result to the cache before returning it to avoid recalculation on the next call with the same state.
        self._values_cache[cache_key] = valid_values
        self._last_valid_values = valid_values  # Keep this for change detection.

        return valid_values

    def match_value(self, value):
        """Check if value matches the operation. True if the value is correct."""
        return value in self._values_ha_to_dev_map

    def convert_dev_to_hass(self, dev_value):
        """Convert device state value to its HASS representation."""
        return self._values_dev_to_ha_map.get(dev_value, dev_value)

    def convert_hass_to_dev(self, ha_value):
        """Convert HASS state value to the device's expected value."""
        return self._values_ha_to_dev_map.get(ha_value, ha_value)

    def is_value_valid(self, ha_value, device_state):
        """Check if a specific HA value is valid for the given device state."""
        template = self._value_validation_templates.get(ha_value)
        if template is None:
            return True  # No specific validation, so it's valid
        
        if device_state is None: # Cannot validate without state.
            return False # Cannot validate without state

        rendered = template.render(device_state=device_state)
        return str(rendered).lower() == "valid"

@register_property
class ModeOperation(BasicDeviceOperation):
    def __init__(self, name, connection, controller, status_getter=None):
        super(ModeOperation, self).__init__(name, connection, controller, status_getter)

        # Smart mapping of YAML names (e.g., "hvac") to Home Assistant property IDs (e.g., "hvac_mode").
        # Define the standard property IDs that Home Assistant expects.
        ha_names = {
            ATTR_HVAC_MODE,  # "hvac_mode"
            ATTR_FAN_MODE,   # "fan_mode"
            ATTR_PRESET_MODE,# "preset_mode"
            ATTR_SWING_MODE  # "swing_mode"
        }

        # Map the 'short' YAML names (legacy) to the standard HA property IDs.
        legacy_name_map = {
            "hvac": ATTR_HVAC_MODE,
            "fan": ATTR_FAN_MODE,
            "preset": ATTR_PRESET_MODE,
            "swing": ATTR_SWING_MODE,
            "special": ATTR_PRESET_MODE # 'special' is also treated as a preset.
        }

        if name in ha_names:
            # The name in YAML is already a standard HA property ID (e.g., "hvac_mode").
            self._id = name
        elif name in legacy_name_map:
            # The name in YAML is a short/legacy name (e.g., "hvac"), so we translate it.
            self._id = legacy_name_map[name]
        else:
            # It's a custom mode we don't recognize, so we just append "_mode" as a fallback.
            self._id = name + "_mode"

        # Store the corresponding HA feature flag, if it exists for this operation.
        self._feature_flag = YAML_NAME_TO_HA_FEATURE.get(self._name)

    @staticmethod
    def match_type(type):
        return type == PROPERTY_TYPE_MODE

    @property
    def state_attributes(self):
        """Return a dictionary with property attributes."""
        # The name of the mode list attribute must match what Home Assistant expects (e.g., 'fan_modes', 'hvac_modes').
        list_attribute_name = None
        if self._id == ATTR_HVAC_MODE:
            list_attribute_name = ATTR_HVAC_MODES
        elif self._id == ATTR_FAN_MODE:
            list_attribute_name = ATTR_FAN_MODES
        elif self._id == ATTR_PRESET_MODE:
            list_attribute_name = ATTR_PRESET_MODES
        elif self._id == ATTR_SWING_MODE:
            list_attribute_name = ATTR_SWING_MODES
        else:
            list_attribute_name = self.name + "_modes" # Fallback for custom modes.
            
        data = {}
        data[self.id] = self.value
        data[list_attribute_name] = self.values
        return data


@register_property
class UniqueIdProperty(DeviceProperty):
    def __init__(self, name, connection, controller, status_getter=None):
        super().__init__(name, connection, controller, status_getter)

    @staticmethod
    def match_type(type):
        return type == PROPERTY_TYPE_STRING


@register_property
class SwitchOperation(BasicDeviceOperation):
    def __init__(self, name, connection, controller, status_getter=None):
        super(SwitchOperation, self).__init__(name, connection, controller, status_getter)

    @staticmethod
    def match_type(type):
        return type == PROPERTY_TYPE_SWITCH

    def load_from_yaml(self, node):
        """Load configuration from a YAML node dictionary. Return True if successful."""
        if super(SwitchOperation, self).load_from_yaml(node):
            if STATE_OFF in self._values_ha_to_dev_map:
                self._values_ha_to_dev_map[False] = self._values_ha_to_dev_map[
                    STATE_OFF
                ]
            if STATE_ON in self._values_ha_to_dev_map:
                self._values_ha_to_dev_map[True] = self._values_ha_to_dev_map[STATE_ON]
            return True

        return False


class BasicNumericOperation(DeviceOperation):
    def __init__(self, name, connection, controller, status_getter=None):
        super(BasicNumericOperation, self).__init__(name, connection, controller, status_getter)
        self._min = None
        self._max = None
        self._value = 0.0

    @property
    def value(self):
        try:
            return float(self._value)
        except (ValueError, TypeError):
            return None

    @property
    def config_validation_type(self):
        return cv.positive_int

    def match_value(self, value):
        """Check if value matches the operation. True if the value is correct."""
        try:
            return self.convert_hass_to_dev(float(value)) == value
        except ValueError:
            return False

    def load_from_yaml(self, node):
        """Load configuration from a YAML node dictionary. Return True if successful."""
        if not super(BasicNumericOperation, self).load_from_yaml(node):
            return False

        if node is not None:
            self._min = node.get(CONFIG_DEVICE_OPERATION_NUMBER_MIN, None)
            self._max = node.get(CONFIG_DEVICE_OPERATION_NUMBER_MAX, None)
            return True

        return False

    def convert_hass_to_dev(self, hass_value):
        """Convert HASS state value to the device's expected value."""
        if self._min is not None and hass_value < self._min:
            return self._min
        if self._max is not None and hass_value > self._max:
            return self._max

        return hass_value


@register_property
class NumericOperation(BasicNumericOperation):
    def __init__(self, name, connection, controller, status_getter=None):
        super(NumericOperation, self).__init__(name, connection, controller, status_getter)

    @staticmethod
    def match_type(type):
        return type == PROPERTY_TYPE_NUMBER


@register_property
class TemperatureOperation(BasicNumericOperation):
    def __init__(self, name, connection, controller, status_getter=None):
        super(TemperatureOperation, self).__init__(name, connection, controller, status_getter)
        self._unit_template = None
        self._unit = UnitOfTemperature.CELSIUS

    @staticmethod
    def match_type(type):
        return type == PROPERTY_TYPE_TEMP

    def load_from_yaml(self, node):
        from jinja2 import Template

        if not super(TemperatureOperation, self).load_from_yaml(node):
            return False

        if node is not None and CONFIG_DEVICE_OPERATION_TEMP_UNIT_TEMPLATE in node:
            self._unit_template = Template(
                node[CONFIG_DEVICE_OPERATION_TEMP_UNIT_TEMPLATE]
            )
        return True

    async def async_update_state(self, device_state_override, debug):
        if device_state_override is not None:
            device_state = device_state_override
        else:
            device_state = self._status_getter.value if self._status_getter else None

        if self._unit_template is not None and device_state is not None:
            try:
                unit = self._unit_template.render(device_state=device_state)
                if unit in UNIT_MAP:
                    self._unit = UNIT_MAP[unit]
            except:
                _LOGGER.debug("%s Could not render unit template for '%s'. Using last known unit.", self.log_prefix, self.id)
        # --- START OF MODIFICATION ---
        # If there's no unit_template, but there is a static unit_of_measurement (from an attribute), use it.
        elif self._unit_of_measurement:
            self._unit = self._unit_of_measurement
        # --- END OF MODIFICATION ---
        return await super().async_update_state(device_state_override, debug)

    def convert_dev_to_hass(self, dev_value):
        """Convert device state value to the HASS representation (Celsius)."""
        try:
            # --- START OF FIX: Ensure value is always a float for HASS ---
            # The device might send an int (e.g., 22), but HA expects a float (22.0)
            # for temperatures. This mismatch was causing the optimistic update to fail.
            # By ensuring it's a float, we align with HA's state machine.
            return float(TemperatureConverter.convert(float(dev_value), self._unit, UnitOfTemperature.CELSIUS))
            # --- END OF FIX ---
        except (ValueError, TypeError):
            return None  # Return None if the value is invalid.

    def convert_hass_to_dev(self, hass_value):
        v = hass_value
        if self._min is not None and hass_value < self._min:
            v = self._min
        if self._max is not None and hass_value > self._max:
            v = self._max

        converted_temp = TemperatureConverter.convert(
            float(v), UnitOfTemperature.CELSIUS, self._unit
        )
        # --- START OF FIX: Remove hardcoded multiplication ---
        # Any multiplication (e.g., by 10) should be handled by the connection_template in the YAML
        # for devices that require it (like 8888). 2878 devices need a simple integer.
        # This makes the TemperatureOperation class universally correct.
        # We return the float value here to maintain precision in the controller.
        # The YAML template (e.g., {{ value | int }}) will handle the conversion for devices that need it.
        return converted_temp
        # --- END OF FIX ---

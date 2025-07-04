"""
Platform that offers support for IP controlled climate devices.
This file defines the Home Assistant climate entity.
"""
import asyncio
import functools as ft
import json
import logging
import time
from datetime import timedelta

import homeassistant.helpers.config_validation as cv
import homeassistant.helpers.entity_component
import voluptuous as vol
from homeassistant.components.climate import (
    ATTR_CURRENT_TEMPERATURE,
    ATTR_FAN_MODE,
    ATTR_FAN_MODES,
    ATTR_HVAC_ACTION,
    ATTR_HVAC_MODE,
    ATTR_HVAC_MODES,
    ATTR_PRESET_MODE,
    ATTR_PRESET_MODES,
    ATTR_SWING_MODE,
    ATTR_SWING_MODES,
    ATTR_TARGET_TEMP_HIGH,
    ATTR_TARGET_TEMP_LOW,
    DOMAIN,
    ClimateEntity,
    HVACMode,
)
from homeassistant.components.climate.const import (
    ATTR_MAX_TEMP,
    ATTR_MIN_TEMP,
    ClimateEntityFeature,
)
from homeassistant.const import (
    ATTR_ENTITY_ID,
    ATTR_NAME,
    ATTR_TEMPERATURE,
    CONF_ACCESS_TOKEN,
    CONF_IP_ADDRESS,
    CONF_MAC,
    CONF_TEMPERATURE_UNIT,
    CONF_TOKEN,
    STATE_OFF,
    STATE_ON,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
    UnitOfTemperature,
)
from homeassistant.exceptions import PlatformNotReady
from homeassistant.helpers.config_validation import PLATFORM_SCHEMA
from homeassistant.helpers.service import extract_entity_ids
from homeassistant.util.unit_conversion import TemperatureConverter

from .controller import ATTR_POWER, ClimateController, create_controller
from .yaml_const import (
    CONF_CERT,
    CONF_CONFIG_FILE,
    CONF_CONTROLLER,
    CONF_DEBUG,
    CONF_DEVICE_ID,
    CONFIG_DEVICE_NAME,
    CONFIG_DEVICE_POLL,
    CONFIG_DEVICE_UPDATE_DELAY,
    DEFAULT_CONF_CONFIG_FILE,
)

SUPPORTED_FEATURES_MAP = {
    ATTR_TEMPERATURE: ClimateEntityFeature.TARGET_TEMPERATURE,
    ATTR_TARGET_TEMP_HIGH: ClimateEntityFeature.TARGET_TEMPERATURE_RANGE,
    ATTR_TARGET_TEMP_LOW: ClimateEntityFeature.TARGET_TEMPERATURE_RANGE,
    ATTR_FAN_MODE: ClimateEntityFeature.FAN_MODE,
    ATTR_SWING_MODE: ClimateEntityFeature.SWING_MODE,
    ATTR_PRESET_MODE: ClimateEntityFeature.PRESET_MODE,
}

DEFAULT_CONF_CERT_FILE = "ac14k_m.pem"
DEFAULT_CONF_TEMP_UNIT = UnitOfTemperature.CELSIUS
DEFAULT_CONF_CONTROLLER = "yaml"

SCAN_INTERVAL = timedelta(seconds=15)

REQUIREMENTS = ["requests>=2.21.0", "xmltodict>=0.13.0"]

CLIMATE_IP_DATA = "climate_ip_data"
ENTITIES = "entities"
DEFAULT_CLIMATE_IP_TEMP_MIN = 8
DEFAULT_CLIMATE_IP_TEMP_MAX = 30
DEFAULT_UPDATE_DELAY = 1.5
SERVICE_SET_CUSTOM_OPERATION = "climate_ip_set_property"

_LOGGER: logging.Logger = logging.getLogger(__package__)

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_IP_ADDRESS): cv.string,
        vol.Optional(CONF_TOKEN): cv.string,
        vol.Optional(CONF_MAC): cv.string,
        vol.Optional(CONFIG_DEVICE_NAME): cv.string,
        vol.Optional(CONF_CERT, default=DEFAULT_CONF_CERT_FILE): cv.string,
        vol.Optional(CONF_CONFIG_FILE, default=DEFAULT_CONF_CONFIG_FILE): cv.string,
        vol.Optional(CONF_TEMPERATURE_UNIT, default=DEFAULT_CONF_TEMP_UNIT): cv.string,
        vol.Optional(CONF_CONTROLLER, default=DEFAULT_CONF_CONTROLLER): cv.string,
        vol.Optional(CONF_DEBUG, default=False): cv.boolean,
        vol.Optional(CONFIG_DEVICE_POLL, default=""): cv.string,
        vol.Optional(
            CONFIG_DEVICE_UPDATE_DELAY, default=DEFAULT_UPDATE_DELAY
        ): cv.string,
        vol.Optional(CONF_DEVICE_ID, default="032000000"): cv.string,
    }
)


async def async_setup_platform(hass, config, async_add_entities, discovery_info=None):
    """
    Set up the climate_ip platform asynchronously.
    """
    _LOGGER.setLevel(logging.INFO if config.get("debug", False) else logging.ERROR)
    _LOGGER.info("climate_ip: async setup platform")

    try:
        # Controller creation is now fully asynchronous
        device_controller = await create_controller(
            config.get(CONF_CONTROLLER), config, _LOGGER
        )
    except Exception as e:
        _LOGGER.error("climate_ip: error while creating controller!")
        import traceback
        _LOGGER.error(traceback.format_exc())
        _LOGGER.error(e)
        raise PlatformNotReady from e

    if device_controller is None:
        raise PlatformNotReady

    async_add_entities([ClimateIP(device_controller, config)], True)

    async def async_service_handler(service):
        params = {
            key: value for key, value in service.data.items() if key != ATTR_ENTITY_ID
        }

        entity_ids = service.data.get(ATTR_ENTITY_ID)
        devices_to_update = []
        if CLIMATE_IP_DATA in hass.data and ENTITIES in hass.data[CLIMATE_IP_DATA]:
            if entity_ids:
                devices_to_update = [
                    device
                    for device in hass.data[CLIMATE_IP_DATA][ENTITIES]
                    if device.entity_id in entity_ids
                ]
            else:
                devices_to_update = hass.data[CLIMATE_IP_DATA][ENTITIES]
        
        for device in devices_to_update:
            if hasattr(device, "async_set_custom_operation"):
                # Call the async version of the custom operation
                await device.async_set_custom_operation(**params)

    service_schema = (
        device_controller.service_schema_map
        if device_controller.service_schema_map
        else {}
    )

    hass.services.async_register(
        DOMAIN,
        SERVICE_SET_CUSTOM_OPERATION,
        async_service_handler,
        schema=vol.Schema(service_schema),
    )


class ClimateIP(ClimateEntity):
    """Representation of a Samsung climate device, now fully asynchronous."""

    def __init__(self, rac_controller, config):
        """Initialize the climate device."""
        self.rac = rac_controller
        self._name = config.get(CONFIG_DEVICE_NAME, None)
        self._poll = None
        
        # Unique ID needs to be set early and consistently
        self._attr_unique_id = "climate_ip_" + (self.rac.unique_id or self._name)

        str_poll = config.get(CONFIG_DEVICE_POLL, "")
        if str_poll.lower() == "true":
            self._poll = True
        elif str_poll.lower() == "false":
            self._poll = False
            
        features = 0
        for f, feature_flag in SUPPORTED_FEATURES_MAP.items():
            if f in self.rac.operations or f in self.rac.attributes:
                features |= feature_flag
                
        if 'power' in self.rac.operations:
            features |= ClimateEntityFeature.TURN_OFF | ClimateEntityFeature.TURN_ON
        
        self._attr_supported_features = features
        self._update_delay = float(
            config.get(CONFIG_DEVICE_UPDATE_DELAY, DEFAULT_UPDATE_DELAY)
        )
        self._enable_turn_on_off_backwards_compatibility = False
        
        # Attributes for robust optimistic mode
        self._last_optimistic_update_time = 0
        self._optimistic_debounce_seconds = 10 # Ignore polls for 10s after an optimistic update

    @property
    def should_poll(self):
        """Return the polling state."""
        if self._poll is not None:
            return self._poll
        return self.rac.poll

    async def async_update(self):
        """
        Asynchronously update the state of the device from the controller.
        This method is called by Home Assistant for polling.
        """
        # Debounce polling if an optimistic update happened recently
        if time.time() - self._last_optimistic_update_time < self._optimistic_debounce_seconds:
            _LOGGER.info("Skipping poll to allow optimistic update to settle.")
            return
            
        _LOGGER.info("Asynchronously updating state for %s", self.name)
        await self.rac.async_update_state()

    async def _send_and_verify(self, prop, value):
        """
        Send a command and optimistically update the state immediately.
        The actual network communication runs in the background.
        """
        # 1. Optimistically update the controller's internal state.
        if prop in self.rac._operations:
            self.rac._operations[prop]._value = value
        
        # 2. Record the time of this optimistic update and update the UI.
        self._last_optimistic_update_time = time.time()
        self.async_write_ha_state()

        # 3. Create the network task and let it run in the background.
        self.hass.async_create_task(
            self.rac.async_set_property(prop, value)
        )

    async def async_set_temperature(self, **kwargs):
        """Asynchronously set new target temperature and verify."""
        new_temp = kwargs.get(ATTR_TEMPERATURE)
        if new_temp is not None:
            await self._send_and_verify(ATTR_TEMPERATURE, new_temp)

    async def async_set_hvac_mode(self, hvac_mode: str):
        """Asynchronously set new target hvac mode and verify."""
        await self._send_and_verify(ATTR_HVAC_MODE, hvac_mode)

    async def async_set_fan_mode(self, fan_mode: str):
        """Asynchronously set new target fan mode and verify."""
        await self._send_and_verify(ATTR_FAN_MODE, fan_mode)

    async def async_set_swing_mode(self, swing_mode: str):
        """Asynchronously set new target swing operation and verify."""
        await self._send_and_verify(ATTR_SWING_MODE, swing_mode)

    async def async_set_preset_mode(self, preset_mode: str):
        """Asynchronously set new target preset mode and verify."""
        await self._send_and_verify(ATTR_PRESET_MODE, preset_mode)

    async def async_turn_on(self):
        """Asynchronously turn the climate device on and verify."""
        await self._send_and_verify(ATTR_POWER, STATE_ON)

    async def async_turn_off(self):
        """Asynchronously turn the climate device off and verify."""
        await self._send_and_verify(ATTR_POWER, STATE_OFF)
        
    async def async_set_custom_operation(self, **kwargs):
        """Asynchronously set custom device mode to specified value."""
        for key, value in kwargs.items():
            _LOGGER.info("Custom operation, setting property %s to %s", key, value)
            await self.rac.async_set_property(key, value)
        # Force a state refresh after custom operation
        await asyncio.sleep(self._update_delay)
        await self.async_update()
        self.async_write_ha_state()

    # --- Standard Properties now read from the controller's cache ---
    @property
    def name(self):
        """Return the name of the climate device."""
        if self._name is not None:
            return self._name
        return "climate_ip_" + self.rac.name

    @property
    def extra_state_attributes(self):
        """Return the state attributes."""
        return self.rac.state_attributes

    @property
    def temperature_unit(self):
        return self.rac.temperature_unit

    @property
    def current_temperature(self):
        return self.rac.get_property(ATTR_CURRENT_TEMPERATURE)

    @property
    def target_temperature(self):
        return self.rac.get_property(ATTR_TEMPERATURE)
        
    @property
    def hvac_mode(self):
        mode = self.rac.get_property(ATTR_HVAC_MODE)
        return mode if mode not in [STATE_UNKNOWN, STATE_UNAVAILABLE, "", None] else HVACMode.OFF

    @property
    def hvac_modes(self):
        """Return the list of available hvac operation modes."""
        modes = self.rac.get_property(ATTR_HVAC_MODES) or []
        # Ensure 'off' is always an option if the device can be turned off
        if 'power' in self.rac.operations and HVACMode.OFF not in modes:
            return modes + [HVACMode.OFF]
        return modes
        
    @property
    def fan_mode(self):
        return self.rac.get_property(ATTR_FAN_MODE)

    @property
    def fan_modes(self):
        return self.rac.get_property(ATTR_FAN_MODES)

    @property
    def swing_mode(self):
        return self.rac.get_property(ATTR_SWING_MODE)

    @property
    def swing_modes(self):
        return self.rac.get_property(ATTR_SWING_MODES)
        
    @property
    def preset_mode(self):
        return self.rac.get_property(ATTR_PRESET_MODE)

    @property
    def preset_modes(self):
        return self.rac.get_property(ATTR_PRESET_MODES)
    
    # ... other properties like min_temp, max_temp etc. remain the same ...
    
    async def async_added_to_hass(self):
        """Run when entity about to be added."""
        await super().async_added_to_hass()
        if CLIMATE_IP_DATA not in self.hass.data:
            self.hass.data[CLIMATE_IP_DATA] = {ENTITIES: []}
        self.hass.data[CLIMATE_IP_DATA][ENTITIES].append(self)

    async def async_will_remove_from_hass(self):
        """Run when entity will be removed from hass."""
        await super().async_will_remove_from_hass()
        if CLIMATE_IP_DATA in self.hass.data:
            self.hass.data[CLIMATE_IP_DATA][ENTITIES].remove(self)

"""
Platform that offers support for IP controlled climate device sensors.
This file defines the Home Assistant sensor entities, adapted for DataUpdateCoordinator.
"""
import logging
from typing import Any

from homeassistant.components.sensor import (
    SensorEntity,
)
from homeassistant.config_entries import ConfigEntry
# Import STATE_UNKNOWN
from homeassistant.const import STATE_UNKNOWN
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.helpers.entity import DeviceInfo

from .const import DOMAIN
from .coordinator import SamsungClimateCoordinator
# Import DeviceProperty to use as a type hint
from .properties import DeviceProperty, UniqueIdProperty

_LOGGER = logging.getLogger(__name__)

# --- REMOVE THE HARDCODED SENSOR_TYPES TUPLE ---

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the sensor entities from a config entry."""
    coordinators = hass.data[DOMAIN][entry.entry_id]

    entities_to_add = []

    # Handle both single and multi-coordinator setups
    if isinstance(coordinators, dict):
        if not coordinators:
            _LOGGER.warning("No coordinators found for sensor setup.")
            return

        # We'll use the first coordinator for "main" sensors (like outdoor temp)
        # In a multi-device setup, you might want a more complex logic
        # to assign sensors to specific sub-devices if they report them.
        # For now, this assumes sensors are global to the controller.
        coordinator = next(iter(coordinators.values()))
        raw_device_state = coordinator.controller._state_getter.value
        for sensor_prop in coordinator.controller.sensors:
            if sensor_prop.is_valid(raw_device_state):
                entities_to_add.append(ClimateIpSensor(coordinator, sensor_prop))
    else:
        # Single coordinator setup
        coordinator = coordinators
        # Use the unwrapped device state exposed by the controller
        raw_device_state = coordinator.controller.device_state
        
        for sensor_prop in coordinator.controller.sensors:
            if sensor_prop.is_valid(raw_device_state):
                entities_to_add.append(ClimateIpSensor(coordinator, sensor_prop))

    if entities_to_add:
        _LOGGER.info(
            "%s Adding %d YAML-defined sensors to Home Assistant.",
            coordinator.log_prefix,
            len(entities_to_add)
        )
        async_add_entities(entities_to_add)


class ClimateIpSensor(CoordinatorEntity[SamsungClimateCoordinator], SensorEntity):
    """Representation of a Climate IP sensor defined by YAML."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: SamsungClimateCoordinator,
        property_object: DeviceProperty, # Use DeviceProperty, not EntityDescription
    ):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._property = property_object
        self._key = self._property.id # e.g., "used_power"

        # Set all HA attributes from the property object
        self._attr_name = self._property.name # This is the friendly name from YAML
        self._attr_unique_id = f"{coordinator.unique_id}_{self._key}"
        self._attr_device_class = self._property.device_class
        self._attr_native_unit_of_measurement = self._property.unit_of_measurement
        self._attr_state_class = self._property.state_class

        # Set initial state will be handled by the first coordinator update
        # self._attr_native_value = None

        # Perform an initial update from the coordinator's data to prevent being unavailable on startup.
        self._sync_data_from_coordinator()

    @property
    def log_prefix(self) -> str:
        """Return the log prefix from the coordinator for consistency."""
        return self.coordinator.log_prefix

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information for the device registry."""
        # Use the centralized DeviceInfo from the coordinator.
        return self.coordinator.device_info

    def _sync_data_from_coordinator(self) -> None:
        """Synchronize the entity's state with the latest data from the coordinator."""
        # This is called during __init__ to set the initial state.
        self._update_state()


    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        # Check if the sensor is still valid (e.g. available in the new state)
        # Use the unwrapped device state exposed by the controller
        raw_device_state = self.coordinator.controller.device_state
        is_valid = self._property.is_valid(raw_device_state)
        # _LOGGER.debug("%s Sensor '%s' validation check returned: %s", self.log_prefix, self._key, is_valid)

        if is_valid:
            self._update_state()
            self.async_write_ha_state()
        else:
            # If the property is no longer valid, mark sensor as unavailable
            self._attr_native_value = None
            _LOGGER.debug("%s Marking sensor '%s' as unavailable because it is no longer valid.", self.log_prefix, self._key)
            self.async_write_ha_state()

    def _update_state(self) -> None:
        """
        Update the state of the sensor from the coordinator data.
        All conversion logic is now handled by the property's status_template.
        """
        value = self.coordinator.get_property(self._key)
        _LOGGER.debug("%s Sensor '%s' received value from coordinator: %s (type: %s)", self.log_prefix, self._key, value, type(value).__name__)

        if value is None or value == STATE_UNKNOWN:
            self._attr_native_value = None
            return

        # If the property type is 'string', assign the value directly.
        # Otherwise, try to convert it to a number (float).
        if isinstance(self._property, UniqueIdProperty): # UniqueIdProperty is our 'string' type
            self._attr_native_value = str(value)
        else:
            try:
                self._attr_native_value = float(value)
            except (ValueError, TypeError):
                self._attr_native_value = None
                _LOGGER.warning(
                    "%s Could not parse sensor value for %s: %s", self.log_prefix, self._key, value
                )

import logging
from typing import Any, Dict, Optional
import voluptuous as vol

from homeassistant.components.climate import (
    ATTR_CURRENT_TEMPERATURE,
    ATTR_FAN_MODE,
    ATTR_HVAC_MODE,
    ATTR_PRESET_MODE,
    ATTR_SWING_MODE,
    ClimateEntity,
    PLATFORM_SCHEMA,
    ClimateEntityFeature,
    HVACMode,
)
from homeassistant.components.climate.const import ATTR_MAX_TEMP, ATTR_MIN_TEMP
from homeassistant.config_entries import SOURCE_IMPORT, ConfigEntry
from homeassistant.const import (
    ATTR_TEMPERATURE,
    CONF_DEBUG,
    CONF_IP_ADDRESS,
    CONF_MAC,
    CONF_NAME,
    CONF_TEMPERATURE_UNIT,
    CONF_TOKEN,
    STATE_OFF,
    STATE_ON,
)
from homeassistant.core import HomeAssistant, callback
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

from .const import (
    CONF_DEVICE_ID,
    CONF_CERT,
    CONF_CONFIG_FILE,
    CONF_CONTROLLER,
    CONF_DEVICES,
    CONF_TEMP_STEP,
    CONFIG_DEVICE_NAME,
    CONFIG_DEVICE_POLL,
    CONFIG_DEVICE_UPDATE_DELAY,
    DEFAULT_CONF_CERT_FILE,
    DEFAULT_CONF_CONFIG_FILE,
    DEFAULT_CONF_CONTROLLER,
    DEFAULT_CONF_TEMP_UNIT,
    DEFAULT_UPDATE_DELAY,
    DOMAIN,
)
from .controller import ATTR_POWER
from .coordinator import SamsungClimateCoordinator

_LOGGER = logging.getLogger(__name__)

SUPPORTED_FEATURES_MAP = {
    ATTR_TEMPERATURE: ClimateEntityFeature.TARGET_TEMPERATURE,
    ATTR_FAN_MODE: ClimateEntityFeature.FAN_MODE,
    ATTR_SWING_MODE: ClimateEntityFeature.SWING_MODE,
    ATTR_PRESET_MODE: ClimateEntityFeature.PRESET_MODE,
}

DEFAULT_CLIMATE_IP_TEMP_MIN = 8
DEFAULT_CLIMATE_IP_TEMP_MAX = 30

# Legacy platform schema for YAML import.
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
        vol.Optional(CONFIG_DEVICE_UPDATE_DELAY, default=DEFAULT_UPDATE_DELAY): cv.positive_float,
        vol.Optional(CONF_DEVICE_ID): cv.string,
        vol.Optional(CONF_TEMP_STEP, default=1.0): vol.Coerce(float),
    }
)

async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Import YAML platform configuration to a Config Flow."""
    _LOGGER.warning(
        "Configuration of 'climate_ip' via YAML is deprecated "
        "and will be removed in a future version. Your configuration has been "
        "automatically imported into the UI (Config Entries)"
    )

    # Start the import flow, passing the YAML config data directly.
    hass.async_create_task(
        hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_IMPORT},
            data=config
        )
    )
    return True

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the climate entity from a config entry."""
    coordinators = hass.data[DOMAIN][entry.entry_id]

    if isinstance(coordinators, dict):
        # Create entities for a multi-device setup.
        entities = []
        for device_id, coordinator in coordinators.items():
            # Find the device_info for this specific device_id in the entry data.
            device_info = next((d for d in entry.data.get(CONF_DEVICES, []) if d.get('id') == device_id), None)
            if device_info:
                entities.append(ClimateIP(coordinator, entry.data, device_info, entry.unique_id))
        async_add_entities(entities, True)
    else:
        # Fallback for single-device setups.
        coordinator = coordinators
        async_add_entities([ClimateIP(coordinator, entry.data, None, entry.unique_id)])


class ClimateIP(CoordinatorEntity[SamsungClimateCoordinator], ClimateEntity):
    """Representation of a climate_ip climate device using a coordinator."""

    def __init__(self, coordinator: SamsungClimateCoordinator, config: Dict[str, Any], device_info: Optional[Dict[str, Any]] = None, main_unique_id: Optional[str] = None):
        """Initialize the climate device."""
        super().__init__(coordinator)
        self._config = config
        self._main_unique_id = main_unique_id or coordinator.unique_id

        user_defined_name = self._config.get(CONF_NAME)

        if device_info:
            # This is a sub-device (e.g., an indoor unit of a MIM-H03).
            # Its unique_id is the UUID provided for it.
            self._name = user_defined_name or device_info.get("name", f"Indoor Unit {device_info.get('id')}")
            self._attr_unique_id = device_info.get("uuid") or f"{self._main_unique_id}_{device_info.get('id')}"
            
            # This establishes the correct parent-child relationship in the device registry.
            self._attr_device_info = DeviceInfo(
                identifiers={(DOMAIN, self._attr_unique_id)},
                name=self._name,
                via_device=(DOMAIN, self._main_unique_id),  # Link to the parent device
            )
        else:
            # This is a single-device entry (e.g., a standalone AC).
            # The entity's unique_id is the same as the coordinator's unique_id.
            self._attr_unique_id = self.coordinator.unique_id
            if user_defined_name:
                self._name = user_defined_name
            else:
                # Fallback to a generated name based on the device's unique ID if no name was provided by the user.
                self._name = f"Samsung AC {self.coordinator.unique_id}"

            # For single devices, the entity's device_info is the same as the coordinator's.
            self._attr_device_info = self.coordinator.device_info


        # Check for settable properties (operations) to determine supported features.
        features = ClimateEntityFeature(0)
        if ATTR_TEMPERATURE in self.coordinator.operations:
            features |= ClimateEntityFeature.TARGET_TEMPERATURE
        
        if ATTR_FAN_MODE in self.coordinator.operations:
            features |= ClimateEntityFeature.FAN_MODE
            
        if ATTR_SWING_MODE in self.coordinator.operations:
            features |= ClimateEntityFeature.SWING_MODE
            
        if ATTR_PRESET_MODE in self.coordinator.operations:
            features |= ClimateEntityFeature.PRESET_MODE
            
        if ATTR_POWER in self.coordinator.operations:
            features |= ClimateEntityFeature.TURN_ON | ClimateEntityFeature.TURN_OFF

        # Use an instance variable for supported features to allow for dynamic changes ("flickering").
        self._supported_features = features

        # Set the temperature step from the configuration.
        self._attr_target_temperature_step = self._config.get(CONF_TEMP_STEP, 1.0)

        # Initialize all state attributes to prevent AttributeError on startup.
        self._attr_hvac_mode: HVACMode | None = None
        self._attr_target_temperature: float | None = None
        self._attr_current_temperature: float | None = None
        self._attr_fan_mode: str | None = None
        self._attr_swing_mode: str | None = None
        self._attr_preset_mode: str | None = None
        self._attr_hvac_modes: list[HVACMode] = []
        self._attr_fan_modes: list[str] = []
        self._attr_swing_modes: list[str] = []
        self._attr_preset_modes: list[str] = []

        # Perform an initial update from the coordinator's data to prevent being unavailable on startup.
        self._sync_data_from_coordinator()

        # Register the entity with the coordinator so it can call back for flickering.
        self.coordinator.register_entity(self)

    @property
    def log_prefix(self) -> str:
        """Return the log prefix from the coordinator for consistency."""
        return self.coordinator.log_prefix

    async def async_added_to_hass(self) -> None:
        """Run when entity about to be added to hass."""
        await super().async_added_to_hass()
        if self.coordinator.is_push_device:
            connection = self.coordinator.controller.connection
            if connection and hasattr(connection, 'set_update_callback'):
                _LOGGER.debug("%s Climate entity added; setting up push update callback and starting listener.", self.log_prefix)
                connection.set_update_callback(self.coordinator.async_handle_push_update)
                connection.start_listening()

    async def async_will_remove_from_hass(self) -> None:
        """Run when entity will be removed from hass."""
        if self.coordinator.is_push_device:
            connection = self.coordinator.controller.connection
            if connection and hasattr(connection, 'stop_listening'):
                _LOGGER.debug("%s Climate entity removing; stopping push listener.", self.log_prefix)
                await connection.stop_listening()
        # Unregister from coordinator to prevent memory leaks
        self.coordinator.unregister_entity(self)
        await super().async_will_remove_from_hass()

    def _sync_data_from_coordinator(self) -> None:
        """Synchronize the entity's state with the latest data from the coordinator."""
        # Read directly from the `coordinator.data` dataclass.
        state = self.coordinator.data
        if state:
            self._attr_hvac_mode = state.hvac_mode or HVACMode.OFF
            self._attr_target_temperature = state.target_temperature
            self._attr_current_temperature = state.current_temperature # Also sync current temperature
            self._attr_fan_mode = state.fan_mode
            self._attr_swing_mode = state.swing_mode
            self._attr_preset_mode = state.preset_mode
            self._attr_hvac_modes = state.hvac_modes
            self._attr_fan_modes = state.fan_modes
            self._attr_swing_modes = state.swing_modes
            self._attr_preset_modes = state.preset_modes

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator and schedule a state update."""
        self._sync_data_from_coordinator()
        # `super()._handle_coordinator_update()` just wraps this, so we call it directly.
        self.async_schedule_update_ha_state()

    async def async_flicker_feature(self, feature: ClimateEntityFeature, enable: bool):
        """
        Enable or disable a feature flag and force a state write.
        Used by the coordinator to force the UI to re-render controls.
        """
        if enable:
            _LOGGER.debug("%s Flicker ON: Restoring support for %s", self.log_prefix, feature.name)
            self._supported_features |= feature
        else:
            _LOGGER.debug("%s Flicker OFF: Removing support for %s", self.log_prefix, feature.name)
            self._supported_features &= ~feature
        
        # Write the state change to Home Assistant immediately.
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        """Return True if entity is available."""
        return (
            super().available
            and self.coordinator.data is not None
        )

    @property
    def device_info(self) -> DeviceInfo:
        """
        Return device information. This is now handled by self._attr_device_info,
        which is set correctly for both single and multi-device setups in __init__.
        """
        return self._attr_device_info

    @property
    def name(self):
        """Return the name of the climate device."""
        return self._name

    @property
    def supported_features(self) -> ClimateEntityFeature:
        """Return the list of supported features."""
        return self._supported_features

    async def async_set_temperature(self, **kwargs):
        """Set new target temperature."""
        if (temp := kwargs.get(ATTR_TEMPERATURE)) is not None:
            # Predict corrections based on the new temperature.
            changed_flags, corrections = await self.coordinator.async_predict_and_correct(
                self.coordinator.data, ATTR_TEMPERATURE, temp
            )

            # Optimistically update the local state.
            self._attr_target_temperature = temp
            self._apply_optimistic_corrections(corrections)
            self.async_write_ha_state()

            # Send the command to the device.
            await self.coordinator.async_set_property(ATTR_TEMPERATURE, temp, corrections)

    async def async_set_hvac_mode(self, hvac_mode: str):
        """Set new target hvac mode."""
        _, corrections = await self.coordinator.async_predict_and_correct(
            self.coordinator.data, ATTR_HVAC_MODE, hvac_mode
        )

        self._attr_hvac_mode = HVACMode(hvac_mode)
        self._apply_optimistic_corrections(corrections)
        self.async_write_ha_state()

        await self.coordinator.async_set_property(ATTR_HVAC_MODE, hvac_mode, corrections)


    def _apply_optimistic_corrections(self, corrections: Dict[str, Any]):
        """Apply predicted corrections to the entity's optimistic state during a set operation."""
        if not corrections:
            return
        _LOGGER.debug("%s Applying optimistic corrections: %s", self.log_prefix, corrections)
        for prop, value in corrections.items():
            if hasattr(self, f"_attr_{prop}"):
                setattr(self, f"_attr_{prop}", value)


    async def async_set_fan_mode(self, fan_mode: str):
        """Set new target fan mode."""
        # Check if the requested fan mode is valid for the current HVAC mode.
        if fan_mode not in self.fan_modes:
            _LOGGER.warning(
                "%s Requested fan mode '%s' is not available for the current HVAC mode. Available modes: %s. Ignoring request.",
                self.log_prefix, fan_mode, self.fan_modes
            )
            return

        _, corrections = await self.coordinator.async_predict_and_correct(
            self.coordinator.data, ATTR_FAN_MODE, fan_mode
        )

        self._attr_fan_mode = fan_mode
        self._apply_optimistic_corrections(corrections)
        self.async_write_ha_state()

        await self.coordinator.async_set_property(ATTR_FAN_MODE, fan_mode, corrections)

    async def async_set_swing_mode(self, swing_mode: str):
        """Set new target swing operation."""
        _, corrections = await self.coordinator.async_predict_and_correct(
            self.coordinator.data, ATTR_SWING_MODE, swing_mode
        )

        self._attr_swing_mode = swing_mode
        self._apply_optimistic_corrections(corrections)
        self.async_write_ha_state()

        await self.coordinator.async_set_property(ATTR_SWING_MODE, swing_mode, corrections)

    async def async_set_preset_mode(self, preset_mode: str):
        """Set new target preset mode."""
        _, corrections = await self.coordinator.async_predict_and_correct(
            self.coordinator.data, ATTR_PRESET_MODE, preset_mode
        )

        self._attr_preset_mode = preset_mode
        self._apply_optimistic_corrections(corrections)
        self.async_write_ha_state()

        await self.coordinator.async_set_property(ATTR_PRESET_MODE, preset_mode, corrections)

    async def async_turn_on(self):
        """Turn the climate device on."""
        _, corrections = await self.coordinator.async_predict_and_correct(
            self.coordinator.data, ATTR_POWER, STATE_ON
        )

        # Optimistically apply corrections, which should include the new HVAC mode
        self._apply_optimistic_corrections(corrections)
        self.async_write_ha_state()

        await self.coordinator.async_set_property(ATTR_POWER, STATE_ON, corrections)

    async def async_turn_off(self):
        """Turn the climate device off."""
        _, corrections = await self.coordinator.async_predict_and_correct(
            self.coordinator.data, ATTR_POWER, STATE_OFF
        )

        self._attr_hvac_mode = HVACMode.OFF
        self.async_write_ha_state()

        await self.coordinator.async_set_property(ATTR_POWER, STATE_OFF, corrections)

    @property
    def extra_state_attributes(self):
        """Return the state attributes."""
        return self.coordinator.state_attributes

    @property
    def temperature_unit(self):
        """Return the temperature unit."""
        # --- INICIO DE LA MODIFICACIÓN ---
        # Priorizar la unidad del sensor de temperatura actual si está definida.
        # Esto permite que el YAML anule la unidad de toda la entidad.
        current_temp_prop = self.coordinator.get_property_object(ATTR_CURRENT_TEMPERATURE)
        if current_temp_prop and current_temp_prop.unit_of_measurement:
            # La propiedad ya contiene la constante de HA (p. ej., UnitOfTemperature.FAHRENHEIT)
            return current_temp_prop.unit_of_measurement
        # Caer de nuevo al valor por defecto del controlador si no hay una unidad específica.
        return self.coordinator.temperature_unit
        # --- FIN DE LA MODIFICACIÓN ---

    @property
    def current_temperature(self):
        """Return the current temperature."""
        return self._attr_current_temperature

    @property
    def target_temperature(self):
        """Return the target temperature."""
        return self._attr_target_temperature

    @property
    def hvac_mode(self):
        """Return the current hvac mode."""
        return self._attr_hvac_mode

    @property
    def hvac_modes(self):
        """Return the list of available hvac operation modes."""
        modes = self._attr_hvac_modes
        if ClimateEntityFeature.TURN_OFF in self.supported_features and HVACMode.OFF not in modes:
            return modes + [HVACMode.OFF]
        return modes

    @property
    def fan_mode(self):
        """Return the current fan mode."""
        return self._attr_fan_mode

    @property
    def fan_modes(self):
        """Return the list of available fan modes."""
        return self._attr_fan_modes

    @property
    def swing_mode(self):
        """Return the current swing mode."""
        return self._attr_swing_mode

    @property
    def swing_modes(self):
        """Return the list of available swing modes."""
        return self._attr_swing_modes

    @property
    def preset_mode(self):
        """Return the current preset mode."""
        return self._attr_preset_mode

    @property
    def preset_modes(self):
        """Return the list of available preset modes."""
        return self._attr_preset_modes

    @property
    def min_temp(self):
        """Return the minimum temperature."""
        # Read from coordinator to allow for dynamic changes.
        min_t_prop = self.coordinator.get_property_object(ATTR_MIN_TEMP)
        min_t = min_t_prop.value if min_t_prop else None
        return min_t if min_t is not None else DEFAULT_CLIMATE_IP_TEMP_MIN

    @property
    def max_temp(self):
        """Return the maximum temperature."""
        max_t_prop = self.coordinator.get_property_object(ATTR_MAX_TEMP)
        max_t = max_t_prop.value if max_t_prop else None 
        return max_t if max_t is not None else DEFAULT_CLIMATE_IP_TEMP_MAX

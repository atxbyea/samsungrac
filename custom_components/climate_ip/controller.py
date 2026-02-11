"""Base class for a climate device controller."""
import logging
from typing import Any, Dict, List, Optional, Type

from homeassistant.const import UnitOfTemperature

ATTR_POWER = "power"

CLIMATE_CONTROLLERS: List[Type["ClimateController"]] = []


class ClimateController:
    """Abstract base class for a device controller."""
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """Initialize the controller."""
        self._logger = logger
        self._connection = None
        self.discovered_devices: Optional[List[Dict[str, Any]]] = None

    @staticmethod
    def match_type(controller_type: str) -> bool:
        """Check if this controller class matches the given type string."""
        # This is a class method that subclasses must override.
        return False

    async def initialize(self) -> bool:
        """
        Perform asynchronous initialization for the controller.

        Returns:
            True if initialization was successful, False otherwise.
        """
        return False
        
    async def async_get_status(self) -> Optional[Dict[str, Any]]:
        """
        Get the current status of the device for the DataUpdateCoordinator.
        """
        self._logger.warning("async_get_status not implemented for this controller")
        return None

    @property
    def connection(self) -> Optional[Any]:
        """Return the connection object for the controller."""
        return self._connection

    @property
    def is_push_device(self) -> bool:
        """Return True if the device uses push-based updates."""
        if not self._connection:
            return False
        return (
            hasattr(self._connection, "start_listening")
            and hasattr(self._connection, "stop_listening")
            and hasattr(self._connection, "set_update_callback")
        )

    @property
    def available(self) -> bool:
        """Return True if the controller is connected and available."""
        # Subclasses should override this to reflect the actual connection state
        return True

    @property
    def poll(self) -> Optional[bool]:
        """Return the polling state of the controller."""
        return None

    @property
    def id(self) -> Optional[str]:
        """Return the unique id of the controller."""
        return None
        
    @property
    def unique_id(self) -> Optional[str]:
        """Return the unique id of the controller."""
        # Subclasses are expected to override this
        return None

    @property
    def device_id(self) -> Optional[str]:
        """Return the device id of the controller."""
        # Subclasses are expected to override this
        return None

    @property
    def log_prefix(self) -> str:
        """Return a short identifier for logging purposes."""
        unique_id = self.unique_id
        if unique_id and len(unique_id) >= 6: # Use last 6 chars for a short prefix
            return f"[{unique_id[-6:]}]"
        return f"[{self.name or 'NO_ID'}]"

    @property
    def name(self) -> Optional[str]:
        """Return the name of the controller."""
        return None

    @property
    def debug(self) -> bool:
        """Return the debug state of the controller."""
        return False

    async def update_state(self) -> bool:
        """Asynchronously update the state of the controller from the device."""
        return False

    async def async_set_property(self, property_name: str, new_value: Any) -> bool:
        """Asynchronously set the value of a property on the device."""
        return False
        
    async def async_refresh_from_connection(self):
        """Refresh the controller's properties from the connection's internal state."""
        pass

    def get_property(self, property_name: str) -> Any:
        """Return the value of a property."""
        return None

    @property
    def state_attributes(self) -> Dict[str, Any]:
        """Return the state attributes of the device."""
        raise NotImplementedError()

    @property
    def temperature_unit(self) -> str:
        """Return the temperature unit of the controller."""
        return UnitOfTemperature.CELSIUS

    @property
    def service_schema_map(self) -> Optional[Dict]:
        """Return the service schema map for custom services."""
        return None

    @property
    def operations(self) -> List[str]:
        """Return a list of available operations (settable properties)."""
        return []

    @property
    def attributes(self) -> List[str]:
        """Return a list of available attributes (read-only properties)."""
        return []


def register_controller(controller: Type["ClimateController"]):
    """A decorator to register a controller class."""
    CLIMATE_CONTROLLERS.append(controller)
    return controller


async def create_controller(
    controller_type: str, config: Dict[str, Any], logger: logging.Logger
) -> Optional[ClimateController]:
    """Factory function to create a controller instance based on its type."""
    for controller_class in CLIMATE_CONTROLLERS:
        if controller_class.match_type(controller_type):
            try:
                controller = controller_class(config, logger)
                if await controller.initialize():
                    return controller
                logger.error(
                    "Failed to initialize controller for type %s", controller_type
                )
            except Exception as e:
                logger.error(
                    "climate_ip: Error while creating controller for type %s: %s",
                    controller_type,
                    e,
                    exc_info=True,
                )
    
    logger.error("Controller for type %s not found", controller_type)
    return None

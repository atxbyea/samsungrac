"""Diagnostics support for climate_ip."""
from __future__ import annotations

from dataclasses import asdict
from typing import Any, Mapping

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .helpers import mask_sensitive_data
from .const import DOMAIN

async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    # Move imports inside the function to prevent blocking calls during startup.
    from .coordinator import SamsungClimateCoordinator


    # The data stored could be a single coordinator or a dict of coordinators
    entry_data = hass.data[DOMAIN][entry.entry_id]

    # Filter the entry data to only include relevant information for this integration.
    filtered_entry_data = {
        "data": dict(entry.data),
        "options": entry.options,
        "unique_id": entry.unique_id,
    }
    diagnostics_data = {"entry": filtered_entry_data}

    if isinstance(entry_data, SamsungClimateCoordinator):
        # Handle single device entry
        if entry_data.data:
            diagnostics_data["coordinator_data"] = asdict(entry_data.data)
        diagnostics_data["controller_state"] = entry_data.controller.state_attributes
        diagnostics_data["last_poll_response"] = entry_data.controller._state_getter.value
    elif isinstance(entry_data, dict):
        # Handle multi-device entry
        diagnostics_data["coordinators"] = {}
        for device_id, coordinator in entry_data.items():
            if isinstance(coordinator, SamsungClimateCoordinator):                
                coordinator_diag = {
                    "controller_state": coordinator.controller.state_attributes,
                    "last_poll_response": coordinator.controller._state_getter.value,
                }
                if coordinator.data:
                    coordinator_diag["coordinator_data"] = asdict(coordinator.data)
                diagnostics_data["coordinators"][device_id] = coordinator_diag

    return mask_sensitive_data(diagnostics_data)

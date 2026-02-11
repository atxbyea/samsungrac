"""Dataclass for representing the state of a climate device."""
from dataclasses import dataclass, field
from typing import Optional

from homeassistant.components.climate import HVACMode


@dataclass
class ClimateIPDeviceState:
    """
    Dataclass to represent the state of the climate device.
    This provides strong typing for coordinator.data and helps optimize state updates.
    """
    # Main state fields
    hvac_mode: Optional[HVACMode] = None
    target_temperature: Optional[float] = None
    current_temperature: Optional[float] = None
    fan_mode: Optional[str] = None
    swing_mode: Optional[str] = None
    preset_mode: Optional[str] = None

    # Available mode lists (can be dynamic)
    hvac_modes: list[HVACMode] = field(default_factory=list)
    fan_modes: list[str] = field(default_factory=list)
    swing_modes: list[str] = field(default_factory=list)
    preset_modes: list[str] = field(default_factory=list)
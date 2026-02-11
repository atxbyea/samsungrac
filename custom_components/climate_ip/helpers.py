import logging
import copy
import re
from typing import Any

_LOGGER = logging.getLogger(__name__)

def find_key_in_data(data, key):
    """
    Recursively search for a key in a dictionary or a list of dictionaries.
    """
    _LOGGER.debug(f"find_key_in_data: Searching for '{key}' in data type: {type(data)}")

    if isinstance(data, dict):
        _LOGGER.debug(f"find_key_in_data: Current dict keys: {data.keys()}")
        if key in data:
            _LOGGER.debug(f"find_key_in_data: Found '{key}' in current dict: {data[key]}")
            return data[key]
        for k, v in data.items():
            _LOGGER.debug(f"find_key_in_data: Recursing into dict item '{k}'")
            item = find_key_in_data(v, key)
            if item is not None:
                return item
    elif isinstance(data, list):
        _LOGGER.debug(f"find_key_in_data: Recursing into list with {len(data)} items.")
        for i, item_in_list in enumerate(data):
            _LOGGER.debug(f"find_key_in_data: Recursing into list item {i}")
            item = find_key_in_data(item_in_list, key)
            if item is not None:
                return item
    _LOGGER.debug(f"find_key_in_data: '{key}' not found in current data structure.")
    return None

def get_value_by_path(data: dict, path: list) -> any:
    """
    Navigate through a nested dictionary using a list of keys.
    Returns the found value or None if the path does not exist.
    """
    if not data or not path:
        return None
    current = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current

def stream_wrapper(data: str, token: str | None, ip_address: str | None, device_id: str | None) -> str:
    """
    Replaces placeholder values in a string.
    """
    if token is not None:
        data = data.replace("__CLIMATE_IP_TOKEN__", token)
    if ip_address is not None:
        data = data.replace("__CLIMATE_IP_HOST__", ip_address)
    if device_id is not None:
        data = data.replace("__DEVICE_ID__", str(device_id))
    return data

def mask_sensitive_data(data: Any) -> Any:
    """
    Recursively mask sensitive data in a dictionary or list.
    Handles: uuid, Authorization, token, mac, device_id, unique_id, DeviceToken, DUID.
    """
    SENSITIVE_KEYS = ["uuid", "Authorization", "token", "mac", "device_id", "unique_id", "DeviceToken", "DUID"]
    
    if isinstance(data, dict):
        masked = data.copy()
        for key, value in masked.items():
            if key in SENSITIVE_KEYS and isinstance(value, str) and len(value) > 6:
                masked[key] = "***" + value[-6:]
            elif key in SENSITIVE_KEYS and isinstance(value, str) and len(value) > 4:
                masked[key] = "***" + value[-4:]
            elif isinstance(value, (dict, list)):
                masked[key] = mask_sensitive_data(value)
        return masked
    elif isinstance(data, list):
        return [mask_sensitive_data(item) for item in data]
    elif isinstance(data, str):
        # Mask Token="..." pattern
        data = re.sub(r'(Token=")([a-fA-F0-9-]{36})(")', r'\1***\3', data)
        # Mask DeviceToken="..." or DeviceToken : "..." pattern
        data = re.sub(r'(DeviceToken["\s:]+)([^"\s}]+)', r'\1***', data)
        # Mask DUID="..." pattern
        data = re.sub(r'(DUID=")([^"]+)(")', r'\1***\3', data)
        return data
    return data

import asyncio
import logging
from .const import (
    CONFIG_DEVICE_CONNECTION_PARAMS,
    CONFIG_DEVICE_CONNECTION_TEMPLATE,
)
from typing import Any, Dict, Optional, Tuple

CLIMATE_IP_CONNECTIONS = []

def register_connection(conn):
    """Decorate a function to register a propery."""
    CLIMATE_IP_CONNECTIONS.append(conn)
    return conn


class Connection:
    def __init__(self, config, logger):
        self._params = {}
        self._logger = logger
        self._config = config

    @property
    def logger(self):
        return self._logger

    @property
    def config(self):
        return self._config

    def load_from_yaml(self, node, connection_base):
        """Load configuration from yaml node dictionary. Use connection base as base but DO NOT modify it.
        Return True if successful False otherwise."""
        return False
    
    # --- START OF MODIFICATION (Milestone 0) ---
    
    # Interface for synchronous engines (requests, 2878)
    def execute(self, *args, **kwargs) -> Any:
        """Executes a synchronous command."""
        raise NotImplementedError
    
    # Interface for asynchronous engines (aiohttp)
    async def async_execute(self, *args, **kwargs) -> Tuple[str, Optional[Dict[str, str]]]:
        """Executes an asynchronous command."""
        raise NotImplementedError
    
    # Helper property for Milestone 1
    @property
    def is_async_native(self) -> bool:
        """Indicates if the connection is native asynchronous (aiohttp)."""
        # Subclasses can override this if 'async_execute' is just a wrapper.
        return False
    
    # --- END OF MODIFICATION (Milestone 0) ---
    
    def execute_legacy(self, template, value, device_state, device_id):
        """execute connection and return JSON object as result or None if unsuccesful."""
        return None

    def create_updated(self, yaml_node):
        """Create a copy of connection object and update this object from YAML configuration node"""
        return None
import aiofiles
import asyncio
import json
import logging
import os
import time
from datetime import datetime

import homeassistant.helpers.config_validation as cv
import homeassistant.helpers.entity_component
import voluptuous as vol
import yaml
from homeassistant.const import (
    ATTR_ENTITY_ID,
    ATTR_NAME,
    ATTR_TEMPERATURE,
    CONF_IP_ADDRESS,
    CONF_TEMPERATURE_UNIT,
    CONF_TOKEN,
    STATE_ON,
    UnitOfTemperature,
)
from homeassistant.helpers.config_validation import PLATFORM_SCHEMA

from .connection import create_connection
from .controller import ATTR_POWER, ClimateController, register_controller
from .properties import create_property, create_status_getter
from .yaml_const import (
    CONF_CONFIG_FILE,
    CONF_DEVICE_ID,
    CONFIG_DEVICE,
    CONFIG_DEVICE_ATTRIBUTES,
    CONFIG_DEVICE_CONNECTION,
    CONFIG_DEVICE_CONNECTION_PARAMS,
    CONFIG_DEVICE_NAME,
    CONFIG_DEVICE_OPERATIONS,
    CONFIG_DEVICE_POLL,
    CONFIG_DEVICE_STATUS,
    CONFIG_DEVICE_UNIQUE_ID,
    CONFIG_DEVICE_VALIDATE_PROPS,
)

CONST_CONTROLLER_TYPE = "yaml"
CONST_MAX_GET_STATUS_RETRIES = 4


async def StreamWrapper(stream, token, ip_address, device_id):
    """
    Asynchronously reads a stream and replaces placeholder values.
    """
    data = "".join([line async for line in stream])
    if token is not None:
        data = data.replace("__CLIMATE_IP_TOKEN__", token)
    if ip_address is not None:
        data = data.replace("__CLIMATE_IP_HOST__", ip_address)
    if device_id is not None:
        data = data.replace("__DEVICE_ID__", device_id)
    return data


@register_controller
class YamlController(ClimateController):
    """
    YAML-based controller, refactored to support asynchronous operations.
    """

    def __init__(self, config, logger):
        super(YamlController, self).__init__(config, logger)
        self._logger = logger
        self._operations = {}
        self._operations_list = []
        self._properties = {}
        self._properties_list = []
        self._name = CONST_CONTROLLER_TYPE
        self._attributes = {"controller": self.id}
        self._state_getter = None
        self._debug = config.get("debug", False)
        self._temp_unit = UnitOfTemperature.CELSIUS
        self._service_schema_map = {vol.Optional(ATTR_ENTITY_ID): cv.comp_entity_ids}
        self._logger.setLevel(logging.INFO if self._debug else logging.ERROR)
        self._yaml = config.get(CONF_CONFIG_FILE)
        self._ip_address = config.get(CONF_IP_ADDRESS, None)
        self._device_id = config.get(CONF_DEVICE_ID, "032000000")
        self._token = config.get(CONF_TOKEN, None)
        self._config = config
        self._retries_count = 0
        self._last_device_state = None
        self._poll = None
        self._unique_id = self._device_id
        self._uniqe_id_prop = None

    @property
    def poll(self):
        return self._poll

    @property
    def unique_id(self):
        return self._unique_id

    @property
    def id(self):
        return CONST_CONTROLLER_TYPE

    async def initialize(self):
        """
        Initializes the controller by reading the YAML configuration.
        This method is now async to allow for async file I/O and initial state update.
        """
        file = self._yaml
        if file is not None and file.find("\\") == -1 and file.find("/") == -1:
            file = os.path.join(os.path.dirname(__file__), file)
        self._logger.info("Loading configuration file: {}".format(file))

        try:
            # Use aiofiles for non-blocking file reading
            async with aiofiles.open(file, "r") as stream:
                yaml_device = yaml.safe_load(
                    await StreamWrapper(
                        stream, self._token, self._ip_address, self._device_id
                    )
                )
        except yaml.YAMLError as exc:
            self._logger.error("YAML error: {}".format(exc))
            return False
        except FileNotFoundError:
            self._logger.error(
                "Cannot open YAML configuration file '{}'".format(self._yaml)
            )
            return False

        if CONFIG_DEVICE not in yaml_device:
            self._logger.error("Configuration file is missing the 'device' root key.")
            return False

        ac = yaml_device.get(CONFIG_DEVICE, {})
        self._poll = ac.get(CONFIG_DEVICE_POLL, None)
        validate_props = ac.get(CONFIG_DEVICE_VALIDATE_PROPS, False)
        
        connection_node = ac.get(CONFIG_DEVICE_CONNECTION, {})
        connection = create_connection(connection_node, self._config, self._logger)

        if connection is None:
            self._logger.error("Cannot create connection object!")
            return False

        self._state_getter = create_status_getter(
            "state", ac.get(CONFIG_DEVICE_STATUS, {}), connection
        )
        if self._state_getter is None:
            self._logger.error("Missing 'status' configuration node.")
            return False

        nodes = ac.get(CONFIG_DEVICE_OPERATIONS, {})
        for op_key in nodes.keys():
            op = create_property(op_key, nodes[op_key], connection)
            if op is not None:
                self._operations[op.id] = op
                self._service_schema_map[vol.Optional(op.id)] = op.config_validation_type

        nodes = ac.get(CONFIG_DEVICE_ATTRIBUTES, {})
        for key in nodes.keys():
            prop = create_property(key, nodes[key], connection)
            if prop is not None:
                self._properties[prop.id] = prop
        
        # ... Other property initializations ...

        self._name = ac.get(ATTR_NAME, CONST_CONTROLLER_TYPE)

        # Perform the first state update asynchronously
        await self.async_update_state()

        # Property validation logic
        if validate_props:
            device_state = self._state_getter.value
            ops = {
                op.id: op
                for op in self._operations.values()
                if op.is_valid(device_state)
            }
            invalid_ops = set(self._operations.keys()) - set(ops.keys())
            if invalid_ops:
                self._logger.info(f"Removing invalid operations: {', '.join(invalid_ops)}")
            self._operations = ops

        self._operations_list = list(self._operations.keys())
        self._properties_list = list(self._properties.keys())
        # ...
        
        return True

    @staticmethod
    def match_type(type):
        return str(type).lower() == CONST_CONTROLLER_TYPE

    @property
    def name(self):
        device_name = self.get_property(ATTR_NAME)
        return device_name if device_name is not None else self._name

    @property
    def debug(self):
        return self._debug

    async def async_update_state(self):
        """
        Asynchronously updates the state of all properties and attributes.
        """
        debug = self._debug
        self._logger.info("Updating state asynchronously...")
        if self._state_getter is None:
            return

        self._attributes = {ATTR_NAME: self.name}
        
        # The state getter now has an async update method
        device_state = await self._state_getter.async_update_state(
            self._last_device_state, debug
        )
        
        if device_state is None and self._retries_count > 0:
            self._retries_count -= 1
            device_state = self._last_device_state
            self._attributes["failed_retries"] = (
                CONST_MAX_GET_STATUS_RETRIES - self._retries_count
            )
        else:
            self._retries_count = CONST_MAX_GET_STATUS_RETRIES
            self._last_device_state = device_state

        if debug:
             self._attributes.update(self._state_getter.state_attributes)

        # Create a list of tasks to update all properties concurrently
        update_tasks = []
        for op in self._operations.values():
            update_tasks.append(op.async_update_state(device_state, debug))
        for prop in self._properties.values():
            update_tasks.append(prop.async_update_state(device_state, debug))

        # Run all updates in parallel for efficiency
        if update_tasks:
            await asyncio.gather(*update_tasks)
        
        # Collect results after they have been updated
        for op in self._operations.values():
            self._attributes.update(op.state_attributes)
        for prop in self._properties.values():
            self._attributes.update(prop.state_attributes)

        if self._unique_id is None and self._uniqe_id_prop is not None:
            self._unique_id = await self._uniqe_id_prop.async_update_state(device_state, debug)

    async def async_set_property(self, property_name, new_value):
        """
        Asynchronously sets a property on the device.
        """
        self._logger.info(
            f"Asynchronously setting property '{property_name}' to '{new_value}'"
        )
        op = self._operations.get(property_name, None)
        if op is not None:
            # The set_value method is now async, so we await it
            result = await op.async_set_value(new_value)
            self._logger.info(
                f"Set property '{property_name}' finished with result: {result}"
            )
            return result
            
        self._logger.error(
            f"Failed to set property '{property_name}': property not found."
        )
        return False

    def get_property(self, property_name):
        if property_name in self._operations:
            return self._operations[property_name].value
        if property_name in self._properties:
            return self._properties[property_name].value
        if property_name in self._attributes:
            return self._attributes[property_name]
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
        """Return a list of available operations"""
        return self._operations_list

    @property
    def attributes(self):
        """Return a list of available attributes"""
        return self._properties_list

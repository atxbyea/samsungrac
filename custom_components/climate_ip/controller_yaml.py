import aiofiles
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
    data = ''.join([line async for line in stream])
    if token is not None:
        data = data.replace("__CLIMATE_IP_TOKEN__", token)
    if ip_address is not None:
        data = data.replace("__CLIMATE_IP_HOST__", ip_address)
    if device_id is not None:
        data = data.replace("__DEVICE_ID__", device_id)
    return data


@register_controller
class YamlController(ClimateController):
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
        connection_params = {CONFIG_DEVICE_CONNECTION_PARAMS: {}}

        file = self._yaml
        if file is not None and file.find("\\") == -1 and file.find("/") == -1:
            file = os.path.join(os.path.dirname(__file__), file)
        self._logger.info("Loading configuration file: {}".format(file))

        if self._ip_address is not None:
            self._logger.info("ip_address: {}".format(self._ip_address))
        if self._token is not None:
            self._logger.info("token: {}".format(self._token))
        if self._device_id is not None:
            self._logger.info("device id: {}".format(self._device_id))

        async with aiofiles.open(file, "r") as stream:
            try:
                yaml_device = yaml.load(
                    await StreamWrapper(
                        stream, self._token, self._ip_address, self._device_id
                    ),
                    Loader=yaml.FullLoader,
                )
            except yaml.YAMLError as exc:
                if self._logger is not None:
                    self._logger.error("YAML error: {}".format(exc))
                return False
            except FileNotFoundError:
                if self._logger is not None:
                    self._logger.error(
                        "Cannot open YAML configuration file '{}'".format(self._yaml)
                    )
                return False

        validate_props = False
        if CONFIG_DEVICE in yaml_device:
            ac = yaml_device.get(CONFIG_DEVICE, {})
            self._poll = ac.get(CONFIG_DEVICE_POLL, None)
            validate_props = ac.get(CONFIG_DEVICE_VALIDATE_PROPS, False)
            self._logger.info(
                "Validate properties: {} ({})".format(
                    validate_props, ac.get(CONFIG_DEVICE_VALIDATE_PROPS, False)
                )
            )
            connection_node = ac.get(CONFIG_DEVICE_CONNECTION, {})
            connection = create_connection(connection_node, self._config, self._logger)

            if connection is None:
                self._logger.error("Cannot create connection object!")
                return False

            self._state_getter = create_status_getter(
                "state", ac.get(CONFIG_DEVICE_STATUS, {}), connection
            )
            if self._state_getter == None:
                self._logger.error("Missing 'state' configuration node")
                return False

            nodes = ac.get(CONFIG_DEVICE_OPERATIONS, {})
            for op_key in nodes.keys():
                op = create_property(op_key, nodes[op_key], connection)
                if op is not None:
                    self._operations[op.id] = op
                    self._service_schema_map[
                        vol.Optional(op.id)
                    ] = op.config_validation_type

            nodes = ac.get(CONFIG_DEVICE_ATTRIBUTES, {})
            for key in nodes.keys():
                prop = create_property(key, nodes[key], connection)
                if prop is not None:
                    self._properties[prop.id] = prop

            unique_id_prop = create_property(
                CONFIG_DEVICE_UNIQUE_ID, ac.get(CONFIG_DEVICE_UNIQUE_ID, {}), connection
            )
            if unique_id_prop is not None:
                self._uniqe_id_prop = unique_id_prop

            self._name = ac.get(ATTR_NAME, CONST_CONTROLLER_TYPE)

        self.update_state()

        if validate_props:
            ops = {}
            device_state = self._state_getter.value
            for op in self._operations.values():
                self._logger.info("Removing invalid operation '{}'".format(op.id))

                if op.is_valid(device_state):
                    ops[op.id] = op
                else:
                    self._logger.info("Removing invalid operation '{}'".format(op.id))
                self._operations = ops
            ops = {}

        self._operations_list = [v for v in self._operations.keys()]
        self._properties_list = [v for v in self._properties.keys()]
        self._properties_list.append("last_sync")
        self._properties_list.append("AC_FUN_ENABLE")
        self._properties_list.append("AC_FUN_COMODE")
        self._properties_list.append("AC_FUN_ERROR")
        self._properties_list.append("AC_SG_WIFI")
        self._properties_list.append("AC_SG_INTERNET")
        self._properties_list.append("AC_ADD2_USEDWATT")
        self._properties_list.append("AC_ADD2_VERSION")
        self._properties_list.append("AC_ADD2_PANEL_VERSION")
        self._properties_list.append("AC_ADD2_OUT_VERSION")
        self._properties_list.append("AC_ADD2_OPTIONCODE")
        self._properties_list.append("AC_ADD2_USEDTIME")
        self._properties_list.append("AC_ADD2_FILTER_USE_TIME")

        return (len(self._operations) + len(self._properties)) > 0

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

    def update_state(self):
        debug = self._debug
        self._logger.info("Updating state...")
        if self._state_getter is not None:
            self._attributes = {ATTR_NAME: self.name}
            self._logger.info("Updating getter...")
            self._state_getter.update_state(self._state_getter.value, debug)
            device_state = self._state_getter.value
            self._logger.info("Getter updated with value: {}".format(device_state))

            if device_state is None and self._retries_count > 0:
                --self._retries_count
                device_state = self._last_device_state
                self._attributes["failed_retries"] = (
                    CONST_MAX_GET_STATUS_RETRIES - --self._retries_count
                )
            else:
                self._retries_count = CONST_MAX_GET_STATUS_RETRIES
                self._last_device_state = device_state

            # [lucadjc]: preferred to have always the attributed evaluated, removed condition on debug
            # if debug:
            self._attributes.update(self._state_getter.state_attributes)

            # [lucadjc]: added last sync date to send some alerts from hassio in case of connection error
            self._attributes["last_sync"] = ""

            toJSON = json.dumps(self._state_getter.state_attributes["device_state"])
            try:
                json_data = json.loads(json.loads(toJSON))
                self._attributes["AC_FUN_ENABLE"] = json_data["AC_FUN_ENABLE"]
                self._attributes["AC_FUN_COMODE"] = json_data["AC_FUN_ENABLE"]
                self._attributes["AC_FUN_ERROR"] = json_data["AC_FUN_ERROR"]
                self._attributes["AC_SG_WIFI"] = json_data["AC_SG_WIFI"]
                self._attributes["AC_SG_INTERNET"] = json_data["AC_SG_INTERNET"]
                self._attributes["AC_ADD2_USEDWATT"] = json_data["AC_ADD2_USEDWATT"]
                self._attributes["AC_ADD2_VERSION"] = json_data["AC_ADD2_VERSION"]
                self._attributes["AC_ADD2_PANEL_VERSION"] = json_data[
                    "AC_ADD2_PANEL_VERSION"
                ]
                self._attributes["AC_ADD2_OUT_VERSION"] = json_data[
                    "AC_ADD2_OUT_VERSION"
                ]
                self._attributes["AC_ADD2_OPTIONCODE"] = json_data["AC_ADD2_OPTIONCODE"]
                self._attributes["AC_ADD2_USEDTIME"] = json_data["AC_ADD2_USEDTIME"]
                self._attributes["AC_ADD2_FILTER_USE_TIME"] = json_data[
                    "AC_ADD2_FILTER_USE_TIME"
                ]

                if len(json_data["AC_ADD2_OUT_VERSION"]) != 0:
                    self._attributes["last_sync"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
            except:
                self._logger.info("Error: update_state")

            self._logger.info("Updating operations...")
            for op in self._operations.values():
                op.update_state(device_state, debug)
                self._attributes.update(op.state_attributes)
            self._logger.info("Updating properties...")
            for prop in self._properties.values():
                prop.update_state(device_state, debug)
                self._attributes.update(prop.state_attributes)
                for p in prop.state_attributes:
                    self._logger.info(p)
            if self._unique_id is None and self._uniqe_id_prop is not None:
                self._unique_id = self._uniqe_id_prop.update_state(device_state, debug)

    def set_property(self, property_name, new_value):
        self._logger.info(
            "SETTING UP property {} to {}".format(property_name, new_value)
        )
        op = self._operations.get(property_name, None)
        if op is not None:
            result = op.set_value(new_value)
            self._logger.info(
                "SETTING UP property {} to {} -> FINISHED with result {}".format(
                    property_name, new_value, result
                )
            )
            return result
        self._logger.info(
            "SETTING UP property {} to {} -> FAILED - wrong property".format(
                property_name, new_value
            )
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
        self._logger.info("Controller::state_attributes")
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

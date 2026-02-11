"""Constants for the Climate IP integration."""

DOMAIN = "climate_ip"

PLATFORMS = ["climate"]

# --- Configurable Options ---
CONF_POLL_INTERVAL = "poll_interval"
DEFAULT_POLL_INTERVAL = 60
MIN_POLL_INTERVAL = 5
MAX_POLL_INTERVAL = 300

# --- Configuration Flow and Entry Keys ---
CONF_DEVICE_TYPE = "device_type"
CONF_CONFIG_FILE = "config_file"
CONF_DISCOVERED_DEVICES = "discovered_devices"
CONF_NAME = "name"
CONF_SELECTED_DEVICES = "selected_devices"
CONF_DEVICES = "devices"
CONF_DEVICE_ID = "device_id"

# --- Connection Method Constants (for Dual Engine) ---
CONF_CONN_METHOD = "connection_method"
CONN_METHOD_AIOHTTP = "aiohttp"
CONN_METHOD_REQUESTS = "requests"
CONN_METHOD_RAW = "raw"

# Connection types for config_flow logic
CONN_METHOD_HTTPS_VERIFY = "https_verify"
CONN_METHOD_HTTPS_TLS_AUTO = "tls_auto"

# --- Device Types ---
DEVICE_TYPE_SAMSUNG_8888 = "samsung_8888"
DEVICE_TYPE_SAMSUNG_2878 = "samsung_2878"
DEVICE_TYPE_MIM_H03 = "mim_h03"
DEVICE_TYPE_SMARTTHINGS_HVAC = "smartthings_hvac"
DEVICE_TYPE_SMARTTHINGS_DHW = "smartthings_dhw"

# Group for modern devices that will get the new engine option
DEVICE_TYPE_8888_GROUP = [DEVICE_TYPE_SAMSUNG_8888, DEVICE_TYPE_MIM_H03]

# Group for all devices that support the aiohttp engine (Modern + SmartThings)
DEVICE_TYPE_AIOHTTP_SUPPORTED = DEVICE_TYPE_8888_GROUP + [DEVICE_TYPE_SMARTTHINGS_HVAC, DEVICE_TYPE_SMARTTHINGS_DHW]

# Maps device types to their corresponding YAML configuration files.
DEVICE_TYPE_TO_CONFIG_FILE = {
    DEVICE_TYPE_SAMSUNG_8888: "samsungrac.yaml",
    DEVICE_TYPE_SAMSUNG_2878: "samsung_2878.yaml",
    DEVICE_TYPE_MIM_H03: "mim-h03_heatpump.yaml",
    DEVICE_TYPE_SMARTTHINGS_HVAC: "samsung_smartthings_hvac.yaml",
    DEVICE_TYPE_SMARTTHINGS_DHW: "samsung_smartthings_dhw.yaml",
}

# Reverse map for inferring device type during YAML import.
CONFIG_FILE_TO_DEVICE_TYPE = {
    filename: device_type
    for device_type, filename in DEVICE_TYPE_TO_CONFIG_FILE.items()
}

# --- Legacy Constants for YAML Import ---
CONF_CERT = "cert"
CONF_CONTROLLER = "controller"
CONF_TEMP_STEP = "temp_step"
# This is intentionally different from HA's CONF_NAME to avoid conflicts during import.
CONFIG_DEVICE_NAME = "name"
CONFIG_DEVICE_POLL = "poll"
CONFIG_DEVICE_UPDATE_DELAY = "update_delay"

DEFAULT_CONF_CERT_FILE = "ac14k_m.pem"
DEFAULT_CONF_CONFIG_FILE = "samsungrac.yaml"
DEFAULT_CONF_TEMP_UNIT = "C"
DEFAULT_CONF_CONTROLLER = "yaml"
DEFAULT_UPDATE_DELAY = 0.5

# --- Protocol Constants (Samsung 2878) ---
PROTOCOL_2878_DPLUG = "DPLUG-1.6"
PROTOCOL_2878_DRC = "DRC-1.00"
PROTOCOL_2878_INVALIDATE = "InvalidateAccount"
PROTOCOL_2878_DEVICE_STATE = "DeviceState"
PROTOCOL_2878_DEVICE_CONTROL = "DeviceControl"
PROTOCOL_2878_STATUS_OK = 'Status="Okay"'
PROTOCOL_2878_STATUS = "Status"
PROTOCOL_2878_UPDATE = "Update"
PROTOCOL_2878_RESPONSE = "Response"
PROTOCOL_2878_ATTR = "Attr"
PROTOCOL_2878_ATTR_ID = "@ID"
PROTOCOL_2878_ATTR_VALUE = "@Value"
PROTOCOL_2878_POWER_ID = "AC_FUN_POWER"
PROTOCOL_2878_VALUE_ON = "On"

# --- YAML Configuration Constants (Merged from yaml_const.py) ---
CONFIG_DEVICE = "device"
CONFIG_DEVICE_UNIQUE_ID = "unique_id"
CONFIG_DEVICE_VALIDATE_PROPS = "validate_properties"
CONFIG_DEVICE_CONNECTION = "connection"
CONFIG_DEVICE_CONNECTION_PARAMS = "params"
CONFIG_DEVICE_STATUS = "status"
CONFIG_DEVICE_ATTRIBUTES = "attributes"
CONFIG_DEVICE_OPERATIONS = "operations"
CONFIG_DEVICE_SENSORS = "sensors"
CONFIG_TYPE = "type"
CONFIG_DEVICE_OPERATION_VALUES = "values"
CONFIG_DEVICE_OPERATION_VALUE = "value"
CONFIG_DEVICE_OPERATION_NUMBER_MIN = "min"
CONFIG_DEVICE_OPERATION_NUMBER_MAX = "max"
CONFIG_DEVICE_OPERATION_TEMP_UNIT_TEMPLATE = "unit_template"
CONFIG_DEVICE_STATUS_TEMPLATE = "status_template"
CONFIG_DEVICE_CONNECTION_TEMPLATE = "connection_template"
CONFIG_DEVICE_VALIDATION_TEMPLATE = "validation_template"
CONFIG_DEVICE_CONDITION_TEMPLATE = "condition_template"
CONFIG_DEVICE_POWER_TEMPLATE = "power_template"
CONFIG_DEVICE_CONNECTION_TYPE = "type"

CONF_DEBUG = "debug"
ERROR_MESSAGE = "ERROR"
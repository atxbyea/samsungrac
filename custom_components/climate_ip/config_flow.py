# custom_components/climate_ip/config_flow.py
"""Config flow for the Climate IP integration."""
import asyncio
import logging
import os
from typing import Any, Dict, Optional

import voluptuous as vol
from homeassistant.helpers.selector import (
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    NumberSelector,
    NumberSelectorMode,
    NumberSelectorConfig,
)
import homeassistant.helpers.config_validation as cv
from getmac import get_mac_address
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant import config_entries
from homeassistant.const import (
    CONF_IP_ADDRESS,
    CONF_MAC,
    CONF_TOKEN,
)
from homeassistant.core import callback
from homeassistant.config_entries import SOURCE_IMPORT, ConfigEntry
from homeassistant.data_entry_flow import FlowResult
from homeassistant.exceptions import HomeAssistantError

from .const import (
    CONF_NAME,
    CONF_CERT,
    CONF_POLL_INTERVAL,
    DEFAULT_POLL_INTERVAL,
    MIN_POLL_INTERVAL,
    MAX_POLL_INTERVAL,
    CONF_CONFIG_FILE,
    CONF_DEVICE_ID,
    CONF_DEVICES,
    CONF_DISCOVERED_DEVICES,
    CONF_SELECTED_DEVICES,
    CONF_DEVICE_TYPE,
    DEVICE_TYPE_MIM_H03,
    # --- START OF MODIFICATION (Milestone 4) ---
    DEVICE_TYPE_8888_GROUP,
    # --- START OF MODIFICATION (Milestone 4) ---
    DEVICE_TYPE_AIOHTTP_SUPPORTED,
    # --- END OF MODIFICATION (Milestone 4) ---
    # --- END OF MODIFICATION (Milestone 4) ---
    DEVICE_TYPE_SAMSUNG_2878,
    DEVICE_TYPE_SAMSUNG_8888,
    DEVICE_TYPE_SMARTTHINGS_DHW,
    DEVICE_TYPE_SMARTTHINGS_HVAC,
    CONFIG_DEVICE_NAME,
    DOMAIN,
    CONFIG_FILE_TO_DEVICE_TYPE,
    # --- START OF MODIFICATION (Milestone 4) ---
    CONF_CONN_METHOD,
    CONN_METHOD_AIOHTTP,
    CONN_METHOD_REQUESTS,
    CONN_METHOD_RAW,
    # --- END OF MODIFICATION (Milestone 4) ---
    DEVICE_TYPE_TO_CONFIG_FILE,
)
from .controller_yaml import YamlController
from .token_acquirer import SamsungTokenAcquirer
# Import the new token acquirer
from .token_acquirer_8888 import SamsungTokenAcquirer8888
from .exceptions import CannotConnect, TokenAcquisitionError, AuthTurnedOffError

_LOGGER = logging.getLogger(__name__)

@config_entries.HANDLERS.register(DOMAIN)
class ClimateIpConfigFlow(config_entries.ConfigFlow):
    """Config flow implementing a robust, multi-step pairing process with safe task wrappers."""

    VERSION = 2  # Updated to reflect significant changes

    def __init__(self):
        """Initialize the config flow."""
        self.flow_data: Dict[str, Any] = {}
        self.task: Optional[asyncio.Task] = None 
        self.acquirer: Optional[Any] = None # Can be SamsungTokenAcquirer or SamsungTokenAcquirer8888
        _LOGGER.debug("Initializing new Climate IP config flow.")

    async def async_step_import(self, user_input: dict[str, Any]) -> FlowResult:
        """
        Handle the import of a YAML configuration (legacy platform).
        This is triggered by async_setup_platform in climate.py.
        """
        _LOGGER.debug(f"Starting YAML import for: {user_input.get(CONF_IP_ADDRESS)}")

        # --- START: Sanitize MAC and set unique_id ---
        # Sanitize the MAC address by removing colons as soon as it's read.
        mac_address = user_input.get(CONF_MAC)
        if mac_address:
            user_input[CONF_MAC] = mac_address.replace(":", "")

        # Use the sanitized MAC or IP as unique_id. MAC is preferred.
        unique_id = user_input.get(CONF_MAC) or user_input.get(CONF_IP_ADDRESS)
        # --- END: Sanitize MAC and set unique_id ---

        await self.async_set_unique_id(unique_id)

        # Stop if already configured (either via UI or previous YAML)
        # 'updates=user_input' allows updating the entry if YAML data has changed
        self._abort_if_unique_id_configured()

        # --- START: Infer device_type from config_file ---
        # This is the crucial step. We determine the device_type from the config_file
        # provided in YAML, so the controller knows how to behave.
        config_file = user_input.get(CONF_CONFIG_FILE)
        if config_file and config_file in CONFIG_FILE_TO_DEVICE_TYPE:
            user_input[CONF_DEVICE_TYPE] = CONFIG_FILE_TO_DEVICE_TYPE[config_file]
            _LOGGER.debug(f"Inferred device_type '{user_input[CONF_DEVICE_TYPE]}' from config_file '{config_file}'")
        # --- END: Infer device_type ---

        device_name = user_input.get(CONFIG_DEVICE_NAME, f"Climate {unique_id}")

        _LOGGER.info("Creating new entry for '%s' from imported YAML.", device_name)
        return self.async_create_entry(title=device_name, data=user_input)

    @callback
    def async_remove(self) -> None:
        """Clean up the flow if the user cancels."""
        _LOGGER.debug("Config flow cancelled by user. Cleaning up task.")
        if self.task:
            self.task.cancel()
        if self.acquirer:
            self.hass.async_create_task(self.acquirer.async_close())

    async def _async_resolve_mac_and_set_unique_id(self, ip_address: str, mac_address: Optional[str]) -> Optional[str]:
        """Resolve MAC address from IP if not provided and set the unique_id."""
        if mac_address:
            self.flow_data[CONF_MAC] = mac_address.replace(":", "").upper()
        else:
            _LOGGER.info("MAC address not provided, attempting to resolve from IP %s", ip_address)
            try:
                resolved_mac = await self.hass.async_add_executor_job(
                    lambda: get_mac_address(ip=ip_address)
                )
                if not resolved_mac:
                    return "mac_resolve_failed"
                
                _LOGGER.info("Successfully resolved MAC %s", resolved_mac)
                self.flow_data[CONF_MAC] = resolved_mac.replace(":", "").upper()
            except (OSError, HomeAssistantError):
                _LOGGER.warning("Could not resolve MAC address. Asking user to provide it.")
                return "mac_resolve_failed"
            except Exception as e:
                _LOGGER.exception("Unexpected error during MAC resolution: %s", e)
                return "unknown"

        await self.async_set_unique_id(self.flow_data[CONF_MAC])
        self._abort_if_unique_id_configured()
        return None

    async def _async_validate_cert_path(self, user_cert_path: Optional[str]) -> bool:
        """Validate that the certificate file exists."""
        if not user_cert_path:
            return True # No certificate provided is a valid scenario
        
        path_to_check = user_cert_path
        if not os.path.dirname(user_cert_path):
            path_to_check = os.path.join(os.path.dirname(__file__), user_cert_path)
        return await self.hass.async_add_executor_job(os.path.exists, path_to_check)

    async def _initiate_pairing_safe(self) -> Dict[str, Any]:
        """Safe wrapper for the initiate_pairing phase to prevent exceptions from escaping."""
        _LOGGER.debug("Executing safe wrapper: _initiate_pairing_safe")
        try:
            # Add a check to ensure acquirer is not None
            if self.acquirer is None:
                _LOGGER.error("Acquirer was not initialized before initiating pairing.")
                return {"ok": False, "error": "unknown_error"}

            # This now returns the path of the certificate that worked
            successful_config = await self.acquirer.async_initiate_pairing()
            _LOGGER.debug("_initiate_pairing_safe successful with config: %s", successful_config)
            # Return the successful certificate path to the config flow
            return {"ok": True, "config": successful_config}
        except CannotConnect:
            _LOGGER.warning("Cannot connect to the device during pairing initiation.")
            return {"ok": False, "error": "cannot_connect"}
        except Exception as e:
            _LOGGER.error("Error during pairing initiation: %s", e, exc_info=True)
            return {"ok": False, "error": "pairing_init_failed"}

    async def _wait_token_safe(self) -> Dict[str, Any]:
        """Safe wrapper for the wait_for_token phase to prevent exceptions from escaping."""
        _LOGGER.debug("Executing safe wrapper: _wait_token_safe")
        try:
            if self.acquirer is None:
                _LOGGER.error("Acquirer was not initialized before waiting for token.")
                return {"ok": False, "error": "unknown_error"}

            token = await self.acquirer.async_wait_for_token()
            _LOGGER.debug("_wait_token_safe successful, token acquired.")
            return {"ok": True, "token": token}
        except AuthTurnedOffError as e:
            _LOGGER.warning("AuthTurnedOffError caught in safe wrapper: %s", e)
            return {"ok": False, "error": "auth_failed_turned_off"}
        except TokenAcquisitionError as e:
            _LOGGER.warning("TokenAcquisitionError caught in safe wrapper: %s", e)
            return {"ok": False, "error": "token_acquisition_failed"}
        except Exception as e:
            _LOGGER.error("Unknown error while waiting for token: %s", e, exc_info=True)
            return {"ok": False, "error": "unknown_error"}

    async def async_step_user(self, user_input: Optional[Dict[str, Any]] = None) -> FlowResult:
        """Handle the initial step where the user chooses the device type."""
        if user_input is not None:
            self.flow_data[CONF_DEVICE_TYPE] = user_input[CONF_DEVICE_TYPE]
            device_type = self.flow_data[CONF_DEVICE_TYPE]

            if device_type == DEVICE_TYPE_SAMSUNG_2878:
                return await self.async_step_samsung_2878()
            
            # --- START OF MODIFICATION (Milestone 4) ---
            if device_type in DEVICE_TYPE_8888_GROUP:
                return await self.async_step_samsung_8888()
            if device_type in [
                DEVICE_TYPE_SMARTTHINGS_HVAC,
                DEVICE_TYPE_SMARTTHINGS_DHW,
            ]:
                return await self.async_step_rest_api()

            return self.async_abort(reason="not_implemented")
            # --- END OF MODIFICATION (Milestone 4) ---

        schema = vol.Schema({
            vol.Required(CONF_DEVICE_TYPE): SelectSelector(
                SelectSelectorConfig(
                    options=[
                        DEVICE_TYPE_SAMSUNG_2878,
                        DEVICE_TYPE_SAMSUNG_8888,
                        DEVICE_TYPE_MIM_H03,
                        DEVICE_TYPE_SMARTTHINGS_HVAC,
                        DEVICE_TYPE_SMARTTHINGS_DHW,
                    ],
                    translation_key="device_type",
                    mode=SelectSelectorMode.DROPDOWN,
                )
            )
        })

        return self.async_show_form(step_id="user", data_schema=schema)

    def _get_samsung_2878_schema(self, mac_required: bool = False) -> vol.Schema:
        """Helper to dynamically generate the schema for the Samsung 2878 step."""
        raw_mac = self.flow_data.get(CONF_MAC, "")
        formatted_mac = ":".join(raw_mac[i:i+2] for i in range(0, len(raw_mac), 2)) if raw_mac else ""

        schema_dict = {vol.Required(CONF_IP_ADDRESS, default=self.flow_data.get(CONF_IP_ADDRESS, "")): str}
        
        if mac_required:
            schema_dict[vol.Required(CONF_MAC, default=formatted_mac)] = str
        else:
            schema_dict[vol.Optional(CONF_MAC, default=formatted_mac)] = str

        schema_dict.update({
            vol.Optional(CONF_NAME, default=self.flow_data.get(CONF_NAME, "")): str,
            vol.Optional(CONF_TOKEN, default=self.flow_data.get(CONF_TOKEN, "")): str,
            vol.Optional(CONF_CERT, default=self.flow_data.get(CONF_CERT, "")): str,
            vol.Optional(
                CONF_POLL_INTERVAL, default=self.flow_data.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
            ): NumberSelector(
                NumberSelectorConfig(
                    min=MIN_POLL_INTERVAL,
                    max=MAX_POLL_INTERVAL,
                    unit_of_measurement="seconds",
                    mode=NumberSelectorMode.BOX,
                )
            ),
        })
        return vol.Schema(schema_dict)

    async def async_step_samsung_2878(self, user_input: Optional[Dict[str, Any]] = None) -> FlowResult:
        """Handle IP/MAC input and resolution for old Samsung devices."""
        errors: Dict[str, str] = {}
        if user_input is not None:
            self.flow_data.update(user_input)
            ip_address = self.flow_data[CONF_IP_ADDRESS]
            mac_address = self.flow_data.get(CONF_MAC)
            
            error_reason = await self._async_resolve_mac_and_set_unique_id(ip_address, mac_address)
            if error_reason:
                errors["base"] = error_reason
                return self.async_show_form(
                    step_id="samsung_2878",
                    data_schema=self._get_samsung_2878_schema(mac_required=(error_reason == "mac_resolve_failed")),
                    errors=errors,
                )

            # Standardize the name to ensure consistency
            if not self.flow_data.get(CONF_NAME):
                mac = self.flow_data[CONF_MAC]
                self.flow_data[CONF_NAME] = f"Samsung AC {mac}"

            if self.flow_data.get(CONF_TOKEN):
                return await self._create_entry()

            # Validate certificate existence without modifying the stored value.
            user_cert_path = self.flow_data.get(CONF_CERT) or ""
            if not await self._async_validate_cert_path(user_cert_path):
                errors["base"] = "cert_not_found"
                return self.async_show_form(
                    step_id="samsung_2878",
                    data_schema=self._get_samsung_2878_schema(),
                    errors=errors,
                )

            # If all validations pass, create the acquirer and proceed.
            self.acquirer = SamsungTokenAcquirer(self.hass, self.flow_data[CONF_IP_ADDRESS], self.flow_data.get(CONF_CERT))
            return await self.async_step_initiate_pairing()

        return self.async_show_form(
            step_id="samsung_2878",
            data_schema=self._get_samsung_2878_schema(),
            errors=errors)

    def _get_samsung_8888_schema(self, mac_required: bool = False) -> vol.Schema:
        """Helper to dynamically generate the schema for the Samsung 8888/MIM-H03 step."""
        raw_mac = self.flow_data.get(CONF_MAC, "")
        formatted_mac = ":".join(raw_mac[i:i+2] for i in range(0, len(raw_mac), 2)) if raw_mac else ""

        schema_dict = {
            vol.Required(CONF_IP_ADDRESS, default=self.flow_data.get(CONF_IP_ADDRESS, "")): str,
        }
        
        if mac_required:
            schema_dict[vol.Required(CONF_MAC, default=formatted_mac)] = str
        else:
            schema_dict[vol.Optional(CONF_MAC, default=formatted_mac)] = str

        schema_dict.update({
            vol.Optional(CONF_NAME, default=self.flow_data.get(CONF_NAME, "")): str,
            vol.Optional(CONF_TOKEN, default=""): str,
            vol.Optional(CONF_CERT, default="ac14k_m.pem"): str,
            vol.Optional(
                CONF_POLL_INTERVAL, default=self.flow_data.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
            ): NumberSelector(
                NumberSelectorConfig(
                    min=MIN_POLL_INTERVAL,
                    max=MAX_POLL_INTERVAL,
                    unit_of_measurement="seconds",
                    mode=NumberSelectorMode.BOX,
                )
            ),
        })
        return vol.Schema(schema_dict)

    async def async_step_samsung_8888(self, user_input: Optional[Dict[str, Any]] = None) -> FlowResult:
        """Handle IP input and token acquisition for modern Samsung devices."""
        errors: Dict[str, str] = {}
        if user_input is not None:
            self.flow_data.update(user_input)
            ip_address = self.flow_data[CONF_IP_ADDRESS]
            mac_address = self.flow_data.get(CONF_MAC)
            
            error_reason = await self._async_resolve_mac_and_set_unique_id(ip_address, mac_address)
            if error_reason:
                errors["base"] = error_reason
                return self.async_show_form(
                    step_id="samsung_8888",
                    data_schema=self._get_samsung_8888_schema(mac_required=(error_reason == "mac_resolve_failed")),
                    errors=errors,
                )

            if self.flow_data.get(CONF_TOKEN):
                return await self._create_entry()

            # Validate certificate existence without modifying the stored value.
            user_cert_path = self.flow_data.get(CONF_CERT) or ""
            if not await self._async_validate_cert_path(user_cert_path):
                errors["base"] = "cert_not_found"
                # Return here to show the error, do not proceed to pairing.
                return self.async_show_form(
                    step_id="samsung_8888", data_schema=self._get_samsung_8888_schema(), errors=errors
                )

            self.acquirer = SamsungTokenAcquirer8888(
                self.hass, self.flow_data[CONF_IP_ADDRESS], self.flow_data.get(CONF_CERT) or 'ac14k_m.pem'
            )
            return await self.async_step_initiate_pairing()

        return self.async_show_form(
            step_id="samsung_8888",
            data_schema=self._get_samsung_8888_schema(),
            errors=errors,
        )

    def _get_rest_api_schema(self) -> vol.Schema:
        """Generate the schema for REST API based devices that require manual token."""
        device_type = self.flow_data.get(CONF_DEVICE_TYPE)
        schema = {}

        if device_type in [DEVICE_TYPE_SMARTTHINGS_HVAC, DEVICE_TYPE_SMARTTHINGS_DHW]:
            schema[vol.Required(CONF_IP_ADDRESS, default="api.smartthings.com")] = str
        else:
            schema[vol.Required(CONF_IP_ADDRESS)] = str

        # MIM-H03 no longer uses this step, so it's removed from this condition.
        if device_type in [DEVICE_TYPE_SMARTTHINGS_HVAC, DEVICE_TYPE_SMARTTHINGS_DHW]:
             schema[vol.Optional(CONF_DEVICE_ID)] = str
        
        schema[vol.Required(CONF_TOKEN)] = str
        schema[vol.Optional(CONF_NAME)] = str
        schema[vol.Optional(
            CONF_POLL_INTERVAL, default=self.flow_data.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
        )] = NumberSelector(
            NumberSelectorConfig(
                min=MIN_POLL_INTERVAL,
                max=MAX_POLL_INTERVAL,
                unit_of_measurement="seconds",
                mode=NumberSelectorMode.BOX,
            )
        )
        
        return vol.Schema(schema)

    async def async_step_rest_api(self, user_input: Optional[Dict[str, Any]] = None) -> FlowResult:
        """Handle configuration for REST API devices with manual token."""
        errors: Dict[str, str] = {}
        if user_input is not None:
            test_data = {**self.flow_data, **user_input}
            device_type = test_data.get(CONF_DEVICE_TYPE)

            if device_type:
                test_data[CONF_CONFIG_FILE] = DEVICE_TYPE_TO_CONFIG_FILE.get(device_type)
            try:
                _LOGGER.debug("Testing connection with data: %s", test_data)
                session = async_get_clientsession(self.hass)
                test_data["hass"] = self.hass
                test_data["session"] = session
                controller = YamlController(config=test_data, logger=_LOGGER)
                if not await controller.initialize():
                    raise CannotConnect

                self.flow_data.update(user_input)
                unique_id = self.flow_data.get(CONF_DEVICE_ID) or self.flow_data.get(CONF_IP_ADDRESS)
                await self.async_set_unique_id(unique_id)
                self._abort_if_unique_id_configured()
                return await self._create_entry()

            except CannotConnect:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error during connection test")
                errors["base"] = "unknown"

        return self.async_show_form(
            step_id="rest_api",
            data_schema=self._get_rest_api_schema(),
            errors=errors,
        )

    async def async_step_initiate_pairing(self, user_input=None) -> FlowResult:
        """Phase 1: Put the device in pairing mode using a safe wrapper."""
        _LOGGER.debug("Entering async_step_initiate_pairing.")
        if not self.task:
            _LOGGER.debug("Creating task for _initiate_pairing_safe.")
            self.task = self.hass.async_create_task(self._initiate_pairing_safe())

        if self.task and self.task.done():
            result = self.task.result()
            self.task = None
            if result["ok"]:
                _LOGGER.debug("Pairing initiation successful, advancing to await_button.")
                # Save the successful connection config to the flow data to be persisted later
                successful_config = result.get("config")
                if successful_config:
                    # Store the entire dictionary (e.g., {'cert': 'ac14k_m.pem', 'verify_mode': 0})
                    self.flow_data["preferred_connection"] = successful_config
                    _LOGGER.info("Successfully found working connection config, will save: %s", successful_config)

                return self.async_show_progress_done(next_step_id="await_button")
            
            self.flow_data["error_key"] = result["error"]
            return self.async_show_progress_done(next_step_id="handle_error")

        return self.async_show_progress(
            step_id="initiate_pairing",
            progress_action="initiating_pairing",
            progress_task=self.task,
        )

    async def async_step_await_button(self, user_input=None) -> FlowResult:
        """Phase 2: Wait for the user to press the required button."""
        _LOGGER.debug("Entering async_step_await_button.")
        if not self.task:
            _LOGGER.debug("Creating task for _wait_token_safe.")
            self.task = self.hass.async_create_task(self._wait_token_safe())

        if self.task and self.task.done():
            result = self.task.result()
            self.task = None
            if result["ok"]:
                self.flow_data[CONF_TOKEN] = result["token"]
                
                if self.flow_data.get(CONF_DEVICE_TYPE) == DEVICE_TYPE_SAMSUNG_2878:
                    _LOGGER.debug("Token for 2878 device acquired. Creating entry.")
                    return self.async_show_progress_done(next_step_id="create_entry")
                else:
                    _LOGGER.debug("Token acquisition successful, advancing to discover_uuid.")
                    return self.async_show_progress_done(next_step_id="discover_uuid")

            self.flow_data["error_key"] = result["error"]
            return self.async_show_progress_done(next_step_id="handle_error")

        # Show different instructions for MIM-H03, which needs a different button press.
        if self.flow_data.get(CONF_DEVICE_TYPE) == DEVICE_TYPE_MIM_H03:
            progress_action = "awaiting_ap_button_press"
        else:
            progress_action = "awaiting_button_press"

        return self.async_show_progress(
            step_id="await_button",
            progress_action=progress_action,
            progress_task=self.task,
            description_placeholders={"ip_address": self.flow_data.get(CONF_IP_ADDRESS)},
        )
        
    async def async_step_discover_uuid(self, user_input=None) -> FlowResult:
        """Step to discover indoor units from the device after getting a token."""
        _LOGGER.debug("Entering async_step_discover_uuid.")

        config_data = self.flow_data.copy()
        if self.unique_id:
            config_data["unique_id"] = self.unique_id
            
        device_type = config_data.get(CONF_DEVICE_TYPE)
        if not config_data.get(CONF_CONFIG_FILE) and device_type:
            # Ensure device_type is not None before using it as a key
            config_data[CONF_CONFIG_FILE] = DEVICE_TYPE_TO_CONFIG_FILE.get(device_type)

        try:
            # --- START OF FIX ---
            # Pass hass and session to the temporary controller instance.
            session = async_get_clientsession(self.hass)
            config_data["hass"] = self.hass
            config_data["session"] = session
            controller = YamlController(config=config_data, logger=_LOGGER)
            # --- END OF FIX ---
            if not await controller.initialize():
                _LOGGER.error("Failed to initialize controller during discovery.")
                return self.async_abort(reason="cannot_connect")

            if not await controller.async_get_status():
                _LOGGER.error("Failed to get device status during discovery.")
                return self.async_abort(reason="cannot_connect")

            discovered_devices = controller.discovered_devices
            if not discovered_devices or not isinstance(discovered_devices, list):
                _LOGGER.warning("Could not discover indoor units. Creating a single entry.")
                if controller.unique_id:
                    await self.async_set_unique_id(controller.unique_id, raise_on_progress=False)
                    if controller.device_id:
                        self.flow_data[CONF_DEVICE_ID] = controller.device_id
                    self._abort_if_unique_id_configured(updates=self.flow_data)
                return await self._create_entry()

            if device_type == DEVICE_TYPE_MIM_H03:
                internal_coordinator = None
                ac_units_info = []
                for device in discovered_devices:
                    # FIX: If there's only one device, it MUST be the coordinator,
                    # regardless of whether it has a "Mode" key or not.
                    # This handles emulators or single-unit setups correctly.
                    if len(discovered_devices) == 1:
                        internal_coordinator = device
                    elif isinstance(device, dict) and "Mode" not in device:
                        internal_coordinator = device
                    else:
                        # This is an actual AC unit
                        device_id = device.get("id")
                        if device_id is None:
                            device_id = str(device)
                        name = device.get("name", f"Indoor Unit {device_id}")
                        ac_units_info.append({
                            "id": device_id,
                            "uuid": device.get("uuid"),
                            "name": name,
                            "description": device.get("description", name)
                        })
                
                if internal_coordinator:
                    coordinator_uuid = internal_coordinator.get("uuid")
                    if coordinator_uuid:
                        await self.async_set_unique_id(coordinator_uuid, raise_on_progress=False)
                        # Store the coordinator's UUID as the main unique_id for the config entry
                        self.flow_data["unique_id"] = coordinator_uuid
                        self.flow_data[CONF_DEVICE_ID] = internal_coordinator.get("id") # Store coordinator's ID
                        self.flow_data[CONF_NAME] = f'{internal_coordinator.get("name", "MIM-H03 Coordinator")} {coordinator_uuid}' # Store coordinator's name
                        
                        # Abort if this specific coordinator is already configured
                        self._abort_if_unique_id_configured(updates=self.flow_data)

                        if ac_units_info:
                            self.flow_data[CONF_DISCOVERED_DEVICES] = ac_units_info
                            return await self.async_step_select_devices()
                        else:
                            _LOGGER.warning("No selectable indoor units found for MIM-H03. Creating a single entry for coordinator.")
                            return await self._create_entry()
                    else:
                        _LOGGER.error("MIM-H03 coordinator found but no UUID. Cannot create entry.")
                        return self.async_abort(reason="no_coordinator_uuid")
                else:
                    _LOGGER.error("No MIM-H03 coordinator found among discovered devices.")
                    return self.async_abort(reason="no_coordinator_found")
            
            if device_type == DEVICE_TYPE_SAMSUNG_8888:
                # 8888 devices are single-unit, no selection needed.
                if len(discovered_devices) > 0:
                    device = discovered_devices[0]
                    # Use uuid if available, otherwise fall back to id
                    device_uuid = device.get("uuid") or device.get("id")
                    if device_uuid:
                        self.flow_data[CONF_DEVICE_ID] = device_uuid
                        # Do not overwrite the unique_id, which should already be the MAC
                        # Per user request, use MAC for the name to be consistent. 
                        self.flow_data[CONF_NAME] = f"Samsung AC {self.flow_data.get(CONF_MAC)}"
                        _LOGGER.info("Discovered 8888 device, creating entry with name: %s", self.flow_data[CONF_NAME])
                        return await self._create_entry()

                _LOGGER.error("Discovered 8888 device but could not find a valid unit or UUID.")
                return self.async_abort(reason="discovery_failed")

            # This part is for other device types that still use select_devices
            devices_info = []
            for device in discovered_devices:
                if not isinstance(device, dict): continue
                
                device_id = device.get("id")
                if device_id is None:
                    device_id = str(device)

                name = device.get("name", f"Indoor Unit {device_id}")
                
                devices_info.append({
                    "id": device_id,
                    "uuid": device.get("uuid"),
                    "name": name,
                    "description": device.get("description", name)
                })

            if not devices_info:
                _LOGGER.warning("No selectable indoor units found for non-MIM-H03 device type.")
                return await self._create_entry()

            self.flow_data[CONF_DISCOVERED_DEVICES] = devices_info
            return await self.async_step_select_devices()

        except Exception as e:
            _LOGGER.exception("Error during device discovery: %s", e)
            return self.async_abort(reason="unknown")

    async def async_step_select_devices(self, user_input: Optional[Dict[str, Any]] = None) -> FlowResult:
        """Allow the user to select which indoor units to add."""
        errors: Dict[str, str] = {}
        
        discovered_devices = self.flow_data.get(CONF_DISCOVERED_DEVICES, [])
        
        device_options = {
            device['id']: f"{device.get('name', 'Unknown Device')} ({device.get('id')})"
            for device in discovered_devices
        }

        if user_input is not None:
            selected_devices_ids = user_input.get(CONF_SELECTED_DEVICES, [])
            if not selected_devices_ids:
                errors["base"] = "no_devices_selected"
            else:
                selected_devices_info = [
                    device for device in discovered_devices 
                    if device['id'] in selected_devices_ids
                ]
                
                # Store the selected devices in the flow data
                self.flow_data[CONF_DEVICES] = selected_devices_info
                
                # Set the unique_id for the main entry (e.g., based on IP/MAC)
                # This unique_id should be the controller's unique_id, not a sub-device's.
                # We can reuse the unique_id that was set during the initial discovery.
                main_unique_id = self.flow_data.get("unique_id")
                if not main_unique_id:
                    main_unique_id = (
                        self.flow_data.get(CONF_MAC) 
                        or self.flow_data.get(CONF_IP_ADDRESS)
                    )
                
                if main_unique_id:
                    await self.async_set_unique_id(main_unique_id, raise_on_progress=False)
                    self._abort_if_unique_id_configured(updates=self.flow_data)
                else:
                    _LOGGER.error("Could not determine a unique_id for the main controller.")
                    return self.async_abort(reason="no_unique_id")

                # Create a single config entry for the main controller,
                # which now contains the list of selected devices.
                return await self._create_entry()

        schema = vol.Schema({
            vol.Required(CONF_SELECTED_DEVICES, default=list(device_options.keys())): cv.multi_select(device_options)
        })

        return self.async_show_form(
            step_id="select_devices",
            data_schema=schema,
            errors=errors,
            description_placeholders={"device_count": len(discovered_devices)},
        )

    async def async_step_handle_error(self, user_input=None) -> FlowResult:
        """Handles the display of errors after a progress step fails."""
        error_key = self.flow_data.pop("error_key", "unknown_error")
        errors = {"base": error_key}

        # Reset the task so the user can try again.
        self.task = None

        _LOGGER.debug("Displaying form with error: %s", error_key)

        device_type = self.flow_data.get(CONF_DEVICE_TYPE)

        if device_type == DEVICE_TYPE_MIM_H03:
            step_id = "mim_h03"
            schema = self._get_samsung_8888_schema(mac_required=False)
        elif device_type == DEVICE_TYPE_SAMSUNG_8888:
            step_id = "samsung_8888"
            schema = self._get_samsung_8888_schema(mac_required=False)
        else: # Default to 2878
            step_id = "samsung_2878"
            schema = self._get_samsung_2878_schema(mac_required=False)

        return self.async_show_form(
            step_id=step_id,
            data_schema=schema,
            errors=errors,
        )

    async def async_step_create_entry(self, user_input=None) -> FlowResult:
        """Final invisible step that creates the config entry."""
        _LOGGER.debug("Entering async_step_create_entry.")
        return await self._create_entry()

    async def _create_entry(self) -> FlowResult:
        """Create the config entry and finish the flow."""
        _LOGGER.debug("Entering _create_entry to finalize configuration.")
        
        device_type = self.flow_data.get(CONF_DEVICE_TYPE)
        
        # 1. Determine the final unique_id for the entry
        final_unique_id = self.flow_data.get("unique_id")
        # For 2878 devices, the unique_id is always the MAC.
        # For 8888/MIM-H03, we set it to the MAC during the initial steps.
        # This logic ensures we don't accidentally overwrite it with the device_id (uuid).
        if not final_unique_id or device_type == DEVICE_TYPE_SAMSUNG_2878:
            final_unique_id = self.flow_data.get(CONF_MAC)
        
        # Fallback if still no unique_id
        if not final_unique_id:
            final_unique_id = self.flow_data.get(CONF_IP_ADDRESS)

        await self.async_set_unique_id(final_unique_id)
        self._abort_if_unique_id_configured(updates=self.flow_data)
        self.flow_data["unique_id"] = final_unique_id

        # 2. Determine the title for the entry
        title = self.flow_data.get(CONF_NAME)
        
        if device_type == DEVICE_TYPE_SAMSUNG_2878:
            # For 2878, the name is already set to "Samsung AC {mac}"
            if not title:
                title = f"Samsung AC {self.flow_data.get(CONF_MAC)}"
        elif device_type in [DEVICE_TYPE_SAMSUNG_8888, DEVICE_TYPE_MIM_H03]:
            # For modern devices, create a title like "Device Name (uuid)"
            # The UUID is the final_unique_id for these types
            device_name = self.flow_data.get(CONF_NAME)
            if not device_name:
                title = f"Samsung AC {final_unique_id}"
            else:
                # If user provided a name, use it. Add the MAC for clarity only if it's not already in the name.
                if final_unique_id not in device_name:
                    title = f"{device_name} ({final_unique_id})"
                else:
                    title = device_name
        
        # Generic fallback for title
        if not title:
            title = f"Climate IP ({final_unique_id})"

        # 3. Ensure CONF_NAME is set for entities to use
        if not self.flow_data.get(CONF_NAME):
            # If user didn't provide a name, we set it to empty string.
            # The entities will use their default naming logic.
            self.flow_data[CONF_NAME] = ""
        
        _LOGGER.debug("Creating config entry with title: %s, name: %s, and unique_id: %s", title, self.flow_data.get(CONF_NAME), final_unique_id)
        _LOGGER.debug("Final data for config entry: %s", self.flow_data)

        return self.async_create_entry(title=title, data=self.flow_data)

    async def async_step_reauth(self, entry_data: Dict[str, Any]) -> FlowResult:
        """Handle re-authentication failure from the integration."""
        _LOGGER.debug(f"Entering async_step_reauth with data: {entry_data}")
        self.reauth_entry = self.hass.config_entries.async_get_entry(self.context["entry_id"])
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input: Optional[Dict[str, Any]] = None) -> FlowResult:
        """Confirm re-authentication."""
        errors = {}

        if user_input is not None:
            # When the user confirms, we assume they have fixed the SmartThings integration.
            # We simply reload the config entry, which will trigger the controller initialization
            # and the automatic token retrieval logic again.
            _LOGGER.debug("Reauth confirmed, reloading entry to retry token acquisition.")
            self.hass.async_create_task(self.hass.config_entries.async_reload(self.reauth_entry.entry_id))
            return self.async_abort(reason="reauth_successful")

        return self.async_show_form(
            step_id="reauth_confirm",
            description_placeholders={
                "device_name": self.reauth_entry.title
            },
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Get the options flow for this handler."""
        return OptionsFlowHandler(config_entry)


class OptionsFlowHandler(config_entries.OptionsFlow):
    """Handle an options flow for climate_ip."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self._config_entry = config_entry

    async def async_step_init(
        self, user_input: Dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage the options."""
        if user_input is not None:
            # If using aiohttp, token must be present.
            # We check against the merged data (current data + new user_input).
            if user_input.get(CONF_CONN_METHOD) == CONN_METHOD_AIOHTTP and not self.config_entry.data.get(CONF_TOKEN):
                return self.async_show_form(
                    step_id="init",
                    data_schema=self._get_options_schema(),
                    errors={"base": "token_required_for_aiohttp"},
                )
            return self.async_create_entry(title="", data=user_input)

        return self.async_show_form(
            step_id="init",
            data_schema=self._get_options_schema(),
        )

    def _get_options_schema(self) -> vol.Schema:
        """Return the schema for the options flow."""
        schema_dict = {}

        # Only show the connection method selector for supported devices (Modern 8888 + SmartThings).
        if self.config_entry.data.get(CONF_DEVICE_TYPE) in DEVICE_TYPE_AIOHTTP_SUPPORTED:
            schema_dict[vol.Required(
                CONF_CONN_METHOD, 
                default=self.config_entry.options.get(CONF_CONN_METHOD, CONN_METHOD_REQUESTS)
            )] = SelectSelector(
                SelectSelectorConfig(
                    options=[
                        {"value": CONN_METHOD_REQUESTS, "label": "Legacy (requests)"},
                        {"value": CONN_METHOD_AIOHTTP, "label": "Modern (aiohttp)"},
                        {"value": CONN_METHOD_RAW, "label": "Robust (raw socket)"},
                    ],
                    mode=SelectSelectorMode.DROPDOWN,
                    translation_key="connection_method",
                )
            )

        # Get the current value for poll_interval from options, falling back to data, then to default.
        current_interval = self.config_entry.options.get(
            CONF_POLL_INTERVAL, self.config_entry.data.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
        )
        schema_dict[vol.Required(
            CONF_POLL_INTERVAL, default=current_interval
        )] = NumberSelector(
            NumberSelectorConfig(min=MIN_POLL_INTERVAL, max=MAX_POLL_INTERVAL, unit_of_measurement="seconds", mode=NumberSelectorMode.BOX)
        )

        return vol.Schema(schema_dict)

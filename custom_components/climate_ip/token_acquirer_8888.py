# climate_ip/token_acquirer_8888.py
"""Helper to acquire a token from modern Samsung AC units (port 8888) using asyncio."""

import asyncio
import json
import logging
import os
import ssl
import re
from typing import Optional

import aiohttp
from aiohttp import web

from .exceptions import TokenAcquisitionError
from .helpers import mask_sensitive_data

_LOGGER = logging.getLogger(__name__)

class SamsungTokenAcquirer8888:
    """Manages the token acquisition process for modern Samsung ACs using asyncio."""

    def __init__(self, hass, ac_ip: str, cert_path: str):
        self._hass = hass
        self._ac_ip = ac_ip

        # Resolve the certificate path.
        if cert_path and not os.path.dirname(cert_path):
            _LOGGER.debug(
                "Certificate path '%s' appears to be a filename. Resolving relative to integration directory.",
                cert_path,
            )
            self._cert_path = os.path.join(os.path.dirname(__file__), cert_path)
        else:
            self._cert_path = cert_path
        _LOGGER.debug("Final resolved certificate path for token acquirer: %s", self._cert_path)

        self._listener_ip = hass.config.api.local_ip
        self._listener_port = 8889
        self._ac_port = 8888
        self._token_received_event = asyncio.Event()
        self._received_token: Optional[str] = None
        
        # Asyncio web server components
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

    async def _handle_token_post(self, request: web.Request) -> web.Response:
        """Handle the POST request from the AC to deliver the token."""
        try:
            _LOGGER.debug("Token listener received POST request. Headers: %s", request.headers)
            
            # Read body
            # We use read() instead of json() initially to handle potential malformed data
            # or to log the raw data for debugging.
            raw_data = await request.read()
            decoded_data = raw_data.decode('utf-8', errors='ignore')
            
            # Try to mask sensitive data for logging
            try:
                json_data = json.loads(decoded_data)
                _LOGGER.debug("Token listener processed data: %s", mask_sensitive_data(json_data))
            except Exception:
                _LOGGER.debug("Token listener processed data: %s", mask_sensitive_data(decoded_data))

            token = None
            
            # STRATEGY 1: Try parsing valid JSON
            try:
                if decoded_data.strip():
                    # Find start of JSON if there's garbage prefix
                    json_start = decoded_data.find('{')
                    if json_start != -1:
                        json_candidate = decoded_data[json_start:]
                        data = json.loads(json_candidate)
                        token = data.get('DeviceToken')
            except Exception as json_err:
                _LOGGER.debug("Standard JSON parsing failed: %s", json_err)

            # STRATEGY 2: Regex fallback
            if not token:
                _LOGGER.debug("Attempting Regex token extraction.")
                match = re.search(r'DeviceToken["\s:]+([^"\s}]+)', decoded_data)
                if match:
                    token = match.group(1).strip('"')

            if token:
                _LOGGER.info("Token successfully received from AC unit.")
                self._received_token = token
                self._token_received_event.set()
                return web.Response(text='OK', status=200)
            else:
                _LOGGER.warning("POST request processed but no token could be extracted.")
                return web.Response(text='Token not found', status=400)

        except Exception as e:
            _LOGGER.error("Error handling POST request from AC: %s", e, exc_info=True)
            return web.Response(text='Internal Server Error', status=500)

    async def _start_listener_server(self) -> bool:
        """Starts the aiohttp listener server."""
        try:
            app = web.Application()
            app.router.add_post('/', self._handle_token_post)
            # Accept any path, as some ACs might post to /devicetoken/response or similar
            app.router.add_post('/{tail:.*}', self._handle_token_post)

            self._runner = web.AppRunner(app)
            await self._runner.setup()

            # Setup SSL for the server
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
            # Explicitly allow TLSv1 to support legacy devices (and emulator)
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1
            # Lower security level to allow legacy certificates (MD5/SHA1 signatures)
            try:
                ssl_context.set_ciphers('HIGH:!aNULL:!MD5:@SECLEVEL=0')
            except Exception as e:
                _LOGGER.warning("Failed to set ciphers with SECLEVEL=0: %s", e)

            # Run blocking I/O in executor to avoid blocking the event loop
            await self._hass.async_add_executor_job(
                ssl_context.load_cert_chain, self._cert_path
            )
            
            # Create the site
            self._site = web.TCPSite(self._runner, '0.0.0.0', self._listener_port, ssl_context=ssl_context)
            await self._site.start()
            
            _LOGGER.info("Async token listener server started on port %s", self._listener_port)
            return True
        except Exception as e:
            _LOGGER.error("Failed to start listener server: %s", e, exc_info=True)
            return False

    async def async_initiate_pairing(self) -> None:
        """
        Phase 1: Starts the local server and sends the initial token request to the AC.
        """
        if not await self._start_listener_server():
            raise TokenAcquisitionError("Failed to start the local listener server")

        # Give server a moment? With asyncio awaiting start(), it should be ready immediately.
        
        url = f"https://{self._ac_ip}:{self._ac_port}/devicetoken/request"
        headers = {'Host': f"{self._listener_ip}:{self._listener_port}"}
        payload = {"DeviceToken": "xxxxxxxxxxx"}

        _LOGGER.info("Requesting token from AC at %s:%s", self._ac_ip, self._ac_port)

        # Setup Client SSL Context
        # We need to be permissive with the AC's SSL implementation
        # Explicitly allow TLSv1 to fix [SSL: UNSUPPORTED_PROTOCOL]
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1
        ssl_context.set_ciphers('HIGH:!aNULL:!MD5:@SECLEVEL=0')
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # The AC expects us to present a certificate
        # Run blocking I/O in executor to avoid blocking the event loop
        await self._hass.async_add_executor_job(
            ssl_context.load_cert_chain, self._cert_path
        )

        try:
            async with aiohttp.ClientSession() as session:
                _LOGGER.debug(
                    "Sending pairing request to URL: %s, Headers: %s, Payload: %s",
                    url, headers, payload
                )
                
                async with session.post(
                    url, 
                    json=payload, 
                    headers=headers, 
                    ssl=ssl_context, 
                    timeout=30
                ) as response:
                    
                    resp_text = await response.text()
                    _LOGGER.debug(
                        "AC responded to pairing request with status %s and body: %s",
                        response.status, resp_text
                    )

                    if response.status != 200:
                        raise TokenAcquisitionError(
                            f"AC responded with non-200 status: {response.status} - {resp_text}"
                        )
                    
                    _LOGGER.info("Token request sent successfully to the AC unit")

        except aiohttp.ClientError as e:
            await self.async_close()
            raise TokenAcquisitionError(f"Failed to connect to AC: {e}")
        except Exception as e:
            await self.async_close()
            raise TokenAcquisitionError(f"Unexpected error during pairing request: {e}")

    async def async_wait_for_token(self) -> str:
        """
        Phase 2: Waits for the token to be received by the local server.
        """
        try:
            await asyncio.wait_for(self._token_received_event.wait(), timeout=60)
            if self._received_token:
                return self._received_token
            raise TokenAcquisitionError("Event was set but no token was stored")
        except asyncio.TimeoutError:
            raise TokenAcquisitionError("Timed out waiting for the AC to send the token")
        finally:
            await self.async_close()

    async def async_close(self) -> None:
        """Shuts down the listener server."""
        if self._runner:
            _LOGGER.info("Shutting down async token listener server")
            await self._runner.cleanup()
            self._runner = None
            self._site = None
# climate_ip/protocol_8888.py
"""
Pure Python library for Samsung AC (Port 8888).
Supports Raw Sockets and SSL auto-negotiation (Legacy vs Modern).
"""
import asyncio
import json
import logging
import ssl
from functools import partial
from typing import Optional, Tuple, Dict

from .exceptions import AuthError
from .helpers import mask_sensitive_data

_LOGGER = logging.getLogger(__name__)

class ProtocolError(Exception):
    pass

class ConnectionError(ProtocolError):
    pass

class AuthError(ProtocolError):
    pass

class Samsung8888Client:
    """Robust asynchronous client with multi-SSL support."""

    def __init__(
        self,
        host: str,
        port: int = 8888,
        cert_path: Optional[str] = None,
        log_prefix: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.cert_path = cert_path
        self.log_prefix = log_prefix or f"[{host}]"
        self._ssl_context: Optional[ssl.SSLContext] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()

    async def _create_ssl_context(self) -> ssl.SSLContext:
        """Configure SSL, replicating the logic from connection_request.py."""
        # Always use Legacy mode (TLSv1) as it is the most compatible
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
        
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # Use ALL:@SECLEVEL=0 as in the original code for maximum compatibility
        ctx.set_ciphers("ALL:@SECLEVEL=0")
        
        # --- START OF FIX: Optimize SSL for low-memory devices ---
        # Disable session tickets and compression to reduce handshake overhead
        try:
            applied_opts = []
            op_no_ticket = getattr(ssl, "OP_NO_TICKET", 0)
            if op_no_ticket:
                ctx.options |= op_no_ticket
                if ctx.options & op_no_ticket:
                    applied_opts.append("OP_NO_TICKET")

            op_no_compression = getattr(ssl, "OP_NO_COMPRESSION", 0)
            if op_no_compression:
                ctx.options |= op_no_compression
                if ctx.options & op_no_compression:
                    applied_opts.append("OP_NO_COMPRESSION")

            if applied_opts:
                _LOGGER.debug("%s SSL Optimizations enabled: %s", self.log_prefix, ", ".join(applied_opts))
        except Exception:
            pass # Ignore if options not supported
        # --- END OF FIX ---

        if self.cert_path:
            try:
                # Use run_in_executor to avoid blocking the event loop
                loop = asyncio.get_running_loop()
                load_chain_func = partial(ctx.load_cert_chain, self.cert_path)
                await loop.run_in_executor(None, load_chain_func)
            except Exception as e:
                _LOGGER.warning(
                    "%s Error loading certificate %s: %s",
                    self.log_prefix,
                    self.cert_path,
                    e,
                )
        
        return ctx

    async def connect(self):
        if self._writer:
            return
        if not self._ssl_context:
            self._ssl_context = await self._create_ssl_context()
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self.host, self.port, ssl=self._ssl_context
            )
        except Exception as e:
            raise ConnectionError(f"Connection error: {e}")

    async def close(self):
        if self._writer:
            try:
                _LOGGER.debug("%s [Samsung8888Client] Closing socket writer...", self.log_prefix)
                self._writer.close()
                await self._writer.wait_closed()
            except Exception as e:
                _LOGGER.debug("%s [Samsung8888Client] Error closing writer: %s", self.log_prefix, e)
        self._writer = None
        self._reader = None

    async def request(self, method: str, path: str, body: Optional[Dict] = None, headers: Optional[Dict] = None) -> Tuple[Optional[str], Optional[str]]:
        # Debug Logging for Lock Contention
        # _LOGGER.debug("%s [Lock] Requesting lock for: %s %s", self.log_prefix, method, path)
        async with self._lock:
            # _LOGGER.debug("%s [Lock] Acquired lock for: %s %s", self.log_prefix, method, path)
            try:
                # Force cleanup if the reader is potentially corrupted (internal check)
                if self._reader and hasattr(self._reader, "_waiter") and self._reader._waiter is not None:
                     _LOGGER.warning("%s [Concurrency] Reader has pending waiter! Forced close.", self.log_prefix)
                     await self.close()

                retry = True
                while retry:
                    retry = False
                    try:
                        await self.connect()
                        
                        # Use local non-Optional references to satisfy type checkers
                        writer = self._writer
                        reader = self._reader
                        if writer is None or reader is None:
                            raise ConnectionError("No connection established")
                        
                        # Manual HTTP construction to avoid header errors
                        req = [f"{method} {path} HTTP/1.1", f"Host: {self.host}:{self.port}", "Connection: keep-alive"]
                        if headers:
                            for k, v in headers.items():
                                req.append(f"{k}: {v}")
                        
                        payload = json.dumps(body) if body else ""
                        if payload:
                            req.append(f"Content-Length: {len(payload)}")
                        else:
                            req.append("Content-Length: 0")
                        
                        request_str = "\r\n".join(req) + "\r\n\r\n" + (payload if payload else "")
                        writer.write(request_str.encode('utf-8'))
                        await writer.drain()
                        
                        status_line = await reader.readline()
                        if not status_line:
                            raise ConnectionResetError("Remote closure")
                        
                        try:
                            status_code = int(status_line.decode('utf-8', 'ignore').strip().split(' ')[1])
                        except Exception:
                            raise ConnectionError(f"Invalid status: {status_line!r}")
                        
                        # Read headers
                        headers_received = []
                        content_length = 0
                        content_type = ""
                        while True:
                            line = await reader.readline()
                            if not line or line == b'\r\n':
                                break
                            line_str = line.decode('utf-8', 'ignore').strip()
                            headers_received.append(line_str)
                            line_lower = line_str.lower()
                            if 'content-length' in line_lower:
                                try:
                                    content_length = int(line_lower.split(':', 1)[1].strip())
                                except Exception:
                                    pass
                            if 'content-type' in line_lower:
                                try:
                                    content_type = line_lower.split(':', 1)[1].strip()
                                except Exception:
                                    pass
    
                        # Read body
                        resp_body = ""
                        if content_length > 0:
                            try:
                                resp_body = (await reader.readexactly(content_length)).decode('utf-8', 'ignore')
                            except Exception:
                                resp_body = ""
                        elif content_length == 0 and "content-length" in [h.lower().split(':')[0] for h in headers_received]:
                             # Explicitly 0, do not read
                             resp_body = ""
                        else:
                            # Fallback: read until closed or timeout and try to assemble complete JSON
                            buffer = b""
                            timeout_task = asyncio.create_task(asyncio.sleep(2.0))
                            read_task = asyncio.create_task(reader.read(8192))
                            try:
                                while True:
                                    done, pending = await asyncio.wait(
                                        [timeout_task, read_task],
                                        return_when=asyncio.FIRST_COMPLETED
                                    )
                                    if timeout_task in done:
                                        # Timeout - give up reading more
                                        for task in pending:
                                            task.cancel()
                                        break
                                    if read_task in done:
                                        chunk = read_task.result()
                                        if not chunk:
                                            # Connection closed or no more data
                                            break
                                        buffer += chunk
                                        # try parse as JSON; if fails, keep reading
                                        try:
                                            resp_body_candidate = buffer.decode('utf-8', 'ignore')
                                            json.loads(resp_body_candidate)
                                            resp_body = resp_body_candidate
                                            break
                                        except (json.JSONDecodeError, UnicodeDecodeError):
                                            # Not complete yet, keep reading
                                            read_task = asyncio.create_task(reader.read(8192))
                                            continue
                            finally:
                                # Cancel any pending tasks
                                for t in (timeout_task, read_task):
                                    if not t.done():
                                        t.cancel()
    
                        _LOGGER.debug("%s Headers received: %s", self.log_prefix, headers_received)
                        _LOGGER.debug("%s Content-Length: %d, Content-Type: %s", self.log_prefix, content_length, content_type)
                        # Compact JSON for logging
                        log_body = resp_body.replace("\r", "").replace("\n", "")
                        try:
                            # Try to compact JSON if possible
                            json_obj = json.loads(resp_body)
                            # Mask sensitive data before logging
                            masked_obj = mask_sensitive_data(json_obj)
                            log_body = json.dumps(masked_obj, separators=(',', ':'))
                        except Exception:
                            pass # Keep original cleaned string if not valid JSON
                        
                        _LOGGER.debug("%s Response body received: '%s'", self.log_prefix, log_body)
    
                        if status_code == 401:
                            raise AuthError("401 Unauthorized")
                        if status_code not in (200, 204):
                            return None, f"HTTP {status_code}: {resp_body}"
    
                        return resp_body, None
    
                    except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
                        await self.close()
                        if not retry:
                            retry = True
                            continue
                        else:
                            raise ConnectionError("Unstable connection")
                    except AuthError:
                        await self.close()
                        raise
                    except Exception as e:
                        await self.close()
                        if "ssl" in str(e).lower():
                            raise ConnectionError(f"Error SSL: {e}")
                        raise ConnectionError(f"Unexpected error: {e}")
            finally:
                pass # Just ensuring cleanup if needed
                
            # If we exit the retry loop without returning, return a tuple explicitly
            return None, "No response"

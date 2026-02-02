#!/usr/bin/env python3
"""
Rudimentary HTTP CONNECT proxy server for tunneling HTTPS connections.
Preserves client certificates by creating a transparent TCP tunnel.

This is a testing tool itself, not a part of the package, so it is not tested.
Start a proxy server, then add `proxy-url` to `~/.kube/config` (note HTTP):

.. code-block: yaml

    clusters:
    - name: …
      cluster:
        server: …
        proxy-url: http://127.0.0.1:8080

Or via the CLI:

    kubectl config get-contexts
    cluster=k3d-k3s-default
    kubectl config set-cluster $cluster --proxy-url=http://127.0.0.1:8080
    kubectl config set-cluster $cluster --proxy-url=  # unset

Written by AI (then adapted to the usage protocol). The prompt is:
Write a simplistic proxy server in Python that serves the CONNECT requests only.
Make it asyncio-based. Listen on 127.0.0.1:8080.
"""
import asyncio
from typing import NamedTuple


class ProxiedRequest(NamedTuple):
    client_host: str
    client_port: int
    server_host: str
    server_port: int


class ProxyServer:
    requests: list[ProxiedRequest]
    host: str
    port: int | None

    def __init__(
            self,
            *,
            host: str = '127.0.0.1',
            port: int | None = None,
    ) -> None:
        super().__init__()
        self.requests = []
        self.host = host
        self.port = port

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    async def run(self) -> None:
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addr = server.sockets[0].getsockname()
        self.host = addr[0]
        self.port = addr[1]

        print(f"[*] CONNECT Proxy Server listening on {addr[0]}:{addr[1]}")
        print(f"[*] Press Ctrl+C to stop or cancel programmatically.")

        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        """Handle incoming client connection."""
        addr = writer.get_extra_info('peername')
        print(f"[+] Connection from {addr}")

        try:
            # Read the CONNECT request
            request_line = await reader.readline()
            request = request_line.decode('utf-8').strip()

            if not request.startswith('CONNECT'):
                print(f"[-] Non-CONNECT request from {addr}: {request}")
                writer.write(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

            # Parse CONNECT request: CONNECT host:port HTTP/1.1
            parts = request.split()
            if len(parts) < 2:
                print(f"[-] Malformed CONNECT request from {addr}")
                writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

            target = parts[1]  # host:port

            # Read and discard headers
            while True:
                header = await reader.readline()
                if header == b'\r\n' or header == b'\n' or header == b'':
                    break

            # Parse target host and port
            if ':' in target:
                host, port_str = target.rsplit(':', 1)
                try:
                    port = int(port_str)
                except ValueError:
                    port = 443
            else:
                host = target
                port = 443

            print(f"[*] Connecting to {host}:{port}")

            # Connect to the target server
            try:
                target_reader, target_writer = await asyncio.open_connection(host, port)
            except Exception as e:
                print(f"[-] Failed to connect to {host}:{port} - {e}")
                writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

            # Send success response to client
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()

            print(f"[+] Tunnel established: {addr} <-> {host}:{port}")
            self.requests.append(ProxiedRequest(addr[0], addr[1], host, port))

            # Run both directions concurrently
            await asyncio.gather(
                self.forward(reader, target_writer, "client->target"),
                self.forward(target_reader, writer, "target->client"),
                return_exceptions=True
            )

            print(f"[-] Tunnel closed: {addr} <-> {host}:{port}")

        except Exception as e:
            print(f"[-] Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def forward(self, src, dst, direction):
        """Bidirectional data forwarding"""
        try:
            while True:
                data = await src.read(8192)
                if not data:
                    break
                dst.write(data)
                await dst.drain()
        except Exception as e:
            print(f"[!] Error in {direction}: {e}")
            raise
        finally:
            dst.close()
            await dst.wait_closed()


if __name__ == '__main__':
    asyncio.run(ProxyServer(port=8080).run())

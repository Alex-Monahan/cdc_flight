"""A TCP relay that can be *blackholed* — the network fault rubric 1.7 needs.

Killing a backend, dropping a slot or stopping the cluster are all things the
suite already does, and none of them is a network fault: they all produce a prompt
error. The dangerous shape - the one rubric 4.6 calls "silently-dead-connection"
and the one that made a blackholed Postgres exit `ok: true` - is packets that stop
arriving while both sockets stay open. Nothing notices until a timeout somewhere
expires, and if nothing has a timeout, nothing ever notices.

Reproducing that normally needs `pf`/`iptables` and root. This does it in-process:
the pipeline is pointed at `127.0.0.1:<relay port>`, the relay forwards bytes to
the real Postgres, and `blackhole()` makes it stop forwarding **without closing
anything**. Existing connections hang; new connections are accepted and then
ignored, which is exactly what a black-holing firewall looks like from the client.

Deliberately not a fixture: `tests/1.7_fault_injection/` and any later suite that
needs a severed source (4.5/4.6) share it.
"""

from __future__ import annotations

import contextlib
import socket
import threading


class TcpRelay:
    """Forward `127.0.0.1:<port>` to `(host, target_port)` until blackholed."""

    def __init__(self, host: str, target_port: int, *, backlog: int = 32):
        self.target = (host, target_port)
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(backlog)
        self.port = self._listener.getsockname()[1]
        self._stop = threading.Event()
        self._blackholed = threading.Event()
        self._lock = threading.Lock()
        self.bytes_relayed = 0
        self.connections = 0
        self._threads: list[threading.Thread] = []
        self._sockets: list[socket.socket] = []

    # -- lifecycle ---------------------------------------------------------- #
    def start(self) -> TcpRelay:
        thread = threading.Thread(target=self._accept_loop, name="tcp-relay", daemon=True)
        thread.start()
        self._threads.append(thread)
        return self

    def blackhole(self) -> None:
        """Stop forwarding bytes. Sockets stay open; nothing is reset."""
        self._blackholed.set()

    def heal(self) -> None:
        self._blackholed.clear()

    @property
    def blackholed(self) -> bool:
        return self._blackholed.is_set()

    def stop(self) -> None:
        self._stop.set()
        for sock in [self._listener, *self._sockets]:
            with contextlib.suppress(OSError):
                sock.close()

    # -- internals ---------------------------------------------------------- #
    def _accept_loop(self) -> None:
        self._listener.settimeout(0.25)
        while not self._stop.is_set():
            try:
                client, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            with self._lock:
                self.connections += 1
                self._sockets.append(client)
            thread = threading.Thread(
                target=self._serve, args=(client,), name="tcp-relay-conn", daemon=True
            )
            thread.start()
            self._threads.append(thread)

    def _serve(self, client: socket.socket) -> None:
        if self._blackholed.is_set():
            # Accepted and then ignored: a client that connects *during* the
            # blackhole must hang, not be refused. A refusal is a prompt error and
            # would not exercise the silently-dead path at all.
            self._park(client)
            return
        try:
            upstream = socket.create_connection(self.target, timeout=5)
        except OSError:
            client.close()
            return
        with self._lock:
            self._sockets.append(upstream)
        for src, dst in ((client, upstream), (upstream, client)):
            thread = threading.Thread(
                target=self._pump, args=(src, dst), name="tcp-relay-pump", daemon=True
            )
            thread.start()
            self._threads.append(thread)

    def _park(self, sock: socket.socket) -> None:
        sock.settimeout(0.25)
        while not self._stop.is_set():
            try:
                if not sock.recv(65536):
                    return
            except TimeoutError:
                continue
            except OSError:
                return

    def _pump(self, src: socket.socket, dst: socket.socket) -> None:
        src.settimeout(0.25)
        while not self._stop.is_set():
            if self._blackholed.is_set():
                # Neither read nor write: the bytes already in flight are simply
                # never delivered, and both ends stay ESTABLISHED.
                self._stop.wait(0.25)
                continue
            try:
                chunk = src.recv(65536)
            except TimeoutError:
                continue
            except OSError:
                break
            if not chunk:
                break
            with self._lock:
                self.bytes_relayed += len(chunk)
            try:
                dst.sendall(chunk)
            except OSError:
                break
        for sock in (src, dst):
            with contextlib.suppress(OSError):
                sock.close()

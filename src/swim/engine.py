import os
import mmap
import time
import struct
import socket
import select
import random
import threading
import concurrent.futures
from typing import Dict, List, Tuple, Optional, Set, Any

class SystemError(Exception):
    pass

class MemoryArena:
    def __init__(self, size: int):
        self.size = size
        self.fd = -1
        self.buffer = mmap.mmap(-1, size, mmap.MAP_SHARED | mmap.MAP_ANONYMOUS, mmap.PROT_READ | mmap.PROT_WRITE)
        self.offset = 0
        self.lock = threading.Lock()

    def allocate(self, data: bytes) -> int:
        length = len(data)
        with self.lock:
            if self.offset + length > self.size:
                raise SystemError("MemoryArena exhausted")
            addr = self.offset
            self.buffer[addr:addr+length] = data
            self.offset += length
            return addr

    def read(self, addr: int, length: int) -> bytes:
        with self.lock:
            if addr + length > self.size:
                raise SystemError("MemoryArena out of bounds read")
            return bytes(self.buffer[addr:addr+length])

    def reset(self):
        with self.lock:
            self.buffer.seek(0)
            self.buffer.write(b'\x00' * self.size)
            self.offset = 0

    def close(self):
        self.buffer.close()

class RingBuffer:
    def __init__(self, capacity: int, element_size: int):
        self.capacity = capacity
        self.element_size = element_size
        self.arena = MemoryArena(capacity * element_size)
        self.head = 0
        self.tail = 0
        self.count = 0
        self.lock = threading.Lock()

    def push(self, data: bytes) -> bool:
        if len(data) > self.element_size:
            raise SystemError("Data exceeds ring buffer element size")
        padded = data.ljust(self.element_size, b'\x00')
        with self.lock:
            if self.count >= self.capacity:
                return False
            addr = self.tail * self.element_size
            self.arena.buffer[addr:addr+self.element_size] = padded
            self.tail = (self.tail + 1) % self.capacity
            self.count += 1
            return True

    def pop(self) -> Optional[bytes]:
        with self.lock:
            if self.count == 0:
                return None
            addr = self.head * self.element_size
            data = bytes(self.arena.buffer[addr:addr+self.element_size])
            self.head = (self.head + 1) % self.capacity
            self.count -= 1
            return data.rstrip(b'\x00')

    def __len__(self) -> int:
        with self.lock:
            return self.count

class TicketLock:
    def __init__(self):
        self.ticket_counter = 0
        self.turn = 0
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            my_ticket = self.ticket_counter
            self.ticket_counter += 1
        while True:
            with self.lock:
                if self.turn == my_ticket:
                    return
            time.sleep(0.0001)

    def release(self):
        with self.lock:
            self.turn += 1

class ReaderWriterLock:
    def __init__(self):
        self.read_count = 0
        self.wlock = threading.Lock()
        self.rlock = threading.Lock()
        self.resource_lock = threading.Lock()

    def acquire_read(self):
        with self.rlock:
            with self.read_count_lock if hasattr(self, 'read_count_lock') else threading.Lock():
                pass
        with self.resource_lock:
            self.read_count += 1
            if self.read_count == 1:
                self.wlock.acquire()

    def release_read(self):
        with self.resource_lock:
            self.read_count -= 1
            if self.read_count == 0:
                self.wlock.release()

    def acquire_write(self):
        self.wlock.acquire()

    def release_write(self):
        self.wlock.release()

class VectorClock:
    def __init__(self, clocks: Optional[Dict[str, int]] = None):
        self.clocks = dict(clocks) if clocks else {}
        self.lock = threading.Lock()

    def clone(self) -> 'VectorClock':
        with self.lock:
            return VectorClock(self.clocks)

    def increment(self, node_id: str):
        with self.lock:
            self.clocks[node_id] = self.clocks.get(node_id, 0) + 1

    def merge(self, other: 'VectorClock'):
        with self.lock:
            with other.lock:
                all_keys = set(self.clocks.keys()).union(set(other.clocks.keys()))
                for k in all_keys:
                    self.clocks[k] = max(self.clocks.get(k, 0), other.clocks.get(k, 0))

    def dominates(self, other: 'VectorClock') -> bool:
        with self.lock:
            with other.lock:
                keys = set(self.clocks.keys()).union(set(other.clocks.keys()))
                strictly_greater = False
                for k in keys:
                    v1 = self.clocks.get(k, 0)
                    v2 = other.clocks.get(k, 0)
                    if v1 < v2:
                        return False
                    if v1 > v2:
                        strictly_greater = True
                return strictly_greater

    def to_bytes(self) -> bytes:
        with self.lock:
            items = sorted(self.clocks.items())
            fmt = "!H"
            data = struct.pack(fmt, len(items))
            for k, v in items:
                kb = k.encode('utf-8')
                data += struct.pack("!H", len(kb)) + kb + struct.pack("!Q", v)
            return data

    @staticmethod
    def from_bytes(b: bytes) -> Tuple['VectorClock', int]:
        offset = 0
        count, = struct.unpack_from("!H", b, offset)
        offset += 2
        clocks = {}
        for _ in range(count):
            k_len, = struct.unpack_from("!H", b, offset)
            offset += 2
            kb = b[offset:offset+k_len]
            offset += k_len
            v, = struct.unpack_from("!Q", b, offset)
            offset += 8
            clocks[kb.decode('utf-8')] = v
        return VectorClock(clocks), offset

class NodeDescriptor:
    def __init__(self, node_id: str, host: str, port: int, incarnation: int, state: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.incarnation = incarnation
        self.state = state  # 0: Alive, 1: Suspended, 2: Dead, 3: Left
        self.vector_clock = VectorClock({node_id: incarnation})
        self.suspicion_ttl = 0.0
        self.last_updated = time.time()

    def serialize(self) -> bytes:
        id_bytes = self.node_id.encode('utf-8')
        host_bytes = self.host.encode('utf-8')
        vc_bytes = self.vector_clock.to_bytes()
        fmt = f"!H{len(id_bytes)}sH{len(host_bytes)}sHBIQd"
        header = struct.pack("!H", len(id_bytes))
        data = id_bytes + struct.pack("!H", len(host_bytes)) + host_bytes + struct.pack("!HHIQd", self.port, self.incarnation, self.state, int(self.suspicion_ttl * 1000), self.last_updated)
        return header + data + struct.pack("!H", len(vc_bytes)) + vc_bytes

    @staticmethod
    def deserialize(b: bytes) -> Tuple['NodeDescriptor', int]:
        offset = 0
        id_len, = struct.unpack_from("!H", b, offset)
        offset += 2
        id_bytes = b[offset:offset+id_len]
        offset += id_len
        host_len, = struct.unpack_from("!H", b, offset)
        offset += 2
        host_bytes = b[offset:offset+host_len]
        offset += host_len
        port, incarnation, state, ttl_ms, last_updated = struct.unpack_from("!HBIQd", b, offset)
        offset += struct.calcsize("!HBIQd")
        vc_len, = struct.unpack_from("!H", b, offset)
        offset += 2
        vc_bytes = b[offset:offset+vc_len]
        offset += vc_len
        vc, _ = VectorClock.from_bytes(vc_bytes)
        node = NodeDescriptor(id_bytes.decode('utf-8'), host_bytes.decode('utf-8'), port, incarnation, state)
        node.vector_clock = vc
        node.suspicion_ttl = ttl_ms / 1000.0
        node.last_updated = last_updated
        return node, offset

class BoundedChannel:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.queue = []
        self.lock = threading.Lock()
        self.not_empty = threading.Condition(self.lock)

    def send(self, item: Any) -> bool:
        with self.lock:
            if len(self.queue) >= self.capacity:
                self.queue.pop(0)
            self.queue.append(item)
            self.not_empty.notify()
            return True

    def recv(self, timeout: Optional[float] = None) -> Any:
        with self.lock:
            if not self.queue:
                self.not_empty.wait(timeout)
            if not self.queue:
                raise SystemError("Channel receive timeout")
            return self.queue.pop(0)

class SwimEngine:
    ALIVE = 0
    SUSPENDED = 1
    DEAD = 2
    LEFT = 3

    def __init__(self, node_id: str, host: str, port: int, seeds: List[Tuple[str, int]]):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.seeds = seeds
        self.incarnation = 0
        self.state = self.ALIVE
        self.vector_clock = VectorClock({node_id: 0})
        self.members: Dict[str, NodeDescriptor] = {}
        self.members_lock = ReaderWriterLock()
        self.piggyback_queue = RingBuffer(1024, 512)
        self.telemetry_arena = MemoryArena(65536)
        self.inbound_channel = BoundedChannel(2048)
        self.running = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((host, port))
        self.socket.setblocking(False)
        self.epoll = select.epoll()
        self.epoll.register(self.socket.fileno(), select.EPOLLIN | select.EPOLLET)
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._event_loop, daemon=True)
        self.dispatcher_thread = threading.Thread(target=self._dispatcher_loop, daemon=True)
        self.sweeper_thread = threading.Thread(target=self._sweeper_loop, daemon=True)
        self_node = NodeDescriptor(node_id, host, port, self.incarnation, self.ALIVE)
        self.members[node_id] = self_node

    def start(self):
        self.running = True
        self.thread.start()
        self.dispatcher_thread.start()
        self.sweeper_thread.start()
        for seed_host, seed_port in self.seeds:
            if seed_host != self.host or seed_port != self.port:
                self._send_ping(seed_host, seed_port)

    def stop(self):
        self.running = False
        try:
            self.epoll.unregister(self.socket.fileno())
        except Exception:
            pass
        self.epoll.close()
        self.socket.close()
        self.telemetry_arena.close()

    def _event_loop(self):
        while self.running:
            try:
                events = self.epoll.poll(0.1)
                for fileno, event in events:
                    if fileno == self.socket.fileno():
                        try:
                            data, addr = self.socket.recvfrom(65535)
                            self.inbound_channel.send((data, addr))
                        except BlockingIOError:
                            pass
            except Exception:
                if not self.running:
                    break

    def _dispatcher_loop(self):
        while self.running:
            try:
                packet, addr = self.inbound_channel.recv(timeout=0.1)
                self._handle_packet(packet, addr)
            except SystemError:
                continue
            except Exception:
                if not self.running:
                    break

    def _sweeper_loop(self):
        while self.running:
            time.sleep(1.0)
            self._do_periodic_sweep()

    def _send_packet(self, host: str, port: int, msg_type: int, payload: bytes):
        piggyback_data = b""
        while len(piggyback_data) < 256:
            msg = self.piggyback_queue.pop()
            if not msg:
                break
            if len(piggyback_data) + len(msg) + 2 > 256:
                self.piggyback_queue.push(msg)
                break
            piggyback_data += struct.pack("!H", len(msg)) + msg

        header = struct.pack("!BHI", msg_type, len(payload), len(piggyback_data))
        packet = header + payload + piggyback_data
        try:
            self.socket.sendto(packet, (host, port))
        except Exception:
            pass

    def _send_ping(self, host: str, port: int):
        self.members_lock.acquire_read()
        try:
            node = self.members.get(self.node_id)
            payload = node.serialize() if node else b""
        finally:
            self.members_lock.release_read()
        self._send_packet(host, port, 1, payload)

    def _send_ack(self, host: str, port: int):
        self.members_lock.acquire_read()
        try:
            node = self.members.get(self.node_id)
            payload = node.serialize() if node else b""
        finally:
            self.members_lock.release_read()
        self._send_packet(host, port, 2, payload)

    def _send_ping_req(self, host: str, port: int, target_host: str, target_port: int):
        payload = struct.pack("!H", target_port) + target_host.encode('utf-8')
        self._send_packet(host, port, 3, payload)

    def _handle_packet(self, packet: bytes, addr: Tuple[str, int]):
        if len(packet) < 7:
            return
        msg_type, payload_len, piggyback_len = struct.unpack_from("!B
from __future__ annotations

import mmap
import struct
import time
import uuid
import threading
import atomic
from typing import Dict, List, Optional, Tuple, Set, Union
from enum import IntEnum

class NodeState(IntEnum):
    ALIVE = 0
    SUSPENDED = 1
    DEAD = 2
    LEFT = 3

class MessageType(IntEnum):
    PING = 1
    ACK = 2
    PING_REQ = 3
    SYNC = 4

class SwimError(Exception):
    pass

class BufferOverflowError(SwimError):
    pass

class InvalidPayloadError(SwimError):
    pass

class VectorClock:
    __slots__ = ('_clock_map',)

    def __init__(self, initial_map: Optional[Dict[str, int]] = None) -> None:
        if initial_map is None:
            self._clock_map: Dict[str, int] = {}
        else:
            self._clock_map = dict(initial_map)

    def clone(self) -> VectorClock:
        return VectorClock(self._clock_map)

    def increment(self, node_id: str) -> VectorClock:
        new_map = dict(self._clock_map)
        new_map[node_id] = new_map.get(node_id, 0) + 1
        return VectorClock(new_map)

    def merge(self, other: VectorClock) -> VectorClock:
        new_map = dict(self._clock_map)
        for k, v in other._clock_map.items():
            new_map[k] = max(new_map.get(k, 0), v)
        return VectorClock(new_map)

    def compare(self, other: VectorClock) -> int:
        keys = set(self._clock_map.keys()).union(set(other._clock_map.keys()))
        self_greater = False
        other_greater = False

        for k in keys:
            v_self = self._clock_map.get(k, 0)
            v_other = other._clock_map.get(k, 0)
            if v_self > v_other:
                self_greater = True
            elif v_self < v_other:
                other_greater = True

        if self_greater and not other_greater:
            return 1
        elif other_greater and not self_greater:
            return -1
        elif not self_greater and not other_greater:
            return 0
        else:
            return 2  # Concurrent / ambiguous

    def to_bytes(self) -> bytes:
        items = sorted(self._clock_map.items())
        buf = bytearray()
        buf.extend(struct.pack(">I", len(items)))
        for k, v in items:
            k_bytes = k.encode('utf-8')
            buf.extend(struct.pack(">H", len(k_bytes)))
            buf.extend(k_bytes)
            buf.extend(struct.pack(">Q", v))
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> Tuple[VectorClock, int]:
        if offset + 4 > len(data):
            raise InvalidPayloadError("Truncated vector clock payload")
        count, = struct.unpack_from(">I", data, offset)
        offset += 4
        clock_map = {}
        for _ in range(count):
            if offset + 2 > len(data):
                raise InvalidPayloadError("Truncated vector clock key length")
            k_len, = struct.unpack_from(">H", data, offset)
            offset += 2
            if offset + k_len + 8 > len(data):
                raise InvalidPayloadError("Truncated vector clock entry data")
            k_bytes = data[offset:offset+k_len]
            offset += k_len
            v, = struct.unpack_from(">Q", data, offset)
            offset += 8
            clock_map[k_bytes.decode('utf-8')] = v
        return cls(clock_map), offset


class ZeroCopyRingBuffer:
    def __init__(self, capacity_bytes: int) -> None:
        self.capacity_bytes = capacity_bytes
        self._mmap = mmap.mmap(-1, capacity_bytes)
        self._head = 0
        self._tail = 0
        self._lock = threading.Lock()

    def write(self, data: bytes) -> None:
        data_len = len(data)
        if data_len + 8 > self.capacity_bytes:
            raise BufferOverflowError("Data exceeds total ring buffer capacity")

        with self._lock:
            free_space = (self._tail - self._head - 1) % self.capacity_bytes
            if data_len + 8 > free_space and self._head != self._tail:
                raise BufferOverflowError("Ring buffer exhausted")

            header = struct.pack(">I", data_len)
            self._write_bytes(header)
            self._write_bytes(data)

    def read(self) -> Optional[bytes]:
        with self._lock:
            if self._head == self._tail:
                return None

            if (self._head + 4) % self.capacity_bytes == self._tail:
                raise InvalidPayloadError("Corrupted ring buffer state")

            header_bytes = self._read_bytes(4)
            data_len, = struct.unpack(">I", header_bytes)

            data = self._read_bytes(data_len)
            return data

    def _write_bytes(self, data: bytes) -> None:
        length = len(data)
        end_space = self.capacity_bytes - self._tail
        if length <= end_space:
            self._mmap[self._tail:self._tail + length] = data
            self._tail = (self._tail + length) % self.capacity_bytes
        else:
            self._mmap[self._tail:self.capacity_bytes] = data[:end_space]
            self._mmap[0:length - end_space] = data[end_space:]
            self._tail = length - end_space

    def _read_bytes(self, length: int) -> bytes:
        end_space = self.capacity_bytes - self._head
        if length <= end_space:
            data = bytes(self._mmap[self._head:self._head + length])
            self._head = (self._head + length) % self.capacity_bytes
        else:
            data = bytes(self._mmap[self._head:self.capacity_bytes]) + bytes(self._mmap[0:length - end_space])
            self._head = length - end_space
        return data

    def close(self) -> None:
        self._mmap.close()


class NodeDescriptor:
    __slots__ = ('node_id', 'host', 'port', 'state', 'incarnation', 'vector_clock', 'last_updated')

    FORMAT_STRING = "36s 64s H B Q d"
    STRUCT_SIZE = struct.calcsize(FORMAT_STRING)

    def __init__(self, node_id: str, host: str, port: int, state: NodeState, incarnation: int, vector_clock: VectorClock, last_updated: float) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.state = state
        self.incarnation = incarnation
        self.vector_clock = vector_clock
        self.last_updated = last_updated

    def to_bytes(self) -> bytes:
        nid_bytes = self.node_id.encode('utf-8').ljust(36, b'\x00')
        host_bytes = self.host.encode('utf-8').ljust(64, b'\x00')
        vc_bytes = self.vector_clock.to_bytes()
        vc_len = len(vc_bytes)
        
        base_header = struct.pack(
            self.FORMAT_STRING,
            nid_bytes,
            host_bytes,
            self.port,
            int(self.state),
            self.incarnation,
            self.last_updated
        )
        return struct.pack(">I", vc_len) + vc_bytes + base_header

    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> Tuple[NodeDescriptor, int]:
        if offset + 4 > len(data):
            raise InvalidPayloadError("Truncated descriptor length header")
        vc_len, = struct.unpack_from(">I", data, offset)
        offset += 4

        if offset + vc_len > len(data):
            raise InvalidPayloadError("Truncated vector clock bytes in descriptor")
        vector_clock, offset = VectorClock.from_bytes(data, offset)

        if offset + cls.STRUCT_SIZE > len(data):
            raise InvalidPayloadError("Truncated base node descriptor bytes")
        
        nid_bytes, host_bytes, port, state_val, incarnation, last_updated = struct.unpack_from(
            cls.FORMAT_STRING, data, offset
        )
        offset += cls.STRUCT_SIZE

        node_id = nid_bytes.rstrip(b'\x00').decode('utf-8')
        host = host_bytes.rstrip(b'\x00').decode('utf-8')
        state = NodeState(state_val)

        return cls(node_id, host, port, state, incarnation, vector_clock, last_updated), offset


class NodeDescriptorArena:
    def __init__(self, max_nodes: int) -> None:
        self.max_nodes = max_nodes
        sample_desc = NodeDescriptor("dummy", "localhost", 0, NodeState.ALIVE, 0, VectorClock(), 0.0)
        sample_bytes = sample_desc.to_bytes()
        self.slot_size = len(sample_bytes) + 4  # 4 bytes for valid flag
        self.capacity_bytes = self.slot_size * max_nodes
        self._mmap = mmap.mmap(-1, self.capacity_bytes)
        self._lock = threading.Lock()

    def set_node(self, index: int, desc: NodeDescriptor) -> None:
        if index < 0 or index >= self.max_nodes:
            raise IndexError("Arena index out of bounds")
        desc_bytes = desc.to_bytes()
        offset = index * self.slot_size
        with self._lock:
            self._mmap[offset:offset+4] = struct.pack(">I", 1)
            self._mmap[offset+4:offset+4+len(desc_bytes)] = desc_bytes

    def get_node(self, index: int) -> Optional[NodeDescriptor]:
        if index < 0 or index >= self.max_nodes:
            raise IndexError("Arena index out of bounds")
        offset = index * self.slot_size
        with self._lock:
            valid, = struct.unpack_from(">I", self._mmap, offset)
            if not valid:
                return None
            # Scan until next slot boundary to find exact size or read header safely
            # We store descriptor bytes. Let's read remainder of slot
            slot_data = bytes(self._mmap[offset+4:offset+self.slot_size])
        desc, _ = NodeDescriptor.from_bytes(slot_data, 0)
        return desc

    def clear_node(self, index: int) -> None:
        if index < 0 or index >= self.max_nodes:
            raise IndexError("Arena index out of bounds")
        offset = index * self.slot_size
        with self._lock:
            self._mmap[offset:offset+4] = struct.pack(">I", 0)

    def close(self) -> None:
        self._mmap.close()


class RWLock:
    def __init__(self) -> None:
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._readers = 0

    def acquire_read(self) -> None:
        with self._read_lock:
            self._readers += 1
            if self._readers == 1:
                self._write_lock.acquire()

    def release_read(self) -> None:
        with self._read_lock:
            self._readers -= 1
            if self._readers == 0:
                self._write_lock.release()

    def acquire_write(self) -> None:
        self._write_lock.acquire()

    def release_write(self) -> None:
        self._write_lock.release()


class BoundedChannelQueue:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self._queue: List[bytes] = []
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def put_drop_tail(self, item: bytes) -> bool:
        with self._lock:
            if len(self._queue) >= self.capacity:
                # Drop tail or drop oldest item
                return False
            self._queue.append(item)
            self._not_empty.notify()
            return True

    def get(self, timeout: Optional[float] = None) -> Optional[bytes]:
        with self._lock:
            if not self._queue:
                if timeout is None:
                    self._not_empty.wait()
                else:
                    if not self._not_empty.wait(timeout):
                        return None
            if not self._queue:
                return None
            item = self._queue.pop(0)
            self._not_full.notify()
            return item

    def qsize(self) -> int:
        with self._lock:
            return len(self._queue)


class SwimMessage:
    __slots__ = ('msg_type', 'sender_id', 'target_id', 'incarnation', 'vector_clock', 'piggyback_payloads')

    def __init__(self, msg_type: MessageType, sender_id: str, target_id: str, incarnation: int, vector_clock: VectorClock, piggyback_payloads: List[bytes]) -> None:
        self.msg_type = msg_type
        self.sender_id = sender_id
        self.target_id = target_id
        self.incarnation = incarnation
        self.vector_clock = vector_clock
        self.piggyback_payloads = piggyback_payloads

    def to_bytes(self, max_piggyback_bytes: int = 1024) -> bytes:
        sender_bytes = self.sender_id.encode('utf-8').ljust(36, b'\x00')
        target_bytes = self.target_id.encode('utf-8').ljust(36, b'\x00')
        vc_bytes = self.vector_clock.to_bytes()
        
        header = struct.pack(
            ">B 36s 36s Q I",
            int(self.msg_type),
            sender_bytes,
            target_bytes,
            self.incarnation,
            len(vc_bytes)
        )
        
        payload_buffer = bytearray()
        current_size = 0
        valid_piggybacks = 0
        for p in self.piggyback_payloads:
            if current_size + len(p) + 4 > max_piggyback_bytes:
                break
            payload_buffer.extend(struct.pack(">I", len(p)))
            payload_buffer.extend(p)
            current_size += len(p) + 4
            valid_piggybacks += 1

        piggyback_count_bytes = struct.pack(">I", valid_piggybacks)
        return header + vc_bytes + piggyback_count_bytes + bytes(payload_buffer)

    @classmethod
    def from_bytes(cls, data: bytes) -> SwimMessage:
        offset = 0
        if offset + 1 + 36 + 36 + 8 + 4 > len(data):
            raise InvalidPayloadError("Truncated SWIM message header")
        
        msg_type_val, sender_bytes, target_bytes, incarnation, vc_len = struct.unpack_from(
            ">B 36s 36s Q I", data, offset
        )
        offset += struct.calcsize(">B 36s 36s Q I")

        if offset + vc_len > len(data):
            raise InvalidPayloadError("Truncated vector
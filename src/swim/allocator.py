import mmap
import os
import sys
import ctypes
import threading
import atomic
from typing import Optional, List, Tuple

class AllocatorError(Exception):
    """Base exception for allocator faults."""
    pass

class OutOfMemoryError(AllocatorError):
    """Raised when the arena or ring buffer is exhausted."""
    pass

class InvalidAlignmentError(AllocatorError):
    """Raised when alignment requirements are violated."""
    pass

class ArenaBuffer:
    """
    Fixed-size memory arena backed by anonymous mmap for zero-copy operations
    and elimination of garbage collection pressure in hot paths.
    """
    def __init__(self, size_bytes: int, alignment: int = 64) -> None:
        if size_bytes <= 0:
            raise ValueError("Arena size must be greater than zero.")
        if (alignment & (alignment - 1)) != 0 or alignment <= 0:
            raise InvalidAlignmentError("Alignment must be a positive power of two.")

        self._alignment = alignment
        # Allocate extra space to guarantee alignment padding
        self._raw_size = size_bytes + alignment
        
        # Create an anonymous memory-mapped region
        self._mmap = mmap.mmap(-1, self._raw_size, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
        self._base_addr = ctypes.addressof(ctypes.c_char.from_buffer(self._mmap))
        
        # Calculate aligned starting address
        aligned_addr = (self._base_addr + (alignment - 1)) & ~(alignment - 1)
        self._aligned_offset = aligned_addr - self._base_addr
        
        self._capacity = size_bytes
        self._offset = 0
        self._lock = threading.Lock()

    def allocate(self, size: int) -> int:
        """
        Allocates a contiguous block of memory of the specified size within the arena.
        Returns the absolute memory address (integer) of the allocation.
        """
        if size <= 0:
            raise ValueError("Allocation size must be positive.")

        # Align allocation size to the cache line alignment boundary
        aligned_size = (size + (self._alignment - 1)) & ~(self._alignment - 1)

        with self._lock:
            if self._offset + aligned_size > self._capacity:
                raise OutOfMemoryError(
                    f"Arena capacity exhausted. Requested: {aligned_size}, Remaining: {self._capacity - self._offset}"
                )
            
            allocation_addr = self._base_addr + self._aligned_offset + self._offset
            self._offset += aligned_size
            return allocation_addr

    def reset(self) -> None:
        """Resets the arena allocation pointer, clearing all allocations instantly."""
        with self._lock:
            self._offset = 0

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def used(self) -> int:
        with self._lock:
            return self._offset

    @property
    def available(self) -> int:
        with self._lock:
            return self._capacity - self._offset

    def close(self) -> None:
        if self._mmap:
            self._mmap.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class ZeroCopyRingBuffer:
    """
    Lock-free, fixed-size ring buffer backed by mmap for high-throughput,
    zero-copy telemetry and message queue ingestion.
    """
    def __init__(self, capacity: int, element_size: int) -> None:
        if capacity <= 0 or (capacity & (capacity - 1)) != 0:
            raise ValueError("Ring buffer capacity must be a positive power of two.")
        if element_size <= 0:
            raise ValueError("Element size must be positive.")

        self._capacity = capacity
        self._element_size = element_size
        self._total_size = capacity * element_size

        self._mmap = mmap.mmap(-1, self._total_size, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
        self._base_addr = ctypes.addressof(ctypes.c_char.from_buffer(self._mmap))

        self._head = atomic.AtomicLong(0)
        self._tail = atomic.AtomicLong(0)

    def write(self, data: bytes) -> bool:
        """
        Writes data into the ring buffer. Fails and returns False if the buffer is full
        (backpressure drop-tail semantics).
        """
        if len(data) > self._element_size:
            raise ValueError(f"Data size {len(data)} exceeds element size {self._element_size}")

        head = self._head.load()
        tail = self._tail.load()

        if tail - head >= self._capacity:
            return False  # Drop-tail backpressure

        slot = tail & (self._capacity - 1)
        dest_addr = self._base_addr + (slot * self._element_size)

        # Copy bytes directly into mmapped region
        ctypes.memmove(dest_addr, data, len(data))

        self._tail.store(tail + 1)
        return True

    def read(self) -> Optional[bytes]:
        """
        Reads and removes the oldest data block from the ring buffer.
        Returns None if the buffer is empty.
        """
        head = self._head.load()
        tail = self._tail.load()

        if head == tail:
            return None  # Buffer is empty

        slot = head & (self._capacity - 1)
        src_addr = self._base_addr + (slot * self._element_size)

        # Extract bytes directly from mmapped region
        data = ctypes.string_at(src_addr, self._element_size)

        self._head.store(head + 1)
        return data

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def size(self) -> int:
        return self._tail.load() - self._head.load()

    def close(self) -> None:
        if self._mmap:
            self._mmap.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class CacheAlignedNodeArray:
    """
    Dense contiguous array of cache-aligned blocks representing node descriptors
    for efficient periodic cluster sweeps.
    """
    def __init__(self, max_nodes: int, descriptor_size: int, cache_line_size: int = 64) -> None:
        if max_nodes <= 0:
            raise ValueError("Max nodes must be positive.")
        if descriptor_size <= 0:
            raise ValueError("Descriptor size must be positive.")

        # Ensure stride is a multiple of cache_line_size
        self._stride = ((descriptor_size + cache_line_size - 1) // cache_line_size) * cache_line_size
        self._max_nodes = max_nodes
        self._total_size = self._stride * max_nodes

        self._mmap = mmap.mmap(-1, self._total_size, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
        self._base_addr = ctypes.addressof(ctypes.c_char.from_buffer(self._mmap))
        self._lock = threading.Lock()

    def get_descriptor_address(self, index: int) -> int:
        """Returns the memory address of the node descriptor at the given index."""
        if index < 0 or index >= self._max_nodes:
            raise IndexError(f"Node index {index} out of bounds for max_nodes {self._max_nodes}")
        return self._base_addr + (index * self._stride)

    def write_descriptor(self, index: int, data: bytes) -> None:
        """Writes raw descriptor bytes into the cache-aligned slot."""
        if len(data) > self._stride:
            raise ValueError(f"Descriptor data length {len(data)} exceeds cache-aligned stride {self._stride}")
        addr = self.get_descriptor_address(index)
        with self._lock:
            ctypes.memset(addr, 0, self._stride)
            ctypes.memmove(addr, data, len(data))

    def read_descriptor(self, index: int, length: int) -> bytes:
        """Reads raw descriptor bytes from the cache-aligned slot."""
        if length > self._stride:
            raise ValueError(f"Requested length {length} exceeds cache-aligned stride {self._stride}")
        addr = self.get_descriptor_address(index)
        with self._lock:
            return ctypes.string_at(addr, length)

    @property
    def max_nodes(self) -> int:
        return self._max_nodes

    @property
    def stride(self) -> int:
        return self._stride

    def close(self) -> None:
        if self._mmap:
            self._mmap.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class SwimAllocatorSystem:
    """
    Unified high-performance memory allocator and buffer management subsystem
    for swim-gossip-core.
    """
    def __init__(self, arena_size: int = 1024 * 1024 * 16, ring_capacity: int = 4096, max_nodes: int = 1024) -> None:
        self._arena = ArenaBuffer(arena_size)
        self._telemetry_ring = ZeroCopyRingBuffer(capacity=ring_capacity, element_size=256)
        self._node_array = CacheAlignedNodeArray(max_nodes=max_nodes, descriptor_size=128)

    @property
    def arena(self) -> ArenaBuffer:
        return self._arena

    @property
    def telemetry_ring(self) -> ZeroCopyRingBuffer:
        return self._telemetry_ring

    @property
    def node_array(self) -> CacheAlignedNodeArray:
        return self._node_array

    def shutdown(self) -> None:
        self._arena.close()
        self._telemetry_ring.close()
        self._node_array.close()
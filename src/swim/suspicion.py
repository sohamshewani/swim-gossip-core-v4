import os
import mmap
import time
import math
import struct
import random
import threading
import atomic
from typing import Dict, List, Optional, Tuple, Set

class SuspicionConfig:
    """Configuration parameters for the multi-phase suspicion detector."""
    def __init__(self, cluster_size: int, base_timeout: float = 1.0, k_intermediaries: int = 3):
        self.cluster_size = max(1, cluster_size)
        self.base_timeout = base_timeout
        self.k_intermediaries = k_intermediaries
        # Randomized TTL decay proportional to cluster size and logarithmic scaling
        self.suspicion_timeout = base_timeout * (1.0 + math.log2(float(self.cluster_size)))

class VectorClock:
    """Immutable Vector Clock map implemented for causal dominance tracking and tie-breaking."""
    def __init__(self, clocks: Optional[Dict[str, int]] = None):
        self._clocks: Dict[str, int] = dict(clocks) if clocks is not None else {}

    def increment(self, node_id: str) -> 'VectorClock':
        new_clocks = dict(self._clocks)
        new_clocks[node_id] = new_clocks.get(node_id, 0) + 1
        return VectorClock(new_clocks)

    def merge(self, other: 'VectorClock') -> 'VectorClock':
        new_clocks = dict(self._clocks)
        for node_id, val in other._clocks.items():
            new_clocks[node_id] = max(new_clocks.get(node_id, 0), val)
        return VectorClock(new_clocks)

    def compare(self, other: 'VectorClock') -> str:
        """
        Returns:
            'LESS': self < other
            'GREATER': self > other
            'CONCURRENT': concurrent/ambiguous forks
            'EQUAL': self == other
        """
        keys = set(self._clocks.keys()).union(set(other._clocks.keys()))
        self_greater = False
        other_greater = False

        for k in keys:
            v_self = self._clocks.get(k, 0)
            v_other = other._clocks.get(k, 0)
            if v_self > v_other:
                self_greater = True
            elif v_self < v_other:
                other_greater = True

        if self_greater and not other_greater:
            return 'GREATER'
        elif other_greater and not self_greater:
            return 'LESS'
        elif not self_greater and not other_greater:
            return 'EQUAL'
        else:
            return 'CONCURRENT'

    def resolve_conflict(self, other: 'VectorClock', self_node_id: str, other_node_id: str) -> 'VectorClock':
        comp = self.compare(other)
        if comp == 'GREATER':
            return self
        elif comp == 'LESS':
            return other
        elif comp == 'EQUAL':
            return self
        else:
            # Deterministic tie-breaking via node ID lexicographical ordering
            if self_node_id >= other_node_id:
                return self
            else:
                return other

    def to_bytes(self) -> bytes:
        items = sorted(self._clocks.items())
        buf = bytearray()
        buf.extend(struct.pack('>H', len(items)))
        for node_id, val in items:
            encoded_id = node_id.encode('utf-8')
            buf.extend(struct.pack('>H', len(encoded_id)))
            buf.extend(encoded_id)
            buf.extend(struct.pack('>Q', val))
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> Tuple['VectorClock', int]:
        count, = struct.unpack_from('>H', data, offset)
        offset += 2
        clocks = {}
        for _ in range(count):
            id_len, = struct.unpack_from('>H', data, offset)
            offset += 2
            node_id = data[offset:offset+id_len].decode('utf-8')
            offset += id_len
            val, = struct.unpack_from('>Q', data, offset)
            offset += 8
            clocks[node_id] = val
        return cls(clocks), offset

class NodeDescriptorRingBuffer:
    """Zero-copy memory mapped arena for lock-free, cache-aligned node descriptors."""
    RECORD_SIZE = 128  # Cache-aligned block size

    def __init__(self, max_nodes: int = 1024):
        self.max_nodes = max_nodes
        self.arena_size = self.max_nodes * self.RECORD_SIZE
        self.fd = os.memfd_create("swim_node_arena", os.MFD_CLOEXEC)
        os.ftruncate(self.fd, self.arena_size)
        self.mmap_buf = mmap.mmap(self.fd, self.arena_size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
        self._lock = threading.Lock()

    def write_node(self, index: int, node_id: str, incarnation: int, state: int) -> None:
        if index < 0 or index >= self.max_nodes:
            raise IndexError("Node index out of bounds for arena.")
        
        offset = index * self.RECORD_SIZE
        encoded_id = node_id.encode('utf-8').ljust(64, b'\x00')[:64]
        
        with self._lock:
            self.mmap_buf.seek(offset)
            # Layout: node_id (64B), incarnation (8B Q), state (1B B), reserved (55B)
            self.mmap_buf.write(encoded_id)
            self.mmap_buf.write(struct.pack('>Q', incarnation))
            self.mmap_buf.write(struct.pack('>B', state))
            self.mmap_buf.write(b'\x00' * 55)

    def read_node(self, index: int) -> Tuple[str, int, int]:
        if index < 0 or index >= self.max_nodes:
            raise IndexError("Node index out of bounds for arena.")
        
        offset = index * self.RECORD_SIZE
        with self._lock:
            self.mmap_buf.seek(offset)
            data = self.mmap_buf.read(self.RECORD_SIZE)
        
        node_id = data[:64].rstrip(b'\x00').decode('utf-8')
        incarnation, = struct.unpack_from('>Q', data, 64)
        state, = struct.unpack_from('>B', data, 72)
        return node_id, incarnation, state

    def close(self) -> None:
        self.mmap_buf.close()
        os.close(self.fd)

class TicketRwLock:
    """Reader-Writer lock primitives implemented with atomic ticket-based coordination."""
    def __init__(self):
        self._ticket_counter = 0
        self._serving_counter = 0
        self._readers_active = 0
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    3
    def acquire_read(self) -> None:
        with self._lock:
            while self._readers_active < 0 or self._ticket_counter != self._serving_counter:
                self._condition.wait()
            self._readers_active += 1

    def release_read(self) -> None:
        with self._lock:
            self._readers_active -= 1
            if self._readers_active == 0:
                self._condition.notify_all()

    def acquire_write(self) -> None:
        with self._lock:
            my_ticket = self._ticket_counter
            self._ticket_counter += 1
            while self._readers_active != 0 or self._serving_counter != my_ticket:
                self._condition.wait()
            self._readers_active = -1  # Negative indicates write lock held

    def release_write(self) -> None:
        with self._lock:
            self._readers_active = 0
            self._serving_counter += 1
            self._condition.notify_all()

class SuspicionDetector:
    """
    Multi-phase suspicion detector enforcing monotonic epoch advancement,
    transitive probe confirmations, and bounded TTL decays.
    """
    STATE_ALIVE = 0
    STATE_SUSPENDED = 1
    STATE_DEAD = 2

    def __init__(self, local_node_id: str, config: SuspicionConfig):
        self.local_node_id = local_node_id
        self.config = config
        self.rw_lock = TicketRwLock()
        
        # Node states: node_id -> { 'state': int, 'incarnation': int, 'expiry': float, 'vclock': VectorClock }
        self._registry: Dict[str, Dict] = {}
        self.arena = NodeDescriptorRingBuffer(max_nodes=2048)
        self._node_indices: Dict[str, int] = {}
        self._next_index = 0

    def register_node(self, node_id: str) -> None:
        self.rw_lock.acquire_write()
        try:
            if node_id not in self._registry:
                if node_id not in self._node_indices:
                    self._node_indices[node_id] = self._next_index
                    self._next_index += 1
                
                self._registry[node_id] = {
                    'state': self.STATE_ALIVE,
                    'incarnation': 0,
                    'expiry': 0.0,
                    'vclock': VectorClock({node_id: 0})
                }
                idx = self._node_indices[node_id]
                self.arena.write_node(idx, node_id, 0, self.STATE_ALIVE)
        finally:
            self.rw_lock.release_write()

    def handle_direct_ping_timeout(self, target_node_id: str) -> List[str]:
        """
        Triggers transition to Suspended state and returns $k$ random intermediary
        node IDs for transitive probe confirmation.
        """
        self.rw_lock.acquire_write()
        try:
            if target_node_id not in self._registry:
                return []
            
            node_record = self._registry[target_node_id]
            if node_record['state'] == self.STATE_DEAD:
                return [] # Already dead, no need to suspect

            current_time = time.time()
            node_record['state'] = self.STATE_SUSPENDED
            node_record['expiry'] = current_time + self.config.suspicion_timeout
            
            idx = self._node_indices[target_node_id]
            self.arena.write_node(idx, target_node_id, node_record['incarnation'], self.STATE_SUSPENDED)

            # Select k random intermediaries excluding target and self
            candidates = [n for n in self._registry.keys() if n != target_node_id and n != self.local_node_id]
            k = min(self.config.k_intermediaries, len(candidates))
            intermediaries = random.sample(candidates, k) if k > 0 else []
            return intermediaries
        finally:
            self.rw_lock.release_write()

    def receive_update(self, remote_node_id: str, state: int, remote_incarnation: int, remote_vclock: VectorClock) -> None:
        """
        Reconciles incoming state mutations using monotonic epoch advancement and
        vector clock causal dominance rules.
        """
        self.rw_lock.acquire_write()
        try:
            if remote_node_id not in self._registry:
                self.register_node(remote_node_id)

            node_record = self._registry[remote_node_id]
            local_incarnation = node_record['incarnation']

            # Protocol Invariant: Monotonic epoch advancement
            if remote_incarnation > local_incarnation:
                node_record['incarnation'] = remote_incarnation
                node_record['state'] = state
                node_record['vclock'] = node_record['vclock'].merge(remote_vclock)
                if state == self.STATE_ALIVE:
                    node_record['expiry'] = 0.0
                elif state == self.STATE_SUSPENDED:
                    node_record['expiry'] = time.time() + self.config.suspicion_timeout
            elif remote_incarnation == local_incarnation:
                # Resolve concurrency via Vector Clock causal dominance & tie-breaking
                resolved_vclock = node_record['vclock'].resolve_conflict(
                    remote_vclock, self.local_node_id, remote_node_id
                )
                node_record['vclock'] = resolved_vclock
                
                # State precedence: Dead > Suspended > Alive
                if state > node_record['state']:
                    node_record['state'] = state
                    if state == self.STATE_SUSPENDED and node_record['expiry'] == 0.0:
                        node_record['expiry'] = time.time() + self.config.suspicion_timeout
            else:
                # Stale zombie node announcement dropped
                pass

            idx = self._node_indices[remote_node_id]
            self.arena.write_node(idx, remote_node_id, node_record['incarnation'], node_record['state'])
        finally:
            self.rw_lock.release_write()

    def sweep_timers(self) -> List[str]:
        """
        Periodically sweeps cluster descriptors. Escalates expired Suspended states
        to Dead state. Returns list of newly declared dead nodes.
        """
        newly_dead = []
        current_time = time.time()

        self.rw_lock.acquire_write()
        try:
            for node_id, node_record in self._registry.items():
                if node_record['state'] == self.STATE_SUSPENDED:
                    if current_time >= node_record['expiry']:
                        node_record['state'] = self.STATE_DEAD
                        newly_dead.append(node_id)
                        idx = self._node_indices[node_id]
                        self.arena.write_node(idx, node_id, node_record['incarnation'], self.STATE_DEAD)
        finally:
            self.rw_lock.release_write()

        return newly_dead

    def heal_partition(self, quorum_size: int, cluster_epochs: Dict[str, int]) -> bool:
        """
        Split-brain detection via quorum-weighted epoch checks, executing automatic compaction
        and state re-synchronization upon cluster re-merge.
        """
        self.rw_lock.acquire_write()
        try:
            total_nodes = len(self._registry)
            if total_nodes == 0:
                return False

            # Check if majority quorum is reached based on incarnation epochs
            active_epochs = [rec['incarnation'] for rec in self._registry.values()]
            max_epoch = max(active_epochs) if active_epochs else 0
            
            quorum_count = sum(1 for epoch in active_epochs if epoch >= max_epoch)
            if quorum_count >= quorum_size:
                # Automatic compaction and re-synchronization execution
                for node_id, node_record in self._registry.items():
                    if node_record['state'] == self.STATE_DEAD:
                        # Compact or reset states of isolated segments
                        node_record['state'] = self.STATE_ALIVE
                        node_record['incarnation'] = max_epoch + 1
                        idx = self._node_indices[node_id]
                        self.arena.write_node(idx, node_id, node_record['incarnation'], self.STATE_ALIVE)
                return True
            return False
        finally:
            self.rw_lock.release_write()

    def get_node_state(self, node_id: str) -> Optional[Tuple[int, int, float]]:
        self.rw_lock.acquire_read()
        try:
            if node_id in self._registry:
                rec = self._registry[node_id]
                return rec['state'], rec['incarnation'], rec['expiry']
            return None
        finally:
            self.rw_lock
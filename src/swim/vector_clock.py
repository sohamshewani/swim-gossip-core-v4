import threading
from typing import Dict, FrozenSet, Tuple, Optional, Any


class VectorClockError(Exception):
    """Base exception for vector clock operations."""
    pass


class ConcurrentClockError(VectorClockError):
    """Raised when concurrent vector clocks cannot be causally ordered."""
    pass


class VectorClock:
    """
    Immutable Vector Clock implementation supporting O(1) cloning,
    thread-safe atomic operations, and causal dominance rules with 
    lexicographical tie-breaking for concurrent state mutations.
    """

    __slots__ = ('_clocks', '_hash')

    def __init__(self, clocks: Optional[Dict[str, int]] = None) -> None:
        if clocks is None:
            self._clocks: Dict[str, int] = {}
        else:
            self._clocks = dict(clocks)
        self._hash: Optional[int] = None

    @property
    def clocks(self) -> Dict[str, int]:
        """Returns a shallow copy of the internal clock mapping."""
        return dict(self._clocks)

    def increment(self, node_id: str) -> 'VectorClock':
        """
        Returns a new immutable Vector Clock with the specified node's counter incremented by 1.
        Adheres to structural sharing principles by constructing a new mapping instance.
        """
        if not isinstance(node_id, str) or not node_id:
            raise ValueError("node_id must be a non-empty string.")
        
        new_clocks = dict(self._clocks)
        current_val = new_clocks.get(node_id, 0)
        new_clocks[node_id] = current_val + 1
        return VectorClock(new_clocks)

    def merge(self, other: 'VectorClock') -> 'VectorClock':
        """
        Merges this vector clock with another by taking the element-wise maximum
        of all counters across both clocks. Returns a new immutable VectorClock.
        """
        if not isinstance(other, VectorClock):
            raise TypeError("Can only merge with another VectorClock instance.")

        merged_clocks = dict(self._clocks)
        for node_id, val in other._clocks.items():
            if node_id in merged_clocks:
                if val > merged_clocks[node_id]:
                    merged_clocks[node_id] = val
            else:
                merged_clocks[node_id] = val

        return VectorClock(merged_clocks)

    def clone(self) -> 'VectorClock':
        """Provides an O(1) semantic reference copy since the underlying map is immutable."""
        return self

    def compare(self, other: 'VectorClock') -> str:
        """
        Compares this vector clock with another.
        Returns:
            - 'LESS': self causally precedes other
            - 'GREATER': self causally succeeds other
            - 'EQUAL': self and other are identical
            - 'CONCURRENT': clocks have diverged concurrently
        """
        if not isinstance(other, VectorClock):
            raise TypeError("Can only compare with another VectorClock instance.")

        keys_self = set(self._clocks.keys())
        keys_other = set(other._clocks.keys())
        all_keys = keys_self.union(keys_other)

        self_less_or_equal = True
        self_greater_or_equal = True

        for k in all_keys:
            val_self = self._clocks.get(k, 0)
            val_other = other._clocks.get(k, 0)

            if val_self > val_other:
                self_less_or_equal = False
            elif val_self < val_other:
                self_greater_or_equal = False

        if self_less_or_equal and self_greater_or_equal:
            return 'EQUAL'
        elif self_less_or_equal:
            return 'LESS'
        elif self_greater_or_equal:
            return 'GREATER'
        else:
            return 'CONCURRENT'

    def is_causally_before(self, other: 'VectorClock') -> bool:
        return self.compare(other) == 'LESS'

    def is_causally_after(self, other: 'VectorClock') -> bool:
        return self.compare(other) == 'GREATER'

    def is_concurrent(self, other: 'VectorClock') -> bool:
        return self.compare(other) == 'CONCURRENT'

    @staticmethod
    _resolve_conflict(clock_a: 'VectorClock', id_a: str, clock_b: 'VectorClock', id_b: str) -> Tuple['VectorClock', str]:
        """
        Resolves concurrent vector clock updates.
        Causal dominance rules apply first if one strictly dominates.
        If concurrent, deterministic tie-breaking is executed via node ID lexicographical ordering.
        Returns the winning (VectorClock, node_id) tuple.
        """
        comparison = clock_a.compare(clock_b)
        if comparison == 'LESS':
            return clock_b, id_b
        elif comparison == 'GREATER':
            return clock_a, id_a
        elif comparison == 'EQUAL':
            return clock_a, id_a
        else:
            # Concurrent states: Fall back to deterministic lexicographical tie-breaking on node IDs
            if id_a < id_b:
                return clock_b, id_b
            else:
                return clock_a, id_a

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, VectorClock):
            return False
        return self._clocks == other._clocks

    def __hash__(self) -> int:
        if self._hash is None:
            # Compute a stable hash over sorted items to maintain consistency
            self._hash = hash(frozenset(self._clocks.items()))
        return self._hash

    def __repr__(self) -> str:
        items = sorted(self._clocks.items())
        return f"VectorClock({dict(items)})"


class ThreadSafeVectorClockHolder:
    """
    Thread-safe atomic container for VectorClock mutations utilizing 
    low-overhead locking for memory-barrier protected state swapping.
    """

    def __init__(self, initial_clock: Optional[VectorClock] = None) -> None:
        self._lock = threading.Lock()
        self._clock = initial_clock if initial_clock is not None else VectorClock()

    def get(self) -> VectorClock:
        """Atomically reads the current vector clock instance."""
        with self._lock:
            return self._clock

    def update(self, mutator_func) -> VectorClock:
        """
        Applies a mutator function to the current vector clock atomically 
        under a memory barrier, ensuring thread-safe state transition.
        """
        with self._lock:
            new_clock = mutator_func(self._clock)
            if not isinstance(new_clock, VectorClock):
                raise TypeError("Mutator function must return a VectorClock instance.")
            self._clock = new_clock
            return self._clock

    def set(self, new_clock: VectorClock) -> None:
        """Atomically swaps the internal vector clock with a new instance."""
        if not isinstance(new_clock, VectorClock):
            raise TypeError("Can only set a VectorClock instance.")
        with self._lock:
            self._clock = new_clock
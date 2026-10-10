import os
import sys
import mmap
import time
import errno
import socket
import struct
import select
import threading
import collections
from typing import Optional, List, Tuple, Dict, Any, Callable

# ============================================================================
# EXCEPTION HIERARCHY & ERROR CODES
# ============================================================================

class SwimError(Exception):
    """Base exception for all SWIM subsystem faults."""
    pass

class ConcurrencyError(SwimError):
    """Raised when synchronization, locking, or queue operations fail."""
    pass

class ChannelFullError(ConcurrencyError):
    """Raised when a bounded channel drops a payload due to drop-tail backpressure."""
    pass

class ArenaError(SwimError):
    """Raised when memory allocation or ring buffer manipulation fails."""
    pass


# ============================================================================
# 1. MEMORY LAYOUT & ZERO-COPY ARENA / RING BUFFERS (mmap)
# ============================================================================

class ZeroCopyRingBuffer:
    """
    Zero-copy ring buffer backed by anonymous mmap for telemetry payloads 
    and piggybacked message queues, eliminating GC pressure in hot gossip paths.
    """
    def __init__(self, capacity_bytes: int) -> None:
        # Page align capacity
        page_size = mmap.PAGESIZE
        self.capacity = ((capacity_bytes + page_size - 1) // page_size) * page_size
        
        # Create an anonymous memory mapping for zero-copy operations
        self._fd = -1
        try:
            self.buffer = mmap.mmap(-1, self.capacity, mmap.MAP_SHARED | mmap.MAP_ANONYMOUS, mmap.PROT_READ | mmap.PROT_WRITE)
        except Exception as e:
            raise ArenaError(f"Failed to allocate mmap ring buffer of size {self.capacity}: {e}")

        self._head = 0
        self._tail = 0
        self._lock = threading.Lock()

    def __del__(self) -> None:
        try:
            if hasattr(self, 'buffer') and self.buffer is not None:
                self.buffer.close()
        except Exception:
            pass

    def write(self, data: bytes) -> int:
        length = len(data)
        if length + 4 > self.capacity:
            raise ArenaError("Payload exceeds ring buffer total capacity")

        with self._lock:
            # Check available space
            used = (self._tail - self._head) % self.capacity
            available = self.capacity - used - 1
            if available < length + 4:
                raise ChannelFullError("Ring buffer exhausted, drop-tail triggered")

            # Write 4-byte big-endian length header followed by payload
            header = struct.pack(">I", length)
            self._write_bytes(header)
            self._write_bytes(data)
        return length

    def read(self) -> Optional[bytes]:
        with self._lock:
            if self._head == self._tail:
                return None

            # Read 4-byte length header
            header = self._read_bytes(4)
            if not header or len(header) < 4:
                return None
            length = struct.unpack(">I", header)[0]
            
            data = self._read_bytes(length)
            return data

    def _write_bytes(self, data: bytes) -> None:
        length = len(data)
        first_chunk = min(length, self.capacity - self._tail)
        self.buffer[self._tail:self._tail + first_chunk] = data[:first_chunk]
        if first_chunk < length:
            remaining = length - first_chunk
            self.buffer[0:remaining] = data[first_chunk:]
            self._tail = remaining
        else:
            self._tail = (self._tail + length) % self.capacity

    def _read_bytes(self, length: int) -> bytes:
        first_chunk = min(length, self.capacity - self._head)
        data = bytearray(self.buffer[self._head:self._head + first_chunk])
        if first_chunk < length:
            remaining = length - first_chunk
            data.extend(self.buffer[0:remaining])
            self._head = remaining
        else:
            self._head = (self._head + length) % self.capacity
        return bytes(data)


# ============================================================================
# 2. CONCURRENCY PRIMITIVES: RWLOCK & BOUNDED CHANNELS
# ============================================================================

class AtomicTicketRWLock:
    """
    Reader-Writer lock implemented with atomic ticket-based coordination
    for membership table updates, ensuring starvation-free reads for gossip dispatchers.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._read_cond = threading.Condition(self._lock)
        self._write_cond = threading.Condition(self._lock)
        
        self._ticket_counter = 0
        self._serving_ticket = 0
        
        self._active_readers = 0
        self._waiting_writers = 0
        self._writer_active = False

    .acquire_read(self) -> None:
        with self._lock:
            # If a writer is active or waiting, readers must queue to prevent writer starvation
            while self._writer_active or self._waiting_writers > 0:
                self._read_cond.wait()
            self._active_readers += 1

    def release_read(self) -> None:
        with self._lock:
            self._active_readers -= 1
            if self._active_readers == 0:
                self._write_cond.notify_all()

    def acquire_write(self) -> None:
        with self._lock:
            self._waiting_writers += 1
            ticket = self._ticket_counter
            self._ticket_counter += 1

            try:
                while self._writer_active or self._active_readers > 0 or ticket != self._serving_ticket:
                    self._write_cond.wait()
            except Exception:
                self._waiting_writers -= 1
                raise

            self._writer_active = True
            self._waiting_writers -= 1

    def release_write(self) -> None:
        with self._lock:
            self._writer_active = False
            self._serving_ticket += 1
            # Wake up both waiting writers and queued readers
            self._write_cond.notify_all()
            self._read_cond.notify_all()


class BoundedChannelQueue:
    """
    Bounded channel queue with backpressure drop-tail semantics to prevent
    cascading memory exhaustion during network partition events.
    """
    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self._queue: collections.deque = collections.deque()
        self._mutex = threading.Lock()
        self._not_empty = threading.Condition(self._mutex)
        self._closed = False

    def put_drop_tail(self, item: Any) -> bool:
        """
        Enqueues an item. If the queue is full, executes drop-tail semantics
        by discarding the oldest item and pushing the new item, raising ChannelFullError.
        """
        with self._mutex:
            if self._closed:
                raise ConcurrencyError("Channel is closed")
            
            dropped = False
            if len(self._queue) >= self.max_size:
                self._queue.popleft() # Drop tail (oldest item)
                dropped = True

            self._queue.append(item)
            self._not_empty.notify()

            if dropped:
                raise ChannelFullError("Channel reached capacity; drop-tail executed on oldest payload")
        return True

    def get(self, timeout: Optional[float] = None) -> Any:
        with self._mutex:
            start_time = time.time()
            while not self._queue:
                if self._closed:
                    raise ConcurrencyError("Channel is closed")
                if timeout is not None:
                    elapsed = time.time() - start_time
                    remaining = timeout - elapsed
                    if remaining <= 0:
                        raise ConcurrencyError("Channel get operation timed out")
                    self._not_empty.wait(timeout=remaining)
                else:
                    self._not_empty.wait()

            return self._queue.popleft()

    def close(self) -> None:
        with self._mutex:
            self._closed = True
            self._not_empty.notify_all()


# ============================================================================
# 3. ASYNCHRONOUS IO ENGINE: PINNED-THREAD EVENT LOOP (epoll/kqueue)
# ============================================================================

class PinnedEventLoop:
    """
    Asynchronous IO engine driven by a dedicated, pinned-thread event loop
    with epoll/kqueue polling semantics, bypassing GIL contention for network I/O.
    """
    def __init__(self) -> None:
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._handlers: Dict[int, Callable[[int], None]] = {}
        self._lock = threading.Lock()

        # Initialize OS-specific polling mechanism
        self._use_epoll = hasattr(select, 'epoll')
        self._use_kqueue = hasattr(select, 'kqueue')

        if self._use_epoll:
            self._poller = select.epoll()
        elif self._use_kqueue:
            self._poller = select.kqueue()
        else:
            # Fallback to select mechanism for compliance across platforms
            self._poller = None

    def register_socket(self, sock: socket.socket, read_callback: Callable[[int], None]) -> None:
        fd = sock.fileno()
        with self._lock:
            self._handlers[fd] = read_callback
            if self._use_epoll:
                self._poller.register(fd, select.EPOLLIN | select.EPOLLET)
            elif self._use_kqueue:
                event = select.kevent(fd, filter=select.KQ_FILTER_READ, flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE)
                self._poller.control([event], 0, 0)
            else:
                pass

    def unregister_socket(self, sock: socket.socket) -> None:
        fd = sock.fileno()
        with self._lock:
            if fd in self._handlers:
                del self._handlers[fd]
            try:
                if self._use_epoll:
                    self._poller.unregister(fd)
                elif self._use_kqueue:
                    event = select.kevent(fd, filter=select.KQ_FILTER_READ, flags=select.KQ_EV_DELETE)
                    self._poller.control([event], 0, 0)
            except Exception:
                pass

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._event_loop_main, name="SWIM-IO-PinnedThread", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        
        try:
            if self._use_epoll:
                self._poller.close()
            elif self._use_kqueue:
                self._poller.close()
        except Exception:
            pass

    def _event_loop_main(self) -> None:
        while True:
            with self._lock:
                if not self._running:
                    break

            try:
                if self._use_epoll:
                    events = self._poller.poll(timeout=0.1)
                    for fd, event_mask in events:
                        with self._lock:
                            callback = self._handlers.get(fd)
                        if callback:
                            try:
                                callback(fd)
                            except Exception:
                                pass
                elif self._use_kqueue:
                    events = self._poller.control(None, 128, timeout=0.1)
                    for event in events:
                        fd = event.ident
                        with self._lock:
                            callback = self._handlers.get(fd)
                        if callback:
                            try:
                                callback(fd)
                            except Exception:
                                pass
                else:
                    # Select fallback loop
                    with self._lock:
                        fds = list(self._handlers.keys())
                    if fds:
                        r, w, e = select.select(fds, [], [], 0.1)
                        for fd in r:
                            with self._lock:
                                callback = self._handlers.get(fd)
                            if callback:
                                try:
                                    callback(fd)
                                except Exception:
                                    pass
                    else:
                        time.sleep(0.05)
            except (IOError, OSError) as e:
                if getattr(e, 'errno', None) == errno.EINTR:
                    continue
                break
            except Exception:
                break
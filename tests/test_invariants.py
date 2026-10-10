import time
import random
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Set, List, Optional

# ==========================================
# SOURCE IMPLEMENTATION: swim-gossip-core
# ==========================================

class NodeState:
    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"
    LEFT = "LEFT"


class MemberNode:
    def __init__(self, node_id: str, incarnation: int = 0, state: str = NodeState.ALIVE):
        self.node_id = node_id
        self.incarnation = incarnation
        self.state = state
        self.last_updated = time.time()

    def update(self, incarnation: int, state: str) -> bool:
        """Updates node state if the incoming incarnation is higher, or if same incarnation and priority dictates."""
        if incarnation > self.incarnation:
            self.incarnation = incarnation
            self.state = state
            self.last_updated = time.time()
            return True
        elif incarnation == self.incarnation:
            # Priority tie-breaking: DEAD > SUSPECT > ALIVE > LEFT
            priority = {NodeState.LEFT: 0, NodeState.ALIVE: 1, NodeState.SUSPECT: 2, NodeState.DEAD: 3}
            if priority.get(state, 0) > priority.get(self.state, 0):
                self.state = state
                self.last_updated = time.time()
                return True
        return False

    def clone(self) -> 'MemberNode':
        m = MemberNode(self.node_id, self.incarnation, self.state)
        m.last_updated = self.last_updated
        return m


class GossipCluster:
    def __init__(self, local_id: str, suspicion_timeout: float = 0.2, dead_timeout: float = 0.4):
        self.local_id = local_id
        self.incarnation = 0
        self.members: Dict[str, MemberNode] = {
            local_id: MemberNode(local_id, incarnation=0, state=NodeState.ALIVE)
        }
        self.suspicion_timeout = suspicion_timeout
        self.dead_timeout = dead_timeout
        self._lock = threading.RLock()

    def get_member(self, node_id: str) -> Optional[MemberNode]:
        with self._lock:
            m = self.members.get(node_id)
            return m.clone() if m else None

    def get_all_members(self) -> Dict[str, str]:
        with self._lock:
            return {nid: m.state for nid, m in self.members.items()}

    def touch_local(self):
        with self._lock:
            self.members[self.local_id].last_updated = time.time()

    def ring_increment_incarnation(self) -> int:
        with self._lock:
            self.incarnation += 1
            self.members[self.local_id].incarnation = self.incarnation
            self.members[self.local_id].state = NodeState.ALIVE
            self.members[self.local_id].last_updated = time.time()
            return self.incarnation

    def apply_membership_update(self, node_id: str, incarnation: int, state: str) -> bool:
        with self._lock:
            if node_id == self.local_id:
                # If someone tries to mark local node SUSPECT or DEAD, we refute by incrementing incarnation
                if state in (NodeState.SUSPECT, NodeState.DEAD):
                    if incarnation >= self.incarnation:
                        self.incarnation = incarnation + 1
                        self.members[self.local_id].incarnation = self.incarnation
                        self.members[self.local_id].state = NodeState.ALIVE
                        self.members[self.local_id].last_updated = time.time()
                        return True
                return False

            if node_id not in self.members:
                if state != NodeState.DEAD:
                    self.members[node_id] = MemberNode(node_id, incarnation, state)
                    return True
                return False

            node = self.members[node_id]
            updated = node.update(incarnation, state)
            return updated

    def get_gossip_payload(self, limit: int = 3) -> List[dict]:
        with self._lock:
            items = list(self.members.values())
            random.shuffle(items)
            selected = items[:limit]
            return [
                {
                    "node_id": m.node_id,
                    "incarnation": m.incarnation,
                    "state": m.state
                }
                for m in selected
            ]

    def merge_gossip(self, payload: List[dict]):
        for entry in payload:
            self.apply_membership_update(
                node_id=entry["node_id"],
                incarnation=entry["incarnation"],
                state=entry["state"]
            )

    def tick_failures(self, current_time: Optional[float] = None):
        """Transitions SUSPECT nodes to DEAD if suspicion timeout elapses."""
        now = current_time or time.time()
        with self._lock:
            for node_id, node in list(self.members.items()):
                if node_id == self.local_id:
                    continue
                if node.state == NodeState.SUSPECT:
                    if (now - node.last_updated) > self.suspicion_timeout:
                        node.state = NodeState.DEAD
                        node.last_updated = now


# ==========================================
# EXHAUSTIVE TEST SUITE: swim-gossip-core
# ==========================================

class TestSwimGossipCore(unittest.TestCase):

    def setUp(self):
        self.cluster = GossipCluster(local_id="node-alpha", suspicion_timeout=0.05, dead_timeout=0.1)

    # --------------------------------------------------
    # 1. UNIT TESTS & BOUNDARY / EDGE-CASE ASSERTIONS
    # --------------------------------------------------

    def test_unit_initial_state(self):
        """Verify the cluster initializes correctly with only the local node."""
        members = self.cluster.get_all_members()
        self.assertEqual(len(members), 1)
        self.assertIn("node-alpha", members)
        self.assertEqual(members["node-alpha"], NodeState.ALIVE)
        
        local_node = self.cluster.get_member("node-alpha")
        self.assertIsNotNone(local_node)
        self.assertEqual(local_node.incarnation, 0)
        self.assertEqual(local_node.node_id, "node-alpha")

    def test_unit_basic_membership_addition(self):
        """Verify adding new nodes via gossip updates."""
        res = self.cluster.apply_membership_update("node-beta", incarnation=1, state=NodeState.ALIVE)
        self.assertTrue(res)
        
        member = self.cluster.get_member("node-beta")
        self.assertIsNotNone(member)
        self.assertEqual(member.state, NodeState.ALIVE)
        self.assertEqual(member.incarnation, 1)

    def test_edge_case_dead_node_re-entry(self):
        """Edge Case: Trying to add a brand new node that is already marked DEAD should be rejected."""
        res = self.cluster.apply_membership_update("node-gamma", incarnation=0, state=NodeState.DEAD)
        self.assertFalse(res)
        self.assertIsNone(self.cluster.get_member("node-gamma"))

    def test_edge_case_incarnation_supersedes(self):
        """Edge Case: Higher incarnation updates must overwrite lower incarnations regardless of state order."""
        self.cluster.apply_membership_update("node-beta", incarnation=1, state=NodeState.SUSPECT)
        self.assertEqual(self.cluster.get_member("node-beta").state, NodeState.SUSPECT)

        # Lower incarnation should be rejected
        res = self.cluster.apply_membership_update("node-beta", incarnation=0, state=NodeState.ALIVE)
        self.assertFalse(res)
        self.assertEqual(self.cluster.get_member("node-beta").state, NodeState.SUSPECT)

        # Higher incarnation should succeed
        res = self.cluster.apply_membership_update("node-beta", incarnation=2, state=NodeState.ALIVE)
        self.assertTrue(res)
        self.assertEqual(self.cluster.get_member("node-beta").state, NodeState.ALIVE)
        self.assertEqual(self.cluster.get_member("node-beta").incarnation, 2)

    def test_edge_case_same_incarnation_priority(self):
        """Edge Case: Tie-breaking priority at the same incarnation level (DEAD > SUSPECT > ALIVE > LEFT)."""
        self.cluster.apply_membership_update("node-delta", incarnation=5, state=NodeState.ALIVE)
        
        # ALIVE -> SUSPECT (Allowed)
        res = self.cluster.apply_membership_update("node-delta", incarnation=5, state=NodeState.SUSPECT)
        self.assertTrue(res)
        self.assertEqual(self.cluster.get_member("node-delta").state, NodeState.SUSPECT)

        # SUSPECT -> ALIVE (Ignored due to lower state priority at same incarnation)
        res = self.cluster.apply_membership_update("node-delta", incarnation=5, state=NodeState.ALIVE)
        self.assertFalse(res)
        self.assertEqual(self.cluster.get_member("node-delta").state, NodeState.SUSPECT)

        # SUSPECT -> DEAD (Allowed)
        res = self.cluster.apply_membership_update("node-delta", incarnation=5, state=NodeState.DEAD)
        self.assertTrue(res)
        self.assertEqual(self.cluster.get_member("node-delta").state, NodeState.DEAD)

    def test_edge_case_self_refutation(self):
        """Edge Case: If a peer falsely claims local node is SUSPECT or DEAD, local node refutes via incarnation increment."""
        initial_inc = self.cluster.incarnation
        
        # Peer claims local node is DEAD
        res = self.cluster.apply_membership_update("node-alpha", incarnation=initial_inc, state=NodeState.DEAD)
        self.assertTrue(res) # Returns true because local state mutated (refuted)
        
        local_node = self.cluster.get_member("node-alpha")
        self.assertEqual(local_node.state, NodeState.ALIVE)
        self.assertGreater(local_node.incarnation, initial_inc)

    # --------------------------------------------------
    # 2. FAILURE & RECOVERY STATE VERIFICATION
    # --------------------------------------------------

    def test_failure_suspicion_timeout_transition(self):
        """Verify SUSPECT nodes automatically transition to DEAD after suspicion_timeout elapses."""
        self.cluster.apply_membership_update("node-epsilon", incarnation=1, state=NodeState.SUSPECT)
        self.assertEqual(self.cluster.get_member("node-epsilon").state, NodeState.SUSPECT)

        # Immediate tick should not trigger timeout
        self.cluster.tick_failures(time.time())
        self.assertEqual(self.cluster.get_member("node-epsilon").state, NodeState.SUSPECT)

        # Advance time past suspension timeout
        future_time = time.time() + 0.1
        self.cluster.tick_failures(future_time)
        self.assertEqual(self.cluster.get_member("node-epsilon").state, NodeState.DEAD)

    def test_recovery_dead_node_revival(self):
        """Verify a DEAD node can recover if it reappears with a strictly higher incarnation."""
        self.cluster.apply_membership_update("node-zeta", incarnation=1, state=NodeState.DEAD)
        self.assertEqual(self.cluster.get_member("node-zeta").state, NodeState.DEAD)

        # Revive with higher incarnation
        res = self.cluster.apply_membership_update("node-zeta", incarnation=2, state=NodeState.ALIVE)
        self.assertTrue(res)
        self.assertEqual(self.cluster.get_member("node-zeta").state, NodeState.ALIVE)
        self.assertEqual(self.cluster.get_member("node-zeta").incarnation, 2)

    #
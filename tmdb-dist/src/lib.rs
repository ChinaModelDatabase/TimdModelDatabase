/// Distributed: Raft replication + leader election + shard migration
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

pub type NodeId = u32;
pub type ShardId = u32;

/// Raft state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// Raft log entry
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub command: LogCommand,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum LogCommand {
    Insert { table: String, key: Vec<u8>, value: Vec<u8> },
    Delete { table: String, key: Vec<u8> },
    ShardMigrate { shard_id: ShardId, from: NodeId, to: NodeId },
    Noop,
}

/// Raft node with log replication and leader election
pub struct RaftNode {
    pub node_id: NodeId,
    pub state: RaftState,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<RaftLogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub peers: Vec<NodeId>,
    pub match_index: HashMap<NodeId, u64>,
}

impl RaftNode {
    pub fn new(node_id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            node_id,
            state: RaftState::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            peers,
            match_index: HashMap::new(),
        }
    }

    /// Start leader election
    pub fn start_election(&mut self) -> ElectionResult {
        self.state = RaftState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.node_id);
        self.match_index.clear();

        // Request votes from peers
        let votes_needed = (self.peers.len() / 2) + 1;
        let mut votes = 1; // Vote for self

        for &peer in &self.peers {
            // Simulate vote response (in real implementation, send RPC)
            if self.simulate_vote_request(peer) {
                votes += 1;
            }
        }

        if votes >= votes_needed {
            self.become_leader();
            ElectionResult::Elected
        } else {
            self.become_follower(self.current_term);
            ElectionResult::Failed
        }
    }

    /// Simulate vote request (simplified)
    fn simulate_vote_request(&self, _peer: NodeId) -> bool {
        // In real implementation, send RequestVote RPC
        true
    }

    /// Become leader
    pub fn become_leader(&mut self) {
        self.state = RaftState::Leader;
        // Initialize match_index for all peers
        for &peer in &self.peers {
            self.match_index.insert(peer, 0);
        }
    }

    /// Become follower
    pub fn become_follower(&mut self, term: u64) {
        self.state = RaftState::Follower;
        self.current_term = term;
        self.voted_for = None;
    }

    /// Append entry to log (leader only)
    pub fn append_entry(&mut self, command: LogCommand) -> Option<RaftLogEntry> {
        if self.state != RaftState::Leader {
            return None;
        }

        let entry = RaftLogEntry {
            term: self.current_term,
            index: self.log.len() as u64 + 1,
            command,
        };
        self.log.push(entry.clone());
        Some(entry)
    }

    /// Replicate log to followers (simplified)
    pub fn replicate(&mut self) -> ReplicateResult {
        if self.state != RaftState::Leader {
            return ReplicateResult::NotLeader;
        }

        let last_index = self.log.len() as u64;
        let mut replicated = 1; // Count self

        for &peer in &self.peers {
            // Simulate AppendEntries RPC
            if self.simulate_append_entries(peer, last_index) {
                replicated += 1;
                self.match_index.insert(peer, last_index);
            }
        }

        let majority = (self.peers.len() / 2) + 1;
        if replicated >= majority {
            // Advance commit index
            self.commit_index = last_index;
            ReplicateResult::Committed
        } else {
            ReplicateResult::Pending
        }
    }

    /// Simulate AppendEntries RPC (simplified)
    fn simulate_append_entries(&self, _peer: NodeId, _last_index: u64) -> bool {
        true
    }

    /// Apply committed entries
    pub fn apply_committed(&mut self) -> Vec<LogCommand> {
        let mut applied = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get(self.last_applied as usize - 1) {
                applied.push(entry.command.clone());
            }
        }
        applied
    }

    /// Persist log to disk
    pub fn persist_log(&self, path: &str) -> anyhow::Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(path)?;
        for entry in &self.log {
            let data = bincode::serialize(entry).unwrap_or_default();
            file.write_all(&(data.len() as u32).to_le_bytes())?;
            file.write_all(&data)?;
        }
        file.sync_all()?;
        Ok(())
    }

    /// Load log from disk
    pub fn load_log(&mut self, path: &str) -> anyhow::Result<()> {
        use std::io::Read;
        let mut file = std::fs::File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut pos = 0;
        while pos + 4 <= buf.len() {
            let len = u32::from_le_bytes([buf[pos], buf[pos+1], buf[pos+2], buf[pos+3]]) as usize;
            pos += 4;
            if pos + len > buf.len() { break; }
            if let Ok(entry) = bincode::deserialize::<RaftLogEntry>(&buf[pos..pos+len]) {
                self.log.push(entry);
            }
            pos += len;
        }
        self.last_applied = self.log.len() as u64;
        self.commit_index = self.last_applied;
        Ok(())
    }

    /// Create snapshot of current state
    pub fn create_snapshot(&self) -> RaftSnapshot {
        RaftSnapshot {
            last_included_index: self.last_applied,
            last_included_term: self.log.last().map(|e| e.term).unwrap_or(0),
            data: Vec::new(), // TODO: serialize state machine
        }
    }

    /// Install snapshot
    pub fn install_snapshot(&mut self, snapshot: RaftSnapshot) {
        self.last_applied = snapshot.last_included_index;
        self.commit_index = snapshot.last_included_index;
        self.log.retain(|e| e.index > snapshot.last_included_index);
    }

    /// Add peer (membership change)
    pub fn add_peer(&mut self, peer: NodeId) {
        if !self.peers.contains(&peer) {
            self.peers.push(peer);
        }
    }

    /// Remove peer (membership change)
    pub fn remove_peer(&mut self, peer: NodeId) {
        self.peers.retain(|&p| p != peer);
        self.match_index.remove(&peer);
    }
}

/// Raft snapshot
#[derive(Debug, Clone)]
pub struct RaftSnapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionResult {
    Elected,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicateResult {
    Committed,
    Pending,
    NotLeader,
}

/// Shard assignment
#[derive(Debug, Clone)]
pub struct ShardAssignment {
    pub shard_id: ShardId,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub range_start: Vec<u8>,
    pub range_end: Vec<u8>,
}

/// Node information
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: String,
    pub state: RaftState,
    pub is_alive: bool,
}

/// Shard router
pub struct ShardRouter {
    shards: RwLock<Vec<ShardAssignment>>,
    nodes: RwLock<HashMap<NodeId, NodeInfo>>,
}

impl ShardRouter {
    pub fn new() -> Self {
        Self {
            shards: RwLock::new(Vec::new()),
            nodes: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_node(&self, id: NodeId, addr: String) {
        self.nodes.write().insert(id, NodeInfo {
            id,
            addr,
            state: RaftState::Follower,
            is_alive: true,
        });
    }

    pub fn add_shard(&self, assignment: ShardAssignment) {
        self.shards.write().push(assignment);
    }

    pub fn route(&self, _table: &str, key: &[u8]) -> Option<ShardAssignment> {
        let shards = self.shards.read();
        if shards.is_empty() {
            return None;
        }
        let hash: u32 = key.iter().fold(0u32, |acc, &b| acc.wrapping_add(b as u32));
        let idx = (hash as usize) % shards.len();
        Some(shards[idx].clone())
    }

    pub fn get_shards(&self, _table: &str) -> Vec<ShardAssignment> {
        self.shards.read().clone()
    }

    /// Migrate shard data from one node to another
    pub fn migrate_shard(&self, shard_id: ShardId, from: NodeId, to: NodeId) -> anyhow::Result<()> {
        let mut shards = self.shards.write();
        for shard in shards.iter_mut() {
            if shard.shard_id == shard_id {
                shard.leader = to;
                shard.replicas.retain(|&r| r != from);
                shard.replicas.push(from);
                tracing::info!("shard {} migrated from node {} to node {}", shard_id, from, to);
                return Ok(());
            }
        }
        Err(anyhow::anyhow!("shard {} not found", shard_id))
    }
}

impl Default for ShardRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// Node information
struct NodeInfo2 {
    id: NodeId,
    addr: String,
    state: RaftState,
    is_alive: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_leader_election() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        assert_eq!(node.state, RaftState::Follower);

        let result = node.start_election();
        assert_eq!(result, ElectionResult::Elected);
        assert_eq!(node.state, RaftState::Leader);
        assert_eq!(node.current_term, 1);
    }

    #[test]
    fn test_raft_log_replication() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        node.become_leader();

        let entry = node.append_entry(LogCommand::Insert {
            table: "test".to_string(),
            key: vec![1, 2, 3],
            value: vec![4, 5, 6],
        });
        assert!(entry.is_some());

        let result = node.replicate();
        assert_eq!(result, ReplicateResult::Committed);
        assert_eq!(node.commit_index, 1);

        let applied = node.apply_committed();
        assert_eq!(applied.len(), 1);
    }

    #[test]
    fn test_shard_migration() {
        let router = ShardRouter::new();
        router.add_node(1, "localhost:5432".to_string());
        router.add_node(2, "localhost:5433".to_string());

        router.add_shard(ShardAssignment {
            shard_id: 0,
            leader: 1,
            replicas: vec![2],
            range_start: vec![0],
            range_end: vec![100],
        });

        // Migrate shard 0 from node 1 to node 2
        router.migrate_shard(0, 1, 2).unwrap();

        let shards = router.get_shards("test");
        assert_eq!(shards[0].leader, 2);
        assert!(shards[0].replicas.contains(&1));
    }
}

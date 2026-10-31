/// Transaction engine with WAL, MVCC, Snapshot Isolation, and conflict detection
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

pub type TxnId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnStatus { Active, Committed, Aborted }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictType { None, ReadWrite, WriteWrite }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecordType { Write = 1, Commit = 2, Abort = 3, Checkpoint = 4 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub lsn: u64,
    pub txn_id: TxnId,
    pub record_type: WalRecordType,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: TxnId,
    pub start_ts: u64,
    pub commit_ts: Option<u64>,
    pub status: TxnStatus,
    pub write_set: Vec<(String, Vec<u8>)>,
    pub read_set: HashSet<String>,
}

impl Transaction {
    pub fn new(id: TxnId, start_ts: u64) -> Self {
        Self { id, start_ts, commit_ts: None, status: TxnStatus::Active,
               write_set: Vec::new(), read_set: HashSet::new() }
    }
    pub fn commit(&mut self, commit_ts: u64) { self.commit_ts = Some(commit_ts); self.status = TxnStatus::Committed; }
    pub fn abort(&mut self) { self.status = TxnStatus::Aborted; }
    pub fn record_read(&mut self, key: &str) { self.read_set.insert(key.to_string()); }
    pub fn record_write(&mut self, key: &str, value: Vec<u8>) { self.write_set.push((key.to_string(), value)); }

    pub fn has_read_write_conflict(&self, other: &Transaction) -> bool {
        self.write_set.iter().any(|(k, _)| other.read_set.contains(k))
    }
    pub fn has_write_write_conflict(&self, other: &Transaction) -> bool {
        self.write_set.iter().any(|(k, _)| other.write_set.iter().any(|(ok, _)| ok == k))
    }
    pub fn check_conflicts(&self, active: &[Transaction]) -> ConflictType {
        for other in active {
            if other.id == self.id || other.status != TxnStatus::Active { continue; }
            if self.has_write_write_conflict(other) { return ConflictType::WriteWrite; }
            if self.has_read_write_conflict(other) { return ConflictType::ReadWrite; }
        }
        ConflictType::None
    }
}

/// Version store for MVCC
pub struct VersionStore {
    versions: RwLock<BTreeMap<u64, (TxnId, Vec<u8>)>>,
    committed: RwLock<HashSet<TxnId>>,
}

impl VersionStore {
    pub fn new() -> Self {
        Self { versions: RwLock::new(BTreeMap::new()), committed: RwLock::new(HashSet::new()) }
    }
    pub fn write(&self, ts: u64, txn_id: TxnId, data: Vec<u8>) {
        self.versions.write().insert(ts, (txn_id, data));
    }
    pub fn commit_txn(&self, txn_id: TxnId) { self.committed.write().insert(txn_id); }
    pub fn read(&self, snapshot_ts: u64) -> Option<Vec<u8>> {
        let versions = self.versions.read();
        let committed = self.committed.read();
        versions.range(..=snapshot_ts).rev()
            .find(|(_, (tid, _))| committed.contains(tid))
            .map(|(_, (_, data))| data.clone())
    }
}

/// Transaction manager with optimistic concurrency control
pub struct TransactionManager {
    next_txn_id: AtomicU64,
    next_ts: AtomicU64,
    active_txns: RwLock<Vec<Transaction>>,
    committed_txns: RwLock<Vec<(TxnId, u64, Vec<(String, Vec<u8>)>)>>, // (id, commit_ts, write_set)
    version_stores: RwLock<HashMap<String, Arc<VersionStore>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            next_ts: AtomicU64::new(1),
            active_txns: RwLock::new(Vec::new()),
            committed_txns: RwLock::new(Vec::new()),
            version_stores: RwLock::new(HashMap::new()),
        }
    }

    pub fn begin(&self) -> Transaction {
        let id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
        let ts = self.next_ts.load(Ordering::Relaxed);
        let txn = Transaction::new(id, ts);
        self.active_txns.write().push(txn.clone());
        txn
    }

    /// Commit with conflict detection (optimistic locking)
    pub fn commit(&self, txn: &mut Transaction) -> anyhow::Result<u64> {
        // Check conflicts with active transactions
        {
            let active = self.active_txns.read();
            match txn.check_conflicts(&active) {
                ConflictType::WriteWrite => {
                    txn.abort();
                    self.remove_active(txn.id);
                    return Err(anyhow::anyhow!("write-write conflict"));
                }
                ConflictType::ReadWrite => {
                    txn.abort();
                    self.remove_active(txn.id);
                    return Err(anyhow::anyhow!("read-write conflict"));
                }
                ConflictType::None => {}
            }
        }

        // Check conflicts with recently committed transactions
        {
            let committed = self.committed_txns.read();
            for (_, _, write_set) in committed.iter() {
                for (key, _) in &txn.write_set {
                    // Write-write conflict
                    if write_set.iter().any(|(k, _)| k == key) {
                        txn.abort();
                        self.remove_active(txn.id);
                        return Err(anyhow::anyhow!("write-write conflict with committed txn"));
                    }
                }
                for (key, _) in write_set {
                    // Read-write conflict (this txn read a key that was written by committed txn)
                    if txn.read_set.contains(key) {
                        txn.abort();
                        self.remove_active(txn.id);
                        return Err(anyhow::anyhow!("read-write conflict with committed txn"));
                    }
                }
            }
        }

        // Commit
        let commit_ts = self.next_ts.fetch_add(1, Ordering::Relaxed);
        txn.commit(commit_ts);

        // Record in committed list
        self.committed_txns.write().push((txn.id, commit_ts, txn.write_set.clone()));

        // Mark in version stores
        let stores = self.version_stores.read();
        for (_, store) in stores.iter() {
            store.commit_txn(txn.id);
        }

        self.remove_active(txn.id);
        Ok(commit_ts)
    }

    pub fn abort(&self, txn: &mut Transaction) {
        txn.abort();
        self.remove_active(txn.id);
    }

    fn remove_active(&self, txn_id: TxnId) {
        self.active_txns.write().retain(|t| t.id != txn_id);
    }

    pub fn get_version_store(&self, key: &str) -> Arc<VersionStore> {
        self.version_stores.write()
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(VersionStore::new()))
            .clone()
    }

    pub fn write(&self, key: &str, txn_id: TxnId, data: Vec<u8>) {
        let ts = self.next_ts.fetch_add(1, Ordering::Relaxed);
        self.get_version_store(key).write(ts, txn_id, data);
    }

    pub fn read(&self, key: &str, snapshot_ts: u64) -> Option<Vec<u8>> {
        self.get_version_store(key).read(snapshot_ts)
    }
}

impl Default for TransactionManager { fn default() -> Self { Self::new() } }

/// WAL writer
pub struct WalWriter { writer: BufWriter<File>, next_lsn: AtomicU64 }
impl WalWriter {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path.as_ref())?;
        Ok(Self { writer: BufWriter::new(file), next_lsn: AtomicU64::new(1) })
    }
    pub fn append(&mut self, record: &WalRecord) -> anyhow::Result<u64> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::Relaxed);
        let encoded = bincode::serialize(record)?;
        self.writer.write_all(&(encoded.len() as u32).to_le_bytes())?;
        self.writer.write_all(&encoded)?;
        self.writer.flush()?;
        Ok(lsn)
    }
}

/// WAL reader
pub struct WalReader { reader: BufReader<File> }
impl WalReader {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self { reader: BufReader::new(File::open(path)?) })
    }
    pub fn read_all(&mut self) -> anyhow::Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match self.reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut data = vec![0u8; len];
            self.reader.read_exact(&mut data)?;
            records.push(bincode::deserialize(&data)?);
        }
        Ok(records)
    }
}

/// MVCC version store
pub struct VersionStore2 { versions: RwLock<BTreeMap<u64, (TxnId, Vec<u8>)>> }
impl VersionStore2 {
    pub fn new() -> Self { Self { versions: RwLock::new(BTreeMap::new()) } }
    pub fn write(&self, ts: u64, txn_id: TxnId, data: Vec<u8>) {
        self.versions.write().insert(ts, (txn_id, data));
    }
    pub fn read(&self, snapshot_ts: u64) -> Option<Vec<u8>> {
        self.versions.read().range(..=snapshot_ts).rev().next().map(|(_, (_, d))| d.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_lifecycle() {
        let tm = TransactionManager::new();
        let mut txn = tm.begin();
        assert_eq!(txn.status, TxnStatus::Active);
        let ts = tm.commit(&mut txn).unwrap();
        assert_eq!(txn.status, TxnStatus::Committed);
        assert!(ts > 0);
    }

    #[test]
    fn test_write_write_conflict() {
        let tm = TransactionManager::new();
        let mut txn1 = tm.begin();
        let mut txn2 = tm.begin();
        txn1.record_write("key1", vec![1]);
        txn2.record_write("key1", vec![2]);
        let result = tm.commit(&mut txn1);
        assert!(result.is_ok());
        // txn2 should conflict
        let result = tm.commit(&mut txn2);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_write_conflict() {
        let tm = TransactionManager::new();
        let mut txn1 = tm.begin();
        let mut txn2 = tm.begin();
        txn2.record_read("key1");
        txn1.record_write("key1", vec![1]);
        tm.commit(&mut txn1).unwrap();
        let result = tm.commit(&mut txn2);
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_isolation() {
        let tm = TransactionManager::new();
        let mut txn = tm.begin();
        tm.write("key1", txn.id, vec![1, 2, 3]);
        tm.commit(&mut txn).unwrap();
        let snap = tm.get_version_store("key1").read(1);
        assert_eq!(snap, Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_wal_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        { let mut w = WalWriter::open(&path).unwrap();
          for i in 0..10 { w.append(&WalRecord { lsn: 0, txn_id: i, record_type: WalRecordType::Write, data: format!("r{}", i).into_bytes() }).unwrap(); } }
        let mut r = WalReader::open(&path).unwrap();
        assert_eq!(r.read_all().unwrap().len(), 10);
    }
}

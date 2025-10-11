use crate::commands::Command;
use crate::resp::RedisValueRef;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use tokio::sync::RwLock;

// Per-client transaction state
pub struct TransactionState {
    // None = not in transaction
    // Some(queue) = in transaction, commands are queued
    transaction_queue: Option<VecDeque<Command>>,
}

impl TransactionState {
    pub fn new() -> Self {
        TransactionState {
            transaction_queue: None,
        }
    }
}

pub struct Transaction {
    tr: RwLock<HashMap<SocketAddr, TransactionState>>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            tr: RwLock::new(HashMap::new()),
        }
    }

    pub async fn start_transaction(&self, addr: SocketAddr) -> RedisValueRef {
        let mut clients = self.tr.write().await;
        let state = clients.entry(addr).or_insert_with(TransactionState::new);

        if state.transaction_queue.is_some() {
            return RedisValueRef::Error(Bytes::from("ERR MULTI calls can not be nested"));
        }

        state.transaction_queue = Some(VecDeque::new());
        RedisValueRef::String(Bytes::from("OK"))
    }

    pub async fn in_transaction(&self, addr: SocketAddr) -> bool {
        let clients = self.tr.read().await;
        clients
            .get(&addr)
            .map(|state| state.transaction_queue.is_some())
            .unwrap_or(false)
    }

    pub async fn queue_command(&self, addr: SocketAddr, command: Command) -> RedisValueRef {
        let mut clients = self.tr.write().await;
        let state = clients.entry(addr).or_insert_with(TransactionState::new);

        if let Some(queue) = &mut state.transaction_queue {
            queue.push_back(command);
            RedisValueRef::String(Bytes::from("QUEUED"))
        } else {
            // Not in transaction - this shouldn't happen
            RedisValueRef::Error(Bytes::from("ERR EXEC without MULTI"))
        }
    }

    pub async fn discard_transaction(&self, addr: SocketAddr) -> RedisValueRef {
        let mut clients = self.tr.write().await;

        if let Some(state) = clients.get_mut(&addr) {
            if state.transaction_queue.is_none() {
                return RedisValueRef::Error(Bytes::from("ERR DISCARD without MULTI"));
            }
            state.transaction_queue = None;
            RedisValueRef::String(Bytes::from("OK"))
        } else {
            RedisValueRef::Error(Bytes::from("ERR DISCARD without MULTI"))
        }
    }

    pub async fn exec_transaction(&self, addr: SocketAddr) -> Option<VecDeque<Command>> {
        let mut clients = self.tr.write().await;
        clients
            .get_mut(&addr)
            .and_then(|state| state.transaction_queue.take())
    }
}

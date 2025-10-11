use crate::commands::Command;
use crate::resp::RedisValueRef;
use bytes::Bytes;
use std::collections::VecDeque;

// Per-client transaction state
pub struct ClientState {
    // None = not in transaction
    // Some(queue) = in transaction, commands are queued
    transaction_queue: Option<VecDeque<Command>>,
}

impl ClientState {
    pub fn new() -> Self {
        ClientState {
            transaction_queue: None,
        }
    }

    pub fn start_transaction(&mut self) -> RedisValueRef {
        if self.transaction_queue.is_some() {
            // Already in a transaction
            return RedisValueRef::Error(Bytes::from("ERR MULTI calls can not be nested"));
        }

        self.transaction_queue = Some(VecDeque::new());
        RedisValueRef::String(Bytes::from("OK"))
    }

    pub fn in_transaction(&self) -> bool {
        self.transaction_queue.is_some()
    }

    pub fn queue_command(&mut self, command: Command) -> RedisValueRef {
        if let Some(queue) = &mut self.transaction_queue {
            queue.push_back(command);
            RedisValueRef::String(Bytes::from("QUEUED"))
        } else {
            // Not in transaction - this shouldn't happen
            RedisValueRef::Error(Bytes::from("ERR EXEC without MULTI"))
        }
    }

    pub fn discard_transaction(&mut self) -> RedisValueRef {
        if self.transaction_queue.is_none() {
            return RedisValueRef::Error(Bytes::from("ERR DISCARD without MULTI"));
        }

        self.transaction_queue = None;
        RedisValueRef::String(Bytes::from("OK"))
    }

    pub fn exec_transaction(&mut self) -> Option<VecDeque<Command>> {
        self.transaction_queue.take()
    }
}

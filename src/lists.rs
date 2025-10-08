use crate::resp::RedisValueRef;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use tokio::sync::{oneshot, RwLock};
use tokio::time::{timeout, Duration};

// Type alias for the map of blocked client notification channels.
// The oneshot::Sender is used to wake up a single blocked BLPOP task.
type BlockedClientsMap = HashMap<Bytes, VecDeque<oneshot::Sender<bool>>>;

/// A concurrent, thread-safe implementation of a Redis List, supporting
/// both standard L/RPUSH/L/LPOP operations and the blocking BLPOP.
pub struct List {
    // RwLock protects the blocked clients map.
    blocked: RwLock<BlockedClientsMap>,
    // RwLock protects the lists data structure (Key -> List).
    lists: RwLock<HashMap<Bytes, VecDeque<Bytes>>>,
}

impl List {
    /// Creates a new, empty List store.
    pub fn new() -> Self {
        Self {
            lists: RwLock::new(HashMap::new()),
            blocked: RwLock::new(HashMap::new()),
        }
    }

    /// Appends a value to the tail (right) of the list stored at `key`.
    ///
    /// It safely drops the 'lists' write lock before acquiring the 'blocked' lock
    /// to avoid deadlocks, and then attempts to wake up a waiting BLPOP client.
    pub async fn rpush(&self, key: &Bytes, value: Bytes) -> i64 {
        // --- 1. Modify Data ---
        let mut lists = self.lists.write().await;
        let list = lists.entry(key.clone()).or_insert_with(VecDeque::new);
        list.push_back(value);
        let new_len = list.len() as i64;

        // CRITICAL: Drop the 'lists' write lock immediately.
        // This allows a woken-up BLPOP client (which needs to acquire the 'lists' lock)
        // to proceed without deadlocking with this RPUSH task (which is about to
        // acquire the 'blocked' lock).
        drop(lists);

        // --- 2. Notify Blocked Clients ---
        let mut blocked_clients = self.blocked.write().await;
        if let Some(notifiers) = blocked_clients.get_mut(key) {
            // Loop to notify the first live client. If a send fails (because the
            // client timed out and dropped the receiver), we continue to the next
            // client in the queue, effectively cleaning up dead waiters.
            while let Some(notifier) = notifiers.pop_front() {
                if notifier.send(true).is_ok() {
                    // Successfully notified a client, so we stop and they will handle the pop.
                    break;
                }
                // Send failed: the receiver was dropped (client timed out). Clean up and continue.
            }

            // Cleanup: Remove the key entry if the queue of notifiers is now empty.
            if notifiers.is_empty() {
                blocked_clients.remove(key);
            }
        }

        new_len
    }

    /// Prepends a value to the head (left) of the list stored at `key`.
    ///
    /// The logic mirrors `rpush` for thread-safe list modification and client notification.
    pub async fn lpush(&self, key: &Bytes, value: Bytes) -> i64 {
        // --- 1. Modify Data ---
        let mut lists = self.lists.write().await;
        let list = lists.entry(key.clone()).or_insert_with(VecDeque::new);
        list.push_front(value);
        let new_len = list.len() as i64;

        drop(lists);

        // --- 2. Notify Blocked Clients ---
        let mut blocked_clients = self.blocked.write().await;
        if let Some(notifiers) = blocked_clients.get_mut(key) {
            // Robust notification loop (same as in rpush)
            while let Some(notifier) = notifiers.pop_front() {
                if notifier.send(true).is_ok() {
                    break;
                }
            }

            if notifiers.is_empty() {
                blocked_clients.remove(key);
            }
        }

        new_len
    }

    /// Returns the length of the list stored at `key`.
    pub async fn llen(&self, key: &Bytes) -> i64 {
        let lists = self.lists.read().await;
        if let Some(list) = lists.get(key) {
            list.len() as i64
        } else {
            0
        }
    }

    /// Returns the specified elements of the list stored at `key`.
    /// Handles positive and negative indices (where -1 is the last element).
    pub async fn lrange(&self, key: &Bytes, start: isize, end: isize) -> Vec<RedisValueRef> {
        let lists = self.lists.read().await;

        let Some(list) = lists.get(key) else {
            return Vec::new();
        };

        let len = list.len() as isize;
        if len == 0 {
            return Vec::new();
        }

        // Convert negative indices and clamp to bounds
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len - 1)
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        // If the resulting range is invalid (e.g., start > end or end is outside the list)
        if start > end || end < 0 {
            return Vec::new();
        }

        // Collect the elements (range is inclusive)
        (start..=end)
            .filter_map(|i| list.get(i as usize))
            .map(|item| RedisValueRef::BulkString(item.clone()))
            .collect()
    }

    /// Removes and returns up to `count` elements from the head (left) of the list.
    /// Removes the key from the map if the list becomes empty.
    pub async fn lpop(&self, key: &Bytes, count: usize) -> Option<Vec<RedisValueRef>> {
        let mut lists = self.lists.write().await;

        let list = lists.get_mut(key)?;

        if list.is_empty() {
            return None;
        }

        let mut res = Vec::new();
        // Pop elements one by one up to the requested count
        for _ in 0..count {
            if let Some(element) = list.pop_front() {
                res.push(RedisValueRef::BulkString(element));
            } else {
                break;
            }
        }

        // Cleanup: Remove the list key if the list is now empty.
        if list.is_empty() {
            lists.remove(key);
        }

        Some(res)
    }

    /// Removes and returns the first element of the list, or blocks up to `duration`
    /// until an element is available.
    pub async fn blpop(&self, key: &Bytes, duration: Duration) -> RedisValueRef {
        // --- 1. Non-blocking Check ---
        {
            let mut lists = self.lists.write().await;
            if let Some(list) = lists.get_mut(key) {
                if let Some(value) = list.pop_front() {
                    // Clean up empty list
                    if list.is_empty() {
                        lists.remove(key);
                    }
                    // Immediate result found
                    return RedisValueRef::Array(vec![
                        RedisValueRef::BulkString(key.clone()),
                        RedisValueRef::BulkString(value),
                    ]);
                }
            }
        } // Lock for 'lists' dropped here

        // --- 2. Blocking Setup ---
        let (tx, rx) = oneshot::channel::<bool>();

        {
            // Acquire lock to register this client as blocked
            let mut blocked_clients = self.blocked.write().await;
            blocked_clients
                .entry(key.clone())
                .or_default()
                .push_back(tx);
        } // Lock for 'blocked' dropped here

        // Wait for notification or timeout
        match timeout(duration, rx).await {
            // Case 1: Successfully received notification (woken up by RPUSH/LPUSH)
            Ok(Ok(_)) => {
                // Must re-acquire lock to safely access the list and pop the element
                let mut lists = self.lists.write().await;
                if let Some(list) = lists.get_mut(key) {
                    if let Some(value) = list.pop_front() {
                        if list.is_empty() {
                            lists.remove(key);
                        }
                        return RedisValueRef::Array(vec![
                            RedisValueRef::BulkString(key.clone()),
                            RedisValueRef::BulkString(value),
                        ]);
                    }
                }
                // Should not happen if the push logic is correct, but return NullArray as fallback
                RedisValueRef::NullArray
            }
            // Case 2: Timeout or channel closed/error (rx dropped)
            Ok(Err(_)) | Err(_) => {
                // The push logic (rpush/lpush) is now responsible for draining this
                // timed-out client's sender from the queue if a subsequent push happens.
                // We do not need to perform expensive VecDeque iteration here.
                RedisValueRef::NullArray
            }
        }
    }
}

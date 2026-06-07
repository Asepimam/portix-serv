use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

pub struct TtlCache<K, V> {
    ttl: Duration,
    values: RwLock<HashMap<K, CacheEntry<V>>>,
}

struct CacheEntry<V> {
    value: V,
    expires_at: Instant,
}

impl<K, V> TtlCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            values: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, key: &K) -> Option<V> {
        let now = Instant::now();
        let values = self.values.read().await;
        values
            .get(key)
            .filter(|entry| entry.expires_at > now)
            .map(|entry| entry.value.clone())
    }

    pub async fn put(&self, key: K, value: V) {
        let entry = CacheEntry {
            value,
            expires_at: Instant::now() + self.ttl,
        };
        self.values.write().await.insert(key, entry);
    }
}

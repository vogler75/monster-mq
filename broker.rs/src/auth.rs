use crate::stores::topic::match_topic;
use crate::stores::traits::UserStore;
use crate::stores::types::{AclRule, User};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Cache holds users and ACL rules in memory; refreshed every 30 s and on
/// every mutation. Mirrors `internal/auth/auth.go`.
pub struct Cache {
    store: Arc<dyn UserStore>,
    inner: RwLock<Inner>,
    pub anonymous_allow: bool,
}

struct Inner {
    users: HashMap<String, User>,
    rules_by_user: HashMap<String, Vec<AclRule>>, // sorted by priority DESC
}

impl Cache {
    pub fn new(store: Arc<dyn UserStore>, anonymous_allow: bool) -> Arc<Self> {
        Arc::new(Self {
            store,
            inner: RwLock::new(Inner { users: HashMap::new(), rules_by_user: HashMap::new() }),
            anonymous_allow,
        })
    }

    pub async fn refresh(&self) -> anyhow::Result<()> {
        let (users, rules) = self.store.load_all().await?;
        let mut by_user: HashMap<String, Vec<AclRule>> = HashMap::new();
        for r in rules { by_user.entry(r.username.clone()).or_default().push(r); }
        for v in by_user.values_mut() { v.sort_by(|a, b| b.priority.cmp(&a.priority)); }
        let users_map = users.into_iter().map(|u| (u.username.clone(), u)).collect();
        let mut w = self.inner.write();
        w.users = users_map;
        w.rules_by_user = by_user;
        Ok(())
    }

    /// Spawns a refresh loop that runs every `every` (commonly 30 s).
    pub fn start_refresher(self: Arc<Self>, every: Duration) {
        tokio::spawn(async move {
            let mut t = tokio::time::interval(every);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                let _ = self.refresh().await;
            }
        });
    }

    pub async fn validate(&self, username: &str, password: &str) -> bool {
        if username.is_empty() { return self.anonymous_allow; }
        let exists = {
            let r = self.inner.read();
            matches!(r.users.get(username), Some(u) if u.enabled)
        };
        if !exists { return false; }
        // Bcrypt verification through the store
        match self.store.validate_credentials(username, password).await {
            Ok(Some(_)) => true,
            _ => false,
        }
    }

    pub fn allow(&self, username: &str, topic: &str, write: bool) -> bool {
        if username.is_empty() { return self.anonymous_allow; }
        let r = self.inner.read();
        let Some(u) = r.users.get(username) else { return false; };
        if !u.enabled { return false; }
        if u.is_admin { return true; }
        if write && !u.can_publish { return false; }
        if !write && !u.can_subscribe { return false; }
        let Some(rules) = r.rules_by_user.get(username) else { return true; };
        if rules.is_empty() { return true; }
        for rule in rules {
            if match_topic(&rule.topic_pattern, topic) {
                return if write { rule.can_publish } else { rule.can_subscribe };
            }
        }
        false
    }

    pub fn user(&self, username: &str) -> Option<User> {
        self.inner.read().users.get(username).cloned()
    }

    pub fn all_users(&self) -> Vec<User> {
        self.inner.read().users.values().cloned().collect()
    }

    pub fn user_rules(&self, username: &str) -> Vec<AclRule> {
        self.inner.read().rules_by_user.get(username).cloned().unwrap_or_default()
    }
}

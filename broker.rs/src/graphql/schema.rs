use super::mutation::Mutation;
use super::query::Query;
use super::subscription::Subscription;
use crate::archive::Manager as ArchiveManager;
use crate::auth::Cache as AuthCache;
use crate::bridge::mqttclient::Manager as BridgeManager;
use crate::config::Config;
use crate::log_bus::Bus as LogBus;
use crate::metrics::Collector as MetricsCollector;
use crate::pubsub::Bus as PubsubBus;
use crate::stores::storage::Storage;
use async_graphql::Schema;
use std::sync::Arc;

/// Ctx — the type passed into every resolver via `ctx.data::<Ctx>()`.
/// It bundles handles to all subsystems so resolvers can read/write state.
#[derive(Clone)]
pub struct Ctx {
    pub cfg: Config,
    pub storage: Storage,
    pub bus: Arc<PubsubBus>,
    pub log_bus: Arc<LogBus>,
    pub archives: Arc<ArchiveManager>,
    pub auth: Arc<AuthCache>,
    pub bridges: Option<Arc<BridgeManager>>,
    pub collector: Option<Arc<MetricsCollector>>,
    pub publish_fn: crate::broker::server::PublishFn,
}

pub type AppSchema = Schema<Query, Mutation, Subscription>;

pub fn build_schema(ctx: Ctx) -> AppSchema {
    Schema::build(Query, Mutation, Subscription)
        .data(ctx)
        .finish()
}

use super::payload::{encode_payload, DataFormat};
use super::query::{ExceptionInfo, SystemLogEntry};
use super::schema::Ctx;
use async_graphql::{Context, Result, SimpleObject, Subscription};
use chrono::Utc;
use futures::stream::{Stream, StreamExt};
use std::time::Duration;

#[derive(SimpleObject, Clone)]
pub struct TopicUpdate {
    pub topic: String,
    pub payload: String,
    pub format: DataFormat,
    pub timestamp: i64,
    pub qos: i32,
    pub retained: bool,
    pub client_id: Option<String>,
}

#[derive(SimpleObject, Clone)]
pub struct TopicUpdateBulk {
    pub updates: Vec<TopicUpdate>,
    pub count: i32,
    pub timestamp: i64,
}

pub struct Subscription;

#[Subscription]
impl Subscription {
    /// topicUpdates — every PUBLISH matching `topicFilters`.
    async fn topic_updates<'ctx>(
        &self, ctx: &Context<'ctx>,
        topic_filters: Vec<String>,
        format: Option<DataFormat>,
    ) -> Result<impl Stream<Item = TopicUpdate> + 'ctx> {
        let c = ctx.data::<Ctx>()?.clone();
        let sub = c.bus.subscribe(topic_filters, 256);
        Ok(make_topic_updates_stream(sub, format))
    }

    /// topicUpdatesBulk — batched variant.
    async fn topic_updates_bulk<'ctx>(
        &self, ctx: &Context<'ctx>,
        topic_filters: Vec<String>,
        format: Option<DataFormat>,
        timeout_ms: Option<i32>,
        max_size: Option<i32>,
    ) -> Result<impl Stream<Item = TopicUpdateBulk> + 'ctx> {
        let c = ctx.data::<Ctx>()?.clone();
        let mut sub = c.bus.subscribe(topic_filters, 1024);
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(250).max(1) as u64);
        let cap = max_size.unwrap_or(100).max(1) as usize;
        Ok(async_stream::stream! {
            let mut buf: Vec<TopicUpdate> = Vec::with_capacity(cap);
            loop {
                tokio::select! {
                    msg = sub.rx.recv() => {
                        let Some(msg) = msg else { break; };
                        let (payload, fmt) = encode_payload(&msg.payload, format);
                        buf.push(TopicUpdate {
                            topic: msg.topic_name, payload, format: fmt,
                            timestamp: msg.time.timestamp_millis(), qos: msg.qos as i32,
                            retained: msg.is_retain,
                            client_id: if msg.client_id.is_empty() { None } else { Some(msg.client_id) },
                        });
                        if buf.len() >= cap {
                            yield TopicUpdateBulk { count: buf.len() as i32, updates: std::mem::take(&mut buf), timestamp: Utc::now().timestamp_millis() };
                        }
                    }
                    _ = tokio::time::sleep(timeout) => {
                        if !buf.is_empty() {
                            yield TopicUpdateBulk { count: buf.len() as i32, updates: std::mem::take(&mut buf), timestamp: Utc::now().timestamp_millis() };
                        }
                    }
                }
            }
        }.boxed())
    }

    /// systemLogs — live log records.
    async fn system_logs<'ctx>(
        &self, ctx: &Context<'ctx>,
        node: Option<String>, level: Option<Vec<String>>, logger: Option<String>,
        thread: Option<i64>, source_class: Option<String>, source_method: Option<String>, message: Option<String>,
    ) -> Result<impl Stream<Item = SystemLogEntry> + 'ctx> {
        let c = ctx.data::<Ctx>()?.clone();
        let rx = c.log_bus.subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(rx)
            .filter_map(move |r| {
                let node = node.clone();
                let level = level.clone();
                let logger = logger.clone();
                let source_class = source_class.clone();
                let source_method = source_method.clone();
                let message = message.clone();
                async move {
                    let Ok(e) = r else { return None; };
                    if let Some(n) = node { if n != "+" && e.node != n { return None; } }
                    if let Some(ls) = level { if !ls.iter().any(|x| x.eq_ignore_ascii_case(&e.level)) { return None; } }
                    if let Some(s) = logger { if !e.logger.contains(&s) { return None; } }
                    if let Some(_t) = thread { /* thread is always 0 in this port */ }
                    if let Some(s) = source_class { if !e.source_class.as_deref().unwrap_or("").contains(&s) { return None; } }
                    if let Some(s) = source_method { if !e.source_method.as_deref().unwrap_or("").contains(&s) { return None; } }
                    if let Some(s) = message { if !e.message.contains(&s) { return None; } }
                    Some(SystemLogEntry {
                        timestamp: e.timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        level: e.level, logger: e.logger, message: e.message, thread: e.thread, node: e.node,
                        source_class: e.source_class, source_method: e.source_method,
                        parameters: if e.parameters.is_empty() { None } else { Some(e.parameters) },
                        exception: e.exception.map(|x| ExceptionInfo {
                            class: x.class, message: x.message, stack_trace: x.stack_trace,
                        }),
                    })
                }
            });
        Ok(stream)
    }
}

fn make_topic_updates_stream(
    mut sub: crate::pubsub::Subscription,
    format: Option<DataFormat>,
) -> impl Stream<Item = TopicUpdate> {
    async_stream::stream! {
        while let Some(msg) = sub.rx.recv().await {
            let (payload, fmt) = encode_payload(&msg.payload, format);
            yield TopicUpdate {
                topic: msg.topic_name, payload, format: fmt,
                timestamp: msg.time.timestamp_millis(),
                qos: msg.qos as i32, retained: msg.is_retain,
                client_id: if msg.client_id.is_empty() { None } else { Some(msg.client_id) },
            };
        }
    }
}

use super::payload::{DataFormat, MessageArchiveType, MessageStoreType, PayloadFormat};
use super::query::{AclRuleInfo, ArchiveGroupInfo, MqttClient, UserInfo};
use super::schema::Ctx;
use crate::archive::validate_group_name;
use crate::stores::types::{ArchiveGroupConfig, AclRule, DeviceConfig as StoreDeviceConfig, User};
use async_graphql::{Context, InputObject, Object, Result, SimpleObject, ID};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};

// ---------- inputs ----------

#[derive(InputObject)]
pub struct PublishInput {
    pub topic: String,
    pub payload: Option<String>,
    pub payload_base64: Option<String>,
    pub payload_json: Option<async_graphql::Json<serde_json::Value>>,
    pub qos: Option<i32>,
    pub retain: Option<bool>,
    pub format: Option<DataFormat>,
}

#[derive(InputObject)]
pub struct CreateArchiveGroupInput {
    pub name: String,
    pub topic_filter: Vec<String>,
    pub retained_only: Option<bool>,
    pub last_val_type: MessageStoreType,
    pub archive_type: MessageArchiveType,
    pub payload_format: Option<PayloadFormat>,
    pub last_val_retention: Option<String>,
    pub archive_retention: Option<String>,
    pub purge_interval: Option<String>,
}

#[derive(InputObject)]
pub struct UpdateArchiveGroupInput {
    pub name: String,
    pub topic_filter: Option<Vec<String>>,
    pub retained_only: Option<bool>,
    pub last_val_type: Option<MessageStoreType>,
    pub archive_type: Option<MessageArchiveType>,
    pub payload_format: Option<PayloadFormat>,
    pub last_val_retention: Option<String>,
    pub archive_retention: Option<String>,
    pub purge_interval: Option<String>,
}

#[derive(InputObject)]
pub struct CreateUserInput {
    pub username: String,
    pub password: String,
    pub enabled: Option<bool>,
    pub can_subscribe: Option<bool>,
    pub can_publish: Option<bool>,
    pub is_admin: Option<bool>,
}

#[derive(InputObject)]
pub struct UpdateUserInput {
    pub username: String,
    pub enabled: Option<bool>,
    pub can_subscribe: Option<bool>,
    pub can_publish: Option<bool>,
    pub is_admin: Option<bool>,
}

#[derive(InputObject)]
pub struct SetPasswordInput { pub username: String, pub password: String }

#[derive(InputObject)]
pub struct CreateAclRuleInput {
    pub username: String, pub topic_pattern: String,
    pub can_subscribe: Option<bool>, pub can_publish: Option<bool>,
    pub priority: Option<i32>,
}

#[derive(InputObject)]
pub struct UpdateAclRuleInput {
    pub id: String, pub username: String, pub topic_pattern: String,
    pub can_subscribe: Option<bool>, pub can_publish: Option<bool>,
    pub priority: Option<i32>,
}

#[derive(InputObject)]
pub struct UserPropertyInput { pub key: String, pub value: String }

#[derive(InputObject)]
pub struct MqttClientAddressInput {
    pub mode: String,
    pub remote_topic: String,
    pub local_topic: String,
    pub remove_path: Option<bool>,
    pub qos: Option<i32>,
    pub no_local: Option<bool>,
    pub retain_handling: Option<i32>,
    pub retain_as_published: Option<bool>,
    pub message_expiry_interval: Option<i64>,
    pub content_type: Option<String>,
    pub response_topic_pattern: Option<String>,
    pub payload_format_indicator: Option<bool>,
    pub user_properties: Option<Vec<UserPropertyInput>>,
}

#[derive(InputObject)]
pub struct MqttClientConnectionConfigInput {
    pub broker_url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: Option<String>,
    pub clean_session: Option<bool>,
    pub keep_alive: Option<i32>,
    pub reconnect_delay: Option<i64>,
    pub connection_timeout: Option<i64>,
    pub addresses: Option<Vec<MqttClientAddressInput>>,
    pub buffer_enabled: Option<bool>,
    pub buffer_size: Option<i32>,
    pub persist_buffer: Option<bool>,
    pub delete_oldest_messages: Option<bool>,
    pub ssl_verify_certificate: Option<bool>,
    pub protocol_version: Option<i32>,
    pub session_expiry_interval: Option<i64>,
    pub receive_maximum: Option<i32>,
    pub maximum_packet_size: Option<i64>,
    pub topic_alias_maximum: Option<i32>,
}

#[derive(InputObject)]
pub struct MqttClientInput {
    pub name: String,
    pub namespace: String,
    pub node_id: String,
    pub enabled: Option<bool>,
    pub config: MqttClientConnectionConfigInput,
}

// ---------- result envelopes ----------

#[derive(SimpleObject)]
pub struct PublishResult { pub success: bool, pub message: Option<String>, pub topic: String }

#[derive(SimpleObject)]
pub struct PurgeResult { pub success: bool, pub message: Option<String>, pub purged_count: i64 }

#[derive(SimpleObject)]
pub struct SessionRemovalResult { pub success: bool, pub message: Option<String>, pub removed_count: i32 }

#[derive(SimpleObject)]
pub struct LoginResult {
    pub success: bool, pub message: Option<String>,
    pub token: Option<String>, pub username: Option<String>, pub is_admin: Option<bool>,
}

#[derive(SimpleObject)]
pub struct UserManagementResult {
    pub success: bool, pub message: Option<String>,
    pub user: Option<UserInfo>, pub acl_rule: Option<AclRuleInfo>,
}

#[derive(SimpleObject)]
pub struct ArchiveGroupResult {
    pub success: bool, pub message: Option<String>, pub archive_group: Option<ArchiveGroupInfo>,
}

#[derive(SimpleObject)]
pub struct MqttClientResult {
    pub success: bool, pub client: Option<MqttClient>, pub errors: Vec<String>,
}

// ---------- mutation namespaces ----------

pub struct Mutation;

#[Object]
impl Mutation {
    async fn login(&self, ctx: &Context<'_>, username: String, password: String) -> Result<LoginResult> {
        let c = ctx.data::<Ctx>()?;
        if !c.cfg.user_management.enabled || c.cfg.user_management.anonymous_enabled {
            return Ok(LoginResult {
                success: true,
                message: Some("Authentication disabled".to_string()),
                token: None,
                username: Some("anonymous".to_string()),
                is_admin: Some(true),
            });
        }
        match c.storage.users.validate_credentials(&username, &password).await? {
            Some(u) => Ok(LoginResult {
                success: true,
                message: None,
                token: Some(format!("session-{}-{}", u.username, chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0))),
                username: Some(u.username),
                is_admin: Some(u.is_admin),
            }),
            None => Ok(LoginResult {
                success: false,
                message: Some("invalid credentials".to_string()),
                token: None, username: None, is_admin: None,
            }),
        }
    }

    async fn publish(&self, ctx: &Context<'_>, input: PublishInput) -> Result<PublishResult> {
        let c = ctx.data::<Ctx>()?;
        let payload = decode_publish_payload(&input)?;
        let qos = input.qos.unwrap_or(0).clamp(0, 2) as u8;
        let retain = input.retain.unwrap_or(false);
        (c.publish_fn)(&input.topic, payload, retain, qos);
        Ok(PublishResult { success: true, message: None, topic: input.topic })
    }

    async fn publish_batch(&self, ctx: &Context<'_>, inputs: Vec<PublishInput>) -> Result<Vec<PublishResult>> {
        let c = ctx.data::<Ctx>()?;
        let mut out = Vec::with_capacity(inputs.len());
        for input in inputs {
            match decode_publish_payload(&input) {
                Ok(payload) => {
                    let qos = input.qos.unwrap_or(0).clamp(0, 2) as u8;
                    let retain = input.retain.unwrap_or(false);
                    (c.publish_fn)(&input.topic, payload, retain, qos);
                    out.push(PublishResult { success: true, message: None, topic: input.topic });
                }
                Err(e) => out.push(PublishResult { success: false, message: Some(e.message), topic: input.topic }),
            }
        }
        Ok(out)
    }

    async fn purge_queued_messages(&self, ctx: &Context<'_>, client_id: String) -> Result<PurgeResult> {
        let c = ctx.data::<Ctx>()?;
        let n = c.storage.queue.purge_for_client(&client_id).await?;
        Ok(PurgeResult { success: true, message: None, purged_count: n })
    }

    async fn user(&self) -> UserManagementMutations { UserManagementMutations }
    async fn session(&self) -> SessionMutations { SessionMutations }
    async fn archive_group(&self) -> ArchiveGroupMutations { ArchiveGroupMutations }
    async fn mqtt_client(&self) -> MqttClientMutations { MqttClientMutations }
}

fn decode_publish_payload(p: &PublishInput) -> Result<Vec<u8>> {
    if let Some(b64) = &p.payload_base64 {
        return B64.decode(b64).map_err(|e| async_graphql::Error::new(format!("base64 decode: {}", e)));
    }
    if let Some(json) = &p.payload_json {
        return Ok(json.0.to_string().into_bytes());
    }
    Ok(p.payload.clone().unwrap_or_default().into_bytes())
}

// ---------- UserManagementMutations ----------

pub struct UserManagementMutations;

#[Object]
impl UserManagementMutations {
    async fn create_user(&self, ctx: &Context<'_>, input: CreateUserInput) -> Result<UserManagementResult> {
        let c = ctx.data::<Ctx>()?;
        let hash = bcrypt::hash(&input.password, bcrypt::DEFAULT_COST)
            .map_err(|e| async_graphql::Error::new(format!("bcrypt: {}", e)))?;
        let u = User {
            username: input.username.clone(),
            password_hash: hash,
            enabled: input.enabled.unwrap_or(true),
            can_subscribe: input.can_subscribe.unwrap_or(true),
            can_publish: input.can_publish.unwrap_or(true),
            is_admin: input.is_admin.unwrap_or(false),
            ..Default::default()
        };
        c.storage.users.create_user(&u).await?;
        let _ = c.auth.refresh().await;
        Ok(UserManagementResult { success: true, message: None, user: None, acl_rule: None })
    }

    async fn update_user(&self, ctx: &Context<'_>, input: UpdateUserInput) -> Result<UserManagementResult> {
        let c = ctx.data::<Ctx>()?;
        let Some(mut u) = c.storage.users.get_user(&input.username).await? else {
            return Ok(UserManagementResult { success: false, message: Some("user not found".to_string()), user: None, acl_rule: None });
        };
        if let Some(b) = input.enabled { u.enabled = b; }
        if let Some(b) = input.can_subscribe { u.can_subscribe = b; }
        if let Some(b) = input.can_publish { u.can_publish = b; }
        if let Some(b) = input.is_admin { u.is_admin = b; }
        c.storage.users.update_user(&u).await?;
        let _ = c.auth.refresh().await;
        Ok(UserManagementResult { success: true, message: None, user: None, acl_rule: None })
    }

    async fn delete_user(&self, ctx: &Context<'_>, username: String) -> Result<UserManagementResult> {
        let c = ctx.data::<Ctx>()?;
        c.storage.users.delete_user(&username).await?;
        let _ = c.auth.refresh().await;
        Ok(UserManagementResult { success: true, message: None, user: None, acl_rule: None })
    }

    async fn set_password(&self, ctx: &Context<'_>, input: SetPasswordInput) -> Result<UserManagementResult> {
        let c = ctx.data::<Ctx>()?;
        let Some(mut u) = c.storage.users.get_user(&input.username).await? else {
            return Ok(UserManagementResult { success: false, message: Some("user not found".to_string()), user: None, acl_rule: None });
        };
        u.password_hash = bcrypt::hash(&input.password, bcrypt::DEFAULT_COST)
            .map_err(|e| async_graphql::Error::new(format!("bcrypt: {}", e)))?;
        c.storage.users.update_user(&u).await?;
        let _ = c.auth.refresh().await;
        Ok(UserManagementResult { success: true, message: None, user: None, acl_rule: None })
    }

    async fn create_acl_rule(&self, ctx: &Context<'_>, input: CreateAclRuleInput) -> Result<UserManagementResult> {
        let c = ctx.data::<Ctx>()?;
        let r = AclRule {
            id: String::new(),
            username: input.username, topic_pattern: input.topic_pattern,
            can_subscribe: input.can_subscribe.unwrap_or(false),
            can_publish: input.can_publish.unwrap_or(false),
            priority: input.priority.unwrap_or(0),
            created_at: None,
        };
        c.storage.users.create_acl_rule(&r).await?;
        let _ = c.auth.refresh().await;
        Ok(UserManagementResult { success: true, message: None, user: None, acl_rule: None })
    }

    async fn update_acl_rule(&self, ctx: &Context<'_>, input: UpdateAclRuleInput) -> Result<UserManagementResult> {
        let c = ctx.data::<Ctx>()?;
        let r = AclRule {
            id: input.id, username: input.username, topic_pattern: input.topic_pattern,
            can_subscribe: input.can_subscribe.unwrap_or(false),
            can_publish: input.can_publish.unwrap_or(false),
            priority: input.priority.unwrap_or(0),
            created_at: None,
        };
        c.storage.users.update_acl_rule(&r).await?;
        let _ = c.auth.refresh().await;
        Ok(UserManagementResult { success: true, message: None, user: None, acl_rule: None })
    }

    async fn delete_acl_rule(&self, ctx: &Context<'_>, id: String) -> Result<UserManagementResult> {
        let c = ctx.data::<Ctx>()?;
        c.storage.users.delete_acl_rule(&id).await?;
        let _ = c.auth.refresh().await;
        Ok(UserManagementResult { success: true, message: None, user: None, acl_rule: None })
    }
}

// ---------- SessionMutations ----------

pub struct SessionMutations;

#[Object]
impl SessionMutations {
    async fn remove_sessions(&self, ctx: &Context<'_>, client_ids: Vec<String>) -> Result<SessionRemovalResult> {
        let c = ctx.data::<Ctx>()?;
        let mut n = 0;
        for id in client_ids {
            if c.storage.sessions.del_client(&id).await.is_ok() { n += 1; }
        }
        Ok(SessionRemovalResult { success: true, message: None, removed_count: n })
    }
}

// ---------- ArchiveGroupMutations ----------

pub struct ArchiveGroupMutations;

#[Object]
impl ArchiveGroupMutations {
    async fn create(&self, ctx: &Context<'_>, input: CreateArchiveGroupInput) -> Result<ArchiveGroupResult> {
        let c = ctx.data::<Ctx>()?;
        if let Err(e) = validate_group_name(&input.name) {
            return Ok(ArchiveGroupResult { success: false, message: Some(e.to_string()), archive_group: None });
        }
        let cfg = ArchiveGroupConfig {
            name: input.name,
            enabled: true,
            topic_filters: input.topic_filter,
            retained_only: input.retained_only.unwrap_or(false),
            last_val_type: input.last_val_type.into(),
            archive_type: input.archive_type.into(),
            last_val_retention: input.last_val_retention.unwrap_or_default(),
            archive_retention: input.archive_retention.unwrap_or_default(),
            purge_interval: input.purge_interval.unwrap_or_default(),
            payload_format: input.payload_format.unwrap_or(PayloadFormat::Default).into(),
        };
        c.storage.archive_config.save(&cfg).await?;
        c.archives.reload().await?;
        Ok(ArchiveGroupResult { success: true, message: None, archive_group: None })
    }

    async fn update(&self, ctx: &Context<'_>, input: UpdateArchiveGroupInput) -> Result<ArchiveGroupResult> {
        let c = ctx.data::<Ctx>()?;
        let Some(mut existing) = c.storage.archive_config.get(&input.name).await? else {
            return Ok(ArchiveGroupResult { success: false, message: Some("not found".to_string()), archive_group: None });
        };
        if let Some(v) = input.topic_filter { existing.topic_filters = v; }
        if let Some(v) = input.retained_only { existing.retained_only = v; }
        if let Some(v) = input.last_val_type { existing.last_val_type = v.into(); }
        if let Some(v) = input.archive_type { existing.archive_type = v.into(); }
        if let Some(v) = input.payload_format { existing.payload_format = v.into(); }
        if let Some(v) = input.last_val_retention { existing.last_val_retention = v; }
        if let Some(v) = input.archive_retention { existing.archive_retention = v; }
        if let Some(v) = input.purge_interval { existing.purge_interval = v; }
        c.storage.archive_config.update(&existing).await?;
        c.archives.reload().await?;
        Ok(ArchiveGroupResult { success: true, message: None, archive_group: None })
    }

    async fn delete(&self, ctx: &Context<'_>, name: String) -> Result<ArchiveGroupResult> {
        let c = ctx.data::<Ctx>()?;
        c.storage.archive_config.delete(&name).await?;
        c.archives.reload().await?;
        Ok(ArchiveGroupResult { success: true, message: None, archive_group: None })
    }

    async fn enable(&self, ctx: &Context<'_>, name: String) -> Result<ArchiveGroupResult> {
        toggle_enabled(ctx, &name, true).await
    }

    async fn disable(&self, ctx: &Context<'_>, name: String) -> Result<ArchiveGroupResult> {
        toggle_enabled(ctx, &name, false).await
    }
}

async fn toggle_enabled(ctx: &Context<'_>, name: &str, enabled: bool) -> Result<ArchiveGroupResult> {
    let c = ctx.data::<Ctx>()?;
    let Some(mut existing) = c.storage.archive_config.get(name).await? else {
        return Ok(ArchiveGroupResult { success: false, message: Some("not found".to_string()), archive_group: None });
    };
    existing.enabled = enabled;
    c.storage.archive_config.update(&existing).await?;
    c.archives.reload().await?;
    Ok(ArchiveGroupResult { success: true, message: None, archive_group: None })
}

// ---------- MqttClientMutations ----------

pub struct MqttClientMutations;

#[Object]
impl MqttClientMutations {
    async fn create(&self, ctx: &Context<'_>, input: MqttClientInput) -> Result<MqttClientResult> {
        let c = ctx.data::<Ctx>()?;
        let json = mqtt_client_input_to_json(&input);
        let dc = StoreDeviceConfig {
            name: input.name, namespace: input.namespace, node_id: input.node_id,
            r#type: "MQTT_CLIENT".to_string(),
            enabled: input.enabled.unwrap_or(true),
            config: json,
            ..Default::default()
        };
        c.storage.device_config.save(&dc).await?;
        if let Some(b) = c.bridges.clone() { let _ = b.reload().await; }
        Ok(MqttClientResult { success: true, client: None, errors: vec![] })
    }

    async fn update(&self, ctx: &Context<'_>, name: String, input: MqttClientInput) -> Result<MqttClientResult> {
        let c = ctx.data::<Ctx>()?;
        let json = mqtt_client_input_to_json(&input);
        let dc = StoreDeviceConfig {
            name, namespace: input.namespace, node_id: input.node_id,
            r#type: "MQTT_CLIENT".to_string(),
            enabled: input.enabled.unwrap_or(true),
            config: json,
            ..Default::default()
        };
        c.storage.device_config.save(&dc).await?;
        if let Some(b) = c.bridges.clone() { let _ = b.reload().await; }
        Ok(MqttClientResult { success: true, client: None, errors: vec![] })
    }

    async fn delete(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        let c = ctx.data::<Ctx>()?;
        c.storage.device_config.delete(&name).await?;
        if let Some(b) = c.bridges.clone() { let _ = b.reload().await; }
        Ok(true)
    }

    async fn start(&self, ctx: &Context<'_>, name: String) -> Result<MqttClientResult> { toggle_device(ctx, &name, true).await }
    async fn stop(&self, ctx: &Context<'_>, name: String) -> Result<MqttClientResult> { toggle_device(ctx, &name, false).await }

    async fn toggle(&self, ctx: &Context<'_>, name: String, enabled: bool) -> Result<MqttClientResult> {
        toggle_device(ctx, &name, enabled).await
    }

    async fn reassign(&self, ctx: &Context<'_>, name: String, node_id: String) -> Result<MqttClientResult> {
        let c = ctx.data::<Ctx>()?;
        c.storage.device_config.reassign(&name, &node_id).await?;
        if let Some(b) = c.bridges.clone() { let _ = b.reload().await; }
        Ok(MqttClientResult { success: true, client: None, errors: vec![] })
    }

    async fn add_address(&self, ctx: &Context<'_>, device_name: String, input: MqttClientAddressInput) -> Result<MqttClientResult> {
        let c = ctx.data::<Ctx>()?;
        edit_addresses(c, &device_name, |addrs| {
            addrs.push(input_to_address(&input));
        }).await
    }

    async fn update_address(&self, ctx: &Context<'_>, device_name: String, remote_topic: String, input: MqttClientAddressInput) -> Result<MqttClientResult> {
        let c = ctx.data::<Ctx>()?;
        edit_addresses(c, &device_name, |addrs| {
            if let Some(a) = addrs.iter_mut().find(|a| a.remote_topic == remote_topic) {
                *a = input_to_address(&input);
            }
        }).await
    }

    async fn delete_address(&self, ctx: &Context<'_>, device_name: String, remote_topic: String) -> Result<MqttClientResult> {
        let c = ctx.data::<Ctx>()?;
        edit_addresses(c, &device_name, |addrs| {
            addrs.retain(|a| a.remote_topic != remote_topic);
        }).await
    }
}

async fn toggle_device(ctx: &Context<'_>, name: &str, enabled: bool) -> Result<MqttClientResult> {
    let c = ctx.data::<Ctx>()?;
    c.storage.device_config.toggle(name, enabled).await?;
    if let Some(b) = c.bridges.clone() { let _ = b.reload().await; }
    Ok(MqttClientResult { success: true, client: None, errors: vec![] })
}

async fn edit_addresses<F: FnOnce(&mut Vec<crate::bridge::mqttclient::Address>)>(
    c: &Ctx,
    device_name: &str,
    f: F,
) -> Result<MqttClientResult> {
    let Some(mut existing) = c.storage.device_config.get(device_name).await? else {
        return Ok(MqttClientResult { success: false, client: None, errors: vec!["device not found".to_string()] });
    };
    let mut cfg: crate::bridge::mqttclient::BridgeConfig = serde_json::from_str(&existing.config).unwrap_or_default();
    f(&mut cfg.addresses);
    existing.config = serde_json::to_string(&cfg).unwrap_or_default();
    c.storage.device_config.save(&existing).await?;
    if let Some(b) = c.bridges.clone() { let _ = b.reload().await; }
    Ok(MqttClientResult { success: true, client: None, errors: vec![] })
}

fn mqtt_client_input_to_json(input: &MqttClientInput) -> String {
    let cfg = crate::bridge::mqttclient::BridgeConfig {
        broker_url: input.config.broker_url.clone(),
        client_id: input.config.client_id.clone().unwrap_or_else(|| "monstermq-client".to_string()),
        username: input.config.username.clone().unwrap_or_default(),
        password: input.config.password.clone().unwrap_or_default(),
        clean_session: input.config.clean_session.unwrap_or(true),
        keep_alive: input.config.keep_alive.unwrap_or(60),
        connection_timeout: input.config.connection_timeout.unwrap_or(30_000) as i32,
        reconnect_delay: input.config.reconnect_delay.unwrap_or(5_000) as i32,
        addresses: input.config.addresses.as_ref().map(|v| v.iter().map(input_to_address).collect()).unwrap_or_default(),
    };
    serde_json::to_string(&cfg).unwrap_or_default()
}

fn input_to_address(a: &MqttClientAddressInput) -> crate::bridge::mqttclient::Address {
    crate::bridge::mqttclient::Address {
        mode: a.mode.clone(),
        remote_topic: a.remote_topic.clone(),
        local_topic: a.local_topic.clone(),
        qos: a.qos.unwrap_or(0),
        retain: a.retain_as_published.unwrap_or(false),
        remove_path: a.remove_path.unwrap_or(true),
    }
}

#[allow(unused)]
fn _id(s: String) -> ID { ID(s) }

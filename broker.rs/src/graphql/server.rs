use super::schema::AppSchema;
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQL, GraphQLSubscription};
use axum::http::Method;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, on_service, MethodFilter};
use axum::Router;
use std::net::SocketAddr;
use tower_http::cors::{Any, CorsLayer};

async fn playground() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/graphql").subscription_endpoint("/graphql")))
}

async fn health() -> &'static str { "ok" }

pub async fn serve_graphql(schema: AppSchema, port: u16, dashboard_path: Option<String>) -> anyhow::Result<()> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any);

    let mut app = Router::new()
        .route("/playground", get(playground))
        .route("/health", get(health))
        // POST /graphql + GET /graphql (over WS via the GraphQLSubscription service)
        .route("/graphql", on_service(MethodFilter::POST, GraphQL::new(schema.clone())))
        .route_service("/graphql/ws", GraphQLSubscription::new(schema.clone()))
        // alias for some Apollo clients
        .route("/query", on_service(MethodFilter::POST, GraphQL::new(schema.clone())))
        .layer(cors);

    if let Some(path) = dashboard_path {
        if !path.is_empty() && std::path::Path::new(&path).exists() {
            let serve_dir = tower_http::services::ServeDir::new(&path);
            app = app.fallback_service(serve_dir);
        }
    }

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    tracing::info!(%addr, "graphql listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

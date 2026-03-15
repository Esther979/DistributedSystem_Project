// src/server/http_server.rs

use actix_web::{web, App, HttpServer, HttpResponse};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use omnipaxos_kv::common::kv::KVCommand;
use crate::server::HttpRequest;

#[derive(Deserialize)]
pub struct PutReq { pub key: String, pub value: String }

#[derive(Deserialize)]
pub struct GetReq { pub key: String }

#[derive(Deserialize)]
pub struct CasReq { pub key: String, pub from: String, pub to: String }

#[derive(Serialize)]
pub struct Resp {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

type HttpTx = web::Data<mpsc::Sender<HttpRequest>>;

// ── POST /put ─────────────────────────────────────────────────────────────────

async fn handle_put(tx: HttpTx, req: web::Json<PutReq>) -> HttpResponse {
    let (resp_tx, resp_rx) = oneshot::channel();
    let _ = tx.send(HttpRequest {
        kv_cmd: KVCommand::Put(req.key.clone(), req.value.clone()),
        resp_tx,
    }).await;

    match resp_rx.await {
        Ok(Ok(_))  => HttpResponse::Ok().json(Resp { ok: true,  value: None, error: None }),
        Ok(Err(e)) => HttpResponse::Ok().json(Resp { ok: false, value: None, error: Some(e) }),
        Err(_)     => HttpResponse::Ok().json(Resp { ok: false, value: None,
                           error: Some("timeout".into()) }),
    }
}

// ── POST /get ─────────────────────────────────────────────────────────────────
// Get 命令也进 Paxos 日志 → linearizable read（read-as-write 方案）

async fn handle_get(tx: HttpTx, req: web::Json<GetReq>) -> HttpResponse {
    let (resp_tx, resp_rx) = oneshot::channel();
    let _ = tx.send(HttpRequest {
        kv_cmd: KVCommand::Get(req.key.clone()),
        resp_tx,
    }).await;

    match resp_rx.await {
        Ok(Ok(val)) => HttpResponse::Ok().json(Resp { ok: true, value: val, error: None }),
        Ok(Err(e))  => HttpResponse::Ok().json(Resp { ok: false, value: None, error: Some(e) }),
        Err(_)      => HttpResponse::Ok().json(Resp { ok: false, value: None,
                            error: Some("timeout".into()) }),
    }
}

// ── POST /cas ─────────────────────────────────────────────────────────────────

async fn handle_cas(tx: HttpTx, req: web::Json<CasReq>) -> HttpResponse {
    let (resp_tx, resp_rx) = oneshot::channel();
    let _ = tx.send(HttpRequest {
        kv_cmd: KVCommand::Cas(req.key.clone(), req.from.clone(), req.to.clone()),
        resp_tx,
    }).await;

    match resp_rx.await {
        Ok(Ok(_))  => HttpResponse::Ok().json(Resp { ok: true,  value: None, error: None }),
        Ok(Err(e)) => HttpResponse::Ok().json(Resp { ok: false, value: None, error: Some(e) }),
        Err(_)     => HttpResponse::Ok().json(Resp { ok: false, value: None,
                           error: Some("timeout".into()) }),
    }
}

// ── 启动 ──────────────────────────────────────────────────────────────────────

pub async fn start_http_server(
    http_tx: mpsc::Sender<HttpRequest>,
    port: u16,
) -> std::io::Result<()> {
    let data = web::Data::new(http_tx);
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .route("/put", web::post().to(handle_put))
            .route("/get", web::post().to(handle_get))
            .route("/cas", web::post().to(handle_cas))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}

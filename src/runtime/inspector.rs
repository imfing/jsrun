//! Inspector server that mirrors Deno's DevTools bridge while exposing
//! jsrun-friendly metadata. The server runs on a dedicated thread that drives
//! a single-threaded Tokio runtime so that runtime threads can remain isolated.

use anyhow::{Context, Result};
use deno_core::futures::channel::mpsc;
use deno_core::futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use deno_core::futures::channel::oneshot;
use deno_core::futures::future;
use deno_core::futures::prelude::*;
use deno_core::unsync::spawn;
use deno_core::{InspectorMsg, InspectorSessionKind, InspectorSessionProxy, JsRuntimeInspector};
use fastwebsockets::upgrade::upgrade;
use fastwebsockets::{Frame, OpCode, WebSocket};
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{body::Incoming, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde_json::json;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::pin;
use std::process;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::thread;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::LocalSet;
use uuid::Uuid;

type HttpResponse = Response<Box<http_body_util::Full<Bytes>>>;

/// Metadata shared back to Python exposing debugger entry-points.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectorMetadata {
    pub id: String,
    #[serde(rename = "webSocketDebuggerUrl")]
    pub websocket_url: String,
    pub devtools_frontend_url: String,
    pub title: String,
    pub description: String,
    #[serde(rename = "url")]
    pub target_url: String,
    pub favicon_url: String,
    #[serde(skip_serializing)]
    pub host: String,
    #[serde(rename = "type")]
    pub target_type: String,
}

#[derive(Debug)]
pub struct InspectorRegistration {
    metadata: InspectorMetadata,
}

impl InspectorRegistration {
    pub fn metadata(&self) -> &InspectorMetadata {
        &self.metadata
    }
}

#[derive(Debug, Clone, Default)]
pub struct InspectorRegistrationParams {
    pub target_url: Option<String>,
    pub display_name: Option<String>,
    pub wait_for_connection: bool,
}

/// WebSocket/HTTP server exposing inspector endpoints.
pub struct InspectorServer {
    host: SocketAddr,
    register_tx: UnboundedSender<InspectorInfo>,
    shutdown_tx: Option<broadcast::Sender<()>>,
    thread_handle: Option<thread::JoinHandle<()>>,
    name: &'static str,
}

impl InspectorServer {
    pub fn bind(host: SocketAddr, name: &'static str) -> Result<Self> {
        let (register_tx, register_rx) = mpsc::unbounded::<InspectorInfo>();
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        let tcp_listener = std::net::TcpListener::bind(host)
            .with_context(|| format!("Failed to bind inspector socket at {host}"))?;
        tcp_listener
            .set_nonblocking(true)
            .context("Failed to set inspector socket to nonblocking")?;

        let thread_handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("inspector tokio runtime init failed");
            let local = LocalSet::new();
            local.block_on(&rt, server(tcp_listener, register_rx, shutdown_rx, name))
        });

        Ok(Self {
            host,
            register_tx,
            shutdown_tx: Some(shutdown_tx),
            thread_handle: Some(thread_handle),
            name,
        })
    }

    pub fn register_runtime(
        &self,
        inspector: Rc<JsRuntimeInspector>,
        params: InspectorRegistrationParams,
        connection_state: InspectorConnectionState,
    ) -> Result<InspectorRegistration> {
        let session_sender = inspector.get_session_sender();
        let deregister_rx = inspector.add_deregister_handler();

        let info = InspectorInfo::new(
            self.host,
            session_sender,
            deregister_rx,
            InspectorInfoConfig {
                target_url: params
                    .target_url
                    .unwrap_or_else(|| "jsrun://runtime".to_string()),
                display_name: params.display_name,
                wait_for_session: params.wait_for_connection,
                description: self.name.to_string(),
                favicon_url: "https://jsrun.dev/favicon.ico".to_string(),
                connection_state,
            },
        );

        let metadata = info.metadata_for_host(None);
        self.register_tx
            .unbounded_send(info)
            .context("Failed to register inspector")?;

        Ok(InspectorRegistration { metadata })
    }
}

impl Drop for InspectorServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

fn handle_ws_request(
    req: Request<Incoming>,
    inspector_map: Rc<RefCell<HashMap<Uuid, InspectorInfo>>>,
) -> http::Result<HttpResponse> {
    let (parts, body) = req.into_parts();
    let req = Request::from_parts(parts, ());

    let maybe_uuid = req
        .uri()
        .path()
        .strip_prefix("/ws/")
        .and_then(|s| Uuid::parse_str(s).ok());

    let Some(uuid) = maybe_uuid else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Box::new(Bytes::from("Malformed inspector UUID").into()));
    };

    let (new_session_tx, connection_state) = {
        let inspector_map = inspector_map.borrow();
        let Some(info) = inspector_map.get(&uuid) else {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Box::new(Bytes::from("Invalid inspector UUID").into()));
        };
        (info.new_session_tx.clone(), info.connection_state.clone())
    };

    let (parts, _) = req.into_parts();
    let mut req = Request::from_parts(parts, body);

    let Ok((resp, upgrade_fut)) = upgrade(&mut req) else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Box::new(Bytes::from("Invalid WebSocket upgrade").into()));
    };

    spawn(async move {
        let websocket = match upgrade_fut.await {
            Ok(ws) => ws,
            Err(err) => {
                log::error!("Inspector upgrade failed: {err:?}");
                return;
            }
        };

        connection_state.mark_connected();

        let (outbound_tx, outbound_rx) = mpsc::unbounded();
        let (inbound_tx, inbound_rx) = mpsc::unbounded();

        let proxy = InspectorSessionProxy {
            tx: outbound_tx,
            rx: inbound_rx,
            kind: InspectorSessionKind::NonBlocking {
                wait_for_disconnect: true,
            },
        };

        log::info!("Debugger session started.");
        let _ = new_session_tx.unbounded_send(proxy);
        pump_websocket_messages(websocket, inbound_tx, outbound_rx).await;
    });

    let (parts, _) = resp.into_parts();
    Ok(Response::from_parts(
        parts,
        Box::new(http_body_util::Full::new(Bytes::new())),
    ))
}

fn handle_json_request(
    inspector_map: Rc<RefCell<HashMap<Uuid, InspectorInfo>>>,
    host: Option<String>,
) -> http::Result<HttpResponse> {
    let data = inspector_map
        .borrow()
        .values()
        .map(|info| info.metadata_for_host(host.clone()))
        .collect::<Vec<_>>();

    let body = serde_json::to_vec(&data).unwrap();
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Box::new(http_body_util::Full::from(Bytes::from(body))))
}

fn handle_json_version_request(name: &str) -> http::Result<HttpResponse> {
    let body = json!({
        "Browser": name,
        "Protocol-Version": "1.3",
        "V8-Version": deno_core::v8::VERSION_STRING,
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Box::new(http_body_util::Full::from(Bytes::from(
            serde_json::to_vec(&body).unwrap(),
        ))))
}

async fn server(
    listener: std::net::TcpListener,
    register_rx: UnboundedReceiver<InspectorInfo>,
    shutdown_rx: broadcast::Receiver<()>,
    name: &'static str,
) {
    let registry = Rc::new(RefCell::new(HashMap::<Uuid, InspectorInfo>::new()));

    let register_task = listen_for_new_inspectors(register_rx, Rc::clone(&registry)).boxed_local();

    let deregister_task = future::poll_fn(|cx| {
        registry
            .borrow_mut()
            .retain(|_, info| info.deregister_rx.poll_unpin(cx) == Poll::Pending);
        Poll::<core::convert::Infallible>::Pending
    })
    .boxed_local();

    let listener = match TcpListener::from_std(listener) {
        Ok(l) => l,
        Err(err) => {
            log::error!("Cannot create async listener: {err:?}");
            return;
        }
    };

    let registry_for_server = Rc::clone(&registry);
    let shutdown_rx = shutdown_rx;
    let server_loop = async move {
        loop {
            let mut shutdown_listener = shutdown_rx.resubscribe();
            let stream = tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _)) => stream,
                        Err(err) => {
                            log::error!("Inspector accept error: {err:?}");
                            continue;
                        }
                    }
                }
                _ = shutdown_listener.recv() => break,
            };

            let io = TokioIo::new(stream);
            let inspector_map = Rc::clone(&registry_for_server);
            let mut connection_shutdown = shutdown_rx.resubscribe();

            let service = service_fn(move |req: Request<Incoming>| {
                future::ready({
                    let host = req
                        .headers()
                        .get("host")
                        .and_then(|h| h.to_str().ok())
                        .map(|value| value.to_string());
                    match (req.method(), req.uri().path()) {
                        (&Method::GET, path) if path.starts_with("/ws/") => {
                            handle_ws_request(req, Rc::clone(&inspector_map))
                        }
                        (&Method::GET, "/json") | (&Method::GET, "/json/list") => {
                            handle_json_request(Rc::clone(&inspector_map), host)
                        }
                        (&Method::GET, "/json/version") => handle_json_version_request(name),
                        _ => Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Box::new(http_body_util::Full::from(Bytes::from(
                                "Not Found",
                            )))),
                    }
                })
            });

            spawn(async move {
                let server = hyper::server::conn::http1::Builder::new();
                let mut conn = pin!(server.serve_connection(io, service).with_upgrades());
                let mut shutdown = pin!(connection_shutdown.recv());

                tokio::select! {
                    result = conn.as_mut() => {
                        if let Err(err) = result {
                            log::error!("Inspector connection error: {err:?}");
                        }
                    }
                    _ = &mut shutdown => {
                        conn.as_mut().graceful_shutdown();
                        let _ = conn.await;
                    }
                }
            });
        }
    }
    .boxed_local();

    tokio::select! {
        _ = register_task => {},
        _ = deregister_task => {},
        _ = server_loop => {},
    }
}

async fn listen_for_new_inspectors(
    mut register_rx: UnboundedReceiver<InspectorInfo>,
    inspector_map: Rc<RefCell<HashMap<Uuid, InspectorInfo>>>,
) {
    while let Some(info) = register_rx.next().await {
        let host = info.host.to_string();
        log::info!(
            "Debugger listening on {}",
            info.get_websocket_debugger_url(&host)
        );
        log::info!("Open chrome://inspect to connect.");
        if info.wait_for_session {
            log::info!("jsrun is waiting for the debugger to attach");
        }
        if inspector_map.borrow_mut().insert(info.uuid, info).is_some() {
            log::error!("Inspector UUID collision detected");
        }
    }
}

async fn pump_websocket_messages(
    mut websocket: WebSocket<TokioIo<hyper::upgrade::Upgraded>>,
    inbound_tx: UnboundedSender<String>,
    mut outbound_rx: UnboundedReceiver<InspectorMsg>,
) {
    loop {
        tokio::select! {
            Some(msg) = outbound_rx.next() => {
                let frame = Frame::text(msg.content.into_bytes().into());
                if websocket.write_frame(frame).await.is_err() {
                    break;
                }
            }
            Ok(msg) = websocket.read_frame() => {
                match msg.opcode {
                    OpCode::Text => {
                        if let Ok(text) = String::from_utf8(msg.payload.to_vec()) {
                            let _ = inbound_tx.unbounded_send(text);
                        }
                    }
                    OpCode::Close => {
                        log::info!("Debugger session ended");
                        break;
                    }
                    _ => {}
                }
            }
            else => break,
        }
    }
}

struct InspectorInfo {
    host: SocketAddr,
    uuid: Uuid,
    thread_name: Option<String>,
    new_session_tx: UnboundedSender<InspectorSessionProxy>,
    deregister_rx: oneshot::Receiver<()>,
    target_url: String,
    wait_for_session: bool,
    display_name: Option<String>,
    favicon_url: String,
    description: String,
    connection_state: InspectorConnectionState,
}

struct InspectorInfoConfig {
    target_url: String,
    display_name: Option<String>,
    wait_for_session: bool,
    description: String,
    favicon_url: String,
    connection_state: InspectorConnectionState,
}

impl InspectorInfo {
    fn new(
        host: SocketAddr,
        new_session_tx: UnboundedSender<InspectorSessionProxy>,
        deregister_rx: oneshot::Receiver<()>,
        inspector_cfg: InspectorInfoConfig,
    ) -> Self {
        Self {
            host,
            uuid: Uuid::new_v4(),
            thread_name: thread::current().name().map(|n| n.to_owned()),
            new_session_tx,
            deregister_rx,
            target_url: inspector_cfg.target_url,
            wait_for_session: inspector_cfg.wait_for_session,
            display_name: inspector_cfg.display_name,
            favicon_url: inspector_cfg.favicon_url,
            description: inspector_cfg.description,
            connection_state: inspector_cfg.connection_state,
        }
    }

    fn get_websocket_debugger_url(&self, host: &str) -> String {
        format!("ws://{host}/ws/{}", self.uuid)
    }

    fn get_devtools_url(&self, host: &str) -> String {
        format!(
            "devtools://devtools/bundled/js_app.html?ws={host}/ws/{}&experiments=true&v8only=true",
            self.uuid
        )
    }

    fn title(&self) -> String {
        let thread = self
            .thread_name
            .as_ref()
            .map(|n| format!(" - {n}"))
            .unwrap_or_default();
        self.display_name
            .clone()
            .unwrap_or_else(|| format!("jsrun{thread} [pid: {}]", process::id()))
    }

    fn metadata_for_host(&self, host_override: Option<String>) -> InspectorMetadata {
        let host_listen = self.host.to_string();
        let host = host_override.unwrap_or(host_listen);
        InspectorMetadata {
            id: self.uuid.to_string(),
            websocket_url: self.get_websocket_debugger_url(&host),
            devtools_frontend_url: self.get_devtools_url(&host),
            title: self.title(),
            description: self.description.clone(),
            target_url: self.target_url.clone(),
            favicon_url: self.favicon_url.clone(),
            host,
            target_type: "node".to_string(),
        }
    }
}

#[derive(Clone, Default)]
pub struct InspectorConnectionState {
    connected: Arc<AtomicBool>,
}

impl InspectorConnectionState {
    pub fn mark_connected(&self) {
        self.connected.store(true, Ordering::SeqCst);
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

use anyhow::{Context, Result};
use futures::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};

use lockin_proto::message::Message;
use std::sync::{Mutex, atomic::AtomicU64};
use std::{collections::HashMap, sync::Arc};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Receiver};
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tracing::{debug, error, warn};

use crate::codec::Codec;

pub type ConnectionId = u64;

/// Net → Server (inbound from a client socket)
#[derive(Debug)]
pub struct NetToServer {
    pub conn_id: ConnectionId,
    pub msg: Message,
}

/// Server → Net (outbound to a specific client socket)
#[derive(Debug)]
pub struct ServerToNet {
    pub conn_id: ConnectionId,
    pub msg: Message,
}

pub struct NetworkManager {
    next_conn_id: AtomicU64,
    client_listener_addr: String,
    to_server: mpsc::Sender<NetToServer>,
    connections_map: Arc<ConnectionRegistry>,
}

struct ConnectionRegistry {
    writer_txs: Arc<Mutex<HashMap<ConnectionId, mpsc::Sender<Message>>>>,
}

impl ConnectionRegistry {
    fn new() -> Self {
        Self {
            writer_txs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn register(&self, id: ConnectionId) -> mpsc::Receiver<Message> {
        let (tx, rx) = mpsc::channel::<Message>(32);
        self.writer_txs.lock().unwrap().insert(id, tx);
        rx
    }

    async fn send(&self, id: &ConnectionId, msg: Message) -> anyhow::Result<()> {
        let tx = self.writer_txs.lock().unwrap().get(id).cloned();
        let Some(tx) = tx else {
            anyhow::bail!("writer channel not available for id: {:?}", id);
        };

        tx.send(msg).await?;
        Ok(())
    }
}

impl NetworkManager {
    pub fn new(to_server: mpsc::Sender<NetToServer>, client_listener_addr: String) -> Self {
        Self {
            next_conn_id: AtomicU64::new(0),
            connections_map: Arc::new(ConnectionRegistry::new()),
            to_server,
            client_listener_addr,
        }
    }

    pub async fn run(self, from_server: mpsc::Receiver<ServerToNet>) -> anyhow::Result<()> {
        let conn_map = self.connections_map.clone();
        tokio::spawn(async {
            if let Err(e) = dispatch_server_msgs(from_server, conn_map).await {
                warn!("failure while dispatching server messages: {e}");
            }
        });
        let listener = tokio::net::TcpListener::bind(self.client_listener_addr.clone())
            .await
            .context("failed to bind tcp listener")?;
        Arc::new(self)
            .listen(listener)
            .await
            .context("failure while listening for incoming connections")
    }

    async fn listen(self: Arc<Self>, listener: TcpListener) -> anyhow::Result<()> {
        loop {
            let (conn, addr) = match listener.accept().await {
                Ok(val) => val,
                Err(e) => {
                    error!(target: "network", error = %e, "failed to accept incoming TCP connection");
                    continue;
                }
            };

            debug!(target: "network", ?addr, "accepted new raw TCP stream");
            let framed = Framed::new(conn, Codec::new());

            let self_clone = self.clone();
            tokio::spawn(async move {
                if let Err(e) = self_clone.handle_conn(framed).await {
                    warn!("failure while handling client connection {e}");
                }
            });
        }
    }

    async fn handle_conn(&self, framed: Framed<TcpStream, Codec>) -> anyhow::Result<()> {
        let conn_id = self
            .next_conn_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let rx = self.connections_map.register(conn_id).await;

        let (read_conn_task, write_conn_task) = self.spawn_conn_io_tasks(framed, conn_id, rx).await;
        read_conn_task
            .await
            .context("read connection task panic")??;
        write_conn_task
            .await
            .context("write connection task panic")??;
        Ok(())
    }

    async fn spawn_conn_io_tasks(
        &self,
        framed: Framed<TcpStream, Codec>,
        id: ConnectionId,
        rx: Receiver<Message>,
    ) -> (JoinHandle<Result<()>>, JoinHandle<Result<()>>) {
        let (sink, stream) = framed.split();
        let to_server = self.to_server.clone();
        let read_conn_task = tokio::spawn(async move {
            read_conn(id, stream, to_server)
                .await
                .context("failure while reading tcp connection")
        });

        let write_conn_task = tokio::spawn(async move {
            write_conn(sink, rx)
                .await
                .context("failure while writing to tcp connection")
        });
        (read_conn_task, write_conn_task)
    }
}

async fn read_conn(
    conn_id: ConnectionId,
    mut stream: SplitStream<Framed<TcpStream, Codec>>,
    to_server: mpsc::Sender<NetToServer>,
) -> anyhow::Result<()> {
    while let Some(msg) = stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                error!(target: "network", error = %e, "error decoding frame from stream, skipping frame");
                continue;
            }
        };

        to_server
            .send(NetToServer { conn_id, msg })
            .await
            .context("failed to handle forawrd  message to server")?;
    }
    Ok(())
}

async fn write_conn(
    mut sink: SplitSink<Framed<TcpStream, Codec>, Message>,
    mut msg_rx: Receiver<Message>,
) -> anyhow::Result<()> {
    while let Some(msg) = msg_rx.recv().await {
        sink.send(msg)
            .await
            .context("failed to write message to tcp connection")?;
    }
    Ok(())
}

async fn dispatch_server_msgs(
    mut from_server: Receiver<ServerToNet>,
    connections_map: Arc<ConnectionRegistry>,
) -> anyhow::Result<()> {
    while let Some(msg) = from_server.recv().await {
        connections_map
            .send(&msg.conn_id, msg.msg)
            .await
            .context("failed to dispatch message")?;
    }
    Ok(())
}

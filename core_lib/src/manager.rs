use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use ts_rs::TS;

use crate::channel::{ChannelDirection, ChannelMessage};
use crate::errors::AppError;
use crate::hdl::{InboundRequest, OutboundPayload, OutboundRequest, State};
use crate::utils::RemoteDeviceInfo;

const INNER_NAME: &str = "TcpServer";

#[derive(Debug, Deserialize, Serialize, TS)]
#[ts(export)]
pub struct SendInfo {
    pub id: String,
    pub name: String,
    pub addr: String,
    pub ob: OutboundPayload,
}

pub struct TcpServer {
    endpoint_id: [u8; 4],
    tcp_listener: TcpListener,
    sender: Sender<ChannelMessage>,
    connect_receiver: Receiver<SendInfo>,
    download_dir: std::path::PathBuf,
    session_subdirs: bool,
    consent_timeout: std::time::Duration,
}

impl TcpServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint_id: [u8; 4],
        tcp_listener: TcpListener,
        sender: Sender<ChannelMessage>,
        connect_receiver: Receiver<SendInfo>,
        download_dir: std::path::PathBuf,
        session_subdirs: bool,
        consent_timeout: std::time::Duration,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            endpoint_id,
            tcp_listener,
            sender,
            connect_receiver,
            download_dir,
            session_subdirs,
            consent_timeout,
        })
    }

    /// Classify a session failure for the channel's `error` field.
    fn classify(e: &anyhow::Error) -> crate::channel::TransferError {
        if e.downcast_ref::<std::io::Error>().is_some() {
            crate::channel::TransferError::Io
        } else if e.downcast_ref::<prost::DecodeError>().is_some() {
            crate::channel::TransferError::Decode
        } else {
            crate::channel::TransferError::Other
        }
    }

    pub async fn run(&mut self, ctk: CancellationToken) -> Result<(), anyhow::Error> {
        info!("{INNER_NAME}: service starting");

        loop {
            let cctk = ctk.clone();

            tokio::select! {
                _ = ctk.cancelled() => {
                    info!("{INNER_NAME}: tracker cancelled, breaking");
                    break;
                }
                Some(i) = self.connect_receiver.recv() => {
                    info!("{INNER_NAME}: connect_receiver: got {:?}", i);
                    if let Err(e) = self.connect(cctk, i).await {
                        error!("{INNER_NAME}: error sending: {}", e.to_string());
                    }
                }
                r = self.tcp_listener.accept() => {
                    match r {
                        Ok((socket, remote_addr)) => {
                            trace!("{INNER_NAME}: new client: {remote_addr}");
                            let esender = self.sender.clone();
                            let csender = self.sender.clone();
                            let download_dir = if self.session_subdirs {
                                // ip:port → filesystem-safe per-session dir
                                let session = remote_addr.to_string().replace([':', '%', '/'], "-");
                                self.download_dir.join(session)
                            } else {
                                self.download_dir.clone()
                            };
                            let consent_timeout = self.consent_timeout;

                            tokio::spawn(async move {
                                let mut ir = InboundRequest::new(
                                    socket,
                                    remote_addr.to_string(),
                                    csender,
                                    download_dir,
                                    consent_timeout,
                                );

                                loop {
                                    match ir.handle().await {
                                        Ok(_) => {},
                                        Err(e) => match e.downcast_ref() {
                                            Some(AppError::NotAnError) => break,
                                            None => {
                                                if ir.state.state == State::Initial {
                                                    break;
                                                }

                                                if ir.state.state == State::Finished {
                                                    // The transfer completed and the peer
                                                    // hung up: an eof here is the handshake
                                                    // ending, not a failure. The front end
                                                    // is told nothing for the same reason.
                                                    debug!("{INNER_NAME}: client closed after finishing: {e}");
                                                    break;
                                                }

                                                let _ = esender.send(ChannelMessage {
                                                    id: remote_addr.to_string(),
                                                    direction: ChannelDirection::LibToFront,
                                                    state: Some(State::Disconnected),
                                                    error: Some(Self::classify(&e)),
                                                    ..Default::default()
                                                });
                                                error!("{INNER_NAME}: error while handling client: {e} ({:?})", ir.state.state);
                                                break;
                                            }
                                        },
                                    }
                                }
                            });
                        },
                        Err(err) => {
                            error!("{INNER_NAME}: error accepting: {}", err);
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// To be called inside a separate task if we want to handle concurrency
    pub async fn connect(&self, ctk: CancellationToken, si: SendInfo) -> Result<(), anyhow::Error> {
        debug!("{INNER_NAME}: Connecting to: {}", si.addr);
        let socket = TcpStream::connect(si.addr.clone()).await?;

        let mut or = OutboundRequest::new(
            self.endpoint_id,
            socket,
            si.id,
            self.sender.clone(),
            si.ob,
            RemoteDeviceInfo {
                device_type: crate::DeviceType::Unknown,
                name: si.name,
            },
        );

        // Send connection request
        or.send_connection_request().await?;
        // Send UKEY init
        or.send_ukey2_client_init().await?;

        loop {
            tokio::select! {
                _ = ctk.cancelled() => {
                    info!("{INNER_NAME}: tracker cancelled, breaking");
                    break;
                },
                r = or.handle() => {
                    if let Err(e) = r {
                        match e.downcast_ref() {
                            Some(AppError::NotAnError) => break,
                            None => {
                                if or.state.state == State::Initial {
                                    break;
                                }

                                if or.state.state == State::Finished
                                    || or.state.state == State::Cancelled
                                {
                                    // Same as inbound: the session reached its
                                    // end before the socket did.
                                    debug!("{INNER_NAME}: peer closed after {:?}: {e}", or.state.state);
                                    break;
                                }

                                let _ = self.sender.clone().send(ChannelMessage {
                                    id: si.addr,
                                    direction: ChannelDirection::LibToFront,
                                    state: Some(State::Disconnected),
                                    error: Some(Self::classify(&e)),
                                    ..Default::default()
                                });
                                error!("{INNER_NAME}: error while handling client: {e} ({:?})", or.state.state);
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

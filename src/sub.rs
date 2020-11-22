use crate::codec::*;
use crate::endpoint::{Endpoint, TryIntoEndpoint};
use crate::error::{ZmqError, ZmqResult};
use crate::message::*;
use crate::transport::{self, AcceptStopHandle};
use crate::util::{self, PeerIdentity};
use crate::{BlockingRecv, MultiPeerBackend, Socket, SocketBackend, SocketType};

use crate::backend::GenericSocketBackend;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;

pub struct SubSocket {
    backend: Arc<GenericSocketBackend>,
    fair_queue: mpsc::Receiver<(PeerIdentity, Message)>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
}

impl Drop for SubSocket {
    fn drop(&mut self) {
        self.backend.shutdown()
    }
}

impl SubSocket {
    pub async fn subscribe(&mut self, subscription: &str) -> ZmqResult<()> {
        let mut message = BytesMut::with_capacity(subscription.len() + 1);
        message.put_u8(1);
        message.extend_from_slice(subscription.as_bytes());
        // let message = format!("\0x1{}", subscription);
        for mut peer in self.backend.peers.iter_mut() {
            peer.send_queue
                .send(Message::Message(message.clone().into()))
                .await?;
        }
        Ok(())
    }

    pub async fn unsubscribe(&mut self, subscription: &str) -> ZmqResult<()> {
        let mut message = BytesMut::with_capacity(subscription.len() + 1);
        message.put_u8(0);
        message.extend_from_slice(subscription.as_bytes());
        for mut peer in self.backend.peers.iter_mut() {
            peer.send_queue
                .send(Message::Message(message.clone().into()))
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Socket for SubSocket {
    fn new() -> Self {
        // TODO define buffer size
        let default_queue_size = 100;
        let (queue_sender, fair_queue) = mpsc::channel(default_queue_size);
        Self {
            backend: Arc::new(GenericSocketBackend::new(queue_sender, SocketType::SUB)),
            fair_queue,
            binds: HashMap::new(),
        }
    }

    fn backend(&self) -> Arc<dyn MultiPeerBackend> {
        self.backend.clone()
    }

    async fn bind(&mut self, endpoint: impl TryIntoEndpoint + 'async_trait) -> ZmqResult<Endpoint> {
        let endpoint = endpoint.try_into()?;

        let cloned_backend = self.backend.clone();
        let cback = move |result| util::peer_connected(result, cloned_backend.clone());
        let (endpoint, stop_handle) = transport::begin_accept(endpoint, cback).await?;

        self.binds.insert(endpoint.clone(), stop_handle);
        Ok(endpoint)
    }

    async fn unbind(&mut self, endpoint: impl TryIntoEndpoint + 'async_trait) -> ZmqResult<()> {
        let endpoint = endpoint.try_into()?;

        let stop_handle = self.binds.remove(&endpoint);
        let stop_handle = stop_handle.ok_or(ZmqError::NoSuchBind(endpoint))?;
        stop_handle.0.shutdown().await
    }

    async fn connect(&mut self, endpoint: impl TryIntoEndpoint + 'async_trait) -> ZmqResult<()> {
        let endpoint = endpoint.try_into()?;

        let connect_result = transport::connect(endpoint).await;
        util::peer_connected(connect_result, self.backend.clone()).await;
        Ok(())
    }

    fn binds(&self) -> &HashMap<Endpoint, AcceptStopHandle> {
        &self.binds
    }
}

#[async_trait]
impl BlockingRecv for SubSocket {
    async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
        loop {
            match self.fair_queue.next().await {
                Some((_peer_id, Message::Message(message))) => {
                    return Ok(message);
                }
                Some((_peer_id, _)) => todo!(),
                None => todo!(),
            }
        }
    }
}

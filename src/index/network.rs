use super::{ArkelRaftConfig, NodeId, RaftMessage};

/// Connection client structure wrapping an active Iroh endpoint transport channel.
pub struct ArkelRaftConnection {
    pub endpoint: iroh::Endpoint,
    pub target_addr: String,
}

impl ArkelRaftConnection {
    /// Extracts cryptographic parameters and resolves matching connection lines.
    async fn get_connection(&self) -> anyhow::Result<iroh::endpoint::Connection> {
        // Enforces key mapping format: <node_id_hex>@<ip>:<port>
        let endpoint_addr = if let Some((id_str, ip_str)) = self.target_addr.split_once('@') {
            let public_key: iroh::EndpointId = id_str.parse()?;
            let socket_addr: std::net::SocketAddr = ip_str.parse()?;

            iroh::EndpointAddr::new(public_key).with_ip_addr(socket_addr)
        } else {
            return Err(anyhow::anyhow!(
                "Address format must be '<iroh_public_key>@<ip>:<port>' for cryptographic handshakes."
            ));
        };

        let connection = self.endpoint.connect(endpoint_addr, b"arkel-raft").await?;
        Ok(connection)
    }
}

impl openraft::RaftNetwork<ArkelRaftConfig> for ArkelRaftConnection {
    async fn append_entries(
        &mut self,
        req: openraft::raft::AppendEntriesRequest<ArkelRaftConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::impls::BasicNode,
            openraft::error::RaftError<NodeId>,
        >,
    > {
        let conn = self.get_connection().await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::NotConnected, e.to_string()),
            ))
        })?;

        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::ConnectionReset, e.to_string()),
            ))
        })?;

        let payload = serde_json::to_vec(&RaftMessage::AppendEntries(req)).unwrap();
        send.write_all(&payload).await.ok();
        send.finish().ok();

        let ttl = _option.hard_ttl();
        let buffer = tokio::time::timeout(ttl, recv.read_to_end(1024 * 1024))
            .await
            .map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(std::io::ErrorKind::TimedOut, e.to_string()),
                ))
            })?
            .map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
                ))
            })?;

        let resp = serde_json::from_slice(&buffer).map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
            ))
        })?;

        Ok(resp)
    }

    async fn install_snapshot(
        &mut self,
        _req: openraft::raft::InstallSnapshotRequest<ArkelRaftConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::impls::BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let io_err = std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Snapshot engines remain inactive under baseline layout",
        );
        Err(openraft::error::RPCError::Network(
            openraft::error::NetworkError::new(&io_err),
        ))
    }

    async fn vote(
        &mut self,
        req: openraft::raft::VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::impls::BasicNode,
            openraft::error::RaftError<NodeId>,
        >,
    > {
        let conn = self.get_connection().await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::NotConnected, e.to_string()),
            ))
        })?;

        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::ConnectionReset, e.to_string()),
            ))
        })?;

        let payload = serde_json::to_vec(&RaftMessage::Vote(req)).unwrap();
        send.write_all(&payload).await.ok();
        send.finish().ok();

        let ttl = _option.hard_ttl();
        let buffer = tokio::time::timeout(ttl, recv.read_to_end(1024 * 1024))
            .await
            .map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(std::io::ErrorKind::TimedOut, e.to_string()),
                ))
            })?
            .map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
                ))
            })?;

        let resp = serde_json::from_slice(&buffer).map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
            ))
        })?;

        Ok(resp)
    }
}

pub struct ArkelRaftNetwork {
    pub endpoint: iroh::Endpoint,
}

impl ArkelRaftNetwork {
    pub fn new(endpoint: iroh::Endpoint) -> Self {
        Self { endpoint }
    }
}

impl openraft::RaftNetworkFactory<ArkelRaftConfig> for ArkelRaftNetwork {
    type Network = ArkelRaftConnection;

    async fn new_client(
        &mut self,
        _target: NodeId,
        node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        ArkelRaftConnection {
            endpoint: self.endpoint.clone(),
            target_addr: node.addr.clone(),
        }
    }
}

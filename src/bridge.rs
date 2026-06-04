use agent_client_protocol::{ConnectionTo, Dispatch, HandleDispatchFrom, Handled};

pub struct BridgeHandler {
    agent_cx: ConnectionTo<agent_client_protocol::Agent>,
}

impl BridgeHandler {
    pub fn new(agent_cx: ConnectionTo<agent_client_protocol::Agent>) -> Self {
        Self { agent_cx }
    }
}

impl HandleDispatchFrom<agent_client_protocol::Client> for BridgeHandler {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _client_cx: ConnectionTo<agent_client_protocol::Client>,
    ) -> agent_client_protocol::schema::Result<Handled<Dispatch>> {
        self.agent_cx.send_proxied_message(message)?;
        Ok(Handled::Yes)
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        "BridgeHandler"
    }
}

use agent_client_protocol::{
    Agent, ConnectionTo, Responder, Stdio, on_receive_request,
    schema::{
        ContentBlock, ContentChunk, InitializeRequest, InitializeResponse, LoadSessionRequest,
        LoadSessionResponse, NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse,
        SessionNotification, SessionUpdate, StopReason, TextContent,
    },
};

#[tokio::main]
async fn main() {
    Agent
        .builder()
        .name("mock-agent")
        .on_receive_request(
            async move |_req: InitializeRequest,
                        responder: Responder<InitializeResponse>,
                        _cx: ConnectionTo<agent_client_protocol::Client>| {
                responder.respond(InitializeResponse::new(
                    agent_client_protocol::schema::ProtocolVersion::V1,
                ))
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |req: NewSessionRequest,
                        responder: Responder<NewSessionResponse>,
                        _cx: ConnectionTo<agent_client_protocol::Client>| {
                let session_id = format!("mock_sess_{}", req.cwd.display());
                responder.respond(NewSessionResponse::new(session_id))
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: LoadSessionRequest,
                        responder: Responder<LoadSessionResponse>,
                        _cx: ConnectionTo<agent_client_protocol::Client>| {
                responder.respond(LoadSessionResponse::new())
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder: Responder<PromptResponse>,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                // Echo back the prompt content as a session update
                let text = req
                    .prompt
                    .iter()
                    .filter_map(|c| match c {
                        ContentBlock::Text(t) => Some(t.text.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                let response_text = format!("Echo: {text}");
                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new(&response_text),
                    ))),
                ))?;

                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            on_receive_request!(),
        )
        .connect_to(Stdio::new())
        .await
        .expect("mock agent failed");
}

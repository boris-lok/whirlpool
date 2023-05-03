use std::collections::HashMap;
use std::io::StdoutLock;
use std::io::Write;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use whirlpool::{main_loop, Init, Message, Node};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct BroadcastNode {
    node_id: String,
    id: usize,
    messages: Vec<usize>,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(_state: (), init: Init) -> anyhow::Result<Self> {
        Ok(Self {
            node_id: init.node_id,
            id: 1,
            messages: Vec::new(),
        })
    }

    fn step(&mut self, input: Message<Payload>, out: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                self.messages.push(message);
                reply.body.payload = Payload::BroadcastOk;
                serde_json::to_writer(&mut *out, &reply)
                    .context("serialize response to broadcast")?;
                out.write_all(b"\n").context("writing trailing newline")?;
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone(),
                };
                serde_json::to_writer(&mut *out, &reply).context("serialize response to read")?;
                out.write_all(b"\n").context("writing trailing newline")?;
            }
            Payload::Topology { .. } => {
                reply.body.payload = Payload::TopologyOk;
                serde_json::to_writer(&mut *out, &reply).context("serialize response topology")?;
                out.write_all(b"\n").context("writing trailing newline")?;
            }
            Payload::BroadcastOk { .. } | Payload::ReadOk { .. } | Payload::TopologyOk { .. } => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<(), BroadcastNode, _>(())
}

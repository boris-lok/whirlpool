use std::io::StdoutLock;
use std::io::Write;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use whirlpool::{Init, main_loop, Message, Node};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: usize,
}

impl Node<(), Payload> for EchoNode {
    fn from_init(_state: (), _init: Init) -> anyhow::Result<Self> {
        Ok(EchoNode { id: 1 })
    }

    fn step(&mut self, input: Message<Payload>, out: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                serde_json::to_writer(&mut *out, &reply).context("serialize response to echo")?;
                out.write_all(b"\n").context("writing trailing newline")?;
            }
            Payload::EchoOk { .. } => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<(), EchoNode, _>(())
}

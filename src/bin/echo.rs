use std::io::StdoutLock;
use std::io::Write;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use whirlpool::{main_loop, Body, Message, Node};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}

struct EchoNode {
    id: usize,
}

impl Node<Payload> for EchoNode {
    fn step(&mut self, input: Message<Payload>, out: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init { .. } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk,
                    },
                };
                serde_json::to_writer(&mut *out, &reply).context("serialize response to init")?;
                out.write_all(b"\n").context("writing trailing newline")?;
                self.id += 1;
            }
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };
                serde_json::to_writer(&mut *out, &reply).context("serialize response to echo")?;
                out.write_all(b"\n").context("writing trailing newline")?;
                self.id += 1;
            }
            Payload::EchoOk { .. } | Payload::InitOk => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop(EchoNode { id: 0 })
}

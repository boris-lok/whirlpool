use std::io::StdoutLock;
use std::io::Write;

use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use whirlpool::{Body, Message, Node};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}

struct UniqueNode {
    id: usize,
}

impl Node<Payload> for UniqueNode {
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
            Payload::Generate => {
                let guid = ulid::Ulid::new().to_string();
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::GenerateOk { guid },
                    },
                };
                serde_json::to_writer(&mut *out, &reply).context("serialize response to unique")?;
                out.write_all(b"\n").context("writing trailing newline")?;
                self.id += 1;
            }
            Payload::GenerateOk { .. } | Payload::InitOk => {}
        }

        Ok(())
    }
}

fn main_loop<S, Payload>(mut state: S) -> anyhow::Result<()>
where
    S: Node<Payload>,
    Payload: DeserializeOwned,
{
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>();

    for input in inputs {
        let input = input.context("Maelstrom input from STDIN could not be deserialized")?;
        state
            .step(input, &mut stdout)
            .context("Node step function failed")?;
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    return main_loop(UniqueNode { id: 0 });
}

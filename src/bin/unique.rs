use std::io::StdoutLock;
use std::io::Write;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use whirlpool::{Event, Init, main_loop, Node};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

struct UniqueNode {
    node_id: String,
    id: usize,
}

impl Node<(), Payload> for UniqueNode {
    fn from_init(
        _state: (),
        init: Init,
        _tx: std::sync::mpsc::Sender<Event<Payload>>,
    ) -> anyhow::Result<Self> {
        Ok(UniqueNode {
            node_id: init.node_id,
            id: 1,
        })
    }

    fn step(&mut self, input: Event<Payload>, out: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("echo should receive event message");
        };

        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node_id, self.id);
                reply.body.payload = Payload::GenerateOk { guid };
                serde_json::to_writer(&mut *out, &reply).context("serialize response to unique")?;
                out.write_all(b"\n").context("writing trailing newline")?;
            }
            Payload::GenerateOk { .. } => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueNode, _, _>(())
}


// {"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 1, "node_id": "n3", "node_ids": ["n1"]}}
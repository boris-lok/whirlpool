use std::collections::{HashMap, HashSet};
use std::io::StdoutLock;
use std::time::Duration;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use whirlpool::{main_loop, Body, Event, Init, InjectedPayload, Message, Node};

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
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<usize>,
    },
}

struct BroadcastNode {
    node_id: String,
    id: usize,
    messages: HashSet<usize>,
    neighbours: Vec<String>,
    known: HashMap<String, HashSet<usize>>,
}

impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF
            loop {
                std::thread::sleep(Duration::from_millis(500));
                if tx.send(Event::Injected(InjectedPayload::Gossip)).is_err() {
                    break;
                }
            }
        });

        Ok(Self {
            node_id: init.node_id,
            id: 1,
            messages: HashSet::new(),
            neighbours: Vec::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|n| (n, HashSet::new()))
                .collect(),
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        out: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Broadcast { message } => {
                        self.messages.insert(message);
                        reply.body.payload = Payload::BroadcastOk;
                        reply.send(out).context("serialize response to broadcast")?;
                    }
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        reply.send(out).context("serialize response to read")?;
                    }
                    Payload::Topology { mut topology } => {
                        self.neighbours = topology.remove(&self.node_id).unwrap_or_else(|| {
                            panic!("no topology given for node {}", self.node_id)
                        });

                        reply.body.payload = Payload::TopologyOk;
                        reply.send(out).context("serialize response topology")?;
                    }
                    Payload::Gossip { seen } => {
                        self.known
                            .get_mut(&reply.dst)
                            .expect("got gossip from unknown node")
                            .extend(seen.iter().copied());
                        self.messages.extend(seen);
                    }
                    Payload::BroadcastOk { .. }
                    | Payload::ReadOk { .. }
                    | Payload::TopologyOk { .. } => {}
                }
            }
            Event::Injected(payload) => match payload {
                InjectedPayload::Gossip => {
                    for n in &self.neighbours {
                        let known_to_n = &self.known[n];
                        let notify_of = self.messages
                            .difference(known_to_n)
                            .copied()
                            .collect();

                        Message {
                            src: self.node_id.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip { seen: notify_of },
                            },
                        }
                        .send(out)
                        .with_context(|| format!("gossip to {}", n))?;
                    }
                }
            },
            Event::EOF => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<(), BroadcastNode, _, _>(())
}

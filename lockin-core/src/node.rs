use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use bytes::Bytes;

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Deserialize, Serialize)]

pub struct NodeId(u64);

impl NodeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
    pub fn as_u64(&self) -> u64 {
        self.0.clone()
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Serialize, Deserialize)]
pub struct NodeName(String);

impl NodeName {
    pub fn new(name: &str) -> Self {
        Self(String::from(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn to_string(&self) -> String {
        String::from(&self.0)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeKind {
    File {
        data: Bytes,
    },
    Directory {
        children: BTreeMap<NodeName, NodeId>,
    },
}

#[derive(Clone)]
pub struct Node {
    pub id: NodeId,
    pub name: NodeName,
    pub kind: NodeKind,
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub instance: u64,
    pub content_gen: u64,
}

impl Node {
    pub fn new(id: NodeId, name: NodeName, kind: NodeKind) -> Self {
        Node {
            id,
            name,
            kind,
            metadata: Metadata {
                instance: 1,
                content_gen: 0,
            },
        }
    }

    pub fn add_child(&mut self, child_name: NodeName, child_id: NodeId) -> anyhow::Result<()> {
        match &mut self.kind {
            NodeKind::File { data: _ } => {
                anyhow::bail!("cannot add children in file kind node");
            }
            NodeKind::Directory { children } => {
                children.insert(child_name, child_id);
                Ok(())
            }
        }
    }
}

//helper methods
impl Node {
    pub fn is_file(&self) -> bool {
        matches!(self.kind, NodeKind::File { .. })
    }

    pub fn is_dir(&self) -> bool {
        matches!(self.kind, NodeKind::Directory { .. })
    }

    pub fn data(&self) -> Option<&Bytes> {
        match &self.kind {
            NodeKind::File { data } => Some(data),
            NodeKind::Directory { .. } => None,
        }
    }

    pub fn data_mut(&mut self) -> Option<&mut Bytes> {
        match &mut self.kind {
            NodeKind::File { data } => Some(data),
            NodeKind::Directory { .. } => None,
        }
    }

    pub fn children(&self) -> Option<&BTreeMap<NodeName, NodeId>> {
        match &self.kind {
            NodeKind::Directory { children } => Some(children),
            NodeKind::File { .. } => None,
        }
    }

    pub fn children_mut(&mut self) -> Option<&mut BTreeMap<NodeName, NodeId>> {
        match &mut self.kind {
            NodeKind::Directory { children } => Some(children),
            NodeKind::File { .. } => None,
        }
    }
}

// constructors
impl Node {
    pub fn new_file(id: NodeId, name: NodeName) -> Self {
        Self {
            id,
            name,
            kind: NodeKind::File { data: Bytes::new() },
            metadata: Metadata {
                instance: 1,
                content_gen: 0,
            },
        }
    }

    pub fn new_dir(id: NodeId, name: NodeName) -> Self {
        Self {
            id,
            name,
            kind: NodeKind::Directory {
                children: BTreeMap::new(),
            },
            metadata: Metadata {
                instance: 1,
                content_gen: 0,
            },
        }
    }
}

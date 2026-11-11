use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};
use thiserror::Error;
use tracing::{debug, error, info, trace, warn};

const MAX_FILE_SIZE: u64 = 256 * 1024;

use crate::{
    command::{Command, CommandResponse},
    node::{Metadata, Node, NodeId, NodeKind, NodeName},
};

pub struct Namespace {
    /// All live nodes. Root is always present.
    nodes: BTreeMap<NodeId, Node>,

    /// Always points to the root directory.
    /// Root is never deleted and is created in `Namespace::new()`.
    root: NodeId,

    /// Fast path lookup: full path → NodeId
    /// (you can also walk the tree on every Open if you prefer)
    path_index: BTreeMap<String, NodeId>,
    next_id: u64,
}

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum NamespaceError {
    #[error("Namespace or path not found")]
    NotFound,

    #[error("Already exists")]
    AlreadyExists,

    #[error("Path is not a directory")]
    NotDirectory,

    #[error("Path is not a file")]
    NotFile,

    #[error("Directory is not empty")]
    DirectoryNotEmpty,

    #[error("CAS mismatch: expected {expected}, got {current}")]
    CasMismatch { expected: u64, current: u64 },

    #[error("Payload or file is too large")]
    TooLarge,

    #[error("Invalid path format")]
    InvalidPath,

    #[error("cannot delete root")]
    CannotDeleteRoot,

    #[error("internal server error")]
    Internal,
}

impl Namespace {
    pub fn new() -> Self {
        let root_id = NodeId::new(0);
        let root = Node::new_dir(root_id.clone(), NodeName::new("/"));
        let mut nodes = BTreeMap::new();
        nodes.insert(root_id.clone(), root);

        info!(root_id = ?root_id, "namespace created with root");
        Self {
            nodes,
            root: root_id.clone(),
            path_index: BTreeMap::from([("/".to_string(), root_id)]),
            next_id: 1,
        }
    }

    // path / id helpers
    pub fn resolve(&self, path: &str) -> Option<NodeId> {
        match self.path_index.get(path) {
            Some(id) => {
                trace!(path = %path, id = ?id, "resolve: hit");
                Some(id.clone())
            }
            None => {
                trace!(path = %path, "resolve: miss");
                None
            }
        }
    }

    pub fn get_node(&self, id: &NodeId) -> Option<&Node> {
        let node = self.nodes.get(id);
        if node.is_none() {
            trace!(id = ?id, "get_node: not found");
        }
        node
    }

    pub fn get_node_mut(&mut self, id: &NodeId) -> Option<&mut Node> {
        let node = self.nodes.get_mut(id);
        if node.is_none() {
            trace!(id = ?id, "get_node_mut: not found");
        }
        node
    }

    // core chubby operations
    pub fn open(&mut self, path: &str, create: bool) -> Result<(NodeId, Metadata), NamespaceError> {
        debug!(path = %path, create = create, "open: start");

        if let Some(id) = self.resolve(path) {
            if let Some(node) = self.nodes.get(&id) {
                debug!(
                    path = %path,
                    id = ?id,
                    instance = node.metadata.instance,
                    content_gen = node.metadata.content_gen,
                    "open: existing node"
                );
                return Ok((id.clone(), node.metadata.clone()));
            }
        }

        if !create {
            warn!(path = %path, "open: not found and create=false");
            return Err(NamespaceError::NotFound.into());
        }

        let path = Path::new(path);
        let Some(path_str) = path.to_str() else {
            error!(path = ?path, "open: path is not valid UTF-8");
            return Err(NamespaceError::InvalidPath.into());
        };

        self.resolve_parent_dir(path)?;
        let new_node = self.create_new_node(path_str, NodeKind::File { data: Bytes::new() });
        self.update_parent_node(new_node.name.clone(), new_node.id.clone())?;

        info!(
            path = %path_str,
            id = ?new_node.id,
            instance = new_node.metadata.instance,
            content_gen = new_node.metadata.content_gen,
            "open: created new node"
        );
        Ok((new_node.id.clone(), new_node.metadata))
    }

    pub fn mkdir(&mut self, path: &str) -> Result<(NodeId, Metadata), NamespaceError> {
        debug!(path = %path, "mkdir: start");

        if let Some(id) = self.resolve(path) {
            if let Some(node) = self.nodes.get(&id) {
                debug!(
                    path = %path,
                    id = ?id,
                    instance = node.metadata.instance,
                    content_gen = node.metadata.content_gen,
                    "mkdir: already exists"
                );
                return Err(NamespaceError::AlreadyExists.into());
            }
        }

        let path = Path::new(path);
        let Some(path_str) = path.to_str() else {
            error!(path = ?path, "open: path is not valid UTF-8");
            return Err(NamespaceError::InvalidPath.into());
        };

        self.resolve_parent_dir(path)?;
        let new_node = self.create_new_node(
            path_str,
            NodeKind::Directory {
                children: BTreeMap::new(),
            },
        );
        self.update_parent_node(new_node.name.clone(), new_node.id.clone())?;

        info!(
            path = %path_str,
            id = ?new_node.id,
            instance = new_node.metadata.instance,
            "open: created new node"
        );
        Ok((new_node.id.clone(), new_node.metadata))
    }

    pub fn get_contents(&self, id: NodeId) -> Result<(bytes::Bytes, Metadata), NamespaceError> {
        debug!(id = ?id, "get_contents: start");

        let Some(node) = self.nodes.get(&id) else {
            warn!(id = ?id, "get_contents: node not found");
            return Err(NamespaceError::NotFound.into());
        };

        match &node.kind {
            NodeKind::File { data } => {
                debug!(
                    id = ?id,
                    len = data.len(),
                    content_gen = node.metadata.content_gen,
                    "get_contents: success"
                );
                Ok((data.clone(), node.metadata.clone()))
            }
            NodeKind::Directory { .. } => {
                warn!(id = ?id, "get_contents: target is a directory");
                Err(NamespaceError::NotFile.into())
            }
        }
    }

    pub fn set_contents(
        &mut self,
        id: NodeId,
        expected_gen: u64,
        new_data: bytes::Bytes,
    ) -> Result<Metadata, NamespaceError> {
        debug!(
            id = ?id,
            expected_gen = expected_gen,
            new_len = new_data.len(),
            "set_contents: start"
        );

        let Some(node) = self.nodes.get_mut(&id) else {
            warn!(id = ?id, "set_contents: node not found");
            return Err(NamespaceError::NotFound.into());
        };

        if new_data.len() > MAX_FILE_SIZE as usize {
            warn!(
                id = ?id,
                len = new_data.len(),
                max = MAX_FILE_SIZE,
                "set_contents: payload too large"
            );
            return Err(NamespaceError::TooLarge.into());
        }

        if node.metadata.content_gen != expected_gen {
            warn!(
                id = ?id,
                expected = expected_gen,
                current = node.metadata.content_gen,
                "set_contents: CAS mismatch"
            );
            return Err(NamespaceError::CasMismatch {
                expected: node.metadata.content_gen,
                current: expected_gen,
            }
            .into());
        }

        match node.data_mut() {
            Some(old_data) => {
                *old_data = new_data;
                node.metadata.content_gen += 1;
                info!(
                    id = ?id,
                    new_gen = node.metadata.content_gen,
                    "set_contents: success"
                );
                Ok(node.metadata.clone())
            }
            None => {
                warn!(id = ?id, "set_contents: target is not a file");
                Err(NamespaceError::NotFile.into())
            }
        }
    }

    pub fn delete(&mut self, id: NodeId) -> Result<(), NamespaceError> {
        debug!(id = ?id, "delete: start");

        if id == self.root {
            warn!(id = ?id, "delete: attempt to delete root");
            return Err(NamespaceError::CannotDeleteRoot.into());
        }

        let Some(node) = self.nodes.get(&id) else {
            warn!(id = ?id, "delete: node not found");
            return Err(NamespaceError::NotFound.into());
        };

        let name = node.name.clone();
        let mut to_delete = vec![];
        self.collect_subtree(id.clone(), &mut to_delete);

        debug!(id = ?id, count = to_delete.len(), "delete: subtree collected");

        if let Some(parent_id) = self.find_parent(id.clone()) {
            self.remove_child_link(parent_id, name);
        } else {
            warn!(id = ?id, "delete: parent not found (continuing with node removal)");
        }

        for del_id in &to_delete {
            self.nodes.remove(del_id);
            // optional: also clean path_index here if you track full paths
        }

        info!(id = ?id, removed = to_delete.len(), "delete: completed");
        Ok(())
    }

    pub fn apply(&mut self, cmd: Command) -> Result<CommandResponse, NamespaceError> {
        match cmd {
            Command::Open { path, create } => match self.open(path.as_str(), create) {
                Ok((id, meta)) => Ok(CommandResponse::OpenOk {
                    node_id: id,
                    metadata: meta,
                }),
                Err(e) => Err(e.into()),
            },
            Command::Mkdir { path } => match self.mkdir(path.as_str()) {
                Ok((node_id, metadata)) => Ok(CommandResponse::MkdirOk { node_id, metadata }),
                Err(e) => Err(e.into()),
            },
            Command::SetContents {
                id,
                expected_gen,
                data,
            } => match self.set_contents(id, expected_gen, data) {
                Ok(metadata) => Ok(CommandResponse::SetContentsOk { metadata }),
                Err(e) => Err(e.into()),
            },
            Command::Delete { id } => match self.delete(id) {
                Ok(()) => Ok(CommandResponse::DeleteOK),
                Err(e) => Err(e.into()),
            },
        }
    }
}

///Helper functions
impl Namespace {
    fn resolve_parent_dir(&self, path: &Path) -> Result<(), NamespaceError> {
        debug!(path = ?path, "resolve_parent_dir: start");

        for (i, ancestor) in path.ancestors().into_iter().enumerate() {
            if i == 0 {
                continue;
            }
            let Some(ancestor_name) = ancestor.to_str() else {
                error!(path = ?path, "resolve_parent_dir: ancestor is not valid UTF-8");
                return Err(NamespaceError::InvalidPath.into());
            };

            trace!(ancestor = %ancestor_name, "checking ancestor");

            if !self.path_index.contains_key(ancestor_name) {
                warn!(ancestor = %ancestor_name, "ancestor not found in path_index");
                return Err(NamespaceError::NotFound.into());
            }

            let ancestor_node_id = self.path_index.get(ancestor_name).unwrap();
            let Some(ancestor_node) = self.nodes.get(ancestor_node_id) else {
                error!(
                    ancestor = %ancestor_name,
                    id = ?ancestor_node_id,
                    "path_index points to missing node"
                );
                return Err(NamespaceError::NotFound.into());
            };

            if ancestor_node.is_file() {
                warn!(
                    ancestor = %ancestor_name,
                    id = ?ancestor_node_id,
                    "ancestor is a file, expected directory"
                );
                return Err(NamespaceError::NotDirectory.into());
            }
        }

        debug!(path = ?path, "resolve_parent_dir: all ancestors ok");
        Ok(())
    }

    fn create_new_node(&mut self, name: &str, kind: NodeKind) -> Node {
        let new_node_id = NodeId::new(self.next_id);
        let new_node_name = NodeName::new(name);
        let new_node = match kind {
            NodeKind::File { data: _ } => {
                debug!(
                    id = ?new_node_id,
                    name = %name,
                    next_id = self.next_id,
                    "creating new file node"
                );
                Node::new_file(new_node_id.clone(), new_node_name)
            }
            NodeKind::Directory { children: _ } => {
                debug!(
                    id = ?new_node_id,
                    name = %name,
                    next_id = self.next_id,
                    "creating new directory node"
                );
                Node::new_dir(new_node_id.clone(), new_node_name)
            }
        };

        debug!(content_gen = ?new_node.metadata.content_gen, "content gen inside create new node method");
        self.next_id += 1;
        self.nodes.insert(new_node_id.clone(), new_node.clone());
        self.path_index
            .insert(name.to_string(), new_node_id.clone());

        debug!(id = ?new_node_id, name = %name, "node inserted into nodes + path_index");
        new_node
    }

    fn update_parent_node(
        &mut self,
        child_name: NodeName,
        child_id: NodeId,
    ) -> Result<(), NamespaceError> {
        debug!(child_id = ?child_id, child_name = ?child_name, "update_parent_node: start");

        let Some(parent_id) = self.find_parent(child_id.clone()) else {
            error!(child_id = ?child_id, "could not find parent");
            return Err(NamespaceError::InvalidPath.into());
        };

        let Some(parent_node) = self.nodes.get_mut(&parent_id) else {
            error!(parent_id = ?parent_id, "parent id not present in nodes");
            return Err(NamespaceError::InvalidPath.into());
        };

        parent_node
            .add_child(child_name.clone(), child_id.clone())
            .map_err(|_| NamespaceError::Internal)?;
        debug!(
            parent_id = ?parent_id,
            child_id = ?child_id,
            child_name = ?child_name,
            "child link added to parent"
        );
        Ok(())
    }

    fn collect_subtree(&self, id: NodeId, to_delete: &mut Vec<NodeId>) {
        let Some(node) = self.nodes.get(&id) else {
            warn!(id = ?id, "collect_subtree: node not found, skipping");
            return;
        };

        if let Some(children) = node.children() {
            for child in children {
                trace!(parent = ?id, child = ?child.1, "descending into child");
                self.collect_subtree(child.1.clone(), to_delete);
            }
        }

        to_delete.push(id.clone());
        trace!(id = ?id, "marked for deletion");
    }

    fn find_parent(&self, child_id: NodeId) -> Option<NodeId> {
        let Some(node) = self.nodes.get(&child_id) else {
            warn!(child_id = ?child_id, "find_parent: child not found");
            return None;
        };

        let node_path = Path::new(node.name.as_str());
        let Some(parent_path) = node_path.parent() else {
            debug!(child_id = ?child_id, name = %node.name.as_str(), "node has no parent path");
            return None;
        };

        let parent_path = match parent_path.to_str() {
            Some(p) => p,
            None => {
                error!(child_id = ?child_id, "parent path is not valid UTF-8");
                return None;
            }
        };

        match self.path_index.get(parent_path) {
            Some(parent_id) => {
                trace!(
                    child_id = ?child_id,
                    parent_id = ?parent_id,
                    parent_path = %parent_path,
                    "parent resolved"
                );
                Some(parent_id.clone())
            }
            None => {
                warn!(
                    child_id = ?child_id,
                    parent_path = %parent_path,
                    "parent path not present in path_index"
                );
                None
            }
        }
    }

    fn remove_child_link(&mut self, parent_id: NodeId, child_name: NodeName) {
        let Some(parent) = self.nodes.get_mut(&parent_id) else {
            warn!(parent_id = ?parent_id, "remove_child_link: parent not found");
            return;
        };

        let Some(children) = parent.children_mut() else {
            warn!(parent_id = ?parent_id, "remove_child_link: parent is not a directory");
            return;
        };

        let removed = children.remove(&child_name);
        if removed.is_some() {
            debug!(
                parent_id = ?parent_id,
                child_name = ?child_name,
                "child link removed"
            );
        } else {
            warn!(
                parent_id = ?parent_id,
                child_name = ?child_name,
                "child name not present in parent's children map"
            );
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;

    // -------------------------------------------------------------------------
    // open
    // -------------------------------------------------------------------------

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }

    #[test]
    fn open_root() {
        let mut ns = Namespace::new();
        let (id, meta) = ns.open("/", false).expect("root should open");
        assert_eq!(id, ns.root);
        assert_eq!(meta.instance, 1);
        assert_eq!(meta.content_gen, 0);
    }

    #[test]
    fn open_root_with_create_flag() {
        // create=true on an existing node should still succeed and return the same node
        let mut ns = Namespace::new();
        let (id, meta) = ns.open("/", true).expect("root should open");
        assert_eq!(id, ns.root);
        assert_eq!(meta.instance, 1);
        assert_eq!(meta.content_gen, 0);
    }

    #[test]
    fn open_missing_without_create() {
        let mut ns = Namespace::new();
        let err = ns.open("/does-not-exist", false).unwrap_err();
        assert!(matches!(err, NamespaceError::NotFound));
    }

    #[test]
    fn open_create_file() {
        let mut ns = Namespace::new();
        let (id, meta) = ns.open("/hello.txt", true).expect("should create");

        assert_eq!(meta.instance, 1);
        assert_eq!(meta.content_gen, 0);

        let node = ns.get_node(&id).expect("node must exist");
        assert!(node.is_file());
    }

    #[test]
    fn open_existing_file_returns_same_id() {
        let mut ns = Namespace::new();
        let (id1, meta1) = ns.open("/hello.txt", true).unwrap();
        let (id2, meta2) = ns.open("/hello.txt", false).unwrap();

        assert_eq!(id1, id2);
        assert_eq!(meta1.instance, meta2.instance);
        assert_eq!(meta1.content_gen, meta2.content_gen);
    }

    #[test]
    fn open_create_when_parent_missing() {
        let mut ns = Namespace::new();
        // parent "/missing" does not exist
        let err = ns.open("/missing/child.txt", true).unwrap_err();
        assert!(matches!(err, NamespaceError::NotFound));
    }

    #[test]
    fn open_create_under_file_fails() {
        let mut ns = Namespace::new();
        ns.open("/file", true).unwrap(); // creates a file

        let err = ns.open("/file/child", true).unwrap_err();
        assert!(matches!(err, NamespaceError::NotDirectory));
    }

    #[test]
    fn open_multiple_files_under_root() {
        let mut ns = Namespace::new();
        let (id1, _) = ns.open("/a.txt", true).unwrap();
        let (id2, _) = ns.open("/b.txt", true).unwrap();

        assert_ne!(id1, id2);
        assert!(ns.get_node(&id1).unwrap().is_file());
        assert!(ns.get_node(&id2).unwrap().is_file());
    }

    // -------------------------------------------------------------------------
    // get_contents
    // -------------------------------------------------------------------------

    #[test]
    fn get_contents_empty_file() {
        let mut ns = Namespace::new();
        let (id, meta) = ns.open("/empty.txt", true).unwrap();

        let (data, got_meta) = ns.get_contents(id).expect("should succeed");
        assert!(data.is_empty());
        assert_eq!(got_meta.instance, meta.instance);
        assert_eq!(got_meta.content_gen, meta.content_gen);
    }

    #[test]
    fn get_contents_after_set() {
        let mut ns = Namespace::new();
        let (id, meta) = ns.open("/data.txt", true).unwrap();

        // write something
        let payload = Bytes::from_static(b"hello chubby");
        let new_meta = ns
            .set_contents(id.clone(), meta.content_gen, payload.clone())
            .unwrap();

        let (data, got_meta) = ns.get_contents(id).unwrap();
        assert_eq!(data, payload);
        assert_eq!(got_meta.content_gen, new_meta.content_gen);
        assert_eq!(got_meta.content_gen, 1);
    }

    #[test]
    fn get_contents_not_found() {
        let ns = Namespace::new();
        let fake_id = NodeId::new(999);
        let err = ns.get_contents(fake_id).unwrap_err();
        assert!(matches!(err, NamespaceError::NotFound));
    }

    #[test]
    fn get_contents_on_directory() {
        let ns = Namespace::new();
        let err = ns.get_contents(ns.root.clone()).unwrap_err();
        assert!(matches!(err, NamespaceError::NotFile));
    }

    #[test]
    fn get_contents_on_root() {
        // explicit root check (same as above, but clearer name)
        let ns = Namespace::new();
        let err = ns.get_contents(ns.root.clone()).unwrap_err();
        assert!(matches!(err, NamespaceError::NotFile));
    }

    // -------------------------------------------------------------------------
    // set_contents
    // -------------------------------------------------------------------------

    #[test]
    fn set_contents_success() {
        init_tracing();
        let mut ns = Namespace::new();
        let (id, meta) = ns.open("/data.txt", true).unwrap();

        let payload = Bytes::from_static(b"hello");
        let new_meta = ns
            .set_contents(id.clone(), meta.content_gen, payload.clone())
            .expect("CAS should succeed");

        assert_eq!(new_meta.content_gen, meta.content_gen + 1);
        assert_eq!(new_meta.instance, meta.instance); // instance must not change

        let (data, got_meta) = ns.get_contents(id).unwrap();
        assert_eq!(data, payload);
        assert_eq!(got_meta.content_gen, new_meta.content_gen);
    }

    #[test]
    fn set_contents_cas_mismatch() {
        init_tracing();
        let mut ns = Namespace::new();
        let (id, meta) = ns.open("/data.txt", true).unwrap();

        // first write succeeds
        ns.set_contents(id.clone(), meta.content_gen, Bytes::from_static(b"v1"))
            .unwrap();

        // second write with the OLD generation must fail
        let err = ns
            .set_contents(id.clone(), meta.content_gen, Bytes::from_static(b"v2"))
            .unwrap_err();
        assert!(matches!(
            err,
            NamespaceError::CasMismatch {
                expected: _,
                current: _
            }
        ));

        // data must still be the first write
        let (data, _) = ns.get_contents(id).unwrap();
        assert_eq!(data, Bytes::from_static(b"v1"));
    }

    #[test]
    fn set_contents_after_cas_mismatch_still_works() {
        init_tracing();
        let mut ns = Namespace::new();
        let (id, meta) = ns.open("/data.txt", true).unwrap();

        let meta1 = ns
            .set_contents(id.clone(), meta.content_gen, Bytes::from_static(b"v1"))
            .unwrap();

        // stale gen fails
        let _ = ns
            .set_contents(id.clone(), meta.content_gen, Bytes::from_static(b"stale"))
            .unwrap_err();

        // correct gen succeeds
        let meta2 = ns
            .set_contents(id.clone(), meta1.content_gen, Bytes::from_static(b"v2"))
            .unwrap();

        assert_eq!(meta2.content_gen, meta1.content_gen + 1);
        let (data, _) = ns.get_contents(id).unwrap();
        assert_eq!(data, Bytes::from_static(b"v2"));
    }

    #[test]
    fn set_contents_too_large() {
        init_tracing();
        let mut ns = Namespace::new();
        let (id, meta) = ns.open("/big.txt", true).unwrap();

        let big = Bytes::from(vec![0u8; (MAX_FILE_SIZE as usize) + 1]);
        let err = ns.set_contents(id, meta.content_gen, big).unwrap_err();
        assert!(matches!(err, NamespaceError::TooLarge));
    }

    #[test]
    fn set_contents_on_directory() {
        init_tracing();
        let mut ns = Namespace::new();
        let err = ns
            .set_contents(ns.root.clone(), 0, Bytes::from_static(b"nope"))
            .unwrap_err();
        assert!(matches!(err, NamespaceError::NotFile));
    }

    #[test]
    fn set_contents_not_found() {
        init_tracing();
        let mut ns = Namespace::new();
        let fake = NodeId::new(999);
        let err = ns
            .set_contents(fake, 0, Bytes::from_static(b"x"))
            .unwrap_err();
        assert!(matches!(err, NamespaceError::NotFound));
    }

    #[test]
    fn set_contents_does_not_change_instance() {
        init_tracing();
        let mut ns = Namespace::new();
        let (id, meta) = ns.open("/f.txt", true).unwrap();
        let original_instance = meta.instance;

        let new_meta = ns
            .set_contents(id, meta.content_gen, Bytes::from_static(b"data"))
            .unwrap();

        assert_eq!(new_meta.instance, original_instance);
    }

    // -------------------------------------------------------------------------
    // mkdir
    // -------------------------------------------------------------------------

    #[test]
    fn mkdir_under_root() {
        init_tracing();
        let mut ns = Namespace::new();

        let (id, meta) = ns.mkdir("/dir").expect("mkdir should succeed");
        assert_eq!(meta.instance, 1);
        assert_eq!(meta.content_gen, 0);

        let node = ns.get_node(&id).unwrap();
        assert!(node.is_dir());
    }

    #[test]
    fn mkdir_nested() {
        init_tracing();
        let mut ns = Namespace::new();

        ns.mkdir("/a").unwrap();
        let (id_b, _) = ns.mkdir("/a/b").unwrap();

        let node = ns.get_node(&id_b).unwrap();
        assert!(node.is_dir());
    }

    #[test]
    fn mkdir_parent_missing() {
        init_tracing();
        let mut ns = Namespace::new();

        let err = ns.mkdir("/missing/parent/dir").unwrap_err();
        assert!(matches!(err, NamespaceError::NotFound));
    }

    #[test]
    fn mkdir_under_file_fails() {
        init_tracing();
        let mut ns = Namespace::new();

        ns.open("/file", true).unwrap(); // creates a file

        let err = ns.mkdir("/file/subdir").unwrap_err();
        assert!(matches!(err, NamespaceError::NotDirectory));
    }

    #[test]
    fn mkdir_already_exists() {
        init_tracing();
        let mut ns = Namespace::new();

        let (_, _) = ns.mkdir("/dir").unwrap();
        let err = ns.mkdir("/dir").unwrap_err();
        assert!(matches!(err, NamespaceError::AlreadyExists));
    }

    #[test]
    fn open_existing_directory() {
        init_tracing();
        let mut ns = Namespace::new();

        let (dir_id, meta1) = ns.mkdir("/docs").unwrap();
        let (opened_id, meta2) = ns.open("/docs", false).unwrap();

        assert_eq!(dir_id, opened_id);
        assert_eq!(meta1.instance, meta2.instance);
    }

    #[test]
    fn mkdir_does_not_create_file() {
        init_tracing();
        let mut ns = Namespace::new();

        let (id, _) = ns.mkdir("/onlydir").unwrap();
        let node = ns.get_node(&id).unwrap();
        assert!(node.is_dir());
        assert!(node.data().is_none());
    }

    // -------------------------------------------------------------------------
    // delete (updated – now with real directories)
    // -------------------------------------------------------------------------

    #[test]
    fn delete_file() {
        init_tracing();
        let mut ns = Namespace::new();
        let (id, _) = ns.open("/todelete.txt", true).unwrap();

        ns.delete(id.clone()).unwrap();
        assert!(ns.get_node(&id).is_none());

        let err = ns.open("/todelete.txt", false).unwrap_err();
        assert!(matches!(err, NamespaceError::NotFound));
    }

    #[test]
    fn delete_empty_directory() {
        init_tracing();
        let mut ns = Namespace::new();
        let (id, _) = ns.mkdir("/emptydir").unwrap();

        ns.delete(id.clone()).unwrap();
        assert!(ns.get_node(&id).is_none());
    }

    #[test]
    fn delete_non_empty_directory_recursive() {
        // Your delete is recursive, so this must succeed and remove everything.
        init_tracing();
        let mut ns = Namespace::new();

        ns.mkdir("/a").unwrap();
        ns.mkdir("/a/b").unwrap();
        let (file_id, _) = ns.open("/a/b/c.txt", true).unwrap();
        let (dir_a, _) = ns.open("/a", false).unwrap();

        ns.delete(dir_a.clone()).unwrap();

        assert!(ns.get_node(&dir_a).is_none());
        assert!(ns.get_node(&file_id).is_none());
        assert!(ns.open("/a", false).is_err());
        assert!(ns.open("/a/b", false).is_err());
        assert!(ns.open("/a/b/c.txt", false).is_err());
    }

    #[test]
    fn delete_then_recreate_directory() {
        init_tracing();
        let mut ns = Namespace::new();

        let (id1, _) = ns.mkdir("/recreate").unwrap();
        ns.delete(id1.clone()).unwrap();

        let (id2, meta2) = ns.mkdir("/recreate").unwrap();
        assert_ne!(id1, id2);
        assert_eq!(meta2.instance, 1);
        assert_eq!(meta2.content_gen, 0);
    }

    #[test]
    fn delete_root_fails() {
        init_tracing();
        let mut ns = Namespace::new();
        let err = ns.delete(ns.root.clone()).unwrap_err();
        assert!(matches!(err, NamespaceError::CannotDeleteRoot));
    }

    #[test]
    fn delete_not_found() {
        init_tracing();
        let mut ns = Namespace::new();
        let err = ns.delete(NodeId::new(999)).unwrap_err();
        assert!(matches!(err, NamespaceError::NotFound));
    }

    #[test]
    fn delete_removes_child_link_from_parent() {
        init_tracing();
        let mut ns = Namespace::new();
        let (id, _) = ns.mkdir("/child").unwrap();

        {
            let root = ns.get_node(&ns.root).unwrap();
            assert!(root.children().unwrap().values().any(|cid| *cid == id));
        }

        ns.delete(id.clone()).unwrap();

        let root = ns.get_node(&ns.root).unwrap();
        assert!(!root.children().unwrap().values().any(|cid| *cid == id));
    }

    #[test]
    fn delete_file_under_directory() {
        init_tracing();
        let mut ns = Namespace::new();
        ns.mkdir("/docs").unwrap();
        let (file_id, _) = ns.open("/docs/readme.txt", true).unwrap();

        ns.delete(file_id.clone()).unwrap();
        assert!(ns.get_node(&file_id).is_none());

        // parent directory must still exist
        let (dir_id, _) = ns.open("/docs", false).unwrap();
        assert!(ns.get_node(&dir_id).unwrap().is_dir());
    }
}

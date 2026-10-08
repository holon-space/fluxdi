use std::any::TypeId;

use crate::scope::Scope;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyCardinality {
    One,
    All,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphDependency {
    pub type_name: String,
    pub name: Option<String>,
    pub cardinality: DependencyCardinality,
    pub is_dynamic: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphBinding {
    Single,
    Named(String),
    Set(usize),
    Dynamic(String),
}

impl GraphBinding {
    fn as_label(&self) -> String {
        match self {
            Self::Single => "single".to_string(),
            Self::Named(name) => format!("named:{name}"),
            Self::Set(index) => format!("set:{index}"),
            Self::Dynamic(name) => format!("dynamic:{name}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphNode {
    pub id: String,
    pub type_name: String,
    pub scope: Scope,
    pub binding: GraphBinding,
    pub dependencies: Vec<GraphDependency>,
}

impl GraphNode {
    fn label(&self) -> String {
        format!(
            "{} [{} | {}]",
            self.type_name,
            self.scope,
            self.binding.as_label()
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphEdge {
    pub from: String,
    pub to: String,
    pub label: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DependencyGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

impl DependencyGraph {
    pub fn to_dot(&self) -> String {
        fn escape(input: &str) -> String {
            input.replace('\\', "\\\\").replace('"', "\\\"")
        }

        let mut out = String::from("digraph fluxdi {\n  rankdir=LR;\n");
        for node in &self.nodes {
            out.push_str(&format!(
                "  \"{}\" [label=\"{}\"];\n",
                escape(&node.id),
                escape(node.label().as_str())
            ));
        }

        for edge in &self.edges {
            if let Some(label) = &edge.label {
                out.push_str(&format!(
                    "  \"{}\" -> \"{}\" [label=\"{}\"];\n",
                    escape(&edge.from),
                    escape(&edge.to),
                    escape(label)
                ));
            } else {
                out.push_str(&format!(
                    "  \"{}\" -> \"{}\";\n",
                    escape(&edge.from),
                    escape(&edge.to)
                ));
            }
        }

        out.push_str("}\n");
        out
    }

    pub fn to_mermaid(&self) -> String {
        fn sanitize(id: &str) -> String {
            let mut output = String::new();
            for ch in id.chars() {
                if ch.is_ascii_alphanumeric() || ch == '_' {
                    output.push(ch);
                } else {
                    output.push('_');
                }
            }
            if output.is_empty() {
                "node".to_string()
            } else {
                output
            }
        }

        let mut out = String::from("graph TD\n");
        let mut aliases = std::collections::HashMap::new();

        for (index, node) in self.nodes.iter().enumerate() {
            let alias = format!("n{}_{}", index, sanitize(&node.id));
            aliases.insert(node.id.clone(), alias.clone());
            out.push_str(&format!("  {}[\"{}\"]\n", alias, node.label()));
        }

        for edge in &self.edges {
            if let (Some(from), Some(to)) = (aliases.get(&edge.from), aliases.get(&edge.to)) {
                if let Some(label) = &edge.label {
                    out.push_str(&format!("  {} -->|{}| {}\n", from, label, to));
                } else {
                    out.push_str(&format!("  {} --> {}\n", from, to));
                }
            }
        }

        out
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphValidationIssueKind {
    MissingDependency,
    CircularDependency,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphValidationIssue {
    pub kind: GraphValidationIssueKind,
    pub node_id: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GraphValidationReport {
    pub issues: Vec<GraphValidationIssue>,
}

impl GraphValidationReport {
    pub fn is_valid(&self) -> bool {
        self.issues.is_empty()
    }

    pub fn summary(&self) -> String {
        if self.issues.is_empty() {
            return "Dependency graph is valid".to_string();
        }

        self.issues
            .iter()
            .map(|issue| issue.message.clone())
            .collect::<Vec<_>>()
            .join(" | ")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DependencyHint {
    pub(crate) type_id: TypeId,
    pub(crate) type_name: &'static str,
    pub(crate) name: Option<String>,
    pub(crate) cardinality: DependencyCardinality,
    pub(crate) is_dynamic: bool,
}

impl DependencyHint {
    pub(crate) fn one<T>() -> Self
    where
        T: ?Sized + 'static,
    {
        Self {
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>(),
            name: None,
            cardinality: DependencyCardinality::One,
            is_dynamic: false,
        }
    }

    pub(crate) fn named<T>(name: String) -> Self
    where
        T: ?Sized + 'static,
    {
        Self {
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>(),
            name: Some(name),
            cardinality: DependencyCardinality::One,
            is_dynamic: false,
        }
    }

    pub(crate) fn all<T>() -> Self
    where
        T: ?Sized + 'static,
    {
        Self {
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>(),
            name: None,
            cardinality: DependencyCardinality::All,
            is_dynamic: false,
        }
    }

    pub(crate) fn dynamic(name: String) -> Self {
        Self {
            type_id: TypeId::of::<()>(),
            type_name: "<dynamic>",
            name: Some(name),
            cardinality: DependencyCardinality::One,
            is_dynamic: true,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderGraphMeta {
    pub(crate) type_id: TypeId,
    pub(crate) type_name: &'static str,
    pub(crate) scope: Scope,
    pub(crate) dependencies: Vec<DependencyHint>,
}

impl ProviderGraphMeta {
    pub(crate) fn of<T>(scope: Scope, dependencies: Vec<DependencyHint>) -> Self
    where
        T: ?Sized + 'static,
    {
        Self {
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>(),
            scope,
            dependencies,
        }
    }
}

#[cfg_attr(not(feature = "dynamic"), allow(dead_code))]
#[derive(Clone, Debug)]
pub(crate) struct DynamicProviderGraphMeta {
    pub(crate) name: String,
    pub(crate) scope: Scope,
    pub(crate) dependencies: Vec<DependencyHint>,
}

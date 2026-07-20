//! Typed, bounded display representations for a subagent.

use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use unicode_segmentation::UnicodeSegmentation;

const SHORT_THREAD_ID_LEN: usize = 8;
const NICKNAME_MAX_GRAPHEMES: usize = 48;
const ROLE_MAX_GRAPHEMES: usize = 32;
const PATH_MAX_GRAPHEMES: usize = 96;

/// The stable and presentation identities used to render a subagent in the TUI.
///
/// `agent_path` is the stable identity when available. Nickname and role are presentation hints:
/// they may be absent or collide, so [`Self::contextual_label`] returns the most specific bounded
/// label available. The full thread UUID is deliberately limited to [`Self::technical_detail`]
/// and [`Self::search_text`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentDisplayIdentity {
    pub(crate) thread_id: ThreadId,
    pub(crate) agent_path: Option<AgentPath>,
    pub(crate) nickname: Option<String>,
    pub(crate) role: Option<String>,
}

impl AgentDisplayIdentity {
    pub(crate) fn new(
        thread_id: ThreadId,
        agent_path: Option<AgentPath>,
        nickname: Option<String>,
        role: Option<String>,
    ) -> Self {
        Self {
            thread_id,
            agent_path,
            nickname: normalized_hint(nickname, NICKNAME_MAX_GRAPHEMES),
            role: normalized_hint(role, ROLE_MAX_GRAPHEMES),
        }
    }

    /// Returns the compact, friendly representation of this agent.
    pub(crate) fn primary_label(&self) -> String {
        if let Some(presentation) = self.presentation_label() {
            return presentation;
        }
        if let Some(agent_path) = self.agent_path.as_ref() {
            return bounded_path(agent_path);
        }
        self.role_with_short_id()
    }

    /// Returns the normal UI representation, retaining the canonical path when known.
    pub(crate) fn contextual_label(&self) -> String {
        match (self.presentation_label(), self.agent_path.as_ref()) {
            (Some(presentation), Some(agent_path)) => {
                format!("{presentation} · {}", bounded_path(agent_path))
            }
            (Some(presentation), None) => presentation,
            (None, Some(agent_path)) => bounded_path(agent_path),
            (None, None) => self.role_with_short_id(),
        }
    }

    /// Returns the explicit technical detail that may expose the full thread UUID.
    pub(crate) fn technical_detail(&self) -> String {
        format!("thread {}", self.thread_id)
    }

    /// Returns searchable text, including the full UUID without placing it on normal UI rows.
    pub(crate) fn search_text(&self) -> String {
        format!("{} {}", self.contextual_label(), self.technical_detail())
    }

    fn presentation_label(&self) -> Option<String> {
        match (self.nickname.as_deref(), self.role.as_deref()) {
            (Some(nickname), Some(role)) => Some(format!("{nickname} [{role}]")),
            (Some(nickname), None) => Some(nickname.to_string()),
            (None, Some(_)) | (None, None) => None,
        }
    }

    fn role_with_short_id(&self) -> String {
        self.role
            .as_deref()
            .map(|role| format!("[{role}] · {}", self.short_id_label()))
            .unwrap_or_else(|| self.short_id_label())
    }

    fn short_id_label(&self) -> String {
        let short_id: String = self
            .thread_id
            .to_string()
            .chars()
            .filter(char::is_ascii_hexdigit)
            .rev()
            .take(SHORT_THREAD_ID_LEN)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("Agent ({short_id})")
    }
}

fn normalized_hint(value: Option<String>, max_graphemes: usize) -> Option<String> {
    value.and_then(|value| {
        let normalized = value
            .chars()
            .map(|character| {
                if character.is_control() || character.is_whitespace() {
                    ' '
                } else {
                    character
                }
            })
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        (!normalized.is_empty())
            .then(|| crate::text_formatting::truncate_text(&normalized, max_graphemes))
    })
}

fn bounded_path(agent_path: &AgentPath) -> String {
    let path = agent_path.as_str();
    let graphemes = path.graphemes(true).collect::<Vec<_>>();
    if graphemes.len() <= PATH_MAX_GRAPHEMES {
        return path.to_string();
    }

    let fingerprint = stable_path_fingerprint(path);
    let marker = format!("…{fingerprint:016x}…");
    let remaining = PATH_MAX_GRAPHEMES - marker.graphemes(true).count();
    let prefix_len = remaining / 2;
    let suffix_len = remaining - prefix_len;
    format!(
        "{}{marker}{}",
        graphemes[..prefix_len].concat(),
        graphemes[graphemes.len() - suffix_len..].concat()
    )
}

fn stable_path_fingerprint(path: &str) -> u64 {
    path.as_bytes()
        .iter()
        .fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        })
}

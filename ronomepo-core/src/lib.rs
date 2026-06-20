use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use regex::Regex;
use serde::{Deserialize, Serialize};

pub const MANIFEST_FILE_NAME: &str = "ronomepo.json";
pub const REPO_MANIFEST_FILE_NAME: &str = "ronomepo.repo.json";
pub const REPO_MANIFEST_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceManifest {
    pub name: String,
    pub root: PathBuf,
    pub repos: Vec<RepositoryEntry>,
    pub shared_hooks_path: Option<PathBuf>,
    #[serde(default)]
    pub commit_check_rules: Option<Vec<CommitCheckRule>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryEntry {
    pub id: String,
    pub name: String,
    pub dir_name: String,
    pub remote_url: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RepoManifest {
    #[serde(default = "repo_manifest_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub repo_id: Option<String>,
    #[serde(default)]
    pub items: Vec<RepoItem>,
    #[serde(default)]
    pub actions: Vec<RepoActionDefinition>,
    #[serde(default)]
    pub capabilities: Vec<RepoCapabilityDeclaration>,
    #[serde(default)]
    pub repo_actions: Vec<RepoActionCommand>,
    #[serde(default)]
    pub aggregation: Vec<RepoActionAggregation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoActionDefinition {
    pub id: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub workdir: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    #[serde(default = "default_action_output_mode")]
    pub output: ActionOutputMode,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RepoCapabilityDeclaration {
    pub id: String,
    pub capability: String,
    pub status: CapabilityDeclarationStatus,
    #[serde(default)]
    pub item_id: Option<String>,
    #[serde(default)]
    pub root: Option<PathBuf>,
    #[serde(default)]
    pub action_ref: Option<RepoActionRef>,
    #[serde(default)]
    pub standard_action: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub schedule: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityDeclarationStatus {
    Implemented,
    NotApplicable,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RepoActionRef {
    Standard {
        name: String,
        #[serde(default)]
        item_id: Option<String>,
        #[serde(default)]
        root: Option<PathBuf>,
    },
    RepoAction {
        id: String,
    },
    Group {
        #[serde(default)]
        execution: AggregationExecutionMode,
        #[serde(default)]
        failure_policy: AggregationFailurePolicy,
        #[serde(default)]
        merge: AggregationMergeStrategy,
        items: Vec<RepoActionRef>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RepoItem {
    pub id: String,
    #[serde(rename = "type")]
    pub item_type: String,
    pub path: PathBuf,
    #[serde(default)]
    pub config: Option<serde_json::Value>,
    #[serde(default)]
    pub artifacts: Vec<RepoArtifactDefinition>,
    #[serde(default)]
    pub actions: Vec<RepoActionCommand>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoArtifactDefinition {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub build_action: Option<StandardActionName>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoActionCommand {
    pub action: StandardActionName,
    pub command: Vec<String>,
    #[serde(default)]
    pub workdir: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    #[serde(default = "default_action_output_mode")]
    pub output: ActionOutputMode,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoActionAggregation {
    pub action: StandardActionName,
    pub item_ids: Vec<String>,
    #[serde(default)]
    pub execution: AggregationExecutionMode,
    #[serde(default)]
    pub failure_policy: AggregationFailurePolicy,
    #[serde(default)]
    pub merge: AggregationMergeStrategy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StandardActionName {
    ListArtifacts,
    Build,
    Test,
    Clean,
    VerifyDependenciesFreshness,
    Deploy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionOutputMode {
    Text,
    Json,
    JsonLines,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AggregationExecutionMode {
    Parallel,
    #[default]
    Sequential,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AggregationFailurePolicy {
    Continue,
    #[default]
    FailFast,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AggregationMergeStrategy {
    #[default]
    Combined,
    PerItem,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoActionPlan {
    pub action: StandardActionName,
    pub execution: AggregationExecutionMode,
    pub failure_policy: AggregationFailurePolicy,
    pub merge: AggregationMergeStrategy,
    pub steps: Vec<RepoActionStep>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilityActionPlan {
    pub capability_instance_id: String,
    pub capability: String,
    pub execution: AggregationExecutionMode,
    pub failure_policy: AggregationFailurePolicy,
    pub merge: AggregationMergeStrategy,
    pub steps: Vec<RepoActionStep>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilityGitSnapshot {
    pub commit: Option<String>,
    pub dirty: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoActionStep {
    pub item_id: Option<String>,
    pub source: RepoActionSource,
    pub executor: RepoActionExecutor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepoActionSource {
    BuiltInHandler { item_type: String },
    ItemOverride,
    RepoCommand,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepoActionExecutor {
    Command(PlannedCommand),
    BuiltInInspector,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedCommand {
    pub program: String,
    pub args: Vec<String>,
    pub workdir: PathBuf,
    pub env: BTreeMap<String, String>,
    pub timeout_seconds: Option<u64>,
    pub output: ActionOutputMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedArtifact {
    pub item_id: String,
    pub name: String,
    pub kind: String,
    pub path: Option<PathBuf>,
    pub pattern: Option<String>,
    pub build_action: Option<StandardActionName>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityRequirementLevel {
    Required,
    Recommended,
    Optional,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityRequirementScope {
    Repo,
    Item,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilityDefinition {
    pub id: &'static str,
    pub title: &'static str,
    pub family: &'static str,
    pub requirement_level: CapabilityRequirementLevel,
    pub requirement_scope: CapabilityRequirementScope,
    pub item_types: &'static [&'static str],
    pub result_schema: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityResultStatus {
    Ok,
    Warning,
    Failed,
    Error,
    Unknown,
    Running,
    Stale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityFindingSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityFinding {
    pub severity: CapabilityFindingSeverity,
    pub message: String,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub file: Option<PathBuf>,
    #[serde(default)]
    pub line: Option<u32>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub suggested_action: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityResult {
    pub status: CapabilityResultStatus,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<CapabilityFinding>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityState {
    pub repo_id: String,
    pub capability_instance_id: String,
    pub capability: String,
    #[serde(default)]
    pub item_id: Option<String>,
    #[serde(default)]
    pub root: Option<PathBuf>,
    pub status: CapabilityResultStatus,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<CapabilityFinding>,
    #[serde(default)]
    pub checked_at_epoch_secs: Option<u64>,
    #[serde(default)]
    pub commit: Option<String>,
    #[serde(default)]
    pub dirty: bool,
    #[serde(default)]
    pub duration_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityRegistry {
    #[serde(default = "capability_registry_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub states: Vec<CapabilityState>,
}

impl Default for CapabilityRegistry {
    fn default() -> Self {
        Self {
            schema_version: capability_registry_schema_version(),
            states: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilityPolicyIssue {
    pub repo_node_id: String,
    pub capability: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoCapabilityPolicyReport {
    pub issues: Vec<CapabilityPolicyIssue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionLevel {
    Info,
    Opportunity,
    ShouldDo,
    Urgent,
    Blocked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionUrgency {
    Low,
    Medium,
    High,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionImpact {
    Low,
    Medium,
    High,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionKind {
    Git,
    Integrity,
    Maintenance,
    Security,
    Operations,
    Planning,
    Configuration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionSource {
    Git,
    Manifest,
    Capability,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionStatus {
    Open,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttentionSignal {
    pub id: String,
    pub repo_id: String,
    pub repo_name: String,
    pub source: AttentionSource,
    pub source_id: String,
    pub kind: AttentionKind,
    pub status: AttentionStatus,
    pub level: AttentionLevel,
    pub urgency: AttentionUrgency,
    pub impact: AttentionImpact,
    pub summary: String,
    #[serde(default)]
    pub item_id: Option<String>,
    #[serde(default)]
    pub capability_instance_id: Option<String>,
    #[serde(default)]
    pub root: Option<PathBuf>,
    #[serde(default)]
    pub suggested_actions: Vec<String>,
    #[serde(default)]
    pub observed_at_epoch_secs: Option<u64>,
    #[serde(default)]
    pub stale: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityStaleReason {
    DirtyWorktree,
    CommitChanged,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DependencyFreshnessReport {
    pub item_id: String,
    pub findings: Vec<DependencyFreshnessFinding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DependencyFreshnessFinding {
    pub kind: DependencyFreshnessFindingKind,
    pub message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyFreshnessFindingKind {
    OutdatedDependency,
    MissingLockfile,
    GitPinStale,
    CustomWarning,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitCheckRule {
    pub id: String,
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub priority: i32,
    pub effect: CommitCheckRuleEffect,
    pub scope: CommitCheckRuleScope,
    pub matcher: CommitCheckRuleMatcher,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitCheckRuleEffect {
    Block,
    Allow,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommitCheckRuleScope {
    All,
    Repositories { repository_ids: Vec<String> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommitCheckRuleMatcher {
    Regex { pattern: String },
    CommitHash { hash: String },
}

fn default_true() -> bool {
    true
}

fn repo_manifest_schema_version() -> u32 {
    REPO_MANIFEST_SCHEMA_VERSION
}

fn default_action_output_mode() -> ActionOutputMode {
    ActionOutputMode::Text
}

fn capability_registry_schema_version() -> u32 {
    1
}

pub fn capability_catalog() -> &'static [CapabilityDefinition] {
    const ITEM_TYPES: &[&str] = &["cargo", "node", "python", "gradle", "gradle_android"];
    &[
        CapabilityDefinition {
            id: "integrity.build",
            title: "Build",
            family: "integrity",
            requirement_level: CapabilityRequirementLevel::Required,
            requirement_scope: CapabilityRequirementScope::Item,
            item_types: ITEM_TYPES,
            result_schema: "integrity_result_v1",
        },
        CapabilityDefinition {
            id: "integrity.test_build",
            title: "Test Build",
            family: "integrity",
            requirement_level: CapabilityRequirementLevel::Required,
            requirement_scope: CapabilityRequirementScope::Item,
            item_types: ITEM_TYPES,
            result_schema: "integrity_result_v1",
        },
        CapabilityDefinition {
            id: "integrity.tests",
            title: "Tests",
            family: "integrity",
            requirement_level: CapabilityRequirementLevel::Required,
            requirement_scope: CapabilityRequirementScope::Item,
            item_types: ITEM_TYPES,
            result_schema: "integrity_result_v1",
        },
        CapabilityDefinition {
            id: "dependencies.outdated",
            title: "Outdated Dependencies",
            family: "maintenance",
            requirement_level: CapabilityRequirementLevel::Required,
            requirement_scope: CapabilityRequirementScope::Item,
            item_types: ITEM_TYPES,
            result_schema: "findings_result_v1",
        },
        CapabilityDefinition {
            id: "observability.production_logs",
            title: "Production Logs",
            family: "operations",
            requirement_level: CapabilityRequirementLevel::Optional,
            requirement_scope: CapabilityRequirementScope::Repo,
            item_types: &[],
            result_schema: "log_stream_v1",
        },
    ]
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceSummary {
    pub workspace_name: String,
    pub repo_count: usize,
    pub manifest_path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryListItem {
    pub id: String,
    pub name: String,
    pub dir_name: String,
    pub remote_url: String,
    pub status: RepositoryStatus,
    pub repo_manifest: Option<RepoManifestScan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoManifestScan {
    pub path: PathBuf,
    pub state: RepoManifestScanState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepoManifestScanState {
    Missing,
    Valid(RepoManifestSummary),
    Invalid { message: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoManifestSummary {
    pub item_count: usize,
    pub item_types: Vec<String>,
    pub supported_actions: Vec<StandardActionName>,
    pub action_count: usize,
    pub capability_count: usize,
    pub missing_required_capability_count: usize,
    pub unsupported_capability_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryStatus {
    pub state: RepositoryState,
    pub branch: Option<String>,
    pub sync: RepositorySync,
    pub head_commit: Option<String>,
    pub repo_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryDetails {
    pub remotes: Vec<String>,
    pub last_commit: Option<LastCommitInfo>,
    pub changed_files: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastCommitInfo {
    pub short_sha: String,
    pub subject: String,
    pub committed_at_epoch_secs: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryMatch {
    pub repository_id: Option<String>,
    pub repository_name: String,
    pub head_offset: usize,
    pub commit_hash: String,
    pub subject: String,
    pub matching_lines: Vec<String>,
    pub rule_id: String,
    pub rule_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitCheckReport {
    pub matches: Vec<HistoryMatch>,
    pub invalid_rules: Vec<InvalidCommitCheckRule>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidCommitCheckRule {
    pub rule_id: String,
    pub rule_name: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineStatsRow {
    pub repository_name: String,
    pub additions: usize,
    pub deletions: usize,
    pub net: isize,
    pub missing: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceLineStats {
    pub rows: Vec<LineStatsRow>,
    pub total_additions: usize,
    pub total_deletions: usize,
    pub total_net: isize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepositoryState {
    Unknown,
    Missing,
    Clean,
    Dirty,
    Untracked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositorySync {
    Unknown,
    NoUpstream,
    UpToDate,
    Ahead(usize),
    Behind(usize),
    Diverged { ahead: usize, behind: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    CloneMissing,
    Pull,
    Push,
    PushForce,
    ApplyHooks,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationEventKind {
    Started,
    Success,
    Skipped,
    Failed,
    Finished,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationEvent {
    pub kind: OperationEventKind,
    pub repository_id: Option<String>,
    pub repository_name: Option<String>,
    pub message: String,
}

#[derive(Debug)]
pub enum WorkspaceError {
    Io(io::Error),
    Json(serde_json::Error),
    InvalidRepoUrl(String),
    UnsupportedRepoManifestSchemaVersion(u32),
    InvalidRepoManifest(String),
}

impl fmt::Display for WorkspaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Json(error) => write!(f, "{error}"),
            Self::InvalidRepoUrl(url) => write!(f, "invalid repository url: {url}"),
            Self::UnsupportedRepoManifestSchemaVersion(version) => {
                write!(f, "unsupported repo manifest schema version: {version}")
            }
            Self::InvalidRepoManifest(message) => write!(f, "invalid repo manifest: {message}"),
        }
    }
}

impl std::error::Error for WorkspaceError {}

impl From<io::Error> for WorkspaceError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for WorkspaceError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

pub fn default_manifest_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(MANIFEST_FILE_NAME)
}

pub fn default_repo_manifest_path(repo_root: &Path) -> PathBuf {
    repo_root.join(REPO_MANIFEST_FILE_NAME)
}

pub fn normalize_workspace_root(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    let text = path.to_string_lossy();

    if text == "~" {
        return env_home_dir().unwrap_or_else(|| path.to_path_buf());
    }

    if let Some(stripped) = text.strip_prefix("~/") {
        if let Some(home) = env_home_dir() {
            return home.join(stripped);
        }
    }

    path.to_path_buf()
}

pub fn load_manifest(path: &Path) -> Result<WorkspaceManifest, WorkspaceError> {
    let content = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&content)?)
}

pub fn save_manifest(path: &Path, manifest: &WorkspaceManifest) -> Result<(), WorkspaceError> {
    let content = serde_json::to_string_pretty(manifest)?;
    fs::write(path, content)?;
    Ok(())
}

pub fn load_repo_manifest(path: &Path) -> Result<RepoManifest, WorkspaceError> {
    let content = fs::read_to_string(path)?;
    let manifest: RepoManifest = serde_json::from_str(&content)?;
    validate_repo_manifest(&manifest)?;
    Ok(manifest)
}

pub fn save_repo_manifest(path: &Path, manifest: &RepoManifest) -> Result<(), WorkspaceError> {
    validate_repo_manifest(manifest)?;
    let content = serde_json::to_string_pretty(manifest)?;
    fs::write(path, content)?;
    Ok(())
}

fn validate_node_id(id: &str, label: &str) -> Result<(), WorkspaceError> {
    if id.trim().is_empty() {
        return Err(WorkspaceError::InvalidRepoManifest(format!(
            "{label} id cannot be empty"
        )));
    }
    if id.starts_with('/') || id.ends_with('/') {
        return Err(WorkspaceError::InvalidRepoManifest(format!(
            "{label} id {id} must not start or end with /"
        )));
    }
    if id
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(WorkspaceError::InvalidRepoManifest(format!(
            "{label} id {id} contains an invalid path segment"
        )));
    }
    Ok(())
}

fn standard_action_name(name: &str) -> Option<StandardActionName> {
    standard_action_parts(name).map(|(_, action)| action)
}

fn standard_action_parts(name: &str) -> Option<(&str, StandardActionName)> {
    let (item_type, action_name) = name.split_once('.')?;
    if !matches!(
        item_type,
        "cargo" | "node" | "python" | "gradle" | "gradle_android"
    ) {
        return None;
    }
    let action = match action_name {
        "build" => StandardActionName::Build,
        "test" | "tests" => StandardActionName::Test,
        "clean" => StandardActionName::Clean,
        "dependencies_outdated" | "verify_dependencies_freshness" => {
            StandardActionName::VerifyDependenciesFreshness
        }
        "deploy" => StandardActionName::Deploy,
        "list_artifacts" => StandardActionName::ListArtifacts,
        _ => return None,
    };
    Some((item_type, action))
}

fn validate_repo_action_ref(
    action_ref: &RepoActionRef,
    action_ids: &BTreeSet<&str>,
    scope: &str,
) -> Result<(), WorkspaceError> {
    match action_ref {
        RepoActionRef::Standard { name, .. } => {
            if standard_action_name(name).is_none() {
                return Err(WorkspaceError::InvalidRepoManifest(format!(
                    "{scope} references unknown standard action {name}"
                )));
            }
        }
        RepoActionRef::RepoAction { id } => {
            if !action_ids.contains(id.as_str()) {
                return Err(WorkspaceError::InvalidRepoManifest(format!(
                    "{scope} references unknown repo action {id}"
                )));
            }
        }
        RepoActionRef::Group { items, .. } => {
            if items.is_empty() {
                return Err(WorkspaceError::InvalidRepoManifest(format!(
                    "{scope} action group must contain at least one item"
                )));
            }
            for item in items {
                validate_repo_action_ref(item, action_ids, scope)?;
            }
        }
    }
    Ok(())
}
pub fn validate_repo_manifest(manifest: &RepoManifest) -> Result<(), WorkspaceError> {
    if !matches!(manifest.schema_version, 1 | REPO_MANIFEST_SCHEMA_VERSION) {
        return Err(WorkspaceError::UnsupportedRepoManifestSchemaVersion(
            manifest.schema_version,
        ));
    }
    if manifest.schema_version == 1
        && (!manifest.actions.is_empty() || !manifest.capabilities.is_empty())
    {
        return Err(WorkspaceError::InvalidRepoManifest(
            "actions and capabilities require schema_version 2".to_string(),
        ));
    }

    let mut seen_node_ids = BTreeSet::new();
    let mut seen_item_ids = BTreeSet::new();
    for item in &manifest.items {
        validate_node_id(&item.id, "item")?;
        if !seen_item_ids.insert(item.id.clone()) || !seen_node_ids.insert(item.id.clone()) {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "duplicate item id: {}",
                item.id
            )));
        }
        if item.item_type.trim().is_empty() {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "item {} has an empty type",
                item.id
            )));
        }
        validate_action_commands(&item.actions, &format!("item {}", item.id))?;
    }

    validate_action_commands(&manifest.repo_actions, "repo")?;

    let mut seen_action_ids = BTreeSet::new();
    for action in &manifest.actions {
        validate_node_id(&action.id, "action")?;
        if !seen_action_ids.insert(action.id.as_str()) {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "duplicate action id: {}",
                action.id
            )));
        }
        if !seen_node_ids.insert(action.id.clone()) {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "action id {} collides with another repo node",
                action.id
            )));
        }
        if action.command.is_empty() || action.command[0].trim().is_empty() {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "action {} command cannot be empty",
                action.id
            )));
        }
    }

    let item_ids = manifest
        .items
        .iter()
        .map(|item| item.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen_capability_ids = BTreeSet::new();
    for capability in &manifest.capabilities {
        validate_node_id(&capability.id, "capability")?;
        if !seen_capability_ids.insert(capability.id.as_str()) {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "duplicate capability id: {}",
                capability.id
            )));
        }
        if capability.capability.trim().is_empty() {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "capability {} has an empty capability name",
                capability.id
            )));
        }
        if let Some(item_id) = &capability.item_id {
            if !item_ids.contains(item_id.as_str()) {
                return Err(WorkspaceError::InvalidRepoManifest(format!(
                    "capability {} references unknown item {}",
                    capability.id, item_id
                )));
            }
        }
        if capability
            .root
            .as_ref()
            .is_some_and(|root| root.as_os_str().is_empty())
        {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "capability {} root cannot be empty",
                capability.id
            )));
        }
        match capability.status {
            CapabilityDeclarationStatus::Implemented => {
                match (&capability.action_ref, &capability.standard_action) {
                    (Some(_), Some(_)) => {
                        return Err(WorkspaceError::InvalidRepoManifest(format!(
                            "capability {} must declare either action_ref or standard_action, not both",
                            capability.id
                        )));
                    }
                    (None, None) => {
                        return Err(WorkspaceError::InvalidRepoManifest(format!(
                            "implemented capability {} must declare action_ref or standard_action",
                            capability.id
                        )));
                    }
                    (Some(action_ref), None) => validate_repo_action_ref(
                        action_ref,
                        &seen_action_ids,
                        &format!("capability {}", capability.id),
                    )?,
                    (None, Some(name)) => {
                        if standard_action_name(name).is_none() {
                            return Err(WorkspaceError::InvalidRepoManifest(format!(
                                "capability {} references unknown standard action {}",
                                capability.id, name
                            )));
                        }
                    }
                }
            }
            CapabilityDeclarationStatus::NotApplicable => {
                if capability
                    .reason
                    .as_ref()
                    .is_none_or(|reason| reason.trim().is_empty())
                {
                    return Err(WorkspaceError::InvalidRepoManifest(format!(
                        "not_applicable capability {} must include a reason",
                        capability.id
                    )));
                }
            }
            CapabilityDeclarationStatus::Unsupported => {}
        }
    }

    let mut seen_aggregations = BTreeSet::new();
    for aggregation in &manifest.aggregation {
        if !seen_aggregations.insert(aggregation.action) {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "duplicate aggregation for action {}",
                action_name_label(aggregation.action)
            )));
        }
        if aggregation.item_ids.is_empty() {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "aggregation for {} must declare at least one item",
                action_name_label(aggregation.action)
            )));
        }
        for item_id in &aggregation.item_ids {
            if !item_ids.contains(item_id.as_str()) {
                return Err(WorkspaceError::InvalidRepoManifest(format!(
                    "aggregation for {} references unknown item {}",
                    action_name_label(aggregation.action),
                    item_id
                )));
            }
        }
    }

    Ok(())
}

fn validate_action_commands(
    commands: &[RepoActionCommand],
    scope: &str,
) -> Result<(), WorkspaceError> {
    let mut seen_actions = BTreeSet::new();
    for command in commands {
        if !seen_actions.insert(command.action) {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "{scope} declares duplicate command for action {}",
                action_name_label(command.action)
            )));
        }
        if command.command.is_empty() || command.command[0].trim().is_empty() {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "{scope} command for action {} cannot be empty",
                action_name_label(command.action)
            )));
        }
    }
    Ok(())
}

pub fn assess_repo_capability_policy(manifest: &RepoManifest) -> RepoCapabilityPolicyReport {
    let mut issues = Vec::new();
    for definition in capability_catalog()
        .iter()
        .filter(|definition| definition.requirement_level == CapabilityRequirementLevel::Required)
    {
        match definition.requirement_scope {
            CapabilityRequirementScope::Repo => {
                assess_capability_scope(
                    manifest,
                    definition,
                    None,
                    None,
                    "repo".to_string(),
                    &mut issues,
                );
            }
            CapabilityRequirementScope::Item => {
                for item in manifest
                    .items
                    .iter()
                    .filter(|item| definition.item_types.contains(&item.item_type.as_str()))
                {
                    assess_capability_scope(
                        manifest,
                        definition,
                        Some(item.id.as_str()),
                        Some(item.path.as_path()),
                        item.id.clone(),
                        &mut issues,
                    );
                }
            }
        }
    }
    RepoCapabilityPolicyReport { issues }
}

fn assess_capability_scope(
    manifest: &RepoManifest,
    definition: &CapabilityDefinition,
    item_id: Option<&str>,
    root: Option<&Path>,
    repo_node_id: String,
    issues: &mut Vec<CapabilityPolicyIssue>,
) {
    let declaration = manifest.capabilities.iter().find(|declaration| {
        declaration.capability == definition.id
            && match item_id {
                Some(item_id) => {
                    declaration.item_id.as_deref() == Some(item_id)
                        || root.is_some_and(|root| declaration.root.as_deref() == Some(root))
                }
                None => declaration.item_id.is_none() && declaration.root.is_none(),
            }
    });

    match declaration.map(|declaration| declaration.status) {
        Some(CapabilityDeclarationStatus::Implemented)
        | Some(CapabilityDeclarationStatus::NotApplicable) => {}
        Some(CapabilityDeclarationStatus::Unsupported) => issues.push(CapabilityPolicyIssue {
            repo_node_id,
            capability: definition.id.to_string(),
            message: format!("required capability {} is unsupported", definition.id),
        }),
        None => issues.push(CapabilityPolicyIssue {
            repo_node_id,
            capability: definition.id.to_string(),
            message: format!("missing required capability {}", definition.id),
        }),
    }
}

pub fn plan_item_action(
    repo_root: &Path,
    manifest: &RepoManifest,
    item_id: &str,
    action: StandardActionName,
) -> Result<RepoActionPlan, WorkspaceError> {
    validate_repo_manifest(manifest)?;

    let item = manifest
        .items
        .iter()
        .find(|item| item.id == item_id)
        .ok_or_else(|| {
            WorkspaceError::InvalidRepoManifest(format!("unknown item id: {item_id}"))
        })?;

    let step = plan_item_step(repo_root, item, action)?;
    Ok(RepoActionPlan {
        action,
        execution: AggregationExecutionMode::Sequential,
        failure_policy: AggregationFailurePolicy::FailFast,
        merge: AggregationMergeStrategy::PerItem,
        steps: vec![step],
    })
}

pub fn plan_repo_action(
    repo_root: &Path,
    manifest: &RepoManifest,
    action: StandardActionName,
) -> Result<RepoActionPlan, WorkspaceError> {
    validate_repo_manifest(manifest)?;

    if let Some(repo_action) = manifest
        .repo_actions
        .iter()
        .find(|entry| entry.action == action)
    {
        return Ok(RepoActionPlan {
            action,
            execution: AggregationExecutionMode::Sequential,
            failure_policy: AggregationFailurePolicy::FailFast,
            merge: AggregationMergeStrategy::Combined,
            steps: vec![RepoActionStep {
                item_id: None,
                source: RepoActionSource::RepoCommand,
                executor: RepoActionExecutor::Command(command_from_definition(
                    repo_root,
                    repo_action,
                )),
            }],
        });
    }

    let applicable_items = manifest
        .items
        .iter()
        .filter(|item| item_supports_action(repo_root, item, action))
        .collect::<Vec<_>>();
    if applicable_items.is_empty() {
        return Err(WorkspaceError::InvalidRepoManifest(format!(
            "no items support repo-level action {}",
            action_name_label(action)
        )));
    }

    if applicable_items.len() == 1 {
        return plan_item_action(repo_root, manifest, &applicable_items[0].id, action);
    }

    let aggregation = manifest
        .aggregation
        .iter()
        .find(|aggregation| aggregation.action == action)
        .ok_or_else(|| {
            WorkspaceError::InvalidRepoManifest(format!(
                "repo-level action {} requires explicit aggregation when multiple items are present",
                action_name_label(action)
            ))
        })?;

    let mut steps = Vec::new();
    for item_id in &aggregation.item_ids {
        let item = manifest
            .items
            .iter()
            .find(|entry| &entry.id == item_id)
            .ok_or_else(|| {
                WorkspaceError::InvalidRepoManifest(format!(
                    "aggregation for {} references unknown item {}",
                    action_name_label(action),
                    item_id
                ))
            })?;
        steps.push(plan_item_step(repo_root, item, action)?);
    }

    Ok(RepoActionPlan {
        action,
        execution: aggregation.execution,
        failure_policy: aggregation.failure_policy,
        merge: aggregation.merge,
        steps,
    })
}

pub fn plan_capability_action(
    repo_root: &Path,
    manifest: &RepoManifest,
    capability_instance_id: &str,
) -> Result<CapabilityActionPlan, WorkspaceError> {
    validate_repo_manifest(manifest)?;
    let capability = manifest
        .capabilities
        .iter()
        .find(|capability| capability.id == capability_instance_id)
        .ok_or_else(|| {
            WorkspaceError::InvalidRepoManifest(format!(
                "unknown capability id: {capability_instance_id}"
            ))
        })?;

    if capability.status != CapabilityDeclarationStatus::Implemented {
        return Err(WorkspaceError::InvalidRepoManifest(format!(
            "capability {} is not implemented",
            capability.id
        )));
    }

    let action_ref = capability_action_ref(capability)?;
    let mut plan = CapabilityActionPlan {
        capability_instance_id: capability.id.clone(),
        capability: capability.capability.clone(),
        execution: AggregationExecutionMode::Sequential,
        failure_policy: AggregationFailurePolicy::FailFast,
        merge: AggregationMergeStrategy::Combined,
        steps: Vec::new(),
    };
    add_capability_action_ref_steps(repo_root, manifest, capability, &action_ref, &mut plan)?;
    Ok(plan)
}

fn capability_action_ref(
    capability: &RepoCapabilityDeclaration,
) -> Result<RepoActionRef, WorkspaceError> {
    match (&capability.action_ref, &capability.standard_action) {
        (Some(action_ref), None) => Ok(action_ref.clone()),
        (None, Some(name)) => Ok(RepoActionRef::Standard {
            name: name.clone(),
            item_id: capability.item_id.clone(),
            root: capability.root.clone(),
        }),
        _ => Err(WorkspaceError::InvalidRepoManifest(format!(
            "implemented capability {} must declare exactly one action reference",
            capability.id
        ))),
    }
}

fn add_capability_action_ref_steps(
    repo_root: &Path,
    manifest: &RepoManifest,
    capability: &RepoCapabilityDeclaration,
    action_ref: &RepoActionRef,
    plan: &mut CapabilityActionPlan,
) -> Result<(), WorkspaceError> {
    match action_ref {
        RepoActionRef::Standard {
            name,
            item_id,
            root,
        } => {
            let (standard_item_type, action) = standard_action_parts(name).ok_or_else(|| {
                WorkspaceError::InvalidRepoManifest(format!(
                    "capability {} references unknown standard action {}",
                    capability.id, name
                ))
            })?;
            let item = resolve_capability_item(
                manifest,
                item_id.as_deref().or(capability.item_id.as_deref()),
                root.as_deref().or(capability.root.as_deref()),
                &capability.id,
            )?;
            if item.item_type != standard_item_type {
                return Err(WorkspaceError::InvalidRepoManifest(format!(
                    "capability {} standard action {} expects item type {}, got {}",
                    capability.id, name, standard_item_type, item.item_type
                )));
            }
            plan.steps.push(plan_item_step(repo_root, item, action)?);
        }
        RepoActionRef::RepoAction { id } => {
            let action = manifest
                .actions
                .iter()
                .find(|action| &action.id == id)
                .ok_or_else(|| {
                    WorkspaceError::InvalidRepoManifest(format!(
                        "capability {} references unknown repo action {}",
                        capability.id, id
                    ))
                })?;
            plan.steps.push(RepoActionStep {
                item_id: capability.item_id.clone(),
                source: RepoActionSource::RepoCommand,
                executor: RepoActionExecutor::Command(command_from_repo_action_definition(
                    repo_root, action,
                )),
            });
        }
        RepoActionRef::Group {
            execution,
            failure_policy,
            merge,
            items,
        } => {
            plan.execution = *execution;
            plan.failure_policy = *failure_policy;
            plan.merge = *merge;
            for item in items {
                add_capability_action_ref_steps(repo_root, manifest, capability, item, plan)?;
            }
        }
    }
    Ok(())
}

fn resolve_capability_item<'a>(
    manifest: &'a RepoManifest,
    item_id: Option<&str>,
    root: Option<&Path>,
    capability_id: &str,
) -> Result<&'a RepoItem, WorkspaceError> {
    if let Some(item_id) = item_id {
        return manifest
            .items
            .iter()
            .find(|item| item.id == item_id)
            .ok_or_else(|| {
                WorkspaceError::InvalidRepoManifest(format!(
                    "capability {capability_id} references unknown item {item_id}"
                ))
            });
    }
    if let Some(root) = root {
        let matches = manifest
            .items
            .iter()
            .filter(|item| item.path == root)
            .collect::<Vec<_>>();
        return match matches.as_slice() {
            [item] => Ok(item),
            [] => Err(WorkspaceError::InvalidRepoManifest(format!(
                "capability {capability_id} root {} does not match a repo item",
                root.display()
            ))),
            _ => Err(WorkspaceError::InvalidRepoManifest(format!(
                "capability {capability_id} root {} matches multiple repo items",
                root.display()
            ))),
        };
    }
    Err(WorkspaceError::InvalidRepoManifest(format!(
        "capability {capability_id} standard action requires item_id or root"
    )))
}

pub fn default_capability_registry_path(workspace_root: &Path) -> PathBuf {
    workspace_root
        .join(".ronomepo")
        .join("capability-state.json")
}

pub fn load_capability_registry(path: &Path) -> Result<CapabilityRegistry, WorkspaceError> {
    if !path.exists() {
        return Ok(CapabilityRegistry::default());
    }
    let content = fs::read_to_string(path)?;
    let registry = serde_json::from_str(&content)?;
    Ok(registry)
}

pub fn save_capability_registry(
    path: &Path,
    registry: &CapabilityRegistry,
) -> Result<(), WorkspaceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string_pretty(registry)?;
    fs::write(path, content)?;
    Ok(())
}

pub fn upsert_capability_state(registry: &mut CapabilityRegistry, state: CapabilityState) {
    if let Some(existing) = registry.states.iter_mut().find(|existing| {
        existing.repo_id == state.repo_id
            && existing.capability_instance_id == state.capability_instance_id
    }) {
        *existing = state;
    } else {
        registry.states.push(state);
    }
}

pub fn parse_capability_result_json(raw: &str) -> Result<CapabilityResult, WorkspaceError> {
    Ok(serde_json::from_str(raw)?)
}

pub fn text_capability_result(success: bool, summary: impl Into<String>) -> CapabilityResult {
    CapabilityResult {
        status: if success {
            CapabilityResultStatus::Ok
        } else {
            CapabilityResultStatus::Failed
        },
        summary: summary.into(),
        findings: Vec::new(),
    }
}

pub fn collect_capability_git_snapshot(repo_path: &Path) -> CapabilityGitSnapshot {
    CapabilityGitSnapshot {
        commit: git_stdout(repo_path, ["rev-parse", "HEAD"]),
        dirty: !matches!(repository_state(repo_path), RepositoryState::Clean),
    }
}

pub fn derive_attention_signals(
    item: &RepositoryListItem,
    manifest: Option<&RepoManifest>,
    registry: &CapabilityRegistry,
) -> Vec<AttentionSignal> {
    let mut signals = Vec::new();
    signals.extend(derive_git_attention_signals(item));
    signals.extend(derive_manifest_attention_signals(item, manifest));
    signals.extend(derive_capability_attention_signals(
        item, manifest, registry,
    ));
    signals
}

pub fn capability_stale_reason(
    item: &RepositoryListItem,
    state: &CapabilityState,
) -> Option<CapabilityStaleReason> {
    if matches!(
        item.status.state,
        RepositoryState::Dirty | RepositoryState::Untracked
    ) {
        return Some(CapabilityStaleReason::DirtyWorktree);
    }

    state
        .commit
        .as_ref()
        .zip(item.status.head_commit.as_ref())
        .and_then(|(recorded, current)| {
            (recorded != current).then_some(CapabilityStaleReason::CommitChanged)
        })
}

pub fn capability_effective_status(
    item: &RepositoryListItem,
    state: &CapabilityState,
) -> CapabilityResultStatus {
    if capability_stale_reason(item, state).is_some() {
        CapabilityResultStatus::Stale
    } else {
        state.status
    }
}

pub fn attention_rank(signals: &[AttentionSignal]) -> u8 {
    if signals
        .iter()
        .any(|signal| signal.level == AttentionLevel::Blocked)
    {
        0
    } else if signals
        .iter()
        .any(|signal| signal.level == AttentionLevel::Urgent)
    {
        1
    } else if signals
        .iter()
        .any(|signal| signal.level == AttentionLevel::ShouldDo)
    {
        2
    } else if signals
        .iter()
        .any(|signal| signal.level == AttentionLevel::Opportunity)
    {
        3
    } else if signals
        .iter()
        .any(|signal| signal.level == AttentionLevel::Info)
    {
        4
    } else {
        5
    }
}

fn derive_git_attention_signals(item: &RepositoryListItem) -> Vec<AttentionSignal> {
    let mut signals = Vec::new();
    match item.status.state {
        RepositoryState::Missing => signals.push(attention_signal(
            item,
            AttentionSource::Git,
            "missing",
            AttentionKind::Git,
            AttentionLevel::Blocked,
            AttentionUrgency::High,
            AttentionImpact::High,
            "repository is missing locally".to_string(),
        )),
        RepositoryState::Unknown => signals.push(attention_signal(
            item,
            AttentionSource::Git,
            "state_unknown",
            AttentionKind::Git,
            AttentionLevel::Urgent,
            AttentionUrgency::Medium,
            AttentionImpact::Medium,
            "repository state could not be determined".to_string(),
        )),
        RepositoryState::Dirty => signals.push(attention_signal(
            item,
            AttentionSource::Git,
            "dirty",
            AttentionKind::Git,
            AttentionLevel::Opportunity,
            AttentionUrgency::Low,
            AttentionImpact::Medium,
            "working tree has modified files".to_string(),
        )),
        RepositoryState::Untracked => signals.push(attention_signal(
            item,
            AttentionSource::Git,
            "untracked",
            AttentionKind::Git,
            AttentionLevel::Opportunity,
            AttentionUrgency::Low,
            AttentionImpact::Medium,
            "working tree has untracked files".to_string(),
        )),
        RepositoryState::Clean => {}
    }

    match item.status.sync {
        RepositorySync::Unknown => signals.push(attention_signal(
            item,
            AttentionSource::Git,
            "sync_unknown",
            AttentionKind::Git,
            AttentionLevel::Urgent,
            AttentionUrgency::Medium,
            AttentionImpact::Medium,
            "repository sync state could not be determined".to_string(),
        )),
        RepositorySync::NoUpstream => signals.push(attention_signal(
            item,
            AttentionSource::Git,
            "no_upstream",
            AttentionKind::Git,
            AttentionLevel::ShouldDo,
            AttentionUrgency::Medium,
            AttentionImpact::Medium,
            "branch has no upstream configured".to_string(),
        )),
        RepositorySync::Ahead(count) => signals.push(attention_signal(
            item,
            AttentionSource::Git,
            "ahead",
            AttentionKind::Git,
            AttentionLevel::Opportunity,
            AttentionUrgency::Low,
            AttentionImpact::Medium,
            format!("branch is ahead by {count} commit(s)"),
        )),
        RepositorySync::Behind(count) => signals.push(attention_signal(
            item,
            AttentionSource::Git,
            "behind",
            AttentionKind::Git,
            AttentionLevel::ShouldDo,
            AttentionUrgency::Medium,
            AttentionImpact::Medium,
            format!("branch is behind by {count} commit(s)"),
        )),
        RepositorySync::Diverged { ahead, behind } => signals.push(attention_signal(
            item,
            AttentionSource::Git,
            "diverged",
            AttentionKind::Git,
            AttentionLevel::Urgent,
            AttentionUrgency::High,
            AttentionImpact::High,
            format!("branch diverged: {ahead} ahead, {behind} behind"),
        )),
        RepositorySync::UpToDate => {}
    }
    signals
}

fn derive_manifest_attention_signals(
    item: &RepositoryListItem,
    manifest: Option<&RepoManifest>,
) -> Vec<AttentionSignal> {
    let Some(scan) = &item.repo_manifest else {
        return Vec::new();
    };

    match &scan.state {
        RepoManifestScanState::Missing => vec![attention_signal(
            item,
            AttentionSource::Manifest,
            "missing",
            AttentionKind::Configuration,
            AttentionLevel::ShouldDo,
            AttentionUrgency::Medium,
            AttentionImpact::Medium,
            format!("missing {REPO_MANIFEST_FILE_NAME}"),
        )],
        RepoManifestScanState::Invalid { message } => vec![attention_signal(
            item,
            AttentionSource::Manifest,
            "invalid",
            AttentionKind::Configuration,
            AttentionLevel::Blocked,
            AttentionUrgency::High,
            AttentionImpact::High,
            format!("repo manifest is invalid: {message}"),
        )],
        RepoManifestScanState::Valid(summary) => {
            if let Some(manifest) = manifest {
                assess_repo_capability_policy(manifest)
                    .issues
                    .into_iter()
                    .map(|issue| {
                        let mut signal = attention_signal(
                            item,
                            AttentionSource::Manifest,
                            &format!("policy:{}:{}", issue.repo_node_id, issue.capability),
                            attention_kind_for_capability(&issue.capability),
                            AttentionLevel::ShouldDo,
                            AttentionUrgency::Medium,
                            AttentionImpact::Medium,
                            issue.message,
                        );
                        signal.item_id = Some(issue.repo_node_id);
                        signal.suggested_actions = vec!["update_repo_manifest".to_string()];
                        signal
                    })
                    .collect()
            } else if summary.missing_required_capability_count > 0 {
                let mut signal = attention_signal(
                    item,
                    AttentionSource::Manifest,
                    "policy",
                    AttentionKind::Configuration,
                    AttentionLevel::ShouldDo,
                    AttentionUrgency::Medium,
                    AttentionImpact::Medium,
                    format!(
                        "{} required capability declaration(s) need attention",
                        summary.missing_required_capability_count
                    ),
                );
                signal.suggested_actions = vec!["update_repo_manifest".to_string()];
                vec![signal]
            } else {
                Vec::new()
            }
        }
    }
}

fn derive_capability_attention_signals(
    item: &RepositoryListItem,
    manifest: Option<&RepoManifest>,
    registry: &CapabilityRegistry,
) -> Vec<AttentionSignal> {
    let states = registry
        .states
        .iter()
        .filter(|state| state.repo_id == item.id)
        .collect::<Vec<_>>();
    let mut signals = states
        .iter()
        .filter_map(|state| capability_state_attention_signal(item, state))
        .collect::<Vec<_>>();

    if let Some(manifest) = manifest {
        let recorded_ids = states
            .iter()
            .map(|state| state.capability_instance_id.as_str())
            .collect::<BTreeSet<_>>();
        for capability in manifest.capabilities.iter().filter(|capability| {
            capability.status == CapabilityDeclarationStatus::Implemented
                && !recorded_ids.contains(capability.id.as_str())
        }) {
            let mut signal = attention_signal(
                item,
                AttentionSource::Capability,
                &capability.capability,
                attention_kind_for_capability(&capability.capability),
                AttentionLevel::ShouldDo,
                AttentionUrgency::Medium,
                AttentionImpact::Medium,
                format!("{} has no recorded result", capability.capability),
            );
            signal.id = format!("{}:{}:unknown", item.id, capability.id);
            signal.item_id = capability.item_id.clone();
            signal.capability_instance_id = Some(capability.id.clone());
            signal.root = capability.root.clone();
            signal.suggested_actions = vec!["run_capability_check".to_string()];
            signals.push(signal);
        }
    }

    signals
}

fn capability_state_attention_signal(
    item: &RepositoryListItem,
    state: &CapabilityState,
) -> Option<AttentionSignal> {
    let stale_reason = capability_stale_reason(item, state);
    let status = if stale_reason.is_some() {
        CapabilityResultStatus::Stale
    } else {
        state.status
    };

    let (level, urgency, impact, summary, stale) = match status {
        CapabilityResultStatus::Ok | CapabilityResultStatus::Running => return None,
        CapabilityResultStatus::Warning => (
            AttentionLevel::ShouldDo,
            AttentionUrgency::Medium,
            AttentionImpact::Medium,
            state.summary.clone(),
            false,
        ),
        CapabilityResultStatus::Failed => (
            AttentionLevel::Urgent,
            AttentionUrgency::High,
            AttentionImpact::High,
            state.summary.clone(),
            false,
        ),
        CapabilityResultStatus::Error => (
            AttentionLevel::Urgent,
            AttentionUrgency::High,
            AttentionImpact::Medium,
            state.summary.clone(),
            false,
        ),
        CapabilityResultStatus::Unknown => (
            AttentionLevel::ShouldDo,
            AttentionUrgency::Medium,
            AttentionImpact::Medium,
            state.summary.clone(),
            false,
        ),
        CapabilityResultStatus::Stale => {
            let reason = match stale_reason {
                Some(CapabilityStaleReason::DirtyWorktree) => "dirty worktree",
                Some(CapabilityStaleReason::CommitChanged) => "commit changed",
                None => "stale result",
            };
            (
                AttentionLevel::ShouldDo,
                AttentionUrgency::Medium,
                AttentionImpact::Medium,
                format!("{} (stale: {reason})", state.summary),
                true,
            )
        }
    };

    let mut signal = attention_signal(
        item,
        AttentionSource::Capability,
        &state.capability,
        attention_kind_for_capability(&state.capability),
        level,
        urgency,
        impact,
        summary,
    );
    signal.id = format!("{}:{}", item.id, state.capability_instance_id);
    signal.item_id = state.item_id.clone();
    signal.capability_instance_id = Some(state.capability_instance_id.clone());
    signal.root = state.root.clone();
    signal.observed_at_epoch_secs = state.checked_at_epoch_secs;
    signal.stale = stale;
    signal.suggested_actions = capability_suggested_actions(&state.capability, status);
    Some(signal)
}

fn attention_signal(
    item: &RepositoryListItem,
    source: AttentionSource,
    source_id: &str,
    kind: AttentionKind,
    level: AttentionLevel,
    urgency: AttentionUrgency,
    impact: AttentionImpact,
    summary: String,
) -> AttentionSignal {
    AttentionSignal {
        id: format!("{}:{source_id}", item.id),
        repo_id: item.id.clone(),
        repo_name: item.name.clone(),
        source,
        source_id: source_id.to_string(),
        kind,
        status: AttentionStatus::Open,
        level,
        urgency,
        impact,
        summary,
        item_id: None,
        capability_instance_id: None,
        root: None,
        suggested_actions: Vec::new(),
        observed_at_epoch_secs: None,
        stale: false,
    }
}

fn attention_kind_for_capability(capability: &str) -> AttentionKind {
    if capability.starts_with("integrity.") {
        AttentionKind::Integrity
    } else if capability.starts_with("dependencies.") {
        AttentionKind::Maintenance
    } else if capability.contains("security") {
        AttentionKind::Security
    } else if capability.starts_with("observability.") {
        AttentionKind::Operations
    } else {
        AttentionKind::Operations
    }
}

fn capability_suggested_actions(capability: &str, status: CapabilityResultStatus) -> Vec<String> {
    match status {
        CapabilityResultStatus::Stale | CapabilityResultStatus::Unknown => {
            vec!["run_capability_check".to_string()]
        }
        _ if capability == "dependencies.outdated" => vec!["update_dependencies".to_string()],
        _ if capability == "integrity.build" => vec!["fix_build".to_string()],
        _ if capability == "integrity.tests" => vec!["fix_tests".to_string()],
        _ if capability == "integrity.test_build" => vec!["fix_test_build".to_string()],
        _ => vec!["inspect_capability".to_string()],
    }
}

pub fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn plan_item_step(
    repo_root: &Path,
    item: &RepoItem,
    action: StandardActionName,
) -> Result<RepoActionStep, WorkspaceError> {
    if let Some(override_command) = item.actions.iter().find(|entry| entry.action == action) {
        return Ok(RepoActionStep {
            item_id: Some(item.id.clone()),
            source: RepoActionSource::ItemOverride,
            executor: RepoActionExecutor::Command(command_from_definition(
                &resolve_command_base(repo_root, item),
                override_command,
            )),
        });
    }

    if supports_built_in_inspector(action) {
        if built_in_supported_actions(repo_root, item).is_empty() {
            return Err(WorkspaceError::InvalidRepoManifest(format!(
                "item {} does not support action {}",
                item.id,
                action_name_label(action)
            )));
        }
        return Ok(RepoActionStep {
            item_id: Some(item.id.clone()),
            source: RepoActionSource::BuiltInHandler {
                item_type: item.item_type.clone(),
            },
            executor: RepoActionExecutor::BuiltInInspector,
        });
    }

    let command = built_in_command(repo_root, item, action).ok_or_else(|| {
        WorkspaceError::InvalidRepoManifest(format!(
            "item {} does not support action {}",
            item.id,
            action_name_label(action)
        ))
    })?;

    Ok(RepoActionStep {
        item_id: Some(item.id.clone()),
        source: RepoActionSource::BuiltInHandler {
            item_type: item.item_type.clone(),
        },
        executor: RepoActionExecutor::Command(command),
    })
}

fn item_supports_action(repo_root: &Path, item: &RepoItem, action: StandardActionName) -> bool {
    item.actions.iter().any(|entry| entry.action == action)
        || built_in_supported_actions(repo_root, item).contains(&action)
}

fn supports_built_in_inspector(action: StandardActionName) -> bool {
    matches!(
        action,
        StandardActionName::ListArtifacts | StandardActionName::VerifyDependenciesFreshness
    )
}

fn built_in_supported_actions(repo_root: &Path, item: &RepoItem) -> Vec<StandardActionName> {
    const DEFAULT_ACTIONS: [StandardActionName; 5] = [
        StandardActionName::ListArtifacts,
        StandardActionName::Build,
        StandardActionName::Test,
        StandardActionName::Clean,
        StandardActionName::VerifyDependenciesFreshness,
    ];
    const DEPLOYABLE_ACTIONS: [StandardActionName; 6] = [
        StandardActionName::ListArtifacts,
        StandardActionName::Build,
        StandardActionName::Test,
        StandardActionName::Clean,
        StandardActionName::VerifyDependenciesFreshness,
        StandardActionName::Deploy,
    ];

    match item.item_type.as_str() {
        "cargo" | "gradle" | "gradle_android" => DEFAULT_ACTIONS.to_vec(),
        "python" => DEPLOYABLE_ACTIONS.to_vec(),
        "node" => node_supported_actions(repo_root, item),
        _ => Vec::new(),
    }
}

fn built_in_command(
    repo_root: &Path,
    item: &RepoItem,
    action: StandardActionName,
) -> Option<PlannedCommand> {
    match item.item_type.as_str() {
        "cargo" => cargo_command(repo_root, item, action),
        "gradle" => gradle_command(repo_root, item, action),
        "gradle_android" => gradle_android_command(repo_root, item, action),
        "node" => node_command(repo_root, item, action),
        "python" => python_command(repo_root, item, action),
        _ => None,
    }
}

fn cargo_command(
    repo_root: &Path,
    item: &RepoItem,
    action: StandardActionName,
) -> Option<PlannedCommand> {
    let manifest_path = cargo_manifest_path(repo_root, item);
    let workdir = manifest_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| resolve_item_path(repo_root, item));
    let args = match action {
        StandardActionName::Build => vec![
            "build".to_string(),
            "--manifest-path".to_string(),
            manifest_path.to_string_lossy().to_string(),
        ],
        StandardActionName::Test => vec![
            "test".to_string(),
            "--manifest-path".to_string(),
            manifest_path.to_string_lossy().to_string(),
        ],
        StandardActionName::Clean => vec![
            "clean".to_string(),
            "--manifest-path".to_string(),
            manifest_path.to_string_lossy().to_string(),
        ],
        _ => return None,
    };

    Some(PlannedCommand {
        program: "cargo".to_string(),
        args,
        workdir,
        env: BTreeMap::new(),
        timeout_seconds: None,
        output: ActionOutputMode::Text,
    })
}

fn gradle_command(
    repo_root: &Path,
    item: &RepoItem,
    action: StandardActionName,
) -> Option<PlannedCommand> {
    let workdir = resolve_item_path(repo_root, item);
    let args = match action {
        StandardActionName::Build => vec!["build".to_string()],
        StandardActionName::Test => vec!["test".to_string()],
        StandardActionName::Clean => vec!["clean".to_string()],
        _ => return None,
    };

    Some(PlannedCommand {
        program: "./gradlew".to_string(),
        args,
        workdir,
        env: BTreeMap::new(),
        timeout_seconds: None,
        output: ActionOutputMode::Text,
    })
}

fn gradle_android_command(
    repo_root: &Path,
    item: &RepoItem,
    action: StandardActionName,
) -> Option<PlannedCommand> {
    let workdir = resolve_item_path(repo_root, item);
    let args = match action {
        StandardActionName::Build => vec!["assemble".to_string()],
        StandardActionName::Test => vec!["test".to_string()],
        StandardActionName::Clean => vec!["clean".to_string()],
        _ => return None,
    };

    Some(PlannedCommand {
        program: "./gradlew".to_string(),
        args,
        workdir,
        env: BTreeMap::new(),
        timeout_seconds: None,
        output: ActionOutputMode::Text,
    })
}

fn python_command(
    repo_root: &Path,
    item: &RepoItem,
    action: StandardActionName,
) -> Option<PlannedCommand> {
    let workdir = resolve_item_path(repo_root, item);
    let args = match action {
        StandardActionName::Build => vec!["-m".to_string(), "build".to_string()],
        StandardActionName::Test => vec!["-m".to_string(), "pytest".to_string()],
        StandardActionName::Clean => vec![
            "-c".to_string(),
            "import shutil; [shutil.rmtree(path, ignore_errors=True) for path in ('build', 'dist', '.pytest_cache')]".to_string(),
        ],
        StandardActionName::Deploy => vec![
            "-m".to_string(),
            "twine".to_string(),
            "upload".to_string(),
            "dist/*".to_string(),
        ],
        _ => return None,
    };

    Some(PlannedCommand {
        program: "python".to_string(),
        args,
        workdir,
        env: BTreeMap::new(),
        timeout_seconds: None,
        output: ActionOutputMode::Text,
    })
}

fn node_supported_actions(repo_root: &Path, item: &RepoItem) -> Vec<StandardActionName> {
    let mut actions = vec![
        StandardActionName::ListArtifacts,
        StandardActionName::VerifyDependenciesFreshness,
    ];

    for action in [
        StandardActionName::Build,
        StandardActionName::Test,
        StandardActionName::Clean,
        StandardActionName::Deploy,
    ] {
        if node_command(repo_root, item, action).is_some() {
            actions.push(action);
        }
    }

    actions
}

fn node_command(
    repo_root: &Path,
    item: &RepoItem,
    action: StandardActionName,
) -> Option<PlannedCommand> {
    let script = node_script_name(item, action)?;
    if !node_has_script(repo_root, item, &script) {
        return None;
    }

    let workdir = resolve_item_path(repo_root, item);
    let (program, args) = match node_package_manager(item).as_str() {
        "yarn" => ("yarn".to_string(), vec![script]),
        "pnpm" => ("pnpm".to_string(), vec!["run".to_string(), script]),
        _ => ("npm".to_string(), vec!["run".to_string(), script]),
    };

    Some(PlannedCommand {
        program,
        args,
        workdir,
        env: BTreeMap::new(),
        timeout_seconds: None,
        output: ActionOutputMode::Text,
    })
}

fn node_package_manager(item: &RepoItem) -> String {
    item.config
        .as_ref()
        .and_then(|config| config.get("package_manager"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("npm")
        .to_string()
}

fn node_script_name(item: &RepoItem, action: StandardActionName) -> Option<String> {
    let config_key = action_name_label(action);
    if let Some(script) = item
        .config
        .as_ref()
        .and_then(|config| config.get("scripts"))
        .and_then(|scripts| scripts.get(config_key))
        .and_then(serde_json::Value::as_str)
    {
        return Some(script.to_string());
    }

    match action {
        StandardActionName::Build => Some("build".to_string()),
        StandardActionName::Test => Some("test".to_string()),
        StandardActionName::Clean => Some("clean".to_string()),
        StandardActionName::Deploy => Some("deploy".to_string()),
        _ => None,
    }
}

fn node_has_script(repo_root: &Path, item: &RepoItem, script: &str) -> bool {
    let package_json = resolve_item_path(repo_root, item).join("package.json");
    let Ok(content) = fs::read_to_string(package_json) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&content) else {
        return false;
    };

    value
        .get("scripts")
        .and_then(|scripts| scripts.get(script))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|script_body| !script_body.trim().is_empty())
}

fn cargo_manifest_path(repo_root: &Path, item: &RepoItem) -> PathBuf {
    let item_path = resolve_item_path(repo_root, item);
    if item_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "Cargo.toml")
    {
        item_path
    } else {
        item_path.join("Cargo.toml")
    }
}

fn resolve_item_path(repo_root: &Path, item: &RepoItem) -> PathBuf {
    if item.path.is_absolute() {
        item.path.clone()
    } else {
        repo_root.join(&item.path)
    }
}

fn resolve_command_base(repo_root: &Path, item: &RepoItem) -> PathBuf {
    resolve_item_path(repo_root, item)
}

fn command_from_definition(base: &Path, action: &RepoActionCommand) -> PlannedCommand {
    let program = action.command[0].clone();
    let args = action.command.iter().skip(1).cloned().collect();
    let workdir = action
        .workdir
        .as_ref()
        .map(|path| {
            if path.is_absolute() {
                path.clone()
            } else {
                base.join(path)
            }
        })
        .unwrap_or_else(|| base.to_path_buf());

    PlannedCommand {
        program,
        args,
        workdir,
        env: action.env.clone(),
        timeout_seconds: action.timeout_seconds,
        output: action.output,
    }
}

fn command_from_repo_action_definition(
    repo_root: &Path,
    action: &RepoActionDefinition,
) -> PlannedCommand {
    let program = action.command[0].clone();
    let args = action.command.iter().skip(1).cloned().collect();
    let workdir = action
        .workdir
        .as_ref()
        .map(|path| {
            if path.is_absolute() {
                path.clone()
            } else {
                repo_root.join(path)
            }
        })
        .unwrap_or_else(|| repo_root.to_path_buf());

    PlannedCommand {
        program,
        args,
        workdir,
        env: action.env.clone(),
        timeout_seconds: action.timeout_seconds,
        output: action.output,
    }
}

pub fn list_item_artifacts(
    repo_root: &Path,
    manifest: &RepoManifest,
    item_id: &str,
) -> Result<Vec<ListedArtifact>, WorkspaceError> {
    validate_repo_manifest(manifest)?;

    let item = manifest
        .items
        .iter()
        .find(|entry| entry.id == item_id)
        .ok_or_else(|| {
            WorkspaceError::InvalidRepoManifest(format!("unknown item id: {item_id}"))
        })?;

    if !item_supports_action(repo_root, item, StandardActionName::ListArtifacts) {
        return Err(WorkspaceError::InvalidRepoManifest(format!(
            "item {} does not support action {}",
            item.id,
            action_name_label(StandardActionName::ListArtifacts)
        )));
    }

    let item_root = resolve_item_path(repo_root, item);
    let mut artifacts = item
        .artifacts
        .iter()
        .map(|artifact| ListedArtifact {
            item_id: item.id.clone(),
            name: artifact.name.clone(),
            kind: artifact.kind.clone(),
            path: artifact.path.as_ref().map(|path| item_root.join(path)),
            pattern: artifact.pattern.clone(),
            build_action: artifact.build_action,
        })
        .collect::<Vec<_>>();
    artifacts.extend(default_artifacts_for_item(item, &item_root));
    Ok(artifacts)
}

pub fn list_repo_artifacts(
    repo_root: &Path,
    manifest: &RepoManifest,
) -> Result<Vec<ListedArtifact>, WorkspaceError> {
    let plan = plan_repo_action(repo_root, manifest, StandardActionName::ListArtifacts)?;
    let mut artifacts = Vec::new();
    for step in &plan.steps {
        let Some(item_id) = step.item_id.as_deref() else {
            continue;
        };
        artifacts.extend(list_item_artifacts(repo_root, manifest, item_id)?);
    }
    Ok(artifacts)
}

fn default_artifacts_for_item(item: &RepoItem, item_root: &Path) -> Vec<ListedArtifact> {
    let defaults = match item.item_type.as_str() {
        "cargo" => vec![
            (
                "cargo-debug",
                "binary",
                Some(item_root.join("target/debug")),
                Some("target/debug/*".to_string()),
            ),
            (
                "cargo-release",
                "binary",
                Some(item_root.join("target/release")),
                Some("target/release/*".to_string()),
            ),
        ],
        "gradle" => vec![
            (
                "gradle-libs",
                "archive",
                Some(item_root.join("build/libs")),
                Some("build/libs/*".to_string()),
            ),
            (
                "gradle-distributions",
                "archive",
                Some(item_root.join("build/distributions")),
                Some("build/distributions/*".to_string()),
            ),
        ],
        "gradle_android" => vec![(
            "android-outputs",
            "mobile-package",
            Some(item_root.join("build/outputs")),
            Some("build/outputs/**/*".to_string()),
        )],
        "python" => vec![(
            "python-dist",
            "package",
            Some(item_root.join("dist")),
            Some("dist/*".to_string()),
        )],
        "node" => vec![
            (
                "node-dist",
                "web-bundle",
                Some(item_root.join("dist")),
                Some("dist/*".to_string()),
            ),
            (
                "node-build",
                "web-bundle",
                Some(item_root.join("build")),
                Some("build/*".to_string()),
            ),
        ],
        _ => Vec::new(),
    };

    defaults
        .into_iter()
        .map(|(name, kind, path, pattern)| ListedArtifact {
            item_id: item.id.clone(),
            name: name.to_string(),
            kind: kind.to_string(),
            path,
            pattern,
            build_action: Some(StandardActionName::Build),
        })
        .collect()
}

pub fn verify_item_dependencies_freshness(
    repo_root: &Path,
    manifest: &RepoManifest,
    item_id: &str,
) -> Result<DependencyFreshnessReport, WorkspaceError> {
    validate_repo_manifest(manifest)?;

    let item = manifest
        .items
        .iter()
        .find(|entry| entry.id == item_id)
        .ok_or_else(|| {
            WorkspaceError::InvalidRepoManifest(format!("unknown item id: {item_id}"))
        })?;

    if !item_supports_action(
        repo_root,
        item,
        StandardActionName::VerifyDependenciesFreshness,
    ) {
        return Err(WorkspaceError::InvalidRepoManifest(format!(
            "item {} does not support action {}",
            item.id,
            action_name_label(StandardActionName::VerifyDependenciesFreshness)
        )));
    }

    let item_root = resolve_item_path(repo_root, item);
    Ok(DependencyFreshnessReport {
        item_id: item.id.clone(),
        findings: dependency_freshness_findings(item, &item_root),
    })
}

pub fn verify_repo_dependencies_freshness(
    repo_root: &Path,
    manifest: &RepoManifest,
) -> Result<Vec<DependencyFreshnessReport>, WorkspaceError> {
    let plan = plan_repo_action(
        repo_root,
        manifest,
        StandardActionName::VerifyDependenciesFreshness,
    )?;
    let mut reports = Vec::new();
    for step in &plan.steps {
        let Some(item_id) = step.item_id.as_deref() else {
            continue;
        };
        reports.push(verify_item_dependencies_freshness(
            repo_root, manifest, item_id,
        )?);
    }
    Ok(reports)
}

fn dependency_freshness_findings(
    item: &RepoItem,
    item_root: &Path,
) -> Vec<DependencyFreshnessFinding> {
    match item.item_type.as_str() {
        "cargo" => missing_lockfile_finding(item_root.join("Cargo.lock"), "Cargo.lock"),
        "gradle" | "gradle_android" => {
            let gradle_lock = item_root.join("gradle.lockfile");
            let versions_lock = item_root.join("gradle").join("libs.versions.toml");
            if gradle_lock.exists() || versions_lock.exists() {
                Vec::new()
            } else {
                vec![DependencyFreshnessFinding {
                    kind: DependencyFreshnessFindingKind::MissingLockfile,
                    message: "No Gradle lockfile or version catalog was found.".to_string(),
                }]
            }
        }
        "python" => {
            let candidates = [
                item_root.join("uv.lock"),
                item_root.join("poetry.lock"),
                item_root.join("requirements.txt"),
                item_root.join("requirements-dev.txt"),
            ];
            if candidates.iter().any(|path| path.exists()) {
                Vec::new()
            } else {
                vec![DependencyFreshnessFinding {
                    kind: DependencyFreshnessFindingKind::MissingLockfile,
                    message: "No Python lockfile or pinned requirements file was found."
                        .to_string(),
                }]
            }
        }
        "node" => {
            let candidates = [
                item_root.join("package-lock.json"),
                item_root.join("npm-shrinkwrap.json"),
                item_root.join("pnpm-lock.yaml"),
                item_root.join("yarn.lock"),
                item_root.join("bun.lockb"),
            ];
            if candidates.iter().any(|path| path.exists()) {
                Vec::new()
            } else {
                vec![DependencyFreshnessFinding {
                    kind: DependencyFreshnessFindingKind::MissingLockfile,
                    message: "No Node package manager lockfile was found.".to_string(),
                }]
            }
        }
        _ => vec![DependencyFreshnessFinding {
            kind: DependencyFreshnessFindingKind::CustomWarning,
            message: format!(
                "Ronomepo has no built-in dependency freshness verifier for item type {}.",
                item.item_type
            ),
        }],
    }
}

fn missing_lockfile_finding(path: PathBuf, label: &str) -> Vec<DependencyFreshnessFinding> {
    if path.exists() {
        Vec::new()
    } else {
        vec![DependencyFreshnessFinding {
            kind: DependencyFreshnessFindingKind::MissingLockfile,
            message: format!("{label} is missing."),
        }]
    }
}

fn action_name_label(action: StandardActionName) -> &'static str {
    match action {
        StandardActionName::ListArtifacts => "list_artifacts",
        StandardActionName::Build => "build",
        StandardActionName::Test => "test",
        StandardActionName::Clean => "clean",
        StandardActionName::VerifyDependenciesFreshness => "verify_dependencies_freshness",
        StandardActionName::Deploy => "deploy",
    }
}

pub fn default_commit_check_rules() -> Vec<CommitCheckRule> {
    [
        ("generated-marker", "Generated marker", "(?i)generated:"),
        (
            "generated-by-marker",
            "Generated-by marker",
            "(?i)generated-by:",
        ),
        ("anthropic-marker", "Anthropic marker", "(?i)@anthropic"),
        ("co-author-marker", "Co-author marker", "(?i)co-author"),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (id, name, pattern))| CommitCheckRule {
        id: id.to_string(),
        name: name.to_string(),
        enabled: true,
        priority: (index as i32) * 10,
        effect: CommitCheckRuleEffect::Block,
        scope: CommitCheckRuleScope::All,
        matcher: CommitCheckRuleMatcher::Regex {
            pattern: pattern.to_string(),
        },
    })
    .collect()
}

pub fn ensure_commit_check_rules_initialized(manifest: &mut WorkspaceManifest) -> bool {
    if manifest.commit_check_rules.is_some() {
        return false;
    }

    manifest.commit_check_rules = Some(default_commit_check_rules());
    true
}

pub fn import_repos_txt(
    path: &Path,
    workspace_root: &Path,
    workspace_name: &str,
) -> Result<WorkspaceManifest, WorkspaceError> {
    let content = fs::read_to_string(path)?;
    let mut repos = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let dir_name = derive_dir_name(trimmed)?;
        repos.push(RepositoryEntry {
            id: dir_name.clone(),
            name: dir_name.clone(),
            dir_name,
            remote_url: trimmed.to_string(),
            enabled: true,
        });
    }

    Ok(WorkspaceManifest {
        name: workspace_name.to_string(),
        root: workspace_root.to_path_buf(),
        repos,
        shared_hooks_path: Some(workspace_root.join("hooks")),
        commit_check_rules: None,
    })
}

pub fn build_repository_list(manifest: &WorkspaceManifest) -> Vec<RepositoryListItem> {
    manifest
        .repos
        .iter()
        .map(|repo| build_repository_list_item(&manifest.root, repo))
        .collect()
}

pub fn build_repository_list_item(
    workspace_root: &Path,
    repo: &RepositoryEntry,
) -> RepositoryListItem {
    let repo_path = workspace_root.join(&repo.dir_name);
    RepositoryListItem {
        id: repo.id.clone(),
        name: repo.name.clone(),
        dir_name: repo.dir_name.clone(),
        remote_url: repo.remote_url.clone(),
        status: collect_repository_status(&repo_path),
        repo_manifest: Some(scan_repo_manifest(&repo_path)),
    }
}

pub fn scan_repo_manifest(repo_root: &Path) -> RepoManifestScan {
    let path = default_repo_manifest_path(repo_root);
    if !path.exists() {
        return RepoManifestScan {
            path,
            state: RepoManifestScanState::Missing,
        };
    }

    match load_repo_manifest(&path) {
        Ok(manifest) => RepoManifestScan {
            path,
            state: RepoManifestScanState::Valid(repo_manifest_summary(repo_root, &manifest)),
        },
        Err(error) => RepoManifestScan {
            path,
            state: RepoManifestScanState::Invalid {
                message: error.to_string(),
            },
        },
    }
}

fn repo_manifest_summary(repo_root: &Path, manifest: &RepoManifest) -> RepoManifestSummary {
    let mut item_types = manifest
        .items
        .iter()
        .map(|item| item.item_type.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    item_types.sort();

    let mut supported_actions = manifest
        .items
        .iter()
        .flat_map(|item| {
            built_in_supported_actions(repo_root, item)
                .into_iter()
                .chain(item.actions.iter().map(|action| action.action))
        })
        .chain(manifest.repo_actions.iter().map(|action| action.action))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    supported_actions.sort();

    let policy_report = assess_repo_capability_policy(manifest);
    let unsupported_capability_count = manifest
        .capabilities
        .iter()
        .filter(|capability| capability.status == CapabilityDeclarationStatus::Unsupported)
        .count();

    RepoManifestSummary {
        item_count: manifest.items.len(),
        item_types,
        supported_actions,
        action_count: manifest.actions.len()
            + manifest.repo_actions.len()
            + manifest
                .items
                .iter()
                .map(|item| item.actions.len())
                .sum::<usize>(),
        capability_count: manifest.capabilities.len(),
        missing_required_capability_count: policy_report.issues.len(),
        unsupported_capability_count,
    }
}

pub fn workspace_summary(
    manifest: Option<&WorkspaceManifest>,
    manifest_path: Option<&Path>,
    workspace_root: &Path,
) -> WorkspaceSummary {
    match manifest {
        Some(manifest) => WorkspaceSummary {
            workspace_name: manifest.name.clone(),
            repo_count: manifest.repos.len(),
            manifest_path: manifest_path.map(Path::to_path_buf),
        },
        None => WorkspaceSummary {
            workspace_name: workspace_root
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("Workspace")
                .to_string(),
            repo_count: 0,
            manifest_path: manifest_path.map(Path::to_path_buf),
        },
    }
}

pub fn derive_dir_name(url: &str) -> Result<String, WorkspaceError> {
    let trimmed = url.trim().trim_end_matches('/');
    let candidate = trimmed
        .rsplit(['/', ':'])
        .next()
        .unwrap_or_default()
        .trim_end_matches(".git");
    if candidate.is_empty() {
        return Err(WorkspaceError::InvalidRepoUrl(url.to_string()));
    }
    Ok(candidate.to_string())
}

fn env_home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

pub fn collect_repository_status(repo_path: &Path) -> RepositoryStatus {
    if !repo_path.exists() {
        return RepositoryStatus {
            state: RepositoryState::Missing,
            branch: None,
            sync: RepositorySync::NoUpstream,
            head_commit: None,
            repo_path: repo_path.to_path_buf(),
        };
    }

    let branch = current_branch(repo_path);
    let state = repository_state(repo_path);
    let sync = repository_sync(repo_path);
    let head_commit = git_stdout(repo_path, ["rev-parse", "HEAD"]);

    RepositoryStatus {
        state,
        branch,
        sync,
        head_commit,
        repo_path: repo_path.to_path_buf(),
    }
}

pub fn format_sync_label(sync: &RepositorySync) -> String {
    match sync {
        RepositorySync::Unknown => "?".to_string(),
        RepositorySync::NoUpstream => "-".to_string(),
        RepositorySync::UpToDate => "up-to-date".to_string(),
        RepositorySync::Ahead(ahead) => format!("+{ahead}"),
        RepositorySync::Behind(behind) => format!("-{behind}"),
        RepositorySync::Diverged { ahead, behind } => format!("+{ahead}/-{behind}"),
    }
}

pub fn collect_repository_details(repo_path: &Path) -> RepositoryDetails {
    if !repo_path.exists() {
        return RepositoryDetails {
            remotes: Vec::new(),
            last_commit: None,
            changed_files: Vec::new(),
        };
    }

    RepositoryDetails {
        remotes: git_stdout(repo_path, ["remote", "-v"])
            .map(|output| {
                output
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        last_commit: git_stdout(repo_path, ["log", "-1", "--format=%h|%s|%ct"]).and_then(
            |output| {
                let mut parts = output.splitn(3, '|');
                let short_sha = parts.next()?;
                let subject = parts.next()?;
                let committed_at_epoch_secs = parts.next()?.parse().ok()?;
                Some(LastCommitInfo {
                    short_sha: short_sha.to_string(),
                    subject: subject.to_string(),
                    committed_at_epoch_secs,
                })
            },
        ),
        changed_files: git_stdout(repo_path, ["status", "--short"])
            .map(|output| {
                output
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .take(8)
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
    }
}

pub fn run_workspace_operation<F>(
    manifest: &WorkspaceManifest,
    selected_repo_ids: &[String],
    kind: OperationKind,
    mut emit: F,
) where
    F: FnMut(OperationEvent),
{
    let entries = manifest
        .repos
        .iter()
        .filter(|repo| repo.enabled)
        .filter(|repo| {
            selected_repo_ids.is_empty() || selected_repo_ids.iter().any(|id| id == &repo.id)
        })
        .collect::<Vec<_>>();

    emit(OperationEvent {
        kind: OperationEventKind::Started,
        repository_id: None,
        repository_name: None,
        message: format!(
            "{} started for {}.",
            operation_kind_label(kind),
            if selected_repo_ids.is_empty() {
                "all eligible repositories".to_string()
            } else {
                format!("{} selected repos", entries.len())
            }
        ),
    });

    let mut completed = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;

    if matches!(kind, OperationKind::ApplyHooks) {
        let root_event = apply_hooks_to_workspace_root(manifest);
        match root_event.kind {
            OperationEventKind::Success => completed += 1,
            OperationEventKind::Skipped => skipped += 1,
            OperationEventKind::Failed => failed += 1,
            _ => {}
        }
        emit(root_event);
    }

    if matches!(kind, OperationKind::Pull) && selected_repo_ids.is_empty() {
        let root_event = pull_workspace_root(manifest);
        match root_event.kind {
            OperationEventKind::Success => completed += 1,
            OperationEventKind::Skipped => skipped += 1,
            OperationEventKind::Failed => failed += 1,
            _ => {}
        }
        emit(root_event);
    }

    if matches!(kind, OperationKind::Push) {
        let flagged = generated_history_matches(manifest, selected_repo_ids, 25);
        if !flagged.is_empty() {
            emit(OperationEvent {
                kind: OperationEventKind::Failed,
                repository_id: None,
                repository_name: None,
                message: format!(
                    "Push aborted because commit check rules blocked commits in: {}. Use force push to override.",
                    flagged.join(", ")
                ),
            });
            emit(OperationEvent {
                kind: OperationEventKind::Finished,
                repository_id: None,
                repository_name: None,
                message: format!(
                    "{} finished: {} completed, {} skipped, {} failed.",
                    operation_kind_label(kind),
                    completed,
                    skipped,
                    1
                ),
            });
            return;
        }
    }

    if matches!(kind, OperationKind::Push | OperationKind::PushForce)
        && selected_repo_ids.is_empty()
    {
        let root_event = push_workspace_root(manifest);
        match root_event.kind {
            OperationEventKind::Success => completed += 1,
            OperationEventKind::Skipped => skipped += 1,
            OperationEventKind::Failed => failed += 1,
            _ => {}
        }
        emit(root_event);
    }

    for repo in entries {
        let event = match kind {
            OperationKind::CloneMissing => clone_missing_repo(manifest, repo),
            OperationKind::Pull => pull_repo(manifest, repo),
            OperationKind::Push => push_repo(manifest, repo),
            OperationKind::PushForce => push_repo(manifest, repo),
            OperationKind::ApplyHooks => apply_hooks_repo(manifest, repo),
        };

        match event.kind {
            OperationEventKind::Success => completed += 1,
            OperationEventKind::Skipped => skipped += 1,
            OperationEventKind::Failed => failed += 1,
            _ => {}
        }
        emit(event);
    }

    emit(OperationEvent {
        kind: OperationEventKind::Finished,
        repository_id: None,
        repository_name: None,
        message: format!(
            "{} finished: {} completed, {} skipped, {} failed.",
            operation_kind_label(kind),
            completed,
            skipped,
            failed
        ),
    });
}

pub fn collect_generated_history_matches(
    manifest: &WorkspaceManifest,
    selected_repo_ids: &[String],
    num_commits: usize,
) -> Vec<HistoryMatch> {
    collect_commit_check_report(manifest, selected_repo_ids, num_commits).matches
}

pub fn collect_commit_check_report(
    manifest: &WorkspaceManifest,
    selected_repo_ids: &[String],
    num_commits: usize,
) -> CommitCheckReport {
    let rules = manifest
        .commit_check_rules
        .clone()
        .unwrap_or_else(default_commit_check_rules);
    let compiled_rules = compile_commit_check_rules(&rules);
    let mut commits = collect_repo_recent_commits(&manifest.root, None, "(monorepo)", num_commits);

    for repo in manifest
        .repos
        .iter()
        .filter(|repo| repo.enabled)
        .filter(|repo| {
            selected_repo_ids.is_empty() || selected_repo_ids.iter().any(|id| id == &repo.id)
        })
    {
        let repo_path = manifest.root.join(&repo.dir_name);
        if !repo_path.exists() {
            continue;
        }
        commits.extend(collect_repo_recent_commits(
            &repo_path,
            Some(&repo.id),
            &repo.name,
            num_commits,
        ));
    }

    CommitCheckReport {
        matches: evaluate_commit_check_rules(&commits, &compiled_rules.valid),
        invalid_rules: compiled_rules.invalid,
    }
}

pub fn collect_workspace_line_stats(
    manifest: &WorkspaceManifest,
    since_date: Option<&str>,
) -> WorkspaceLineStats {
    let mut rows = Vec::new();
    let mut total_additions = 0usize;
    let mut total_deletions = 0usize;

    let monorepo = collect_repo_line_stats(&manifest.root, "(monorepo)", since_date);
    total_additions += monorepo.additions;
    total_deletions += monorepo.deletions;
    rows.push(monorepo);

    for repo in manifest.repos.iter().filter(|repo| repo.enabled) {
        let row =
            collect_repo_line_stats(&manifest.root.join(&repo.dir_name), &repo.name, since_date);
        total_additions += row.additions;
        total_deletions += row.deletions;
        rows.push(row);
    }

    WorkspaceLineStats {
        rows,
        total_additions,
        total_deletions,
        total_net: total_additions as isize - total_deletions as isize,
    }
}

fn clone_missing_repo(manifest: &WorkspaceManifest, repo: &RepositoryEntry) -> OperationEvent {
    let repo_path = manifest.root.join(&repo.dir_name);
    if repo_path.exists() {
        return skipped_event(repo, format!("{} already exists locally.", repo.dir_name));
    }

    match Command::new("git")
        .arg("clone")
        .arg(&repo.remote_url)
        .arg(&repo_path)
        .output()
    {
        Ok(output) if output.status.success() => {
            if let Some(hooks_path) = resolved_shared_hooks_path(manifest) {
                let _ = configure_hooks_path(&repo_path, &hooks_path);
            }
            success_event(repo, format!("Cloned {}.", repo.dir_name))
        }
        Ok(output) => failed_event(
            repo,
            format!(
                "Clone failed for {}: {}",
                repo.dir_name,
                stderr_message(&output.stderr)
            ),
        ),
        Err(error) => failed_event(repo, format!("Clone failed for {}: {error}", repo.dir_name)),
    }
}

fn pull_repo(manifest: &WorkspaceManifest, repo: &RepositoryEntry) -> OperationEvent {
    let repo_path = manifest.root.join(&repo.dir_name);
    if !repo_path.exists() {
        return failed_event(repo, format!("{} is missing locally.", repo.dir_name));
    }

    if matches!(repository_state(&repo_path), RepositoryState::Dirty) {
        return skipped_event(
            repo,
            format!(
                "Skipped {} because it has uncommitted changes.",
                repo.dir_name
            ),
        );
    }

    let has_untracked = matches!(repository_state(&repo_path), RepositoryState::Untracked);
    match Command::new("git")
        .arg("-C")
        .arg(&repo_path)
        .arg("pull")
        .arg("--quiet")
        .output()
    {
        Ok(output) if output.status.success() => {
            let suffix = if has_untracked {
                " Untracked files were present."
            } else {
                ""
            };
            success_event(repo, format!("Pulled {}.{suffix}", repo.dir_name))
        }
        Ok(output) => failed_event(
            repo,
            format!(
                "Pull failed for {}: {}",
                repo.dir_name,
                stderr_message(&output.stderr)
            ),
        ),
        Err(error) => failed_event(repo, format!("Pull failed for {}: {error}", repo.dir_name)),
    }
}

fn push_repo(manifest: &WorkspaceManifest, repo: &RepositoryEntry) -> OperationEvent {
    let repo_path = manifest.root.join(&repo.dir_name);
    if !repo_path.exists() {
        return failed_event(repo, format!("{} is missing locally.", repo.dir_name));
    }

    let Some(upstream) = git_stdout(&repo_path, ["rev-parse", "--abbrev-ref", "@{upstream}"])
    else {
        return skipped_event(
            repo,
            format!("Skipped {} because it has no upstream.", repo.dir_name),
        );
    };
    if upstream.is_empty() {
        return skipped_event(
            repo,
            format!("Skipped {} because it has no upstream.", repo.dir_name),
        );
    }

    let ahead = git_stdout(&repo_path, ["rev-list", "--count", "@{upstream}..HEAD"])
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    if ahead == 0 {
        return skipped_event(
            repo,
            format!("Skipped {} because it has nothing to push.", repo.dir_name),
        );
    }

    match Command::new("git")
        .arg("-C")
        .arg(&repo_path)
        .arg("push")
        .output()
    {
        Ok(output) if output.status.success() => success_event(
            repo,
            format!("Pushed {} ({} commits).", repo.dir_name, ahead),
        ),
        Ok(output) => failed_event(
            repo,
            format!(
                "Push failed for {}: {}",
                repo.dir_name,
                stderr_message(&output.stderr)
            ),
        ),
        Err(error) => failed_event(repo, format!("Push failed for {}: {error}", repo.dir_name)),
    }
}

fn push_workspace_root(manifest: &WorkspaceManifest) -> OperationEvent {
    let root = &manifest.root;
    if !root.join(".git").exists() {
        return OperationEvent {
            kind: OperationEventKind::Skipped,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: "Skipped workspace root because it is not a git repository.".to_string(),
        };
    }

    let has_upstream = git_stdout(root, ["rev-parse", "--abbrev-ref", "@{upstream}"])
        .is_some_and(|s| !s.is_empty());
    if !has_upstream {
        return OperationEvent {
            kind: OperationEventKind::Skipped,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: "Skipped workspace root because it has no upstream.".to_string(),
        };
    }

    let ahead = git_stdout(root, ["rev-list", "--count", "@{upstream}..HEAD"])
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    if ahead == 0 {
        return OperationEvent {
            kind: OperationEventKind::Skipped,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: "Skipped workspace root because it has nothing to push.".to_string(),
        };
    }

    match Command::new("git").arg("-C").arg(root).arg("push").output() {
        Ok(output) if output.status.success() => OperationEvent {
            kind: OperationEventKind::Success,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: format!("Pushed workspace root ({ahead} commits)."),
        },
        Ok(output) => OperationEvent {
            kind: OperationEventKind::Failed,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: format!(
                "Push failed for workspace root: {}",
                stderr_message(&output.stderr)
            ),
        },
        Err(error) => OperationEvent {
            kind: OperationEventKind::Failed,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: format!("Push failed for workspace root: {error}"),
        },
    }
}

fn pull_workspace_root(manifest: &WorkspaceManifest) -> OperationEvent {
    let root = &manifest.root;
    if !root.join(".git").exists() {
        return OperationEvent {
            kind: OperationEventKind::Skipped,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: "Skipped workspace root because it is not a git repository.".to_string(),
        };
    }

    if matches!(repository_state(root), RepositoryState::Dirty) {
        return OperationEvent {
            kind: OperationEventKind::Skipped,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: "Skipped workspace root because it has uncommitted changes.".to_string(),
        };
    }

    match Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("pull")
        .arg("--quiet")
        .output()
    {
        Ok(output) if output.status.success() => OperationEvent {
            kind: OperationEventKind::Success,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: "Pulled workspace root.".to_string(),
        },
        Ok(output) => OperationEvent {
            kind: OperationEventKind::Failed,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: format!(
                "Pull failed for workspace root: {}",
                stderr_message(&output.stderr)
            ),
        },
        Err(error) => OperationEvent {
            kind: OperationEventKind::Failed,
            repository_id: None,
            repository_name: Some("workspace root".to_string()),
            message: format!("Pull failed for workspace root: {error}"),
        },
    }
}

fn apply_hooks_repo(manifest: &WorkspaceManifest, repo: &RepositoryEntry) -> OperationEvent {
    let repo_path = manifest.root.join(&repo.dir_name);
    if !repo_path.exists() {
        return failed_event(repo, format!("{} is missing locally.", repo.dir_name));
    }

    let Some(hooks_path) = resolved_shared_hooks_path(manifest) else {
        return skipped_event(repo, "No shared hooks path is configured.".to_string());
    };

    match configure_hooks_path(&repo_path, &hooks_path) {
        Ok(()) => success_event(
            repo,
            format!(
                "Configured core.hooksPath for {} to {}.",
                repo.dir_name,
                hooks_path.display()
            ),
        ),
        Err(error) => failed_event(
            repo,
            format!("Failed to configure hooks for {}: {error}", repo.dir_name),
        ),
    }
}

fn apply_hooks_to_workspace_root(manifest: &WorkspaceManifest) -> OperationEvent {
    let Some(hooks_path) = resolved_shared_hooks_path(manifest) else {
        return OperationEvent {
            kind: OperationEventKind::Skipped,
            repository_id: None,
            repository_name: Some("(monorepo)".to_string()),
            message: "No shared hooks path is configured for the workspace root.".to_string(),
        };
    };

    match configure_hooks_path(&manifest.root, &hooks_path) {
        Ok(()) => OperationEvent {
            kind: OperationEventKind::Success,
            repository_id: None,
            repository_name: Some("(monorepo)".to_string()),
            message: format!(
                "Configured core.hooksPath for (monorepo) to {}.",
                hooks_path.display()
            ),
        },
        Err(error) => OperationEvent {
            kind: OperationEventKind::Failed,
            repository_id: None,
            repository_name: Some("(monorepo)".to_string()),
            message: format!("Failed to configure hooks for (monorepo): {error}"),
        },
    }
}

fn configure_hooks_path(repo_path: &Path, hooks_path: &Path) -> io::Result<()> {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .arg("config")
        .arg("--local")
        .arg("core.hooksPath")
        .arg(hooks_path)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "git config --local core.hooksPath failed",
        ))
    }
}

fn resolved_shared_hooks_path(manifest: &WorkspaceManifest) -> Option<PathBuf> {
    manifest
        .shared_hooks_path
        .clone()
        .or_else(|| {
            let fallback = manifest.root.join("hooks");
            fallback.exists().then_some(fallback)
        })
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                manifest.root.join(path)
            }
        })
}

fn success_event(repo: &RepositoryEntry, message: String) -> OperationEvent {
    OperationEvent {
        kind: OperationEventKind::Success,
        repository_id: Some(repo.id.clone()),
        repository_name: Some(repo.name.clone()),
        message,
    }
}

fn skipped_event(repo: &RepositoryEntry, message: String) -> OperationEvent {
    OperationEvent {
        kind: OperationEventKind::Skipped,
        repository_id: Some(repo.id.clone()),
        repository_name: Some(repo.name.clone()),
        message,
    }
}

fn failed_event(repo: &RepositoryEntry, message: String) -> OperationEvent {
    OperationEvent {
        kind: OperationEventKind::Failed,
        repository_id: Some(repo.id.clone()),
        repository_name: Some(repo.name.clone()),
        message,
    }
}

fn operation_kind_label(kind: OperationKind) -> &'static str {
    match kind {
        OperationKind::CloneMissing => "Clone Missing",
        OperationKind::Pull => "Pull",
        OperationKind::Push => "Push",
        OperationKind::PushForce => "Push Force",
        OperationKind::ApplyHooks => "Apply Hooks",
    }
}

fn generated_history_matches(
    manifest: &WorkspaceManifest,
    selected_repo_ids: &[String],
    num_commits: usize,
) -> Vec<String> {
    collect_commit_check_report(manifest, selected_repo_ids, num_commits)
        .matches
        .into_iter()
        .map(|entry| entry.repository_name)
        .collect()
}

#[cfg(test)]
fn repo_history_has_generated_markers(repo_path: &Path, num_commits: usize) -> bool {
    let manifest = WorkspaceManifest {
        name: "Test".to_string(),
        root: repo_path.to_path_buf(),
        repos: Vec::new(),
        shared_hooks_path: None,
        commit_check_rules: Some(default_commit_check_rules()),
    };
    !collect_commit_check_report(&manifest, &[], num_commits)
        .matches
        .is_empty()
}

#[derive(Clone, Debug)]
struct RecentCommit {
    repository_id: Option<String>,
    repository_name: String,
    head_offset: usize,
    commit_hash: String,
    subject: String,
    body_lines: Vec<String>,
}

struct CompiledCommitCheckRules {
    valid: Vec<CompiledCommitCheckRule>,
    invalid: Vec<InvalidCommitCheckRule>,
}

struct CompiledCommitCheckRule {
    rule: CommitCheckRule,
    matcher: CompiledCommitCheckMatcher,
}

enum CompiledCommitCheckMatcher {
    Regex(Regex),
    CommitHash(String),
}

fn compile_commit_check_rules(rules: &[CommitCheckRule]) -> CompiledCommitCheckRules {
    let mut ordered_rules = rules.to_vec();
    ordered_rules.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for rule in ordered_rules {
        if !rule.enabled {
            continue;
        }

        let matcher = match &rule.matcher {
            CommitCheckRuleMatcher::Regex { pattern } => match Regex::new(pattern) {
                Ok(regex) => CompiledCommitCheckMatcher::Regex(regex),
                Err(error) => {
                    invalid.push(InvalidCommitCheckRule {
                        rule_id: rule.id.clone(),
                        rule_name: rule.name.clone(),
                        message: error.to_string(),
                    });
                    continue;
                }
            },
            CommitCheckRuleMatcher::CommitHash { hash } => {
                CompiledCommitCheckMatcher::CommitHash(hash.trim().to_ascii_lowercase())
            }
        };

        valid.push(CompiledCommitCheckRule { rule, matcher });
    }

    CompiledCommitCheckRules { valid, invalid }
}

fn collect_repo_recent_commits(
    repo_path: &Path,
    repository_id: Option<&str>,
    repository_name: &str,
    num_commits: usize,
) -> Vec<RecentCommit> {
    let limit = num_commits.to_string();
    let Some(output) = git_stdout(
        repo_path,
        [
            "log",
            "-n",
            &limit,
            "--format=HASH:%H%nSUBJ:%s%nBODY:%b%nEND---",
        ],
    ) else {
        return Vec::new();
    };

    let mut commits = Vec::new();
    let mut current_hash = String::new();
    let mut current_subject = String::new();
    let mut body_lines = Vec::new();
    let mut head_offset = 0usize;

    let push_commit = |commits: &mut Vec<RecentCommit>,
                       current_hash: &str,
                       current_subject: &str,
                       body_lines: &[String],
                       head_offset: usize| {
        if current_hash.is_empty() {
            return;
        }
        commits.push(RecentCommit {
            repository_id: repository_id.map(ToOwned::to_owned),
            repository_name: repository_name.to_string(),
            head_offset,
            commit_hash: current_hash.to_string(),
            subject: current_subject.to_string(),
            body_lines: body_lines.to_vec(),
        });
    };

    for line in output.lines() {
        if let Some(hash) = line.strip_prefix("HASH:") {
            current_hash = hash.to_string();
            current_subject.clear();
            body_lines.clear();
            continue;
        }
        if let Some(subject) = line.strip_prefix("SUBJ:") {
            current_subject = subject.to_string();
            continue;
        }
        if let Some(body) = line.strip_prefix("BODY:") {
            body_lines.push(body.to_string());
            continue;
        }
        if line == "END---" {
            push_commit(
                &mut commits,
                &current_hash,
                &current_subject,
                &body_lines,
                head_offset,
            );
            if !current_hash.is_empty() {
                head_offset += 1;
            }
            current_hash.clear();
            current_subject.clear();
            body_lines.clear();
            continue;
        }
        if !current_hash.is_empty() {
            body_lines.push(line.to_string());
        }
    }

    commits
}

fn evaluate_commit_check_rules(
    commits: &[RecentCommit],
    rules: &[CompiledCommitCheckRule],
) -> Vec<HistoryMatch> {
    let mut matches = Vec::new();

    for commit in commits {
        let mut blocked: Option<HistoryMatch> = None;
        for rule in rules {
            if !rule_applies_to_repository(&rule.rule, commit.repository_id.as_deref()) {
                continue;
            }

            let Some(matching_lines) = commit_matches_rule(commit, &rule.matcher) else {
                continue;
            };

            match rule.rule.effect {
                CommitCheckRuleEffect::Block => {
                    blocked = Some(HistoryMatch {
                        repository_id: commit.repository_id.clone(),
                        repository_name: commit.repository_name.clone(),
                        head_offset: commit.head_offset,
                        commit_hash: commit.commit_hash.clone(),
                        subject: commit.subject.clone(),
                        matching_lines,
                        rule_id: rule.rule.id.clone(),
                        rule_name: rule.rule.name.clone(),
                    });
                }
                CommitCheckRuleEffect::Allow => {
                    blocked = None;
                }
            }
        }

        if let Some(entry) = blocked {
            matches.push(entry);
        }
    }

    matches
}

fn rule_applies_to_repository(rule: &CommitCheckRule, repository_id: Option<&str>) -> bool {
    match &rule.scope {
        CommitCheckRuleScope::All => true,
        CommitCheckRuleScope::Repositories { repository_ids } => repository_id
            .map(|repository_id| repository_ids.iter().any(|id| id == repository_id))
            .unwrap_or(false),
    }
}

fn commit_matches_rule(
    commit: &RecentCommit,
    matcher: &CompiledCommitCheckMatcher,
) -> Option<Vec<String>> {
    match matcher {
        CompiledCommitCheckMatcher::Regex(regex) => {
            let mut matching_lines = Vec::new();
            if regex.is_match(&commit.subject) {
                matching_lines.push(commit.subject.clone());
            }
            for line in &commit.body_lines {
                if regex.is_match(line) {
                    matching_lines.push(line.clone());
                }
            }
            if matching_lines.is_empty() {
                let message = commit_message(commit);
                if regex.is_match(&message) {
                    matching_lines.push("<message>".to_string());
                }
            }
            (!matching_lines.is_empty()).then_some(matching_lines)
        }
        CompiledCommitCheckMatcher::CommitHash(hash) => (commit.commit_hash.to_ascii_lowercase()
            == *hash)
            .then(|| vec![commit.commit_hash.clone()]),
    }
}

fn commit_message(commit: &RecentCommit) -> String {
    let mut message = commit.subject.clone();
    for line in &commit.body_lines {
        message.push('\n');
        message.push_str(line);
    }
    message
}

fn collect_repo_line_stats(
    repo_path: &Path,
    repository_name: &str,
    since_date: Option<&str>,
) -> LineStatsRow {
    if !repo_path.exists() {
        return LineStatsRow {
            repository_name: repository_name.to_string(),
            additions: 0,
            deletions: 0,
            net: 0,
            missing: true,
        };
    }

    let mut command = Command::new("git");
    command.arg("-C").arg(repo_path).arg("log");
    if let Some(since_date) = since_date {
        command.arg(format!("--since={since_date}"));
    }
    command.arg("--numstat").arg("--format=");
    let Ok(output) = command.output() else {
        return LineStatsRow {
            repository_name: repository_name.to_string(),
            additions: 0,
            deletions: 0,
            net: 0,
            missing: false,
        };
    };

    let stats = String::from_utf8_lossy(&output.stdout);
    let mut additions = 0usize;
    let mut deletions = 0usize;
    for line in stats.lines() {
        let mut parts = line.split_whitespace();
        let Some(add) = parts.next() else {
            continue;
        };
        let Some(del) = parts.next() else {
            continue;
        };
        if add == "-" || del == "-" {
            continue;
        }
        additions += add.parse::<usize>().unwrap_or(0);
        deletions += del.parse::<usize>().unwrap_or(0);
    }

    LineStatsRow {
        repository_name: repository_name.to_string(),
        additions,
        deletions,
        net: additions as isize - deletions as isize,
        missing: false,
    }
}

fn stderr_message(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr).trim().to_string();
    if text.is_empty() {
        "command exited with an error".to_string()
    } else {
        text
    }
}

fn current_branch(repo_path: &Path) -> Option<String> {
    match git_stdout(repo_path, ["branch", "--show-current"]) {
        Some(output) if !output.is_empty() => Some(output),
        Some(_) => Some("detached".to_string()),
        None => None,
    }
}

fn repository_state(repo_path: &Path) -> RepositoryState {
    let has_diff = git_success(repo_path, ["diff", "--quiet"]).map(|ok| !ok);
    let has_cached_diff = git_success(repo_path, ["diff", "--cached", "--quiet"]).map(|ok| !ok);
    if matches!(has_diff, Some(true)) || matches!(has_cached_diff, Some(true)) {
        return RepositoryState::Dirty;
    }

    match git_stdout(repo_path, ["ls-files", "--others", "--exclude-standard"]) {
        Some(output) if !output.is_empty() => RepositoryState::Untracked,
        Some(_) => RepositoryState::Clean,
        None => RepositoryState::Unknown,
    }
}

fn repository_sync(repo_path: &Path) -> RepositorySync {
    let Some(upstream) = git_stdout(repo_path, ["rev-parse", "--abbrev-ref", "@{upstream}"]) else {
        return RepositorySync::NoUpstream;
    };
    if upstream.is_empty() {
        return RepositorySync::NoUpstream;
    }

    let ahead = git_stdout(repo_path, ["rev-list", "--count", "@{upstream}..HEAD"])
        .and_then(|value| value.parse::<usize>().ok());
    let behind = git_stdout(repo_path, ["rev-list", "--count", "HEAD..@{upstream}"])
        .and_then(|value| value.parse::<usize>().ok());

    match (ahead, behind) {
        (Some(0), Some(0)) => RepositorySync::UpToDate,
        (Some(ahead), Some(0)) => RepositorySync::Ahead(ahead),
        (Some(0), Some(behind)) => RepositorySync::Behind(behind),
        (Some(ahead), Some(behind)) => RepositorySync::Diverged { ahead, behind },
        _ => RepositorySync::Unknown,
    }
}

fn git_stdout<const N: usize>(repo_path: &Path, args: [&str; N]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_success<const N: usize>(repo_path: &Path, args: [&str; N]) -> Option<bool> {
    Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(args)
        .status()
        .ok()
        .map(|status| status.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn derive_dir_name_handles_ssh_urls() {
        let dir = derive_dir_name("git@github.com:lelloman/ronomepo.git").unwrap();
        assert_eq!(dir, "ronomepo");
    }

    #[test]
    fn import_repos_txt_builds_manifest() {
        let path = temp_file_path("repos");
        fs::write(
            &path,
            "git@github.com:lelloman/alpha.git\n\n# comment\nhttps://github.com/lelloman/beta.git\n",
        )
        .unwrap();

        let manifest = import_repos_txt(&path, Path::new("/tmp/workspace"), "Test").unwrap();
        assert_eq!(manifest.name, "Test");
        assert_eq!(manifest.repos.len(), 2);
        assert_eq!(manifest.repos[0].dir_name, "alpha");
        assert_eq!(manifest.repos[1].dir_name, "beta");
    }

    #[test]
    fn manifest_round_trips() {
        let path = temp_file_path("manifest");
        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: PathBuf::from("/tmp/example"),
            repos: vec![RepositoryEntry {
                id: "alpha".to_string(),
                name: "Alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "git@example.com:alpha.git".to_string(),
                enabled: true,
            }],
            shared_hooks_path: Some(PathBuf::from("/tmp/example/hooks")),
            commit_check_rules: Some(default_commit_check_rules()),
        };

        save_manifest(&path, &manifest).unwrap();
        let loaded = load_manifest(&path).unwrap();
        assert_eq!(loaded, manifest);
    }

    #[test]
    fn repo_manifest_round_trips() {
        let path = temp_file_path("repo-manifest");
        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("alpha".to_string()),
            items: vec![RepoItem {
                id: "cli".to_string(),
                item_type: "cargo".to_string(),
                path: PathBuf::from("."),
                config: None,
                artifacts: vec![RepoArtifactDefinition {
                    name: "cli-bin".to_string(),
                    kind: "binary".to_string(),
                    path: Some(PathBuf::from("target/release/alpha")),
                    pattern: None,
                    build_action: Some(StandardActionName::Build),
                }],
                actions: Vec::new(),
            }],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        save_repo_manifest(&path, &manifest).unwrap();
        let loaded = load_repo_manifest(&path).unwrap();
        assert_eq!(loaded, manifest);
    }

    #[test]
    fn repo_manifest_rejects_unknown_schema_version() {
        let manifest = RepoManifest {
            schema_version: 99,
            repo_id: None,
            items: Vec::new(),
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let error = validate_repo_manifest(&manifest).unwrap_err();
        assert!(matches!(
            error,
            WorkspaceError::UnsupportedRepoManifestSchemaVersion(99)
        ));
    }

    #[test]
    fn repo_manifest_rejects_duplicate_item_ids() {
        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: None,
            items: vec![
                RepoItem {
                    id: "dup".to_string(),
                    item_type: "cargo".to_string(),
                    path: PathBuf::from("."),
                    config: None,
                    artifacts: Vec::new(),
                    actions: Vec::new(),
                },
                RepoItem {
                    id: "dup".to_string(),
                    item_type: "python".to_string(),
                    path: PathBuf::from("py"),
                    config: None,
                    artifacts: Vec::new(),
                    actions: Vec::new(),
                },
            ],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let error = validate_repo_manifest(&manifest).unwrap_err();
        assert!(error.to_string().contains("duplicate item id"));
    }

    #[test]
    fn cargo_item_plans_build_test_and_clean() {
        let repo_root = temp_dir_path("repo-plan-cargo");
        fs::create_dir_all(&repo_root).unwrap();
        fs::write(
            repo_root.join("Cargo.toml"),
            "[package]\nname = \"alpha\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();

        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("alpha".to_string()),
            items: vec![RepoItem {
                id: "cli".to_string(),
                item_type: "cargo".to_string(),
                path: PathBuf::from("."),
                config: None,
                artifacts: Vec::new(),
                actions: Vec::new(),
            }],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let build =
            plan_item_action(&repo_root, &manifest, "cli", StandardActionName::Build).unwrap();
        let test =
            plan_item_action(&repo_root, &manifest, "cli", StandardActionName::Test).unwrap();
        let clean =
            plan_item_action(&repo_root, &manifest, "cli", StandardActionName::Clean).unwrap();

        match &build.steps[0].executor {
            RepoActionExecutor::Command(command) => {
                assert_eq!(command.program, "cargo");
                assert!(command.args.iter().any(|arg| arg == "build"));
            }
            _ => panic!("expected build to resolve to a command"),
        }
        match &test.steps[0].executor {
            RepoActionExecutor::Command(command) => {
                assert_eq!(command.program, "cargo");
                assert!(command.args.iter().any(|arg| arg == "test"));
            }
            _ => panic!("expected test to resolve to a command"),
        }
        match &clean.steps[0].executor {
            RepoActionExecutor::Command(command) => {
                assert_eq!(command.program, "cargo");
                assert!(command.args.iter().any(|arg| arg == "clean"));
            }
            _ => panic!("expected clean to resolve to a command"),
        }
    }

    #[test]
    fn repo_level_action_requires_explicit_aggregation_for_multiple_items() {
        let repo_root = temp_dir_path("repo-aggregation-missing");
        fs::create_dir_all(&repo_root).unwrap();

        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("mixed".to_string()),
            items: vec![
                RepoItem {
                    id: "cli".to_string(),
                    item_type: "cargo".to_string(),
                    path: PathBuf::from("cli"),
                    config: None,
                    artifacts: Vec::new(),
                    actions: Vec::new(),
                },
                RepoItem {
                    id: "backend".to_string(),
                    item_type: "python".to_string(),
                    path: PathBuf::from("backend"),
                    config: None,
                    artifacts: Vec::new(),
                    actions: Vec::new(),
                },
            ],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let error = plan_repo_action(&repo_root, &manifest, StandardActionName::Test).unwrap_err();
        assert!(error.to_string().contains("requires explicit aggregation"));
    }

    #[test]
    fn repo_level_action_uses_declared_aggregation() {
        let repo_root = temp_dir_path("repo-aggregation-explicit");
        fs::create_dir_all(repo_root.join("cli")).unwrap();
        fs::create_dir_all(repo_root.join("backend")).unwrap();
        fs::write(
            repo_root.join("cli/Cargo.toml"),
            "[package]\nname = \"cli\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();

        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("mixed".to_string()),
            items: vec![
                RepoItem {
                    id: "cli".to_string(),
                    item_type: "cargo".to_string(),
                    path: PathBuf::from("cli"),
                    config: None,
                    artifacts: Vec::new(),
                    actions: Vec::new(),
                },
                RepoItem {
                    id: "backend".to_string(),
                    item_type: "python".to_string(),
                    path: PathBuf::from("backend"),
                    config: None,
                    artifacts: Vec::new(),
                    actions: Vec::new(),
                },
            ],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: vec![RepoActionAggregation {
                action: StandardActionName::Test,
                item_ids: vec!["cli".to_string(), "backend".to_string()],
                execution: AggregationExecutionMode::Parallel,
                failure_policy: AggregationFailurePolicy::Continue,
                merge: AggregationMergeStrategy::Combined,
            }],
        };

        let plan = plan_repo_action(&repo_root, &manifest, StandardActionName::Test).unwrap();
        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.execution, AggregationExecutionMode::Parallel);
        assert_eq!(plan.failure_policy, AggregationFailurePolicy::Continue);
        assert_eq!(plan.steps[0].item_id.as_deref(), Some("cli"));
        assert_eq!(plan.steps[1].item_id.as_deref(), Some("backend"));
    }

    #[test]
    fn item_override_takes_precedence_over_built_in_handler() {
        let repo_root = temp_dir_path("repo-item-override");
        fs::create_dir_all(&repo_root).unwrap();

        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("alpha".to_string()),
            items: vec![RepoItem {
                id: "backend".to_string(),
                item_type: "python".to_string(),
                path: PathBuf::from("."),
                config: None,
                artifacts: Vec::new(),
                actions: vec![RepoActionCommand {
                    action: StandardActionName::Test,
                    command: vec!["tox".to_string(), "-q".to_string()],
                    workdir: None,
                    env: BTreeMap::new(),
                    timeout_seconds: Some(30),
                    output: ActionOutputMode::Text,
                }],
            }],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let plan =
            plan_item_action(&repo_root, &manifest, "backend", StandardActionName::Test).unwrap();
        assert!(matches!(
            plan.steps[0].source,
            RepoActionSource::ItemOverride
        ));
        match &plan.steps[0].executor {
            RepoActionExecutor::Command(command) => {
                assert_eq!(command.program, "tox");
                assert_eq!(command.args, vec!["-q".to_string()]);
                assert_eq!(command.timeout_seconds, Some(30));
            }
            _ => panic!("expected override to resolve to a command"),
        }
    }

    #[test]
    fn list_artifacts_merges_declared_and_builtin_artifacts() {
        let repo_root = temp_dir_path("repo-artifacts");
        fs::create_dir_all(&repo_root).unwrap();

        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("alpha".to_string()),
            items: vec![RepoItem {
                id: "pkg".to_string(),
                item_type: "python".to_string(),
                path: PathBuf::from("."),
                config: None,
                artifacts: vec![RepoArtifactDefinition {
                    name: "wheel".to_string(),
                    kind: "package".to_string(),
                    path: None,
                    pattern: Some("dist/*.whl".to_string()),
                    build_action: Some(StandardActionName::Build),
                }],
                actions: Vec::new(),
            }],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let artifacts = list_item_artifacts(&repo_root, &manifest, "pkg").unwrap();
        assert!(artifacts.iter().any(|artifact| artifact.name == "wheel"));
        assert!(artifacts
            .iter()
            .any(|artifact| artifact.name == "python-dist"
                && artifact.pattern.as_deref() == Some("dist/*")));
    }

    #[test]
    fn cargo_dependency_freshness_reports_missing_lockfile() {
        let repo_root = temp_dir_path("repo-freshness");
        fs::create_dir_all(&repo_root).unwrap();
        fs::write(
            repo_root.join("Cargo.toml"),
            "[package]\nname = \"alpha\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();

        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("alpha".to_string()),
            items: vec![RepoItem {
                id: "cli".to_string(),
                item_type: "cargo".to_string(),
                path: PathBuf::from("."),
                config: None,
                artifacts: Vec::new(),
                actions: Vec::new(),
            }],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let report = verify_item_dependencies_freshness(&repo_root, &manifest, "cli").unwrap();
        assert_eq!(report.findings.len(), 1);
        assert!(matches!(
            report.findings[0].kind,
            DependencyFreshnessFindingKind::MissingLockfile
        ));
    }

    #[test]
    fn repo_actions_can_be_backed_by_explicit_repo_commands() {
        let repo_root = temp_dir_path("repo-command");
        fs::create_dir_all(&repo_root).unwrap();

        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("deployable".to_string()),
            items: vec![RepoItem {
                id: "cli".to_string(),
                item_type: "cargo".to_string(),
                path: PathBuf::from("."),
                config: None,
                artifacts: Vec::new(),
                actions: Vec::new(),
            }],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: vec![RepoActionCommand {
                action: StandardActionName::Deploy,
                command: vec!["./scripts/deploy.sh".to_string(), "--prod".to_string()],
                workdir: Some(PathBuf::from(".")),
                env: BTreeMap::new(),
                timeout_seconds: None,
                output: ActionOutputMode::Text,
            }],
            aggregation: Vec::new(),
        };

        let plan = plan_repo_action(&repo_root, &manifest, StandardActionName::Deploy).unwrap();
        assert_eq!(plan.steps.len(), 1);
        assert!(matches!(
            plan.steps[0].source,
            RepoActionSource::RepoCommand
        ));
        match &plan.steps[0].executor {
            RepoActionExecutor::Command(command) => {
                assert_eq!(command.program, "./scripts/deploy.sh");
                assert_eq!(command.args, vec!["--prod".to_string()]);
            }
            _ => panic!("expected repo command to resolve to a command"),
        }
    }

    #[test]
    fn node_item_uses_package_json_scripts_and_config_overrides() {
        let repo_root = temp_dir_path("repo-node");
        let web_root = repo_root.join("web");
        fs::create_dir_all(&web_root).unwrap();
        fs::write(
            web_root.join("package.json"),
            r#"{
  "name": "web",
  "scripts": {
    "build": "vite build",
    "test:e2e": "playwright test",
    "deploy": "netlify deploy"
  }
}"#,
        )
        .unwrap();

        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("web".to_string()),
            items: vec![RepoItem {
                id: "web".to_string(),
                item_type: "node".to_string(),
                path: PathBuf::from("web"),
                config: Some(serde_json::json!({
                    "scripts": {
                        "test": "test:e2e"
                    }
                })),
                artifacts: Vec::new(),
                actions: Vec::new(),
            }],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let build =
            plan_item_action(&repo_root, &manifest, "web", StandardActionName::Build).unwrap();
        let test =
            plan_item_action(&repo_root, &manifest, "web", StandardActionName::Test).unwrap();
        let deploy =
            plan_item_action(&repo_root, &manifest, "web", StandardActionName::Deploy).unwrap();
        let clean =
            plan_item_action(&repo_root, &manifest, "web", StandardActionName::Clean).unwrap_err();

        match &build.steps[0].executor {
            RepoActionExecutor::Command(command) => {
                assert_eq!(command.program, "npm");
                assert_eq!(command.args, vec!["run".to_string(), "build".to_string()]);
            }
            _ => panic!("expected build to resolve to a command"),
        }
        match &test.steps[0].executor {
            RepoActionExecutor::Command(command) => {
                assert_eq!(command.program, "npm");
                assert_eq!(
                    command.args,
                    vec!["run".to_string(), "test:e2e".to_string()]
                );
            }
            _ => panic!("expected test to resolve to a command"),
        }
        match &deploy.steps[0].executor {
            RepoActionExecutor::Command(command) => {
                assert_eq!(command.program, "npm");
                assert_eq!(command.args, vec!["run".to_string(), "deploy".to_string()]);
            }
            _ => panic!("expected deploy to resolve to a command"),
        }
        assert!(clean.to_string().contains("does not support action clean"));
    }

    #[test]
    fn node_artifacts_and_dependency_freshness_use_builtin_defaults() {
        let repo_root = temp_dir_path("repo-node-lockfile");
        let web_root = repo_root.join("web");
        fs::create_dir_all(&web_root).unwrap();
        fs::write(
            web_root.join("package.json"),
            r#"{
  "name": "web",
  "scripts": {
    "build": "vite build"
  }
}"#,
        )
        .unwrap();
        fs::write(web_root.join("package-lock.json"), "{}").unwrap();

        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("web".to_string()),
            items: vec![RepoItem {
                id: "web".to_string(),
                item_type: "node".to_string(),
                path: PathBuf::from("web"),
                config: None,
                artifacts: Vec::new(),
                actions: Vec::new(),
            }],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let artifacts = list_item_artifacts(&repo_root, &manifest, "web").unwrap();
        assert!(artifacts
            .iter()
            .any(|artifact| artifact.name == "node-dist"));
        assert!(artifacts
            .iter()
            .any(|artifact| artifact.name == "node-build"));

        let report = verify_item_dependencies_freshness(&repo_root, &manifest, "web").unwrap();
        assert!(report.findings.is_empty());
    }

    #[test]
    fn sample_repo_manifest_is_valid() {
        let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../examples/sample-subrepo/ronomepo.repo.json");
        let raw = fs::read_to_string(manifest_path).unwrap();
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let object = value.as_object().unwrap();

        let filtered = object
            .iter()
            .filter(|(key, _)| key.as_str() != "$schema")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<serde_json::Map<String, serde_json::Value>>();

        let manifest: RepoManifest =
            serde_json::from_value(serde_json::Value::Object(filtered)).unwrap();
        validate_repo_manifest(&manifest).unwrap();
    }

    #[test]
    fn scan_repo_manifest_reports_missing_state() {
        let repo_root = temp_dir_path("repo-manifest-missing");
        fs::create_dir_all(&repo_root).unwrap();

        let scan = scan_repo_manifest(&repo_root);
        assert!(matches!(scan.state, RepoManifestScanState::Missing));
        assert_eq!(scan.path, repo_root.join(REPO_MANIFEST_FILE_NAME));
    }

    #[test]
    fn scan_repo_manifest_reports_valid_summary() {
        let repo_root = temp_dir_path("repo-manifest-valid");
        fs::create_dir_all(&repo_root).unwrap();
        save_repo_manifest(
            &repo_root.join(REPO_MANIFEST_FILE_NAME),
            &RepoManifest {
                schema_version: REPO_MANIFEST_SCHEMA_VERSION,
                repo_id: Some("sample".to_string()),
                items: vec![
                    RepoItem {
                        id: "cli".to_string(),
                        item_type: "cargo".to_string(),
                        path: PathBuf::from("."),
                        config: None,
                        artifacts: Vec::new(),
                        actions: Vec::new(),
                    },
                    RepoItem {
                        id: "tools".to_string(),
                        item_type: "python".to_string(),
                        path: PathBuf::from("tools"),
                        config: None,
                        artifacts: Vec::new(),
                        actions: vec![RepoActionCommand {
                            action: StandardActionName::Clean,
                            command: vec!["tox".to_string(), "-e".to_string(), "clean".to_string()],
                            workdir: None,
                            env: BTreeMap::new(),
                            timeout_seconds: None,
                            output: ActionOutputMode::Text,
                        }],
                    },
                ],
                actions: Vec::new(),
                capabilities: Vec::new(),
                repo_actions: vec![RepoActionCommand {
                    action: StandardActionName::Deploy,
                    command: vec!["./deploy.sh".to_string()],
                    workdir: None,
                    env: BTreeMap::new(),
                    timeout_seconds: None,
                    output: ActionOutputMode::Text,
                }],
                aggregation: Vec::new(),
            },
        )
        .unwrap();

        let scan = scan_repo_manifest(&repo_root);
        match scan.state {
            RepoManifestScanState::Valid(summary) => {
                assert_eq!(summary.item_count, 2);
                assert_eq!(
                    summary.item_types,
                    vec!["cargo".to_string(), "python".to_string()]
                );
                assert!(summary
                    .supported_actions
                    .contains(&StandardActionName::Build));
                assert!(summary
                    .supported_actions
                    .contains(&StandardActionName::Deploy));
            }
            _ => panic!("expected valid repo manifest summary"),
        }
    }

    #[test]
    fn scan_repo_manifest_reports_invalid_state() {
        let repo_root = temp_dir_path("repo-manifest-invalid");
        fs::create_dir_all(&repo_root).unwrap();
        fs::write(
            repo_root.join(REPO_MANIFEST_FILE_NAME),
            "{\"schema_version\":1,\"items\":[{\"id\":\"dup\",\"type\":\"cargo\",\"path\":\".\"},{\"id\":\"dup\",\"type\":\"python\",\"path\":\"tools\"}]}",
        )
        .unwrap();

        let scan = scan_repo_manifest(&repo_root);
        match scan.state {
            RepoManifestScanState::Invalid { message } => {
                assert!(message.contains("duplicate item id"));
            }
            _ => panic!("expected invalid repo manifest state"),
        }
    }

    #[test]
    fn normalize_workspace_root_expands_tilde_prefix() {
        let _home = EnvVarGuard::set("HOME", "/tmp/ronomepo-home");

        assert_eq!(
            normalize_workspace_root("~"),
            PathBuf::from("/tmp/ronomepo-home")
        );
        assert_eq!(
            normalize_workspace_root("~/lelloprojects"),
            PathBuf::from("/tmp/ronomepo-home/lelloprojects")
        );
        assert_eq!(
            normalize_workspace_root("/tmp/already-absolute"),
            PathBuf::from("/tmp/already-absolute")
        );
    }

    #[test]
    fn build_repository_list_collects_real_status() {
        let workspace = temp_dir_path("workspace");
        let repo_path = workspace.join("alpha");
        fs::create_dir_all(&workspace).unwrap();
        init_git_repo(&repo_path);
        save_repo_manifest(
            &repo_path.join(REPO_MANIFEST_FILE_NAME),
            &RepoManifest {
                schema_version: REPO_MANIFEST_SCHEMA_VERSION,
                repo_id: Some("alpha".to_string()),
                items: vec![RepoItem {
                    id: "cli".to_string(),
                    item_type: "cargo".to_string(),
                    path: PathBuf::from("."),
                    config: None,
                    artifacts: Vec::new(),
                    actions: Vec::new(),
                }],
                actions: Vec::new(),
                capabilities: Vec::new(),
                repo_actions: Vec::new(),
                aggregation: Vec::new(),
            },
        )
        .unwrap();
        run_git(&repo_path, ["add", REPO_MANIFEST_FILE_NAME]);
        run_git(&repo_path, ["commit", "-m", "add repo manifest"]);

        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: workspace,
            repos: vec![RepositoryEntry {
                id: "alpha".to_string(),
                name: "Alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "git@example.com:alpha.git".to_string(),
                enabled: true,
            }],
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        let items = build_repository_list(&manifest);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status.state, RepositoryState::Clean);
        assert_eq!(items[0].status.branch.as_deref(), Some("main"));
        assert!(items[0].status.head_commit.is_some());
        assert!(matches!(
            items[0].repo_manifest.as_ref().map(|scan| &scan.state),
            Some(RepoManifestScanState::Valid(_))
        ));
    }

    #[test]
    fn collect_repository_status_marks_missing_repo() {
        let path = temp_dir_path("missing").join("ghost");
        let status = collect_repository_status(&path);
        assert_eq!(status.state, RepositoryState::Missing);
        assert_eq!(status.branch, None);
        assert_eq!(status.head_commit, None);
    }

    #[test]
    fn collect_repository_status_detects_untracked_files() {
        let repo_path = temp_dir_path("untracked");
        init_git_repo(&repo_path);
        fs::write(repo_path.join("scratch.txt"), "hello").unwrap();

        let status = collect_repository_status(&repo_path);
        assert_eq!(status.state, RepositoryState::Untracked);
        assert!(status.head_commit.is_some());
    }

    #[test]
    fn collect_repository_details_reports_commit_and_changes() {
        let repo_path = temp_dir_path("details");
        init_git_repo(&repo_path);
        run_git(
            repo_path.as_path(),
            ["remote", "add", "origin", "git@example.com:alpha.git"],
        );
        fs::write(repo_path.join("scratch.txt"), "hello").unwrap();

        let details = collect_repository_details(&repo_path);
        assert!(details.last_commit.is_some());
        assert!(details
            .remotes
            .iter()
            .any(|line| line.contains("origin") && line.contains("git@example.com:alpha.git")));
        assert!(details
            .changed_files
            .iter()
            .any(|line| line.contains("scratch.txt")));
    }

    #[test]
    fn collect_generated_history_matches_reports_repo_and_subject() {
        let workspace = temp_dir_path("history-report");
        fs::create_dir_all(&workspace).unwrap();
        run_git(workspace.as_path(), ["init", "-b", "main"]);
        run_git(
            workspace.as_path(),
            ["config", "user.name", "Ronomepo Tests"],
        );
        run_git(
            workspace.as_path(),
            ["config", "user.email", "tests@example.com"],
        );
        run_git(
            workspace.as_path(),
            [
                "commit",
                "--allow-empty",
                "-m",
                "workspace bot",
                "-m",
                "Generated-by: Agent",
            ],
        );

        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: workspace,
            repos: vec![],
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        let matches = collect_generated_history_matches(&manifest, &[], 25);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].repository_name, "(monorepo)");
        assert!(matches[0].subject.contains("workspace bot"));
    }

    #[test]
    fn collect_workspace_line_stats_reports_totals() {
        let workspace = temp_dir_path("line-stats");
        let repo_path = workspace.join("alpha");
        fs::create_dir_all(&workspace).unwrap();
        init_git_repo(&workspace);
        init_git_repo(&repo_path);

        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: workspace,
            repos: vec![RepositoryEntry {
                id: "alpha".to_string(),
                name: "alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "git@example.com:alpha.git".to_string(),
                enabled: true,
            }],
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        let stats = collect_workspace_line_stats(&manifest, None);
        assert_eq!(stats.rows.len(), 2);
        assert!(stats.total_additions >= 2);
        assert!(stats.total_net >= 2);
    }

    #[test]
    fn format_sync_label_matches_mono_style() {
        assert_eq!(format_sync_label(&RepositorySync::UpToDate), "up-to-date");
        assert_eq!(format_sync_label(&RepositorySync::NoUpstream), "-");
        assert_eq!(format_sync_label(&RepositorySync::Ahead(2)), "+2");
        assert_eq!(
            format_sync_label(&RepositorySync::Diverged {
                ahead: 1,
                behind: 3
            }),
            "+1/-3"
        );
    }

    #[test]
    fn generated_history_markers_are_detected() {
        let repo_path = temp_dir_path("generated-history");
        init_git_repo(&repo_path);
        run_git(
            repo_path.as_path(),
            [
                "commit",
                "--allow-empty",
                "-m",
                "bot work",
                "-m",
                "Generated: Claude",
            ],
        );

        assert!(repo_history_has_generated_markers(&repo_path, 25));
    }

    #[test]
    fn commit_check_defaults_are_seeded_once() {
        let mut manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: PathBuf::from("/tmp/example"),
            repos: vec![],
            shared_hooks_path: None,
            commit_check_rules: None,
        };

        assert!(ensure_commit_check_rules_initialized(&mut manifest));
        assert_eq!(manifest.commit_check_rules.as_ref().unwrap().len(), 4);
        assert!(!ensure_commit_check_rules_initialized(&mut manifest));

        manifest.commit_check_rules = Some(Vec::new());
        assert!(!ensure_commit_check_rules_initialized(&mut manifest));
        assert_eq!(manifest.commit_check_rules.as_ref().unwrap().len(), 0);
    }

    #[test]
    fn later_commit_hash_allow_rule_unblocks_blocked_commit() {
        let repo_path = temp_dir_path("commit-hash-allow");
        init_git_repo(&repo_path);
        run_git(
            repo_path.as_path(),
            [
                "commit",
                "--allow-empty",
                "-m",
                "bot work",
                "-m",
                "Generated: Claude",
            ],
        );
        let hash = git_stdout(&repo_path, ["rev-parse", "HEAD"]).unwrap();

        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: repo_path,
            repos: vec![],
            shared_hooks_path: None,
            commit_check_rules: Some(vec![
                CommitCheckRule {
                    id: "block-generated".to_string(),
                    name: "Block generated".to_string(),
                    enabled: true,
                    priority: 0,
                    effect: CommitCheckRuleEffect::Block,
                    scope: CommitCheckRuleScope::All,
                    matcher: CommitCheckRuleMatcher::Regex {
                        pattern: "(?i)generated:".to_string(),
                    },
                },
                CommitCheckRule {
                    id: "allow-hash".to_string(),
                    name: "Allow hash".to_string(),
                    enabled: true,
                    priority: 10,
                    effect: CommitCheckRuleEffect::Allow,
                    scope: CommitCheckRuleScope::All,
                    matcher: CommitCheckRuleMatcher::CommitHash { hash },
                },
            ]),
        };

        let report = collect_commit_check_report(&manifest, &[], 25);
        assert!(report.invalid_rules.is_empty());
        assert!(report.matches.is_empty());
    }

    #[test]
    fn invalid_regex_is_reported_without_blocking() {
        let repo_path = temp_dir_path("invalid-regex");
        init_git_repo(&repo_path);
        run_git(
            repo_path.as_path(),
            [
                "commit",
                "--allow-empty",
                "-m",
                "bot work",
                "-m",
                "Generated: Claude",
            ],
        );

        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: repo_path,
            repos: vec![],
            shared_hooks_path: None,
            commit_check_rules: Some(vec![CommitCheckRule {
                id: "bad-regex".to_string(),
                name: "Bad regex".to_string(),
                enabled: true,
                priority: 0,
                effect: CommitCheckRuleEffect::Block,
                scope: CommitCheckRuleScope::All,
                matcher: CommitCheckRuleMatcher::Regex {
                    pattern: "[".to_string(),
                },
            }]),
        };

        let report = collect_commit_check_report(&manifest, &[], 25);
        assert_eq!(report.invalid_rules.len(), 1);
        assert!(report.matches.is_empty());
    }

    #[test]
    fn push_is_blocked_when_generated_history_is_present() {
        let workspace = temp_dir_path("push-preflight");
        fs::create_dir_all(&workspace).unwrap();
        run_git(workspace.as_path(), ["init", "-b", "main"]);
        run_git(
            workspace.as_path(),
            ["config", "user.name", "Ronomepo Tests"],
        );
        run_git(
            workspace.as_path(),
            ["config", "user.email", "tests@example.com"],
        );
        run_git(
            workspace.as_path(),
            [
                "commit",
                "--allow-empty",
                "-m",
                "workspace bot",
                "-m",
                "Generated-by: Agent",
            ],
        );

        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: workspace,
            repos: vec![],
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        let mut events = Vec::new();
        run_workspace_operation(&manifest, &[], OperationKind::Push, |event| {
            events.push(event);
        });

        assert!(events
            .iter()
            .any(|event| matches!(event.kind, OperationEventKind::Failed)
                && event.message.contains("Push aborted")));
    }

    #[test]
    fn apply_hooks_configures_workspace_root_too() {
        let workspace = temp_dir_path("apply-hooks-root");
        let repo_path = workspace.join("alpha");
        let hooks_path = workspace.join("hooks");
        fs::create_dir_all(&hooks_path).unwrap();
        init_git_repo(&workspace);
        init_git_repo(&repo_path);

        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: workspace.clone(),
            repos: vec![RepositoryEntry {
                id: "alpha".to_string(),
                name: "alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "git@example.com:alpha.git".to_string(),
                enabled: true,
            }],
            shared_hooks_path: Some(hooks_path.clone()),
            commit_check_rules: Some(default_commit_check_rules()),
        };

        let mut events = Vec::new();
        run_workspace_operation(&manifest, &[], OperationKind::ApplyHooks, |event| {
            events.push(event);
        });

        let root_hooks = git_stdout(&workspace, ["config", "--local", "core.hooksPath"]);
        assert_eq!(
            root_hooks.as_deref(),
            Some(hooks_path.to_string_lossy().as_ref())
        );
        assert!(events
            .iter()
            .any(|event| event.repository_name.as_deref() == Some("(monorepo)")));
    }

    #[test]
    fn capability_policy_reports_missing_required_item_capabilities() {
        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("sample".to_string()),
            items: vec![RepoItem {
                id: "server".to_string(),
                item_type: "cargo".to_string(),
                path: PathBuf::from("server"),
                config: None,
                artifacts: Vec::new(),
                actions: Vec::new(),
            }],
            actions: Vec::new(),
            capabilities: Vec::new(),
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let report = assess_repo_capability_policy(&manifest);
        assert_eq!(report.issues.len(), 4);
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.capability == "dependencies.outdated"));
    }

    #[test]
    fn not_applicable_required_capability_needs_reason() {
        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("sample".to_string()),
            items: vec![RepoItem {
                id: "script".to_string(),
                item_type: "python".to_string(),
                path: PathBuf::from("."),
                config: None,
                artifacts: Vec::new(),
                actions: Vec::new(),
            }],
            actions: Vec::new(),
            capabilities: vec![RepoCapabilityDeclaration {
                id: "script/dependencies/outdated".to_string(),
                capability: "dependencies.outdated".to_string(),
                status: CapabilityDeclarationStatus::NotApplicable,
                item_id: Some("script".to_string()),
                root: None,
                action_ref: None,
                standard_action: None,
                reason: None,
                schedule: None,
            }],
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let error = validate_repo_manifest(&manifest).unwrap_err();
        assert!(
            matches!(error, WorkspaceError::InvalidRepoManifest(message) if message.contains("must include a reason"))
        );
    }

    #[test]
    fn capability_can_plan_repo_defined_action() {
        let repo_root = temp_dir_path("capability-plan");
        let manifest = RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("sample".to_string()),
            items: vec![RepoItem {
                id: "server".to_string(),
                item_type: "cargo".to_string(),
                path: PathBuf::from("server"),
                config: None,
                artifacts: Vec::new(),
                actions: Vec::new(),
            }],
            actions: vec![RepoActionDefinition {
                id: "server/dependencies/outdated".to_string(),
                command: vec!["./scripts/check-deps".to_string()],
                workdir: Some(PathBuf::from("server")),
                env: BTreeMap::new(),
                timeout_seconds: Some(120),
                output: ActionOutputMode::Json,
            }],
            capabilities: vec![RepoCapabilityDeclaration {
                id: "server/dependencies/outdated-capability".to_string(),
                capability: "dependencies.outdated".to_string(),
                status: CapabilityDeclarationStatus::Implemented,
                item_id: Some("server".to_string()),
                root: None,
                action_ref: Some(RepoActionRef::RepoAction {
                    id: "server/dependencies/outdated".to_string(),
                }),
                standard_action: None,
                reason: None,
                schedule: None,
            }],
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        };

        let plan = plan_capability_action(
            &repo_root,
            &manifest,
            "server/dependencies/outdated-capability",
        )
        .unwrap();
        assert_eq!(plan.capability, "dependencies.outdated");
        assert_eq!(plan.steps.len(), 1);
        match &plan.steps[0].executor {
            RepoActionExecutor::Command(command) => {
                assert_eq!(command.program, "./scripts/check-deps");
                assert_eq!(command.workdir, repo_root.join("server"));
                assert_eq!(command.output, ActionOutputMode::Json);
            }
            RepoActionExecutor::BuiltInInspector => panic!("expected command executor"),
        }
    }

    #[test]
    fn capability_registry_round_trips_json() {
        let path = temp_file_path("capability-registry");
        let registry = CapabilityRegistry {
            schema_version: 1,
            states: vec![CapabilityState {
                repo_id: "sample".to_string(),
                capability_instance_id: "server/test".to_string(),
                capability: "integrity.tests".to_string(),
                item_id: Some("server".to_string()),
                root: Some(PathBuf::from("server")),
                status: CapabilityResultStatus::Ok,
                summary: "Tests passed".to_string(),
                findings: Vec::new(),
                checked_at_epoch_secs: Some(1),
                commit: Some("abc".to_string()),
                dirty: false,
                duration_ms: Some(42),
            }],
        };

        save_capability_registry(&path, &registry).unwrap();
        let loaded = load_capability_registry(&path).unwrap();
        assert_eq!(loaded, registry);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn attention_signals_include_git_and_required_capability_policy() {
        let manifest = cargo_repo_manifest(Vec::new());
        let item = attention_test_item(
            RepositoryState::Clean,
            RepositorySync::Behind(2),
            Some("abc"),
            Some(&manifest),
        );

        let signals =
            derive_attention_signals(&item, Some(&manifest), &CapabilityRegistry::default());

        assert!(signals.iter().any(|signal| {
            signal.source == AttentionSource::Git
                && signal.source_id == "behind"
                && signal.level == AttentionLevel::ShouldDo
        }));
        let policy_signals = signals
            .iter()
            .filter(|signal| signal.source == AttentionSource::Manifest)
            .collect::<Vec<_>>();
        assert_eq!(policy_signals.len(), 4);
        assert!(policy_signals
            .iter()
            .any(|signal| signal.source_id.contains("dependencies.outdated")));
        assert_eq!(attention_rank(&signals), 2);
    }

    #[test]
    fn attention_signals_mark_capability_results_stale_after_commit_change() {
        let manifest = cargo_repo_manifest(vec![implemented_capability(
            "server/dependencies/outdated",
            "dependencies.outdated",
        )]);
        let item = attention_test_item(
            RepositoryState::Clean,
            RepositorySync::UpToDate,
            Some("new"),
            Some(&manifest),
        );
        let registry = CapabilityRegistry {
            schema_version: 1,
            states: vec![CapabilityState {
                repo_id: "sample".to_string(),
                capability_instance_id: "server/dependencies/outdated".to_string(),
                capability: "dependencies.outdated".to_string(),
                item_id: Some("server".to_string()),
                root: Some(PathBuf::from("server")),
                status: CapabilityResultStatus::Ok,
                summary: "dependencies are current".to_string(),
                findings: Vec::new(),
                checked_at_epoch_secs: Some(10),
                commit: Some("old".to_string()),
                dirty: false,
                duration_ms: Some(100),
            }],
        };

        let signals = derive_attention_signals(&item, Some(&manifest), &registry);
        let stale = signals
            .iter()
            .find(|signal| {
                signal.capability_instance_id.as_deref() == Some("server/dependencies/outdated")
            })
            .unwrap();
        assert_eq!(stale.source, AttentionSource::Capability);
        assert_eq!(stale.kind, AttentionKind::Maintenance);
        assert_eq!(stale.level, AttentionLevel::ShouldDo);
        assert!(stale.stale);
        assert!(stale.summary.contains("commit changed"));
        assert_eq!(stale.suggested_actions, vec!["run_capability_check"]);
    }

    #[test]
    fn attention_signals_report_unknown_implemented_capabilities() {
        let manifest = cargo_repo_manifest(vec![implemented_capability(
            "server/build",
            "integrity.build",
        )]);
        let item = attention_test_item(
            RepositoryState::Clean,
            RepositorySync::UpToDate,
            Some("abc"),
            Some(&manifest),
        );

        let signals =
            derive_attention_signals(&item, Some(&manifest), &CapabilityRegistry::default());
        let unknown = signals
            .iter()
            .find(|signal| signal.capability_instance_id.as_deref() == Some("server/build"))
            .unwrap();
        assert_eq!(unknown.source, AttentionSource::Capability);
        assert_eq!(unknown.kind, AttentionKind::Integrity);
        assert_eq!(unknown.level, AttentionLevel::ShouldDo);
        assert_eq!(unknown.suggested_actions, vec!["run_capability_check"]);
    }

    fn cargo_repo_manifest(capabilities: Vec<RepoCapabilityDeclaration>) -> RepoManifest {
        RepoManifest {
            schema_version: REPO_MANIFEST_SCHEMA_VERSION,
            repo_id: Some("sample".to_string()),
            items: vec![RepoItem {
                id: "server".to_string(),
                item_type: "cargo".to_string(),
                path: PathBuf::from("server"),
                config: None,
                artifacts: Vec::new(),
                actions: Vec::new(),
            }],
            actions: Vec::new(),
            capabilities,
            repo_actions: Vec::new(),
            aggregation: Vec::new(),
        }
    }

    fn implemented_capability(id: &str, capability: &str) -> RepoCapabilityDeclaration {
        RepoCapabilityDeclaration {
            id: id.to_string(),
            capability: capability.to_string(),
            status: CapabilityDeclarationStatus::Implemented,
            item_id: Some("server".to_string()),
            root: None,
            action_ref: None,
            standard_action: Some("cargo.test".to_string()),
            reason: None,
            schedule: None,
        }
    }

    fn attention_test_item(
        state: RepositoryState,
        sync: RepositorySync,
        head_commit: Option<&str>,
        manifest: Option<&RepoManifest>,
    ) -> RepositoryListItem {
        RepositoryListItem {
            id: "sample".to_string(),
            name: "Sample".to_string(),
            dir_name: "sample".to_string(),
            remote_url: "git@example.com:sample.git".to_string(),
            status: RepositoryStatus {
                state,
                branch: Some("main".to_string()),
                sync,
                head_commit: head_commit.map(str::to_string),
                repo_path: PathBuf::from("/tmp/sample"),
            },
            repo_manifest: manifest.map(|manifest| RepoManifestScan {
                path: PathBuf::from("/tmp/sample/ronomepo.repo.json"),
                state: RepoManifestScanState::Valid(repo_manifest_summary(
                    Path::new("/tmp/sample"),
                    manifest,
                )),
            }),
        }
    }

    fn init_git_repo(path: &Path) {
        fs::create_dir_all(path).unwrap();
        run_git(path, ["init", "-b", "main"]);
        run_git(path, ["config", "user.name", "Ronomepo Tests"]);
        run_git(path, ["config", "user.email", "tests@example.com"]);
        fs::write(path.join("README.md"), "hello\n").unwrap();
        run_git(path, ["add", "README.md"]);
        run_git(path, ["commit", "-m", "init"]);
    }

    fn run_git<const N: usize>(path: &Path, args: [&str; N]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(path)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn temp_file_path(prefix: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ronomepo-{prefix}-{stamp}.json"))
    }

    fn temp_dir_path(prefix: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ronomepo-{prefix}-{stamp}"))
    }
}

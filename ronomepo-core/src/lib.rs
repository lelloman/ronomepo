use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use regex::Regex;
use serde::{Deserialize, Serialize};

pub const MANIFEST_FILE_NAME: &str = "ronomepo.json";
pub const REPO_MANIFEST_FILE_NAME: &str = "ronomepo.repo.json";
pub const REPO_MANIFEST_SCHEMA_VERSION: u32 = 2;
pub const FUCINA_POLICY_FILE_NAME: &str = "repos.toml";
pub const FUCINA_POLICY_SCRIPT_NAME: &str = "fucina_policy.py";

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct FucinaPolicyManifest {
    pub schema_version: u32,
    pub repositories: Vec<FucinaRepositoryPolicy>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct FucinaRepositoryPolicy {
    pub name: String,
    pub role: String,
    pub publication: String,
    pub forgejo: String,
    pub upstream: Option<String>,
    pub publication_target: Option<String>,
    pub mirror_schedule: Option<String>,
    pub archived: bool,
    pub backup_required: bool,
    #[serde(default)]
    pub backups: Vec<FucinaBackupPolicy>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct FucinaBackupPolicy {
    pub destination: String,
    pub mode: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FucinaPolicyAction {
    Validate,
    Audit,
    ConfigurePreview,
    ConfigureApply,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FucinaPolicyCommandResult {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

pub fn load_fucina_policy(workspace_root: &Path) -> Result<FucinaPolicyManifest, String> {
    let path = workspace_root.join(FUCINA_POLICY_FILE_NAME);
    let raw = fs::read_to_string(&path)
        .map_err(|error| format!("Cannot read {}: {error}", path.display()))?;
    toml::from_str(&raw).map_err(|error| format!("Cannot parse {}: {error}", path.display()))
}

pub fn run_fucina_policy(
    workspace_root: &Path,
    action: FucinaPolicyAction,
    repositories: &[String],
) -> Result<FucinaPolicyCommandResult, String> {
    let script = workspace_root.join(FUCINA_POLICY_SCRIPT_NAME);
    if !script.is_file() {
        return Err(format!(
            "Fucina policy engine not found at {}",
            script.display()
        ));
    }

    let mut command = Command::new("python3");
    command.current_dir(workspace_root).arg(&script);
    match action {
        FucinaPolicyAction::Validate => {
            command.arg("validate");
        }
        FucinaPolicyAction::Audit => {
            command.arg("audit").args(repositories);
        }
        FucinaPolicyAction::ConfigurePreview => {
            command.arg("configure").args(repositories);
        }
        FucinaPolicyAction::ConfigureApply => {
            command.arg("configure").args(repositories).arg("--apply");
        }
    }
    let output = command
        .output()
        .map_err(|error| format!("Cannot run {}: {error}", script.display()))?;
    Ok(FucinaPolicyCommandResult {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

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
    #[serde(default)]
    pub next_due_at_epoch_secs: Option<u64>,
    #[serde(default)]
    pub last_scheduled_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilitySchedule {
    pub mode: CapabilityScheduleMode,
    pub interval_seconds: Option<u64>,
}

impl CapabilitySchedule {
    pub fn manual() -> Self {
        Self {
            mode: CapabilityScheduleMode::Manual,
            interval_seconds: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityScheduleMode {
    Manual,
    Interval,
    OnCommitChange,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityDueReason {
    NeverChecked,
    IntervalElapsed,
    CommitChanged,
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
    /// Push without the commit-check preflight; this is not `git push --force`.
    PushSkipChecks,
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
    normalize_workspace_root_with_home(path.as_ref(), env_home_dir())
}

fn normalize_workspace_root_with_home(path: &Path, home: Option<PathBuf>) -> PathBuf {
    let text = path.to_string_lossy();

    if text == "~" {
        return home.unwrap_or_else(|| path.to_path_buf());
    }

    if let Some(stripped) = text.strip_prefix("~/") {
        if let Some(home) = home {
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
        parse_capability_schedule(capability.schedule.as_ref())?;
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

pub fn parse_capability_schedule(
    value: Option<&serde_json::Value>,
) -> Result<CapabilitySchedule, WorkspaceError> {
    let Some(value) = value else {
        return Ok(CapabilitySchedule::manual());
    };

    match value {
        serde_json::Value::String(raw) => parse_capability_schedule_kind(raw, None),
        serde_json::Value::Object(object) => {
            let kind = object
                .get("kind")
                .or_else(|| object.get("mode"))
                .and_then(serde_json::Value::as_str);
            let interval_seconds = object
                .get("interval_seconds")
                .and_then(serde_json::Value::as_u64);
            let on_commit_change = object
                .get("on_commit_change")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);

            if let Some(kind) = kind {
                parse_capability_schedule_kind(kind, interval_seconds)
            } else if let Some(interval_seconds) = interval_seconds {
                interval_capability_schedule(interval_seconds)
            } else if on_commit_change {
                Ok(CapabilitySchedule {
                    mode: CapabilityScheduleMode::OnCommitChange,
                    interval_seconds: None,
                })
            } else {
                Ok(CapabilitySchedule::manual())
            }
        }
        _ => Err(WorkspaceError::InvalidRepoManifest(
            "capability schedule must be a string or object".to_string(),
        )),
    }
}

pub fn capability_next_due_at_epoch_secs(
    capability: &RepoCapabilityDeclaration,
    state: Option<&CapabilityState>,
) -> Result<Option<u64>, WorkspaceError> {
    let schedule = parse_capability_schedule(capability.schedule.as_ref())?;
    if schedule.mode != CapabilityScheduleMode::Interval {
        return Ok(None);
    }
    Ok(state
        .and_then(|state| state.checked_at_epoch_secs)
        .zip(schedule.interval_seconds)
        .map(|(checked_at, interval)| checked_at.saturating_add(interval)))
}

pub fn capability_due_reason(
    item: &RepositoryListItem,
    capability: &RepoCapabilityDeclaration,
    state: Option<&CapabilityState>,
    now_epoch_secs: u64,
) -> Result<Option<CapabilityDueReason>, WorkspaceError> {
    if capability.status != CapabilityDeclarationStatus::Implemented {
        return Ok(None);
    }

    let schedule = parse_capability_schedule(capability.schedule.as_ref())?;
    match schedule.mode {
        CapabilityScheduleMode::Manual => Ok(None),
        CapabilityScheduleMode::Interval => {
            let Some(interval_seconds) = schedule.interval_seconds else {
                return Ok(None);
            };
            let Some(state) = state else {
                return Ok(Some(CapabilityDueReason::NeverChecked));
            };
            let Some(checked_at) = state.checked_at_epoch_secs else {
                return Ok(Some(CapabilityDueReason::NeverChecked));
            };
            Ok(
                (checked_at.saturating_add(interval_seconds) <= now_epoch_secs)
                    .then_some(CapabilityDueReason::IntervalElapsed),
            )
        }
        CapabilityScheduleMode::OnCommitChange => {
            let Some(state) = state else {
                return Ok(Some(CapabilityDueReason::NeverChecked));
            };
            Ok(state
                .commit
                .as_ref()
                .zip(item.status.head_commit.as_ref())
                .and_then(|(recorded, current)| {
                    (recorded != current).then_some(CapabilityDueReason::CommitChanged)
                }))
        }
    }
}

fn parse_capability_schedule_kind(
    kind: &str,
    interval_seconds: Option<u64>,
) -> Result<CapabilitySchedule, WorkspaceError> {
    match kind {
        "manual" => Ok(CapabilitySchedule::manual()),
        "interval" => interval_capability_schedule(interval_seconds.ok_or_else(|| {
            WorkspaceError::InvalidRepoManifest(
                "interval capability schedule requires interval_seconds".to_string(),
            )
        })?),
        "on_commit_change" => Ok(CapabilitySchedule {
            mode: CapabilityScheduleMode::OnCommitChange,
            interval_seconds: None,
        }),
        _ => Err(WorkspaceError::InvalidRepoManifest(format!(
            "unknown capability schedule kind: {kind}"
        ))),
    }
}

fn interval_capability_schedule(
    interval_seconds: u64,
) -> Result<CapabilitySchedule, WorkspaceError> {
    if interval_seconds == 0 {
        return Err(WorkspaceError::InvalidRepoManifest(
            "capability schedule interval_seconds must be greater than zero".to_string(),
        ));
    }
    Ok(CapabilitySchedule {
        mode: CapabilityScheduleMode::Interval,
        interval_seconds: Some(interval_seconds),
    })
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
    if !is_git_repository(repo_path) {
        return RepositoryStatus {
            state: RepositoryState::Missing,
            branch: None,
            sync: RepositorySync::NoUpstream,
            head_commit: None,
            repo_path: repo_path.to_path_buf(),
        };
    }

    let status = porcelain_status(repo_path);
    RepositoryStatus {
        state: status.state,
        branch: status.branch,
        sync: status.sync,
        head_commit: status.head_commit,
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
    if !is_git_repository(repo_path) {
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
        // The subject goes last so separators inside it cannot shift fields.
        last_commit: git_stdout(repo_path, ["log", "-1", "--format=%h%x00%ct%x00%s"]).and_then(
            |output| {
                let mut parts = output.splitn(3, '\0');
                let short_sha = parts.next()?;
                let committed_at_epoch_secs = parts.next()?.parse().ok()?;
                let subject = parts.next().unwrap_or_default();
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

/// Repositories an operation batch works on concurrently. Network-bound
/// operations dominate, so a small pool hides most of the latency without
/// hammering a single remote host.
const MAX_PARALLEL_REPO_OPERATIONS: usize = 4;

/// Runs `kind` for the selected repositories (all enabled ones when empty).
///
/// Events are emitted as they happen: `Started` first, then the workspace
/// root's outcome (when the operation includes it), then one outcome per
/// repository in completion order, and `Finished` last. Repositories are
/// processed in parallel, so `emit` may be called from several threads.
pub fn run_workspace_operation<F>(
    manifest: &WorkspaceManifest,
    selected_repo_ids: &[String],
    kind: OperationKind,
    emit: F,
) where
    F: Fn(OperationEvent) + Sync,
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

    let tally = std::sync::Mutex::new(OperationTally::default());
    let emit_outcome = |event: OperationEvent| {
        tally
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .record(event.kind);
        emit(event);
    };

    if matches!(kind, OperationKind::ApplyHooks) {
        emit_outcome(apply_hooks_to_workspace_root(manifest));
    }

    if matches!(kind, OperationKind::Pull) && selected_repo_ids.is_empty() {
        emit_outcome(pull_target(&OperationTarget::workspace_root(manifest)));
    }

    if matches!(kind, OperationKind::Push) {
        let report = push_preflight_report(manifest, selected_repo_ids);
        if !report.invalid_rules.is_empty() {
            // There is no warning event kind; a Started event is logged
            // without counting as an outcome.
            emit(OperationEvent {
                kind: OperationEventKind::Started,
                repository_id: None,
                repository_name: None,
                message: format!(
                    "Warning: ignoring invalid commit check rules: {}.",
                    report
                        .invalid_rules
                        .iter()
                        .map(|rule| format!("{} ({})", rule.rule_name, rule.message))
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
            });
        }
        let flagged = report
            .matches
            .into_iter()
            .map(|entry| entry.repository_name)
            .collect::<BTreeSet<_>>();
        if !flagged.is_empty() {
            emit_outcome(OperationEvent {
                kind: OperationEventKind::Failed,
                repository_id: None,
                repository_name: None,
                message: format!(
                    "Push aborted because commit check rules blocked commits in: {}. Use Push (skip checks) to override.",
                    flagged.into_iter().collect::<Vec<_>>().join(", ")
                ),
            });
            emit(finished_event(kind, &tally));
            return;
        }
    }

    if matches!(kind, OperationKind::Push | OperationKind::PushSkipChecks)
        && selected_repo_ids.is_empty()
    {
        emit_outcome(push_target(&OperationTarget::workspace_root(manifest)));
    }

    let next_entry = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..entries.len().min(MAX_PARALLEL_REPO_OPERATIONS) {
            scope.spawn(|| loop {
                let index = next_entry.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(repo) = entries.get(index) else {
                    break;
                };
                emit_outcome(match kind {
                    OperationKind::CloneMissing => clone_missing_repo(manifest, repo),
                    OperationKind::Pull => pull_target(&OperationTarget::repo(manifest, repo)),
                    OperationKind::Push | OperationKind::PushSkipChecks => {
                        push_target(&OperationTarget::repo(manifest, repo))
                    }
                    OperationKind::ApplyHooks => apply_hooks_repo(manifest, repo),
                });
            });
        }
    });

    emit(finished_event(kind, &tally));
}

#[derive(Default)]
struct OperationTally {
    completed: usize,
    skipped: usize,
    failed: usize,
}

impl OperationTally {
    fn record(&mut self, kind: OperationEventKind) {
        match kind {
            OperationEventKind::Success => self.completed += 1,
            OperationEventKind::Skipped => self.skipped += 1,
            OperationEventKind::Failed => self.failed += 1,
            OperationEventKind::Started | OperationEventKind::Finished => {}
        }
    }
}

fn finished_event(kind: OperationKind, tally: &std::sync::Mutex<OperationTally>) -> OperationEvent {
    let tally = tally
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    OperationEvent {
        kind: OperationEventKind::Finished,
        repository_id: None,
        repository_name: None,
        message: format!(
            "{} finished: {} completed, {} skipped, {} failed.",
            operation_kind_label(kind),
            tally.completed,
            tally.skipped,
            tally.failed
        ),
    }
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
    commit_check_report(
        manifest,
        selected_repo_ids,
        true,
        CommitScanRange::Recent(num_commits),
    )
}

/// The push preflight only looks at what a push would publish, and at the
/// workspace root only when the root is pushed too (no explicit selection).
fn push_preflight_report(
    manifest: &WorkspaceManifest,
    selected_repo_ids: &[String],
) -> CommitCheckReport {
    commit_check_report(
        manifest,
        selected_repo_ids,
        selected_repo_ids.is_empty(),
        CommitScanRange::Unpushed {
            fallback: PUSH_PREFLIGHT_FALLBACK_COMMITS,
        },
    )
}

/// Commits checked for a branch without an upstream, where the unpushed
/// range cannot be computed.
const PUSH_PREFLIGHT_FALLBACK_COMMITS: usize = 25;

/// Which commits of a repository the commit check scans.
#[derive(Clone, Copy, Debug)]
enum CommitScanRange {
    /// The most recent commits of HEAD.
    Recent(usize),
    /// `@{upstream}..HEAD`, or the most recent `fallback` commits when the
    /// branch has no upstream.
    Unpushed { fallback: usize },
}

fn commit_check_report(
    manifest: &WorkspaceManifest,
    selected_repo_ids: &[String],
    include_workspace_root: bool,
    range: CommitScanRange,
) -> CommitCheckReport {
    let rules = manifest
        .commit_check_rules
        .clone()
        .unwrap_or_else(default_commit_check_rules);
    let compiled_rules = compile_commit_check_rules(&rules);
    let mut commits = if include_workspace_root {
        collect_repo_recent_commits(&manifest.root, None, "(monorepo)", range)
    } else {
        Vec::new()
    };

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
            range,
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

    let mut command = network_git_command(None);
    command
        .arg("clone")
        .arg("--")
        .arg(&repo.remote_url)
        .arg(&repo_path);
    match run_with_timeout(command, NETWORK_GIT_TIMEOUT) {
        Ok(output) if output.status.success() => {
            if let Some(hooks_path) = resolved_shared_hooks_path(manifest) {
                if let Err(error) = configure_hooks_path(&repo_path, &hooks_path) {
                    return failed_event(
                        repo,
                        format!(
                            "Cloned {} but failed to configure hooks: {error}",
                            repo.dir_name
                        ),
                    );
                }
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
        Err(error) => {
            // A killed clone leaves a partial checkout that would make every
            // later Clone Missing skip the repository.
            if error.kind() == io::ErrorKind::TimedOut {
                let _ = fs::remove_dir_all(&repo_path);
            }
            failed_event(repo, format!("Clone failed for {}: {error}", repo.dir_name))
        }
    }
}

/// A repository targeted by pull/push: a manifest entry or the workspace root.
struct OperationTarget {
    repository_id: Option<String>,
    repository_name: String,
    label: String,
    path: PathBuf,
    /// The workspace root is only included implicitly, so a root that is not a
    /// git repository is skipped instead of reported as a failure.
    is_workspace_root: bool,
}

impl OperationTarget {
    fn repo(manifest: &WorkspaceManifest, repo: &RepositoryEntry) -> Self {
        Self {
            repository_id: Some(repo.id.clone()),
            repository_name: repo.name.clone(),
            label: repo.dir_name.clone(),
            path: manifest.root.join(&repo.dir_name),
            is_workspace_root: false,
        }
    }

    fn workspace_root(manifest: &WorkspaceManifest) -> Self {
        Self {
            repository_id: None,
            repository_name: "workspace root".to_string(),
            label: "workspace root".to_string(),
            path: manifest.root.clone(),
            is_workspace_root: true,
        }
    }

    fn event(&self, kind: OperationEventKind, message: String) -> OperationEvent {
        OperationEvent {
            kind,
            repository_id: self.repository_id.clone(),
            repository_name: Some(self.repository_name.clone()),
            message,
        }
    }

    fn skipped(&self, reason: &str) -> OperationEvent {
        self.event(
            OperationEventKind::Skipped,
            format!("Skipped {} because {reason}.", self.label),
        )
    }

    fn failed(&self, message: String) -> OperationEvent {
        self.event(OperationEventKind::Failed, message)
    }

    /// Returns the event to report when the target cannot be operated on.
    fn unavailable_event(&self) -> Option<OperationEvent> {
        if self.is_workspace_root {
            return (!self.path.join(".git").exists())
                .then(|| self.skipped("it is not a git repository"));
        }
        if !self.path.exists() {
            return Some(self.failed(format!("{} is missing locally.", self.label)));
        }
        if !is_git_repository(&self.path) {
            return Some(self.failed(format!("{} is not a git repository.", self.label)));
        }
        None
    }
}

fn pull_target(target: &OperationTarget) -> OperationEvent {
    if let Some(event) = target.unavailable_event() {
        return event;
    }

    let state = repository_state(&target.path);
    if matches!(state, RepositoryState::Dirty) {
        return target.skipped("it has uncommitted changes");
    }

    let has_untracked = matches!(state, RepositoryState::Untracked);
    let mut command = network_git_command(Some(&target.path));
    command.arg("pull").arg("--quiet");
    match run_with_timeout(command, NETWORK_GIT_TIMEOUT) {
        Ok(output) if output.status.success() => {
            let suffix = if has_untracked {
                " Untracked files were present."
            } else {
                ""
            };
            target.event(
                OperationEventKind::Success,
                format!("Pulled {}.{suffix}", target.label),
            )
        }
        Ok(output) => target.failed(format!(
            "Pull failed for {}: {}",
            target.label,
            stderr_message(&output.stderr)
        )),
        Err(error) => target.failed(format!("Pull failed for {}: {error}", target.label)),
    }
}

fn push_target(target: &OperationTarget) -> OperationEvent {
    if let Some(event) = target.unavailable_event() {
        return event;
    }

    let has_upstream = git_stdout(&target.path, ["rev-parse", "--abbrev-ref", "@{upstream}"])
        .is_some_and(|upstream| !upstream.is_empty());
    if !has_upstream {
        return target.skipped("it has no upstream");
    }

    let ahead = git_stdout(&target.path, ["rev-list", "--count", "@{upstream}..HEAD"])
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    if ahead == 0 {
        return target.skipped("it has nothing to push");
    }

    let mut command = network_git_command(Some(&target.path));
    command.arg("push");
    match run_with_timeout(command, NETWORK_GIT_TIMEOUT) {
        Ok(output) if output.status.success() => target.event(
            OperationEventKind::Success,
            format!("Pushed {} ({} commits).", target.label, ahead),
        ),
        Ok(output) => target.failed(format!(
            "Push failed for {}: {}",
            target.label,
            stderr_message(&output.stderr)
        )),
        Err(error) => target.failed(format!("Push failed for {}: {error}", target.label)),
    }
}

fn apply_hooks_repo(manifest: &WorkspaceManifest, repo: &RepositoryEntry) -> OperationEvent {
    let repo_path = manifest.root.join(&repo.dir_name);
    if !repo_path.exists() {
        return failed_event(repo, format!("{} is missing locally.", repo.dir_name));
    }
    if !is_git_repository(&repo_path) {
        return failed_event(repo, format!("{} is not a git repository.", repo.dir_name));
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
    let status = repo_git(repo_path)
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
        OperationKind::PushSkipChecks => "Push (skip checks)",
        OperationKind::ApplyHooks => "Apply Hooks",
    }
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

/// Commit-hash rules accept abbreviated hashes, as long as they are not so
/// short that they are likely to match unrelated commits.
const MIN_COMMIT_HASH_PREFIX_LEN: usize = 7;

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
                let hash = hash.trim().to_ascii_lowercase();
                if hash.len() < MIN_COMMIT_HASH_PREFIX_LEN
                    || !hash.chars().all(|c| c.is_ascii_hexdigit())
                {
                    invalid.push(InvalidCommitCheckRule {
                        rule_id: rule.id.clone(),
                        rule_name: rule.name.clone(),
                        message: format!(
                            "commit hash must be at least {MIN_COMMIT_HASH_PREFIX_LEN} hex characters"
                        ),
                    });
                    continue;
                }
                CompiledCommitCheckMatcher::CommitHash(hash)
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
    range: CommitScanRange,
) -> Vec<RecentCommit> {
    // NUL separates fields and RS separates commits, so no subject or body
    // text can be mistaken for structure. Signature output would otherwise be
    // mixed into the log when log.showSignature is configured.
    let mut command = read_only_git(repo_path);
    command.args(["log", "--no-show-signature", "--format=%H%x00%s%x00%b%x1e"]);
    match range {
        CommitScanRange::Recent(limit) => {
            command.arg("-n").arg(limit.to_string());
        }
        CommitScanRange::Unpushed { fallback } => {
            let has_upstream = git_stdout(repo_path, ["rev-parse", "--abbrev-ref", "@{upstream}"])
                .is_some_and(|upstream| !upstream.is_empty());
            if has_upstream {
                command.arg("@{upstream}..HEAD");
            } else {
                command.arg("-n").arg(fallback.to_string());
            }
        }
    }
    let Some(output) = command
        .stderr(Stdio::null())
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
    else {
        return Vec::new();
    };

    output
        .split('\x1e')
        .map(|record| record.trim_start_matches('\n'))
        .filter(|record| !record.is_empty())
        .filter_map(|record| {
            let mut fields = record.splitn(3, '\0');
            let hash = fields.next()?;
            let subject = fields.next().unwrap_or_default();
            let body = fields.next().unwrap_or_default();
            Some((hash, subject, body))
        })
        .enumerate()
        .map(|(head_offset, (hash, subject, body))| RecentCommit {
            repository_id: repository_id.map(ToOwned::to_owned),
            repository_name: repository_name.to_string(),
            head_offset,
            commit_hash: hash.to_string(),
            subject: subject.to_string(),
            body_lines: body.trim_end().lines().map(ToOwned::to_owned).collect(),
        })
        .collect()
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
        CompiledCommitCheckMatcher::CommitHash(hash) => commit
            .commit_hash
            .to_ascii_lowercase()
            .starts_with(hash.as_str())
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

    let mut command = read_only_git(repo_path);
    command.arg("log");
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

fn repository_state(repo_path: &Path) -> RepositoryState {
    porcelain_status(repo_path).state
}

/// Everything the status scan needs, read from a single `git status` call.
#[derive(Debug, PartialEq, Eq)]
struct PorcelainStatus {
    state: RepositoryState,
    branch: Option<String>,
    sync: RepositorySync,
    head_commit: Option<String>,
}

fn porcelain_status(repo_path: &Path) -> PorcelainStatus {
    let output = read_only_git(repo_path)
        .args([
            "status",
            "--porcelain=v2",
            "--branch",
            "--untracked-files=normal",
            "-z",
        ])
        .stderr(Stdio::null())
        .output();
    match output {
        Ok(output) if output.status.success() => parse_porcelain_v2_status(&output.stdout),
        _ => PorcelainStatus {
            state: RepositoryState::Unknown,
            branch: None,
            sync: RepositorySync::Unknown,
            head_commit: None,
        },
    }
}

/// Parses `git status --porcelain=v2 --branch -z`. An upstream that is
/// configured but gone has no `branch.ab` header and is treated like no
/// upstream at all, as is a detached HEAD.
fn parse_porcelain_v2_status(output: &[u8]) -> PorcelainStatus {
    let mut head_commit = None;
    let mut branch = None;
    let mut ahead_behind = None;
    let mut has_changes = false;
    let mut has_untracked = false;

    let mut records = output.split(|byte| *byte == 0);
    while let Some(record) = records.next() {
        let record = String::from_utf8_lossy(record);
        if let Some(header) = record.strip_prefix("# ") {
            let (key, value) = header.split_once(' ').unwrap_or((header, ""));
            match key {
                "branch.oid" if value != "(initial)" => head_commit = Some(value.to_string()),
                "branch.head" if value == "(detached)" => branch = Some("detached".to_string()),
                "branch.head" => branch = Some(value.to_string()),
                "branch.ab" => {
                    let mut counts = value.split(' ');
                    let ahead = counts.next().and_then(|v| v.strip_prefix('+'));
                    let behind = counts.next().and_then(|v| v.strip_prefix('-'));
                    ahead_behind = ahead
                        .and_then(|v| v.parse::<usize>().ok())
                        .zip(behind.and_then(|v| v.parse::<usize>().ok()));
                }
                _ => {}
            }
            continue;
        }
        match record.as_bytes().first() {
            Some(b'1') | Some(b'u') => has_changes = true,
            Some(b'2') => {
                has_changes = true;
                // Renames and copies carry their original path as an extra record.
                records.next();
            }
            Some(b'?') => has_untracked = true,
            _ => {}
        }
    }

    let state = if has_changes {
        RepositoryState::Dirty
    } else if has_untracked {
        RepositoryState::Untracked
    } else {
        RepositoryState::Clean
    };
    let sync = match ahead_behind {
        None => RepositorySync::NoUpstream,
        Some((0, 0)) => RepositorySync::UpToDate,
        Some((ahead, 0)) => RepositorySync::Ahead(ahead),
        Some((0, behind)) => RepositorySync::Behind(behind),
        Some((ahead, behind)) => RepositorySync::Diverged { ahead, behind },
    };

    PorcelainStatus {
        state,
        branch,
        sync,
        head_commit,
    }
}

/// A repository directory only counts when it has its own `.git`; otherwise a
/// plain directory inside another checkout would resolve to that checkout.
fn is_git_repository(repo_path: &Path) -> bool {
    repo_path.join(".git").exists()
}

/// `git -C <repo_path>` that never discovers a repository above `repo_path`,
/// so a directory that is not itself a repository fails instead of operating
/// on an enclosing checkout.
fn repo_git(repo_path: &Path) -> Command {
    let mut command = Command::new("git");
    if let Some(parent) = std::path::absolute(repo_path)
        .ok()
        .and_then(|path| path.parent().map(Path::to_path_buf))
    {
        command.env("GIT_CEILING_DIRECTORIES", parent);
    }
    command.arg("-C").arg(repo_path);
    command
}

/// Upper bound for a single network git command (clone, pull, push, fetch)
/// before it is killed and reported as failed.
pub const NETWORK_GIT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// A git command for talking to remotes that can never block on a prompt:
/// terminal credential prompts are disabled and ssh runs in batch mode, so a
/// missing key or unknown host fails instead of hanging a worker forever.
/// `repo_path` is used as `-C` (with the same ceiling as `repo_git`).
pub fn network_git_command(repo_path: Option<&Path>) -> Command {
    let mut command = match repo_path {
        Some(repo_path) => repo_git(repo_path),
        None => Command::new("git"),
    };
    command.env("GIT_TERMINAL_PROMPT", "0");
    if let Some(ssh_command) = batch_mode_ssh_command(repo_path) {
        command.env("GIT_SSH_COMMAND", ssh_command);
    }
    command
}

/// GIT_SSH_COMMAND overrides core.sshCommand, so the user's configured ssh
/// command is kept and only `-o BatchMode=yes` is appended. A plain GIT_SSH
/// program cannot take extra options and is left alone.
fn batch_mode_ssh_command(repo_path: Option<&Path>) -> Option<String> {
    if std::env::var_os("GIT_SSH").is_some() {
        return None;
    }
    let configured = std::env::var("GIT_SSH_COMMAND")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            let mut config = match repo_path {
                Some(repo_path) => repo_git(repo_path),
                None => Command::new("git"),
            };
            let output = config
                .args(["config", "--get", "core.sshCommand"])
                .stderr(Stdio::null())
                .output()
                .ok()?;
            let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
            (output.status.success() && !value.is_empty()).then_some(value)
        })
        .unwrap_or_else(|| "ssh".to_string());
    Some(format!("{configured} -o BatchMode=yes"))
}

/// Like `Command::output`, but kills the child once `timeout` elapses and
/// returns an `io::ErrorKind::TimedOut` error. stdin is closed.
pub fn run_with_timeout(mut command: Command, timeout: Duration) -> io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = drain_pipe(child.stdout.take());
    let stderr = drain_pipe(child.stderr.take());

    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out after {}s", timeout.as_secs()),
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    Ok(Output {
        status,
        stdout: stdout.finish(),
        stderr: stderr.finish(),
    })
}

/// Collects a child pipe on a helper thread. Reading is not joined
/// unconditionally: a grandchild such as a persistent ssh control master can
/// inherit the pipe and keep it open long after git itself has exited.
struct PipeDrain {
    buffer: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    done: std::sync::mpsc::Receiver<()>,
}

impl PipeDrain {
    fn finish(self) -> Vec<u8> {
        let _ = self.done.recv_timeout(Duration::from_secs(1));
        let buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        buffer.clone()
    }
}

fn drain_pipe(pipe: Option<impl Read + Send + 'static>) -> PipeDrain {
    let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (done_tx, done) = std::sync::mpsc::channel();
    if let Some(mut pipe) = pipe {
        let buffer = buffer.clone();
        std::thread::spawn(move || {
            let mut chunk = [0u8; 8192];
            loop {
                match pipe.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => buffer
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .extend_from_slice(&chunk[..read]),
                }
            }
            let _ = done_tx.send(());
        });
    }
    PipeDrain { buffer, done }
}

/// Status scans must not write: `git status` otherwise takes `.git/index.lock`
/// and may rewrite the index, which the repository watcher reports as a change
/// and answers with another scan.
fn read_only_git(repo_path: &Path) -> Command {
    let mut command = repo_git(repo_path);
    command.arg("--no-optional-locks");
    command
}

fn git_stdout<const N: usize>(repo_path: &Path, args: [&str; N]) -> Option<String> {
    let output = read_only_git(repo_path).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Test scratch path (file or directory) that is removed when dropped.
    struct TempPath(PathBuf);

    impl TempPath {
        fn as_path(&self) -> &Path {
            &self.0
        }
    }

    impl std::ops::Deref for TempPath {
        type Target = Path;

        fn deref(&self) -> &Path {
            &self.0
        }
    }

    impl AsRef<Path> for TempPath {
        fn as_ref(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            if self.0.is_dir() {
                let _ = fs::remove_dir_all(&self.0);
            } else {
                let _ = fs::remove_file(&self.0);
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
        let home = Some(PathBuf::from("/tmp/ronomepo-home"));

        assert_eq!(
            normalize_workspace_root_with_home(Path::new("~"), home.clone()),
            PathBuf::from("/tmp/ronomepo-home")
        );
        assert_eq!(
            normalize_workspace_root_with_home(Path::new("~/lelloprojects"), home.clone()),
            PathBuf::from("/tmp/ronomepo-home/lelloprojects")
        );
        assert_eq!(
            normalize_workspace_root_with_home(Path::new("/tmp/already-absolute"), home),
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
            root: workspace.to_path_buf(),
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
    fn plain_directory_inside_a_repository_is_not_treated_as_one() {
        let workspace = temp_dir_path("plain-dir");
        init_git_repo(&workspace);
        let parent_head = git_stdout(&workspace, ["rev-parse", "HEAD"]).unwrap();
        let plain = workspace.join("alpha");
        fs::create_dir_all(&plain).unwrap();
        fs::write(plain.join("notes.txt"), "not a repo").unwrap();

        let status = collect_repository_status(&plain);
        assert_eq!(status.state, RepositoryState::Missing);
        assert_eq!(status.head_commit, None);
        assert!(collect_repository_details(&plain).last_commit.is_none());
        assert_eq!(git_stdout(&plain, ["rev-parse", "HEAD"]), None);

        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: workspace.to_path_buf(),
            repos: vec![RepositoryEntry {
                id: "alpha".to_string(),
                name: "alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "git@example.com:alpha.git".to_string(),
                enabled: true,
            }],
            shared_hooks_path: None,
            commit_check_rules: Some(Vec::new()),
        };
        for kind in [OperationKind::Pull, OperationKind::Push] {
            let events = operation_events(&manifest, &["alpha".to_string()], kind);
            let repo_event = events
                .iter()
                .find(|event| event.repository_id.as_deref() == Some("alpha"))
                .unwrap();
            assert_eq!(repo_event.kind, OperationEventKind::Failed);
            assert!(repo_event.message.contains("not a git repository"));
        }
        assert_eq!(
            git_stdout(&workspace, ["rev-parse", "HEAD"]).as_deref(),
            Some(parent_head.as_str())
        );
    }

    #[test]
    fn run_with_timeout_kills_slow_commands() {
        let mut command = Command::new("sleep");
        command.arg("5");
        let started = Instant::now();
        let error = run_with_timeout(command, Duration::from_millis(200)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(4));

        let mut command = Command::new("sh");
        command.args(["-c", "echo out; echo err >&2; exit 3"]);
        let output = run_with_timeout(command, Duration::from_secs(10)).unwrap();
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(output.stdout, b"out\n");
        assert_eq!(output.stderr, b"err\n");
    }

    #[test]
    fn network_git_commands_never_prompt() {
        let command = network_git_command(None);
        let env = command
            .get_envs()
            .filter_map(|(key, value)| Some((key.to_str()?, value?.to_str()?)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(env.get("GIT_TERMINAL_PROMPT"), Some(&"0"));
        if std::env::var_os("GIT_SSH").is_none() {
            assert!(env["GIT_SSH_COMMAND"].ends_with("-o BatchMode=yes"));
        }
    }

    #[test]
    fn clone_passes_the_url_after_double_dash() {
        let workspace = temp_dir_path("clone-dash");
        fs::create_dir_all(&workspace).unwrap();
        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: workspace.to_path_buf(),
            repos: vec![RepositoryEntry {
                id: "alpha".to_string(),
                name: "alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "--upload-pack=touch injected".to_string(),
                enabled: true,
            }],
            shared_hooks_path: None,
            commit_check_rules: None,
        };

        let event = clone_missing_repo(&manifest, &manifest.repos[0]);
        assert_eq!(event.kind, OperationEventKind::Failed);
        // Parsed as an option, git would complain about the missing repository
        // argument instead of failing to reach the "URL".
        assert!(!event.message.contains("must specify a repository"));
        assert!(!workspace.join("injected").exists());
    }

    #[test]
    fn repository_status_reports_clean_dirty_and_untracked() {
        let repo_path = temp_dir_path("status-states");
        init_git_repo(&repo_path);
        let head = git_stdout(&repo_path, ["rev-parse", "HEAD"]);

        let status = collect_repository_status(&repo_path);
        assert_eq!(status.state, RepositoryState::Clean);
        assert_eq!(status.branch.as_deref(), Some("main"));
        assert_eq!(status.sync, RepositorySync::NoUpstream);
        assert_eq!(status.head_commit, head);

        fs::write(repo_path.join("scratch.txt"), "new").unwrap();
        assert_eq!(
            collect_repository_status(&repo_path).state,
            RepositoryState::Untracked
        );

        fs::write(repo_path.join("README.md"), "changed\n").unwrap();
        assert_eq!(
            collect_repository_status(&repo_path).state,
            RepositoryState::Dirty
        );

        run_git(&repo_path, ["checkout", "--", "README.md"]);
        fs::remove_file(repo_path.join("scratch.txt")).unwrap();
        run_git(&repo_path, ["mv", "README.md", "RENAMED.md"]);
        assert_eq!(
            collect_repository_status(&repo_path).state,
            RepositoryState::Dirty
        );
    }

    #[test]
    fn repository_status_reports_ahead_behind_and_gone_upstream() {
        let repo_path = temp_dir_path("status-sync");
        init_git_repo(&repo_path);
        run_git(&repo_path, ["branch", "base"]);
        run_git(&repo_path, ["branch", "--set-upstream-to=base"]);
        assert_eq!(
            collect_repository_status(&repo_path).sync,
            RepositorySync::UpToDate
        );

        run_git(&repo_path, ["commit", "--allow-empty", "-m", "ahead"]);
        assert_eq!(
            collect_repository_status(&repo_path).sync,
            RepositorySync::Ahead(1)
        );

        run_git(&repo_path, ["checkout", "-q", "base"]);
        run_git(&repo_path, ["commit", "--allow-empty", "-m", "upstream 1"]);
        run_git(&repo_path, ["commit", "--allow-empty", "-m", "upstream 2"]);
        run_git(&repo_path, ["checkout", "-q", "main"]);
        assert_eq!(
            collect_repository_status(&repo_path).sync,
            RepositorySync::Diverged {
                ahead: 1,
                behind: 2
            }
        );

        run_git(&repo_path, ["reset", "-q", "--hard", "HEAD~1"]);
        assert_eq!(
            collect_repository_status(&repo_path).sync,
            RepositorySync::Behind(2)
        );

        run_git(&repo_path, ["branch", "-D", "base"]);
        let status = collect_repository_status(&repo_path);
        assert_eq!(status.sync, RepositorySync::NoUpstream);
        assert_eq!(status.branch.as_deref(), Some("main"));
    }

    #[test]
    fn repository_status_reports_detached_head() {
        let repo_path = temp_dir_path("status-detached");
        init_git_repo(&repo_path);
        run_git(&repo_path, ["checkout", "-q", "--detach"]);

        let status = collect_repository_status(&repo_path);
        assert_eq!(status.branch.as_deref(), Some("detached"));
        assert_eq!(status.state, RepositoryState::Clean);
        assert_eq!(status.sync, RepositorySync::NoUpstream);
        assert!(status.head_commit.is_some());
    }

    #[test]
    fn porcelain_status_parses_unborn_branches_and_renames() {
        let status = parse_porcelain_v2_status(
            b"# branch.oid (initial)\0# branch.head main\0? new file.txt\0",
        );
        assert_eq!(
            status,
            PorcelainStatus {
                state: RepositoryState::Untracked,
                branch: Some("main".to_string()),
                sync: RepositorySync::NoUpstream,
                head_commit: None,
            }
        );

        let status = parse_porcelain_v2_status(
            b"# branch.oid abc\0# branch.head dev\0# branch.upstream origin/dev\0# branch.ab +0 -3\02 R. N... 100644 100644 100644 a b R100 new\0? odd\0",
        );
        assert_eq!(status.state, RepositoryState::Dirty);
        assert_eq!(status.sync, RepositorySync::Behind(3));
        assert_eq!(status.head_commit.as_deref(), Some("abc"));
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
            root: workspace.to_path_buf(),
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
            root: workspace.to_path_buf(),
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
    fn commit_parsing_ignores_marker_like_text_in_messages() {
        let repo_path = temp_dir_path("commit-parsing");
        init_git_repo(&repo_path);
        run_git(
            &repo_path,
            [
                "commit",
                "--allow-empty",
                "-m",
                "fix a|b parsing",
                "-m",
                "END---\nHASH:deadbeef\nSUBJ:fake\nGenerated: Claude",
            ],
        );

        let commits =
            collect_repo_recent_commits(&repo_path, None, "repo", CommitScanRange::Recent(25));
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].subject, "fix a|b parsing");
        assert_eq!(commits[0].head_offset, 0);
        assert_eq!(
            commits[0].body_lines,
            vec!["END---", "HASH:deadbeef", "SUBJ:fake", "Generated: Claude"]
        );
        assert_eq!(commits[1].subject, "init");
        assert_eq!(commits[1].head_offset, 1);
        assert!(commits[1].body_lines.is_empty());

        let last_commit = collect_repository_details(&repo_path).last_commit.unwrap();
        assert_eq!(last_commit.subject, "fix a|b parsing");
        assert!(last_commit.committed_at_epoch_secs > 0);
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
        let hash = git_stdout(&repo_path, ["rev-parse", "--short=7", "HEAD"]).unwrap();

        let mut manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: repo_path.to_path_buf(),
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

        // Prefixes shorter than the minimum are rejected rather than matching.
        if let Some(CommitCheckRuleMatcher::CommitHash { hash }) = manifest
            .commit_check_rules
            .as_mut()
            .map(|rules| &mut rules[1].matcher)
        {
            hash.truncate(6);
        }
        let report = collect_commit_check_report(&manifest, &[], 25);
        assert_eq!(report.invalid_rules.len(), 1);
        assert_eq!(report.matches.len(), 1);
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
            root: repo_path.to_path_buf(),
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
            root: workspace.to_path_buf(),
            repos: vec![],
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        let events = operation_events(&manifest, &[], OperationKind::Push);

        assert!(events
            .iter()
            .any(|event| matches!(event.kind, OperationEventKind::Failed)
                && event.message.contains("Push aborted")));
    }

    #[test]
    fn push_preflight_only_checks_unpushed_commits_of_pushed_repos() {
        let workspace = temp_dir_path("push-preflight-range");
        init_git_repo(&workspace);
        run_git(
            &workspace,
            [
                "commit",
                "--allow-empty",
                "-m",
                "root bot",
                "-m",
                "Generated: Claude",
            ],
        );
        let repo_path = workspace.join("alpha");
        init_git_repo(&repo_path);
        run_git(
            &repo_path,
            [
                "commit",
                "--allow-empty",
                "-m",
                "already pushed bot",
                "-m",
                "Generated: Claude",
            ],
        );
        run_git(&repo_path, ["branch", "published"]);
        run_git(&repo_path, ["branch", "--set-upstream-to=published"]);
        run_git(&repo_path, ["commit", "--allow-empty", "-m", "clean work"]);

        let mut manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: workspace.to_path_buf(),
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

        // The root is not pushed for an explicit selection and alpha's only
        // flagged commit is already on its upstream.
        let report = push_preflight_report(&manifest, &["alpha".to_string()]);
        assert!(report.matches.is_empty());

        // Without a selection the root is pushed and checked too.
        let report = push_preflight_report(&manifest, &[]);
        assert_eq!(report.matches.len(), 1);
        assert_eq!(report.matches[0].repository_name, "(monorepo)");

        // An unpushed flagged commit is caught, and the abort message names
        // each repository once even with several flagged commits.
        for subject in ["bot one", "bot two"] {
            run_git(
                &repo_path,
                [
                    "commit",
                    "--allow-empty",
                    "-m",
                    subject,
                    "-m",
                    "Generated: Claude",
                ],
            );
        }
        manifest
            .commit_check_rules
            .as_mut()
            .unwrap()
            .push(CommitCheckRule {
                id: "bad-regex".to_string(),
                name: "Bad regex".to_string(),
                enabled: true,
                priority: 0,
                effect: CommitCheckRuleEffect::Block,
                scope: CommitCheckRuleScope::All,
                matcher: CommitCheckRuleMatcher::Regex {
                    pattern: "[".to_string(),
                },
            });
        let events = operation_events(&manifest, &["alpha".to_string()], OperationKind::Push);
        assert!(events.iter().any(|event| event
            .message
            .contains("invalid commit check rules: Bad regex")));
        let aborted = events
            .iter()
            .find(|event| event.message.contains("Push aborted"))
            .unwrap();
        assert_eq!(aborted.message.matches("alpha").count(), 1);
        assert!(!aborted.message.contains("(monorepo)"));
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
            root: workspace.to_path_buf(),
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

        let events = operation_events(&manifest, &[], OperationKind::ApplyHooks);

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
    fn operation_batches_report_root_first_every_repo_once_and_finish_last() {
        let workspace = temp_dir_path("parallel-batch");
        let hooks_path = workspace.join("hooks");
        fs::create_dir_all(&hooks_path).unwrap();
        init_git_repo(&workspace);
        let repos = (0..9)
            .map(|index| {
                let dir_name = format!("repo{index}");
                if index != 4 {
                    init_git_repo(&workspace.join(&dir_name));
                }
                RepositoryEntry {
                    id: dir_name.clone(),
                    name: dir_name.clone(),
                    dir_name,
                    remote_url: format!("git@example.com:repo{index}.git"),
                    enabled: true,
                }
            })
            .collect::<Vec<_>>();
        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: workspace.to_path_buf(),
            repos,
            shared_hooks_path: Some(hooks_path),
            commit_check_rules: None,
        };

        let events = operation_events(&manifest, &[], OperationKind::ApplyHooks);
        assert_eq!(events.len(), 12);
        assert_eq!(events[0].kind, OperationEventKind::Started);
        assert_eq!(events[1].repository_name.as_deref(), Some("(monorepo)"));
        let finished = events.last().unwrap();
        assert_eq!(finished.kind, OperationEventKind::Finished);
        assert!(finished
            .message
            .contains("9 completed, 0 skipped, 1 failed"));
        let repo_ids = events[2..11]
            .iter()
            .filter_map(|event| event.repository_id.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(repo_ids.len(), 9);
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
                next_due_at_epoch_secs: None,
                last_scheduled_reason: None,
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
                next_due_at_epoch_secs: None,
                last_scheduled_reason: None,
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
    fn capability_interval_schedule_reports_due_and_next_due() {
        let mut capability = implemented_capability("server/tests", "integrity.tests");
        capability.schedule = Some(serde_json::json!({
            "kind": "interval",
            "interval_seconds": 60
        }));
        let manifest = cargo_repo_manifest(vec![capability.clone()]);
        validate_repo_manifest(&manifest).unwrap();
        let item = attention_test_item(
            RepositoryState::Clean,
            RepositorySync::UpToDate,
            Some("abc"),
            Some(&manifest),
        );
        let state = CapabilityState {
            repo_id: "sample".to_string(),
            capability_instance_id: "server/tests".to_string(),
            capability: "integrity.tests".to_string(),
            item_id: Some("server".to_string()),
            root: Some(PathBuf::from("server")),
            status: CapabilityResultStatus::Ok,
            summary: "tests passed".to_string(),
            findings: Vec::new(),
            checked_at_epoch_secs: Some(100),
            commit: Some("abc".to_string()),
            dirty: false,
            duration_ms: Some(10),
            next_due_at_epoch_secs: None,
            last_scheduled_reason: None,
        };

        assert_eq!(
            capability_next_due_at_epoch_secs(&capability, Some(&state)).unwrap(),
            Some(160)
        );
        assert_eq!(
            capability_due_reason(&item, &capability, Some(&state), 159).unwrap(),
            None
        );
        assert_eq!(
            capability_due_reason(&item, &capability, Some(&state), 160).unwrap(),
            Some(CapabilityDueReason::IntervalElapsed)
        );
    }

    #[test]
    fn capability_on_commit_change_schedule_reports_due_after_new_head() {
        let mut capability = implemented_capability("server/build", "integrity.build");
        capability.schedule = Some(serde_json::json!({ "kind": "on_commit_change" }));
        let manifest = cargo_repo_manifest(vec![capability.clone()]);
        validate_repo_manifest(&manifest).unwrap();
        let item = attention_test_item(
            RepositoryState::Clean,
            RepositorySync::UpToDate,
            Some("new"),
            Some(&manifest),
        );
        let state = CapabilityState {
            repo_id: "sample".to_string(),
            capability_instance_id: "server/build".to_string(),
            capability: "integrity.build".to_string(),
            item_id: Some("server".to_string()),
            root: Some(PathBuf::from("server")),
            status: CapabilityResultStatus::Ok,
            summary: "build passed".to_string(),
            findings: Vec::new(),
            checked_at_epoch_secs: Some(100),
            commit: Some("old".to_string()),
            dirty: false,
            duration_ms: Some(10),
            next_due_at_epoch_secs: None,
            last_scheduled_reason: None,
        };

        assert_eq!(
            capability_due_reason(&item, &capability, Some(&state), 100).unwrap(),
            Some(CapabilityDueReason::CommitChanged)
        );
    }

    #[test]
    fn invalid_capability_schedule_is_rejected() {
        let mut capability = implemented_capability("server/tests", "integrity.tests");
        capability.schedule = Some(serde_json::json!({
            "kind": "interval",
            "interval_seconds": 0
        }));
        let manifest = cargo_repo_manifest(vec![capability]);
        let error = validate_repo_manifest(&manifest).unwrap_err();
        assert!(matches!(
            error,
            WorkspaceError::InvalidRepoManifest(message)
                if message.contains("interval_seconds must be greater than zero")
        ));
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

    #[test]
    fn loads_fucina_policy_for_presentation() {
        let root = temp_dir_path("fucina-policy");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join(FUCINA_POLICY_FILE_NAME),
            r#"schema_version = 2
[[repositories]]
name = "alpha"
role = "mirror"
publication = "github-public"
forgejo = "ssh://git@fucina.homelab:2222/lelloman/alpha.git"
upstream = "git@github.com:lelloman/alpha.git"
mirror_schedule = "01:00 UTC"
archived = false
backup_required = false
"#,
        )
        .unwrap();

        let manifest = load_fucina_policy(&root).unwrap();
        assert_eq!(manifest.schema_version, 2);
        assert_eq!(manifest.repositories.len(), 1);
        assert_eq!(manifest.repositories[0].role, "mirror");
        assert_eq!(
            manifest.repositories[0].upstream.as_deref(),
            Some("git@github.com:lelloman/alpha.git")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delegates_fucina_actions_to_policy_engine_without_a_shell() {
        let root = temp_dir_path("fucina-engine");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join(FUCINA_POLICY_SCRIPT_NAME),
            "import sys\nprint('ARGS=' + '|'.join(sys.argv[1:]))\n",
        )
        .unwrap();

        let repository = "repo;touch-should-not-run".to_string();
        let result =
            run_fucina_policy(&root, FucinaPolicyAction::ConfigureApply, &[repository]).unwrap();
        assert!(result.success);
        assert_eq!(
            result.stdout,
            "ARGS=configure|repo;touch-should-not-run|--apply"
        );
        assert!(!root.join("touch-should-not-run").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reports_fucina_engine_failures_with_output() {
        let root = temp_dir_path("fucina-engine-failure");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join(FUCINA_POLICY_SCRIPT_NAME),
            "import sys\nprint('unsafe remote', file=sys.stderr)\nraise SystemExit(1)\n",
        )
        .unwrap();

        let result = run_fucina_policy(&root, FucinaPolicyAction::Validate, &[]).unwrap();
        assert!(!result.success);
        assert_eq!(result.stderr, "unsafe remote");
        let _ = fs::remove_dir_all(root);
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

    fn operation_events(
        manifest: &WorkspaceManifest,
        selected_repo_ids: &[String],
        kind: OperationKind,
    ) -> Vec<OperationEvent> {
        let events = std::sync::Mutex::new(Vec::new());
        run_workspace_operation(manifest, selected_repo_ids, kind, |event| {
            events.lock().unwrap().push(event);
        });
        events.into_inner().unwrap()
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
        // Keep the user's global hooks and signing setup out of test commits.
        let status = Command::new("git")
            .args([
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "commit.gpgsign=false",
            ])
            .arg("-C")
            .arg(path)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn temp_file_path(prefix: &str) -> TempPath {
        TempPath(std::env::temp_dir().join(format!("{}.json", unique_temp_name(prefix))))
    }

    fn temp_dir_path(prefix: &str) -> TempPath {
        TempPath(std::env::temp_dir().join(unique_temp_name(prefix)))
    }

    fn unique_temp_name(prefix: &str) -> String {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("ronomepo-{prefix}-{}-{stamp}-{seq}", std::process::id())
    }
}

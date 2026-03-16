use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

pub const MANIFEST_FILE_NAME: &str = "ronomepo.json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceManifest {
    pub name: String,
    pub root: PathBuf,
    pub repos: Vec<RepositoryEntry>,
    pub shared_hooks_path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryEntry {
    pub id: String,
    pub name: String,
    pub dir_name: String,
    pub remote_url: String,
    pub enabled: bool,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryStatus {
    pub state: RepositoryState,
    pub branch: Option<String>,
    pub sync: RepositorySync,
    pub repo_path: PathBuf,
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
}

impl fmt::Display for WorkspaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Json(error) => write!(f, "{error}"),
            Self::InvalidRepoUrl(url) => write!(f, "invalid repository url: {url}"),
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

pub fn load_manifest(path: &Path) -> Result<WorkspaceManifest, WorkspaceError> {
    let content = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&content)?)
}

pub fn save_manifest(path: &Path, manifest: &WorkspaceManifest) -> Result<(), WorkspaceError> {
    let content = serde_json::to_string_pretty(manifest)?;
    fs::write(path, content)?;
    Ok(())
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
    })
}

pub fn build_repository_list(manifest: &WorkspaceManifest) -> Vec<RepositoryListItem> {
    manifest
        .repos
        .iter()
        .map(|repo| RepositoryListItem {
            id: repo.id.clone(),
            name: repo.name.clone(),
            dir_name: repo.dir_name.clone(),
            remote_url: repo.remote_url.clone(),
            status: collect_repository_status(&manifest.root.join(&repo.dir_name)),
        })
        .collect()
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

pub fn collect_repository_status(repo_path: &Path) -> RepositoryStatus {
    if !repo_path.exists() {
        return RepositoryStatus {
            state: RepositoryState::Missing,
            branch: None,
            sync: RepositorySync::NoUpstream,
            repo_path: repo_path.to_path_buf(),
        };
    }

    let branch = current_branch(repo_path);
    let state = repository_state(repo_path);
    let sync = repository_sync(repo_path);

    RepositoryStatus {
        state,
        branch,
        sync,
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

    if matches!(kind, OperationKind::Push) {
        let flagged = generated_history_matches(manifest, selected_repo_ids, 25);
        if !flagged.is_empty() {
            emit(OperationEvent {
                kind: OperationEventKind::Failed,
                repository_id: None,
                repository_name: None,
                message: format!(
                    "Push aborted because generated-commit markers were found in: {}. Use force push to override.",
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
            format!("Skipped {} because it has uncommitted changes.", repo.dir_name),
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

    let Some(upstream) = git_stdout(&repo_path, ["rev-parse", "--abbrev-ref", "@{upstream}"]) else {
        return skipped_event(repo, format!("Skipped {} because it has no upstream.", repo.dir_name));
    };
    if upstream.is_empty() {
        return skipped_event(repo, format!("Skipped {} because it has no upstream.", repo.dir_name));
    }

    let ahead = git_stdout(&repo_path, ["rev-list", "--count", "@{upstream}..HEAD"])
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    if ahead == 0 {
        return skipped_event(repo, format!("Skipped {} because it has nothing to push.", repo.dir_name));
    }

    match Command::new("git")
        .arg("-C")
        .arg(&repo_path)
        .arg("push")
        .output()
    {
        Ok(output) if output.status.success() => {
            success_event(repo, format!("Pushed {} ({} commits).", repo.dir_name, ahead))
        }
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
    let mut matches = Vec::new();

    if repo_history_has_generated_markers(&manifest.root, num_commits) {
        matches.push("(monorepo)".to_string());
    }

    for repo in manifest.repos.iter().filter(|repo| repo.enabled).filter(|repo| {
        selected_repo_ids.is_empty() || selected_repo_ids.iter().any(|id| id == &repo.id)
    }) {
        let repo_path = manifest.root.join(&repo.dir_name);
        if !repo_path.exists() {
            continue;
        }
        if repo_history_has_generated_markers(&repo_path, num_commits) {
            matches.push(repo.name.clone());
        }
    }

    matches
}

fn repo_history_has_generated_markers(repo_path: &Path, num_commits: usize) -> bool {
    let limit = num_commits.to_string();
    let Some(output) = git_stdout(
        repo_path,
        [
            "log",
            "-n",
            &limit,
            "--format=%s%n%b%n---END---",
        ],
    ) else {
        return false;
    };

    let lowered = output.to_ascii_lowercase();
    lowered.contains("\ngenerated:")
        || lowered.contains("\ngenerated-by:")
        || lowered.contains("@anthropic")
        || lowered.contains("co-author")
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
    use std::time::{SystemTime, UNIX_EPOCH};

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
        };

        save_manifest(&path, &manifest).unwrap();
        let loaded = load_manifest(&path).unwrap();
        assert_eq!(loaded, manifest);
    }

    #[test]
    fn build_repository_list_collects_real_status() {
        let workspace = temp_dir_path("workspace");
        let repo_path = workspace.join("alpha");
        fs::create_dir_all(&workspace).unwrap();
        init_git_repo(&repo_path);

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
        };

        let items = build_repository_list(&manifest);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status.state, RepositoryState::Clean);
        assert_eq!(items[0].status.branch.as_deref(), Some("main"));
    }

    #[test]
    fn collect_repository_status_marks_missing_repo() {
        let path = temp_dir_path("missing").join("ghost");
        let status = collect_repository_status(&path);
        assert_eq!(status.state, RepositoryState::Missing);
        assert_eq!(status.branch, None);
    }

    #[test]
    fn collect_repository_status_detects_untracked_files() {
        let repo_path = temp_dir_path("untracked");
        init_git_repo(&repo_path);
        fs::write(repo_path.join("scratch.txt"), "hello").unwrap();

        let status = collect_repository_status(&repo_path);
        assert_eq!(status.state, RepositoryState::Untracked);
    }

    #[test]
    fn format_sync_label_matches_mono_style() {
        assert_eq!(format_sync_label(&RepositorySync::UpToDate), "up-to-date");
        assert_eq!(format_sync_label(&RepositorySync::NoUpstream), "-");
        assert_eq!(format_sync_label(&RepositorySync::Ahead(2)), "+2");
        assert_eq!(
            format_sync_label(&RepositorySync::Diverged { ahead: 1, behind: 3 }),
            "+1/-3"
        );
    }

    #[test]
    fn generated_history_markers_are_detected() {
        let repo_path = temp_dir_path("generated-history");
        init_git_repo(&repo_path);
        run_git(repo_path.as_path(), ["commit", "--allow-empty", "-m", "bot work", "-m", "Generated: Claude"]);

        assert!(repo_history_has_generated_markers(&repo_path, 25));
    }

    #[test]
    fn push_is_blocked_when_generated_history_is_present() {
        let workspace = temp_dir_path("push-preflight");
        fs::create_dir_all(&workspace).unwrap();
        run_git(workspace.as_path(), ["init", "-b", "main"]);
        run_git(workspace.as_path(), ["config", "user.name", "Ronomepo Tests"]);
        run_git(workspace.as_path(), ["config", "user.email", "tests@example.com"]);
        run_git(
            workspace.as_path(),
            ["commit", "--allow-empty", "-m", "workspace bot", "-m", "Generated-by: Agent"],
        );

        let manifest = WorkspaceManifest {
            name: "Example".to_string(),
            root: workspace,
            repos: vec![],
            shared_hooks_path: None,
        };

        let mut events = Vec::new();
        run_workspace_operation(&manifest, &[], OperationKind::Push, |event| {
            events.push(event);
        });

        assert!(
            events
                .iter()
                .any(|event| matches!(event.kind, OperationEventKind::Failed)
                    && event.message.contains("Push aborted"))
        );
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

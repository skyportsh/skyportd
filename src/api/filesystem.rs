use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path as StdPath, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use super::{
    resolve_volume_path, DirectoryListingPayload, FileContentsPayload, FilesystemEntryPayload,
};
use crate::config::safe_join_relative;
use crate::server_registry::ManagedServerRecord;

pub(super) fn list_directory(
    server: &ManagedServerRecord,
    requested_path: &str,
) -> Result<DirectoryListingPayload> {
    let volume_path = ensure_server_volume(server)?;
    let normalized_path = normalize_relative_path(requested_path)?;
    let directory_path = resolve_existing_server_path(&volume_path, &normalized_path)?;
    let metadata = fs::metadata(&directory_path)
        .with_context(|| format!("failed to read metadata for {}", directory_path.display()))?;

    if !metadata.is_dir() {
        bail!("The requested path is not a directory.");
    }

    let mut entries = fs::read_dir(&directory_path)
        .with_context(|| format!("failed to read {}", directory_path.display()))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| filesystem_entry_payload(&volume_path, entry.path()).transpose())
        .collect::<Result<Vec<_>>>()?;

    entries.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.name.to_lowercase().cmp(&right.name.to_lowercase()))
    });

    Ok(DirectoryListingPayload {
        entries,
        parent_path: parent_relative_path(&normalized_path),
        path: normalized_path,
    })
}

pub(super) fn read_text_file(
    server: &ManagedServerRecord,
    requested_path: &str,
) -> Result<FileContentsPayload> {
    let volume_path = ensure_server_volume(server)?;
    let normalized_path = normalize_relative_path(requested_path)?;

    if normalized_path.is_empty() {
        bail!("The requested path is not a file.");
    }

    let file_path = resolve_existing_server_path(&volume_path, &normalized_path)?;
    let metadata = fs::metadata(&file_path)
        .with_context(|| format!("failed to read metadata for {}", file_path.display()))?;

    if !metadata.is_file() {
        bail!("The requested path is not a file.");
    }

    if metadata.len() > 1024 * 1024 {
        bail!("This file is too large to open in the editor.");
    }

    let bytes =
        fs::read(&file_path).with_context(|| format!("failed to read {}", file_path.display()))?;

    if bytes.contains(&0) {
        bail!("This file could not be opened as text.");
    }

    let contents = String::from_utf8(bytes).context("This file could not be opened as text.")?;

    Ok(FileContentsPayload {
        contents,
        last_modified_at: modified_at(&metadata),
        path: normalized_path,
        permissions: get_permissions_string(&metadata.permissions()),
        size_bytes: metadata.len(),
    })
}

pub(super) fn write_text_file(
    server: &ManagedServerRecord,
    requested_path: &str,
    contents: &str,
) -> Result<()> {
    if contents.len() > 2 * 1024 * 1024 {
        bail!("This file is too large to save through the editor.");
    }

    let volume_path = ensure_server_volume(server)?;
    let normalized_path = normalize_relative_path(requested_path)?;

    if normalized_path.is_empty() {
        bail!("The requested path is not a file.");
    }

    let file_path = resolve_server_path_for_write(&volume_path, &normalized_path)?;

    if file_path.is_dir() {
        bail!("The requested path is not a file.");
    }

    fs::write(&file_path, contents)
        .with_context(|| format!("failed to write {}", file_path.display()))?;

    Ok(())
}

pub(super) fn create_file(
    server: &ManagedServerRecord,
    requested_path: &str,
    name: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let directory_path =
        resolve_existing_server_path(&volume_path, &normalize_relative_path(requested_path)?)?;

    if !fs::metadata(&directory_path)?.is_dir() {
        bail!("The requested path is not a directory.");
    }

    let file_path = directory_path.join(validate_entry_name(name)?);

    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&file_path)
        .with_context(|| format!("failed to create {}", file_path.display()))?;

    Ok(())
}

pub(super) fn create_directory(
    server: &ManagedServerRecord,
    requested_path: &str,
    name: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let directory_path =
        resolve_existing_server_path(&volume_path, &normalize_relative_path(requested_path)?)?;

    if !fs::metadata(&directory_path)?.is_dir() {
        bail!("The requested path is not a directory.");
    }

    let next_directory = directory_path.join(validate_entry_name(name)?);

    fs::create_dir(&next_directory)
        .with_context(|| format!("failed to create {}", next_directory.display()))?;

    Ok(())
}

pub(super) fn delete_files(server: &ManagedServerRecord, paths: &[String]) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;

    if paths.is_empty() {
        bail!("Please select at least one file or directory.");
    }

    for path in paths {
        let normalized_path = normalize_relative_path(path)?;

        if normalized_path.is_empty() {
            bail!("The server root cannot be deleted.");
        }

        let target_path = resolve_existing_server_path(&volume_path, &normalized_path)?;
        let metadata = fs::metadata(&target_path)
            .with_context(|| format!("failed to read metadata for {}", target_path.display()))?;

        if metadata.is_dir() {
            fs::remove_dir_all(&target_path)
                .with_context(|| format!("failed to delete {}", target_path.display()))?;
        } else {
            fs::remove_file(&target_path)
                .with_context(|| format!("failed to delete {}", target_path.display()))?;
        }
    }

    Ok(())
}

pub(super) fn rename_filesystem_entry(
    server: &ManagedServerRecord,
    requested_path: &str,
    name: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let normalized_path = normalize_relative_path(requested_path)?;

    if normalized_path.is_empty() {
        bail!("The server root cannot be renamed.");
    }

    let source_path = resolve_existing_server_path(&volume_path, &normalized_path)?;
    let target_path = source_path
        .parent()
        .context("path must stay within the server volume")?
        .join(validate_entry_name(name)?);

    if target_path.exists() {
        bail!("A file or directory with that name already exists.");
    }

    fs::rename(&source_path, &target_path).with_context(|| {
        format!(
            "failed to rename {} to {}",
            source_path.display(),
            target_path.display()
        )
    })?;

    Ok(())
}

pub(super) fn move_files(
    server: &ManagedServerRecord,
    paths: &[String],
    destination: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let destination_path = resolve_destination_directory(&volume_path, destination)?;

    for (source_path, target_path) in transfer_targets(&volume_path, paths, &destination_path)? {
        move_path_recursive(&source_path, &target_path)?;
    }

    Ok(())
}

pub(super) fn copy_files(
    server: &ManagedServerRecord,
    paths: &[String],
    destination: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let destination_path = resolve_destination_directory(&volume_path, destination)?;

    for (source_path, target_path) in transfer_targets(&volume_path, paths, &destination_path)? {
        copy_path_recursive(&source_path, &target_path)?;
    }

    Ok(())
}

pub(super) fn update_file_permissions(
    server: &ManagedServerRecord,
    paths: &[String],
    permissions: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let mode = normalize_permissions(permissions)?;

    if paths.is_empty() {
        bail!("Please select at least one file or directory.");
    }

    for path in paths {
        let normalized_path = normalize_relative_path(path)?;

        if normalized_path.is_empty() {
            bail!("The server root permissions cannot be changed here.");
        }

        let target_path = resolve_existing_server_path(&volume_path, &normalized_path)?;
        set_file_mode(&target_path, mode)?;
    }

    Ok(())
}

#[cfg(unix)]
fn set_file_mode(path: &StdPath, mode: u32) -> Result<()> {
    let mut permissions = fs::metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?
        .permissions();
    permissions.set_mode(mode);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("failed to update permissions for {}", path.display()))
}

#[cfg(not(unix))]
fn set_file_mode(_path: &StdPath, _mode: u32) -> Result<()> {
    bail!("File permission changes are not supported on Windows")
}

pub(super) fn create_archive(
    server: &ManagedServerRecord,
    paths: &[String],
    requested_path: &str,
    name: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let destination_path = resolve_destination_directory(&volume_path, requested_path)?;
    let archive_path = destination_path.join(archive_file_name(name)?);

    if archive_path.exists() {
        bail!("A file with that name already exists.");
    }

    let normalized_paths = archive_source_paths(&volume_path, paths, &archive_path)?;
    let output = std::process::Command::new("zip")
        .current_dir(&volume_path)
        .arg("-rq")
        .arg(&archive_path)
        .args(&normalized_paths)
        .output()
        .context("failed to execute zip")?;

    if !output.status.success() {
        let _ = fs::remove_file(&archive_path);
        bail!(command_output_message(
            &output.stderr,
            "The archive could not be created."
        ));
    }

    Ok(())
}

pub(super) fn extract_archive(
    server: &ManagedServerRecord,
    requested_path: &str,
    destination: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let normalized_archive_path = normalize_relative_path(requested_path)?;

    if normalized_archive_path.is_empty() {
        bail!("Please choose an archive to extract.");
    }

    let archive_path = resolve_existing_server_path(&volume_path, &normalized_archive_path)?;

    if !fs::metadata(&archive_path)
        .with_context(|| format!("failed to read metadata for {}", archive_path.display()))?
        .is_file()
    {
        bail!("The requested path is not a file.");
    }

    let destination_path = resolve_destination_directory(&volume_path, destination)?;
    let archive_entries = list_archive_entries(&archive_path)?;
    let top_level_entries = extract_top_level_entries(&archive_entries)?;
    let temp_path = unique_temp_path(&volume_path, ".skyport-extract")?;
    fs::create_dir(&temp_path)
        .with_context(|| format!("failed to create {}", temp_path.display()))?;

    let extraction_result = (|| -> Result<()> {
        let output = std::process::Command::new("unzip")
            .arg("-oq")
            .arg(&archive_path)
            .arg("-d")
            .arg(&temp_path)
            .output()
            .context("failed to execute unzip")?;

        if !output.status.success() {
            bail!(command_output_message(
                &output.stderr,
                "The archive could not be extracted."
            ));
        }

        ensure_tree_contains_no_symlinks(&temp_path)?;

        for entry in &top_level_entries {
            let source_path = temp_path.join(entry);
            let target_path = destination_path.join(entry);

            if !source_path.exists() {
                continue;
            }

            if target_path.exists() {
                bail!(format!(
                    "{} already exists in the destination.",
                    target_path.display()
                ));
            }

            move_path_recursive(&source_path, &target_path)?;
        }

        Ok(())
    })();

    let cleanup_result = fs::remove_dir_all(&temp_path);

    extraction_result?;
    cleanup_result.with_context(|| format!("failed to clean up {}", temp_path.display()))?;

    Ok(())
}

pub(super) fn upload_file(
    server: &ManagedServerRecord,
    requested_path: &str,
    name: &str,
    bytes: &[u8],
) -> Result<()> {
    if bytes.len() > 1024 * 1024 * 1024 {
        bail!("Uploaded files may not be larger than 1 GB.");
    }

    let volume_path = ensure_server_volume(server)?;
    let directory_path = resolve_destination_directory(&volume_path, requested_path)?;
    let file_path = directory_path.join(validate_entry_name(name)?);

    if file_path.exists() {
        bail!("A file with that name already exists.");
    }

    fs::write(&file_path, bytes)
        .with_context(|| format!("failed to write {}", file_path.display()))?;

    Ok(())
}

pub(super) fn resolve_destination_directory(
    volume_path: &PathBuf,
    destination: &str,
) -> Result<PathBuf> {
    let destination_path =
        resolve_existing_server_path(volume_path, &normalize_relative_path(destination)?)?;

    if !fs::metadata(&destination_path)
        .with_context(|| format!("failed to read metadata for {}", destination_path.display()))?
        .is_dir()
    {
        bail!("The requested destination is not a directory.");
    }

    Ok(destination_path)
}

pub(super) fn transfer_targets(
    volume_path: &PathBuf,
    paths: &[String],
    destination_path: &PathBuf,
) -> Result<Vec<(PathBuf, PathBuf)>> {
    if paths.is_empty() {
        bail!("Please select at least one file or directory.");
    }

    let mut seen_targets = BTreeSet::new();
    let mut transfers = Vec::with_capacity(paths.len());

    for path in paths {
        let normalized_path = normalize_relative_path(path)?;

        if normalized_path.is_empty() {
            bail!("The server root cannot be moved or copied.");
        }

        let source_path = resolve_existing_server_path(volume_path, &normalized_path)?;
        let metadata = fs::metadata(&source_path)
            .with_context(|| format!("failed to read metadata for {}", source_path.display()))?;
        let target_name = source_path
            .file_name()
            .context("failed to determine file name")?;
        let target_path = destination_path.join(target_name);

        if target_path.exists() {
            bail!("A file or directory with that name already exists in the destination.");
        }

        if metadata.is_dir() && destination_path.starts_with(&source_path) {
            bail!("A directory cannot be moved or copied into itself.");
        }

        let target_key = target_path.to_string_lossy().to_string();
        if !seen_targets.insert(target_key) {
            bail!("Two selected items would collide in the destination.");
        }

        transfers.push((source_path, target_path));
    }

    Ok(transfers)
}

pub(super) fn copy_path_recursive(source_path: &PathBuf, target_path: &PathBuf) -> Result<()> {
    let metadata = fs::symlink_metadata(source_path)
        .with_context(|| format!("failed to read metadata for {}", source_path.display()))?;

    if metadata.file_type().is_symlink() {
        bail!("Symlinks are not supported in file transfers.");
    }

    if metadata.is_dir() {
        fs::create_dir(target_path)
            .with_context(|| format!("failed to create {}", target_path.display()))?;

        for entry in fs::read_dir(source_path)
            .with_context(|| format!("failed to read {}", source_path.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read {}", source_path.display()))?;
            let child_source = entry.path();
            let child_target = target_path.join(entry.file_name());
            copy_path_recursive(&child_source, &child_target)?;
        }

        fs::set_permissions(target_path, metadata.permissions()).with_context(|| {
            format!("failed to update permissions for {}", target_path.display())
        })?;
    } else {
        fs::copy(source_path, target_path).with_context(|| {
            format!(
                "failed to copy {} to {}",
                source_path.display(),
                target_path.display()
            )
        })?;
        fs::set_permissions(target_path, metadata.permissions()).with_context(|| {
            format!("failed to update permissions for {}", target_path.display())
        })?;
    }

    Ok(())
}

pub(super) fn move_path_recursive(source_path: &PathBuf, target_path: &PathBuf) -> Result<()> {
    match fs::rename(source_path, target_path) {
        Ok(()) => Ok(()),
        Err(_) => {
            copy_path_recursive(source_path, target_path)?;
            remove_path_recursive(source_path)
        }
    }
}

pub(super) fn remove_path_recursive(path: &PathBuf) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?;

    if metadata.is_dir() {
        fs::remove_dir_all(path).with_context(|| format!("failed to delete {}", path.display()))?;
    } else {
        fs::remove_file(path).with_context(|| format!("failed to delete {}", path.display()))?;
    }

    Ok(())
}

pub(super) fn normalize_permissions(permissions: &str) -> Result<u32> {
    let trimmed = permissions.trim();
    let normalized = trimmed.trim_start_matches('0');
    let normalized = if normalized.is_empty() {
        "000"
    } else {
        normalized
    };

    if normalized.len() != 3
        || normalized
            .chars()
            .any(|character| !('0'..='7').contains(&character))
    {
        bail!("Please enter a valid permission mode such as 644 or 755.");
    }

    u32::from_str_radix(normalized, 8).context("failed to parse permissions")
}

pub(super) fn archive_file_name(name: &str) -> Result<String> {
    let name = validate_entry_name(name)?;

    if name.to_ascii_lowercase().ends_with(".zip") {
        Ok(name.to_string())
    } else {
        Ok(format!("{name}.zip"))
    }
}

pub(super) fn archive_source_paths(
    volume_path: &PathBuf,
    paths: &[String],
    archive_path: &PathBuf,
) -> Result<Vec<String>> {
    if paths.is_empty() {
        bail!("Please select at least one file or directory to archive.");
    }

    let mut normalized_paths = Vec::with_capacity(paths.len());

    for path in paths {
        let normalized_path = normalize_relative_path(path)?;

        if normalized_path.is_empty() {
            bail!("The server root cannot be archived this way.");
        }

        let source_path = resolve_existing_server_path(volume_path, &normalized_path)?;

        if source_path == *archive_path {
            bail!("The archive cannot contain itself.");
        }

        normalized_paths.push(format!("./{normalized_path}"));
    }

    Ok(normalized_paths)
}

pub(super) fn list_archive_entries(archive_path: &PathBuf) -> Result<Vec<String>> {
    let output = std::process::Command::new("unzip")
        .arg("-Z1")
        .arg(archive_path)
        .output()
        .context("failed to inspect archive contents")?;

    if !output.status.success() {
        bail!(command_output_message(
            &output.stderr,
            "The archive could not be inspected."
        ));
    }

    Ok(String::from_utf8(output.stdout)
        .context("The archive entry list was not valid UTF-8.")?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| line.trim_end_matches('/').to_string())
        .collect())
}

pub(super) fn extract_top_level_entries(entries: &[String]) -> Result<Vec<String>> {
    let mut top_level = BTreeSet::new();

    for entry in entries {
        let normalized_entry = normalize_relative_path(entry)?;

        if normalized_entry.is_empty() {
            continue;
        }

        let first_segment = normalized_entry
            .split('/')
            .next()
            .context("failed to determine archive entry path")?;
        top_level.insert(first_segment.to_string());
    }

    Ok(top_level.into_iter().collect())
}

pub(super) fn ensure_tree_contains_no_symlinks(path: &PathBuf) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?;

    if metadata.file_type().is_symlink() {
        bail!("Archives containing symlinks are not supported.");
    }

    if metadata.is_dir() {
        for entry in
            fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))?
        {
            let entry = entry.with_context(|| format!("failed to read {}", path.display()))?;
            ensure_tree_contains_no_symlinks(&entry.path())?;
        }
    }

    Ok(())
}

pub(super) fn unique_temp_path(volume_path: &PathBuf, prefix: &str) -> Result<PathBuf> {
    for attempt in 0..16 {
        let candidate = volume_path.join(format!(
            "{prefix}-{}-{attempt}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("The system clock is invalid.")?
                .as_nanos()
        ));

        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    bail!("Failed to reserve a temporary directory.")
}

pub(super) fn command_output_message(stderr: &[u8], fallback: &str) -> String {
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();

    if stderr.is_empty() {
        fallback.to_string()
    } else {
        stderr
    }
}

pub(super) fn filesystem_entry_payload(
    volume_path: &PathBuf,
    entry_path: PathBuf,
) -> Result<Option<FilesystemEntryPayload>> {
    let symlink_metadata = fs::symlink_metadata(&entry_path)
        .with_context(|| format!("failed to read metadata for {}", entry_path.display()))?;

    if symlink_metadata.file_type().is_symlink() {
        return Ok(None);
    }

    let metadata = fs::metadata(&entry_path)
        .with_context(|| format!("failed to read metadata for {}", entry_path.display()))?;
    let relative_path = entry_path
        .strip_prefix(volume_path)
        .context("failed to determine relative path")?
        .to_string_lossy()
        .replace('\\', "/");
    let name = entry_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("failed to determine file name")?
        .to_string();

    Ok(Some(FilesystemEntryPayload {
        kind: if metadata.is_dir() {
            "directory".to_string()
        } else {
            "file".to_string()
        },
        last_modified_at: modified_at(&metadata),
        name,
        path: relative_path,
        permissions: get_permissions_string(&metadata.permissions()),
        size_bytes: if metadata.is_file() {
            Some(metadata.len())
        } else {
            None
        },
    }))
}

pub(crate) fn ensure_server_volume(server: &ManagedServerRecord) -> Result<PathBuf> {
    let volume_path = resolve_volume_path(server)?;
    fs::create_dir_all(&volume_path)
        .with_context(|| format!("failed to create {}", volume_path.display()))?;
    fs::canonicalize(&volume_path)
        .with_context(|| format!("failed to resolve {}", volume_path.display()))
}

pub(super) fn normalize_relative_path(path: &str) -> Result<String> {
    if path.trim().is_empty() {
        return Ok(String::new());
    }

    let mut segments = Vec::new();

    for component in StdPath::new(path).components() {
        match component {
            std::path::Component::Normal(segment) => {
                segments.push(segment.to_string_lossy().to_string())
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => {
                bail!("path must stay within the server volume");
            }
        }
    }

    Ok(segments.join("/"))
}

pub(super) fn resolve_existing_server_path(
    volume_path: &PathBuf,
    relative_path: &str,
) -> Result<PathBuf> {
    if relative_path.is_empty() {
        return Ok(volume_path.clone());
    }

    let candidate = safe_join_relative(volume_path, relative_path)?;
    let resolved = fs::canonicalize(&candidate)
        .with_context(|| format!("failed to resolve {}", candidate.display()))?;

    if !resolved.starts_with(volume_path) {
        bail!("path must stay within the server volume");
    }

    Ok(resolved)
}

pub(super) fn resolve_server_path_for_write(
    volume_path: &PathBuf,
    relative_path: &str,
) -> Result<PathBuf> {
    let candidate = safe_join_relative(volume_path, relative_path)?;

    if candidate.exists() {
        let resolved = fs::canonicalize(&candidate)
            .with_context(|| format!("failed to resolve {}", candidate.display()))?;

        if !resolved.starts_with(volume_path) {
            bail!("path must stay within the server volume");
        }

        return Ok(resolved);
    }

    let parent = candidate
        .parent()
        .context("path must stay within the server volume")?;
    let resolved_parent = fs::canonicalize(parent)
        .with_context(|| format!("failed to resolve {}", parent.display()))?;

    if !resolved_parent.starts_with(volume_path) {
        bail!("path must stay within the server volume");
    }

    let name = candidate
        .file_name()
        .context("path must stay within the server volume")?;

    Ok(resolved_parent.join(name))
}

pub(super) fn validate_entry_name(name: &str) -> Result<&str> {
    let trimmed = name.trim();

    if trimmed.is_empty() {
        bail!("name cannot be empty");
    }

    let path = StdPath::new(trimmed);
    let mut components = path.components();

    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(_)), None) => Ok(trimmed),
        _ => bail!("name must be a single path segment"),
    }
}

pub(super) fn parent_relative_path(path: &str) -> Option<String> {
    if path.is_empty() {
        return None;
    }

    let mut segments = path.split('/').collect::<Vec<_>>();
    segments.pop();

    Some(segments.join("/"))
}

pub(super) fn modified_at(metadata: &fs::Metadata) -> Option<u64> {
    metadata
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

#[cfg(unix)]
pub(super) fn format_permissions(mode: u32) -> String {
    format!("{:03o}", mode & 0o777)
}

#[cfg(unix)]
fn get_permissions_string(permissions: &std::fs::Permissions) -> Option<String> {
    Some(format_permissions(permissions.mode()))
}

#[cfg(not(unix))]
fn get_permissions_string(_permissions: &std::fs::Permissions) -> Option<String> {
    None
}

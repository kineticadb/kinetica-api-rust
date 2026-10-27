//! Common file operation utilities for upload and download.
//!
//! Everything here that touches the local file system is blocking. The
//! callers run it through `tokio::task::spawn_blocking` (or use the
//! `tokio::fs` equivalent) so that a slow disk or a large directory walk does
//! not stall the async runtime's worker threads.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use log::{debug, error};

use crate::KineticaError;
use super::{KIFS_PATH_SEPARATOR, KifsFileHandlerOptions};
use super::types::{OpMode, OperationResult};

/// Information about a batch of files ready for full file upload.
#[derive(Debug, Clone, Default)]
pub struct FileBatch {
    /// Pairs of (local file path, remote file name), in the order the files
    /// were found. A list rather than a map so that the files of a batch are
    /// uploaded in a fixed order; a `HashMap` here made both the batch
    /// contents' order and the callback's `full_file_names` order vary from
    /// run to run.
    pub files: Vec<(String, String)>,
    /// Total size of all files in this batch
    pub total_size: i64,
}

/// Manages batches of files for full file uploads.
///
/// Files are collected in the order they are added, keyed by local path, and
/// split into batches only when [`create_batches`](Self::create_batches) is
/// called.
#[derive(Debug)]
pub struct FullFileBatcher {
    /// Every file added, as (local path, remote name, size), in insertion order
    files: Vec<(String, String, i64)>,
    /// Position of each local path in `files`
    index: HashMap<String, usize>,
    /// List of file batches
    batches: Vec<FileBatch>,
    /// Maximum size for a batch
    max_batch_size: i64,
}

impl FullFileBatcher {
    /// Creates a new batch manager with the given options.
    pub fn new(options: &KifsFileHandlerOptions) -> Self {
        Self {
            files: Vec::new(),
            index: HashMap::new(),
            batches: Vec::new(),
            max_batch_size: options.file_size_to_split(),
        }
    }

    /// Adds a file to be batched.
    ///
    /// Adding a local file a second time (say, because two patterns matched
    /// it) does not upload it twice: the file keeps its original position and
    /// takes the latest remote name and size.
    pub fn add_file(&mut self, local_path: &str, remote_path: &str, file_size: i64) {
        match self.index.get(local_path) {
            Some(&i) => {
                self.files[i].1 = remote_path.to_string();
                self.files[i].2 = file_size;
            }
            None => {
                self.index.insert(local_path.to_string(), self.files.len());
                self.files.push((local_path.to_string(), remote_path.to_string(), file_size));
            }
        }
    }

    /// Splits the added files into batches. Call this after adding all files.
    ///
    /// Files are taken in order and added to the current batch; a file that
    /// would take the batch past the size limit starts the next one instead,
    /// and a batch that reaches the limit exactly is closed.
    pub fn create_batches(&mut self) {
        self.batches.clear();

        let mut sum: i64 = 0;
        let mut bucket = FileBatch::default();
        let mut i = 0;

        while i < self.files.len() {
            let (local, remote, size) = &self.files[i];
            let mut advance = true;

            if sum + size > self.max_batch_size && !bucket.files.is_empty() {
                // This file overshoots the limit: close the bucket and re-read
                // the file into the next one. A full file is never larger than
                // the limit, but the guard for an empty bucket keeps a caller
                // that adds one from looping forever.
                advance = false;
                sum = self.max_batch_size;
            } else {
                bucket.files.push((local.clone(), remote.clone()));
                bucket.total_size += size;
                sum += size;
            }

            if sum >= self.max_batch_size {
                self.batches.push(std::mem::take(&mut bucket));
                sum = 0;
            }

            if advance {
                i += 1;
            }
        }

        if !bucket.files.is_empty() {
            self.batches.push(bucket);
        }
    }

    /// Clears all files and batches.
    pub fn clear_batches(&mut self) {
        self.files.clear();
        self.index.clear();
        self.batches.clear();
    }

    /// Returns the number of batches.
    pub fn number_of_batches(&self) -> usize {
        self.batches.len()
    }

    /// Returns the nth batch.
    pub fn nth_batch(&self, n: usize) -> Option<&FileBatch> {
        self.batches.get(n)
    }

    /// Returns all batches.
    pub fn batches(&self) -> &[FileBatch] {
        &self.batches
    }
}

/// Parsed file name components for upload operations.
#[derive(Debug, Clone)]
pub struct ParsedFileName {
    /// Root of the path (e.g., "/" on Unix, "C:\" on Windows)
    pub root: String,
    /// Base directory for searching
    pub base_dir: String,
    /// File name or pattern
    pub pattern: String,
}

impl ParsedFileName {
    /// The local path of a file named without a pattern: the base directory,
    /// the platform's separator and the name. A bare name is therefore
    /// `./<name>`.
    pub fn file_path(&self) -> String {
        format!("{}{}{}", self.base_dir, std::path::MAIN_SEPARATOR, self.pattern)
    }
}

/// Checks if a string contains glob pattern characters: `*`, `?`, `{`, `[`
/// or `\`.
///
/// So on Unix a file whose name contains a literal `[`, `{`, `?` or `\` is
/// searched for as a pattern rather than opened by name (and, with a
/// character class, may match other files or none), and on Windows, where
/// `\` is the path separator, every path counts as a pattern.
pub fn is_glob_pattern(s: &str) -> bool {
    s.contains('*') || s.contains('?') || s.contains('{') || s.contains('[') || s.contains('\\')
}

/// Checks if a local directory exists.
///
/// Uses `tokio::fs` so the check does not block the async runtime.
pub async fn local_dir_exists(path: &str) -> bool {
    // A whitespace-only name also fails.
    if path.trim().is_empty() {
        return false;
    }
    tokio::fs::metadata(path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
}

/// Checks if a local file exists.
/// For glob patterns, returns true to allow the pattern resolver to handle it.
///
/// Blocking; call it from a blocking context.
///
/// Only `*`, `?`, `{` and `[` mark a pattern here — not the backslash that
/// [`is_glob_pattern`] also counts — so a name with a backslash but no other
/// glob character must exist as given.
pub fn local_file_exists(path: &str) -> bool {
    // A whitespace-only name also fails.
    if path.trim().is_empty() {
        return false;
    }
    // If it looks like a pattern, assume true and let the resolver handle it
    if path.contains(['*', '?', '{', '[']) {
        return true;
    }
    Path::new(path).exists()
}

/// Parses a file path into its components (root, base directory, pattern).
///
/// For a pattern, the base directory is everything before the first path
/// component that contains a glob character. It therefore never contains a
/// glob character itself, which is why [`search_local_directories`] needs no
/// escaping for it.
pub fn parse_file_name(raw_path: &str) -> ParsedFileName {
    let path = Path::new(raw_path);

    if is_glob_pattern(raw_path) {
        // For glob patterns, find the last non-glob parent directory
        let mut base_dir = PathBuf::new();
        let mut pattern_parts: Vec<&str> = Vec::new();

        for component in path.components() {
            let component_str = component.as_os_str().to_string_lossy();
            if is_glob_pattern(&component_str) || !pattern_parts.is_empty() {
                pattern_parts.push(component.as_os_str().to_str().unwrap_or(""));
            } else {
                base_dir.push(component);
            }
        }

        let base_dir_str = if base_dir.as_os_str().is_empty() {
            ".".to_string()
        } else {
            base_dir.to_string_lossy().to_string()
        };

        let root = path
            .components()
            .next()
            .and_then(|c| {
                if let std::path::Component::Prefix(_) = c {
                    Some(c.as_os_str().to_string_lossy().to_string())
                } else if let std::path::Component::RootDir = c {
                    Some("/".to_string())
                } else {
                    None
                }
            })
            .unwrap_or_default();

        ParsedFileName {
            root,
            base_dir: base_dir_str,
            pattern: pattern_parts.join(std::path::MAIN_SEPARATOR_STR),
        }
    } else {
        // Standard path without glob characters
        let parent = path.parent().map(|p| p.to_string_lossy().to_string()).unwrap_or_else(|| ".".to_string());
        let file_name = path.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();

        let root = path
            .components()
            .next()
            .and_then(|c| {
                if let std::path::Component::Prefix(_) = c {
                    Some(c.as_os_str().to_string_lossy().to_string())
                } else if let std::path::Component::RootDir = c {
                    Some("/".to_string())
                } else {
                    None
                }
            })
            .unwrap_or_default();

        ParsedFileName {
            root,
            base_dir: if parent.is_empty() { ".".to_string() } else { parent },
            pattern: file_name,
        }
    }
}

/// Makes a path absolute and removes `.` and `..` components lexically,
/// without touching the file system.
pub(crate) fn normalize_absolute(path: &Path) -> std::io::Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other),
        }
    }
    Ok(normalized)
}

/// Translates a glob pattern into a regular expression:
///
/// - `*` matches within one path component and `**` across components, so
///   `base/**/*.csv` needs at least one directory below `base` (unlike the
///   `glob` crate, whose `**/` also matches none);
/// - `?` matches one character other than the separator;
/// - `[...]` is a class (`[!...]` negated) that never matches the separator;
/// - `{a,b}` is a group of alternatives, which may not nest;
/// - a backslash escapes the next character.
///
/// `is_dos` selects Windows' `\` separator.
// The `glob` crate is not used here: it has no groups or escapes, and
// differs on `**`.
fn nio_glob_to_regex(glob: &str, is_dos: bool) -> Result<String, String> {
    const REGEX_META: &str = ".^$+{[]|()";
    const GLOB_META: &str = "\\*?[{";
    let separator = if is_dos { "\\\\" } else { "/" };
    let not_separator = if is_dos { "[^\\\\]" } else { "[^/]" };

    let chars: Vec<char> = glob.chars().collect();
    let next = |i: usize| chars.get(i).copied();
    let error = |what: &str| Err(format!("{what} in glob pattern '{glob}'"));

    let mut regex = String::from("^");
    let mut in_group = false;
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        i += 1;
        match c {
            '\\' => {
                // escape special characters
                let Some(escaped) = next(i) else {
                    return error("Nothing to escape after a trailing '\\'");
                };
                if GLOB_META.contains(escaped) || REGEX_META.contains(escaped) {
                    regex.push('\\');
                }
                regex.push(escaped);
                i += 1;
            }
            '/' => regex.push_str(separator),
            '[' => {
                // don't match name separator in class
                regex.push_str(&format!("[{not_separator}&&["));
                if next(i) == Some('^') {
                    // escape the regex negation char if it appears
                    regex.push_str("\\^");
                    i += 1;
                } else {
                    // negation
                    if next(i) == Some('!') {
                        regex.push('^');
                        i += 1;
                    }
                    // hyphen allowed at start
                    if next(i) == Some('-') {
                        regex.push_str("\\-");
                        i += 1;
                    }
                }

                let mut has_range_start = false;
                let mut last = '\0';
                let mut closed = false;
                while i < chars.len() {
                    let c = chars[i];
                    i += 1;
                    if c == ']' {
                        closed = true;
                        break;
                    }
                    if c == '/' || (is_dos && c == '\\') {
                        return error("A path separator inside [...]");
                    }
                    if c == '-' {
                        if !has_range_start {
                            return error("Invalid range");
                        }
                        let end = next(i);
                        i += 1;
                        match end {
                            None => break,
                            Some(']') => {
                                closed = true;
                                // A '-' just before the ']' is a literal
                                // hyphen; this crate's regex needs it escaped.
                                regex.push_str("\\-");
                                break;
                            }
                            Some(end) => {
                                if end < last {
                                    return error("Invalid range");
                                }
                                regex.push('-');
                                push_class_char(&mut regex, end);
                                has_range_start = false;
                            }
                        }
                    } else {
                        // Every punctuation character is escaped, as this
                        // crate's regex gives several of them meaning in a
                        // class ("--", "~~").
                        push_class_char(&mut regex, c);
                        has_range_start = true;
                        last = c;
                    }
                }
                if !closed {
                    return error("Missing ']'");
                }
                regex.push_str("]]");
            }
            '{' => {
                if in_group {
                    return error("Nested '{' groups are not allowed");
                }
                regex.push_str("(?:(?:");
                in_group = true;
            }
            '}' => {
                if in_group {
                    regex.push_str("))");
                    in_group = false;
                } else {
                    regex.push_str("\\}");
                }
            }
            ',' => {
                if in_group {
                    regex.push_str(")|(?:");
                } else {
                    regex.push(',');
                }
            }
            '*' => {
                if next(i) == Some('*') {
                    // crosses directory boundaries
                    regex.push_str(".*");
                    i += 1;
                } else {
                    // within directory boundary
                    regex.push_str(not_separator);
                    regex.push('*');
                }
            }
            '?' => regex.push_str(not_separator),
            _ => {
                if REGEX_META.contains(c) {
                    regex.push('\\');
                }
                regex.push(c);
            }
        }
    }

    if in_group {
        return error("Missing '}'");
    }
    regex.push('$');
    Ok(regex)
}

/// Appends one literal character of a `[...]` class to a regex.
fn push_class_char(regex: &mut String, c: char) {
    if c.is_ascii_punctuation() {
        regex.push('\\');
    }
    regex.push(c);
}

/// Compiles a glob pattern into a matcher.
///
/// Case-insensitive on Windows only.
fn compile_glob(glob: &str) -> Result<regex::Regex, String> {
    let is_dos = cfg!(windows);
    let pattern = nio_glob_to_regex(glob, is_dos)?;
    regex::RegexBuilder::new(&pattern)
        .case_insensitive(is_dos)
        .build()
        .map_err(|e| format!("{e}"))
}
/// Lists the non-directory entries under `dir`, depth first, each
/// directory's entries before the next sibling. `depth` is the depth of
/// `dir`'s entries.
///
/// Symbolic links to directories are not followed, and a link to a directory
/// is not reported as a file, while a link to a file (or a dangling link) is.
///
/// Each directory's entries are sorted by name so that the upload order, and
/// the batches built from it, do not vary from run to run.
fn walk_files(dir: &Path, depth: usize, max_depth: usize, out: &mut Vec<PathBuf>) -> Result<(), SearchError> {
    let listing_failed = |e: std::io::Error| {
        if depth == 1 {
            SearchError::StartDirUnreadable(e)
        } else {
            SearchError::Failed(KineticaError::Config(format!(
                "Failed to search directory '{}': {e}",
                dir.display()
            )))
        }
    };

    // read_dir follows a link, so a start directory reached through one is
    // searched
    let mut entries = std::fs::read_dir(dir)
        .and_then(|entries| entries.collect::<Result<Vec<_>, _>>())
        .map_err(listing_failed)?;
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();

        let is_dir_following_links = std::fs::metadata(&path).map(|m| m.is_dir()).unwrap_or(false);
        if !is_dir_following_links {
            out.push(path.clone());
        }

        // `file_type` does not follow links, so a link to a directory is not
        // descended into.
        let is_real_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        if is_real_dir && depth < max_depth {
            walk_files(&path, depth + 1, max_depth, out)?;
        }
    }

    Ok(())
}

/// Why [`search_local_directories`] failed.
///
/// The two cases are handled differently by the upload.
#[derive(Debug)]
pub enum SearchError {
    /// The search's start directory could not be listed. The failure is
    /// logged, and the upload goes ahead without that name's files.
    StartDirUnreadable(std::io::Error),
    /// Anything else: an invalid pattern or a subdirectory that could not be
    /// listed. The upload fails.
    Failed(KineticaError),
}

impl From<SearchError> for KineticaError {
    fn from(e: SearchError) -> Self {
        match e {
            SearchError::StartDirUnreadable(e) => {
                KineticaError::Config(format!("Failed to read directory: {e}"))
            }
            SearchError::Failed(e) => e,
        }
    }
}

/// Resolves glob patterns in local directories.
///
/// Returns a tuple of (local_paths, remote_paths) where remote paths are
/// calculated relative to the target directory.
///
/// Blocking; call it from a blocking context.
///
/// The search works as follows:
///
/// - The search starts at `base_dir`, made absolute and normalized.
/// - A pattern with no path separator is matched against each file's *name*;
///   one with a separator is matched against the file's absolute path, as
///   `<base_dir>/<pattern>`.
/// - The search descends into subdirectories when `recursive` is set or the
///   pattern contains `**`, and otherwise looks only at `base_dir`'s own
///   entries. So with `recursive`, `sub/*.csv` still matches only
///   `<base_dir>/sub/*.csv`, since `*` does not cross a separator; and
///   without it, `*/x.csv` matches nothing at all, because the files it names
///   are one level deeper than the search looks.
/// - Patterns use the glob syntax described at [`nio_glob_to_regex`].
/// - Remote names mirror the directory structure below `base_dir`.
///
/// `base_dir` is not escaped in a full-path pattern. By construction
/// ([`parse_file_name`]) it has no glob characters of its own, but a relative
/// one is made absolute from the current directory, and a glob character in
/// *that* path is read as pattern syntax — so a full-path pattern run from
/// such a directory may match nothing.
pub fn search_local_directories(
    base_dir: &str,
    pattern: &str,
    target_dir: &str,
    recursive: bool,
) -> Result<(Vec<String>, Vec<String>), SearchError> {
    let mut local_paths = Vec::new();
    let mut remote_paths = Vec::new();

    let start_path = normalize_absolute(Path::new(base_dir)).map_err(|e| {
        SearchError::Failed(KineticaError::Config(format!(
            "Failed to resolve directory '{base_dir}': {e}"
        )))
    })?;
    if !start_path.is_dir() {
        // A missing directory is logged and yields no files rather than
        // failing the upload.
        error!("Directory does not exist: {base_dir}");
        return Ok((local_paths, remote_paths));
    }

    // Match the full path, or just the file name, depending on the pattern
    let match_full_path = pattern.contains(std::path::MAIN_SEPARATOR) || pattern.contains('/');
    let glob_pattern = if match_full_path {
        let full = format!("{}{}{}", start_path.display(), std::path::MAIN_SEPARATOR, pattern);
        // On Windows the path's backslashes would read as glob escapes, so
        // they are doubled.
        if cfg!(windows) {
            full.replace('\\', "\\\\")
        } else {
            full
        }
    } else {
        pattern.to_string()
    };

    debug!("Searching {} with pattern: {glob_pattern}", start_path.display());

    let matcher = compile_glob(&glob_pattern).map_err(|e| {
        SearchError::Failed(KineticaError::Config(format!(
            "Invalid glob pattern '{glob_pattern}': {e}"
        )))
    })?;

    // Max depth depends on recursive flag or if pattern contains **
    let max_depth = if recursive || pattern.contains("**") { usize::MAX } else { 1 };

    // An unreadable directory is reported rather than skipped; see
    // `SearchError` for how the upload treats each case.
    let mut found = Vec::new();
    walk_files(&start_path, 1, max_depth, &mut found)?;

    for path in found {
        // Match against full path or filename depending on pattern type
        let matched = if match_full_path {
            matcher.is_match(&path.to_string_lossy())
        } else {
            let name = path.file_name().map(|n| n.to_string_lossy()).unwrap_or_default();
            matcher.is_match(&name)
        };
        if !matched {
            continue;
        }

        // Calculate Remote Path: TargetDir + (File Relative to Start)
        let relative = path
            .strip_prefix(&start_path)
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| path.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default());

        // Convert path separators to KIFS format
        let remote_relative = relative.replace(std::path::MAIN_SEPARATOR_STR, KIFS_PATH_SEPARATOR);
        let remote_path = if target_dir.is_empty() {
            remote_relative
        } else {
            format!("{target_dir}{KIFS_PATH_SEPARATOR}{remote_relative}")
        };

        local_paths.push(path.to_string_lossy().to_string());
        remote_paths.push(remote_path);
    }

    debug!("Found {} files matching pattern", local_paths.len());
    Ok((local_paths, remote_paths))
}

/// Gets the file size for a local file.
///
/// Blocking; call it from a blocking context.
pub fn file_size(path: &str) -> Result<i64, KineticaError> {
    std::fs::metadata(path)
        .map(|m| m.len() as i64)
        .map_err(|e| KineticaError::Config(format!("Failed to get file size for '{path}': {e}")))
}

/// Categorizes files into full file and multi-part lists based on size.
///
/// Blocking; call it from a blocking context.
///
/// A file whose size cannot be read (say, because it was deleted after the
/// search found it) is logged and left out, rather than failing the whole
/// upload. The risk is that such a file is then missing from the upload with
/// only a log line to say so.
pub fn categorize_files_by_size(
    local_files: &[String],
    remote_files: &[String],
    options: &KifsFileHandlerOptions,
) -> (FullFileBatcher, Vec<(String, String)>) {
    let mut batch_manager = FullFileBatcher::new(options);
    let mut multipart_files = Vec::new();

    for (local_path, remote_path) in local_files.iter().zip(remote_files.iter()) {
        let file_size = match file_size(local_path) {
            Ok(size) => size,
            Err(e) => {
                error!("Error getting attributes for file '{local_path}': {e}");
                continue;
            }
        };

        if file_size > options.file_size_to_split() {
            multipart_files.push((local_path.clone(), remote_path.clone()));
        } else {
            batch_manager.add_file(local_path, remote_path, file_size);
        }
    }

    batch_manager.create_batches();
    (batch_manager, multipart_files)
}

/// Extracts the file name from a KiFS path: the part after the last `/`.
///
/// For a name with no separator this returns the whole name. KiFS file names
/// always include a directory, so in practice there is always a separator.
pub fn kifs_file_name(kifs_path: &str) -> &str {
    kifs_path
        .rsplit(KIFS_PATH_SEPARATOR)
        .next()
        .unwrap_or(kifs_path)
}

/// Where below the local directory a downloaded KiFS file goes: its path
/// relative to the KiFS path it was asked for by, with `/` as the separator.
///
/// Downloading the directory `dir` saves `dir/a.csv` as `a.csv`, and
/// `dir/sub/x.csv` as `sub/x.csv`. A file asked for by its own name, or by a
/// pattern, goes relative to the directory holding the name asked for.
///
/// Fails for a name that would leave the local directory, through `..`.
pub(crate) fn download_relative_path(requested: &str, kifs_name: &str) -> Result<String, String> {
    let requested = requested.trim_end_matches(KIFS_PATH_SEPARATOR);
    let below = |base: &str| {
        kifs_name.strip_prefix(base).and_then(|rest| rest.strip_prefix(KIFS_PATH_SEPARATOR)).filter(|r| !r.is_empty())
    };
    let relative = (kifs_name != requested)
        .then(|| below(requested))
        .flatten()
        .or_else(|| requested.rsplit_once(KIFS_PATH_SEPARATOR).and_then(|(holder, _)| below(holder)))
        .unwrap_or_else(|| kifs_file_name(kifs_name));
    let components: Vec<&str> = relative.split(KIFS_PATH_SEPARATOR).filter(|c| !c.is_empty() && *c != ".").collect();
    if components.is_empty() || components.contains(&"..") {
        return Err(format!("KiFS file <{kifs_name}> would be saved outside the local directory"));
    }
    Ok(components.join(KIFS_PATH_SEPARATOR))
}

/// The message for `error` on `path`: a missing file, a refused access or a
/// file in the way names just the path; anything else gives the system's
/// reason.
pub(crate) fn io_error_message(path: &str, error: &std::io::Error) -> String {
    use std::io::ErrorKind;
    match error.kind() {
        ErrorKind::NotFound | ErrorKind::PermissionDenied | ErrorKind::AlreadyExists => path.to_string(),
        _ => {
            let text = error.to_string();
            match text.find(" (os error ") {
                Some(at) => text[..at].to_string(),
                None => text,
            }
        }
    }
}

/// Common functionality for file operations.
pub struct FileOperationContext {
    /// Operation mode (upload or download)
    pub op_mode: OpMode,
    /// File handler options
    pub options: KifsFileHandlerOptions,
    /// List of source file names/patterns
    pub file_names: Vec<String>,
    /// Target directory name (KIFS for upload, local for download)
    pub dir_name: String,
    /// Whether to recursively search directories
    pub recursive: bool,
}

impl FileOperationContext {
    /// Creates a new file operation context.
    pub fn new(
        op_mode: OpMode,
        options: KifsFileHandlerOptions,
        file_names: Vec<String>,
        dir_name: String,
        recursive: bool,
    ) -> Self {
        Self {
            op_mode,
            options,
            file_names,
            dir_name,
            recursive,
        }
    }
}

// ============================================================================
// Batched error reporting
// ============================================================================

/// Folds the per-file results of a batched operation into a single error, or
/// `None` if every file succeeded.
///
/// Upload and download both attempt every file and record a result for each,
/// so that one bad file does not cost the whole batch. This is the other half
/// of that contract: the failures are reported together, in the order the files
/// were attempted, as one error carrying every message, a line each.
pub(crate) fn aggregate_failures(results: &[OperationResult]) -> Option<KineticaError> {
    let messages: Vec<&str> = results
        .iter()
        .filter(|r| !r.successful)
        .filter_map(|r| r.error_message.as_deref())
        .collect();

    if messages.is_empty() {
        return None;
    }

    Some(KineticaError::Api(messages.join("\n")))
}

#[cfg(test)]
mod batched_error_reporting_tests {
    use super::*;

    fn ok(name: &str) -> OperationResult {
        OperationResult::success(name.to_string(), OpMode::Upload)
    }

    fn bad(name: &str, msg: &str) -> OperationResult {
        OperationResult::failure(name.to_string(), OpMode::Upload, msg)
    }

    #[test]
    fn all_successful_yields_no_error() {
        let results = vec![ok("a"), ok("b"), ok("c")];
        assert!(aggregate_failures(&results).is_none());
    }

    #[test]
    fn empty_results_yield_no_error() {
        assert!(aggregate_failures(&[]).is_none());
    }

    /// The point of the whole design: a batch where one file failed still
    /// reports the successes, and the single error names only the file that
    /// actually failed.
    #[test]
    fn one_failure_among_successes_reports_only_that_file() {
        let results = vec![ok("a"), bad("b", "could not read 'b'"), ok("c")];

        let err = aggregate_failures(&results).expect("expected an error");
        let msg = err.to_string();

        assert!(msg.contains("could not read 'b'"), "got: {msg}");
        assert!(!msg.contains("'a'"), "must not implicate 'a': {msg}");
        assert!(!msg.contains("'c'"), "must not implicate 'c': {msg}");
        assert_eq!(msg.lines().count(), 1, "one failure, one line: {msg}");

        // The successes are still in the report.
        assert_eq!(results.iter().filter(|r| r.successful).count(), 2);
    }

    /// Every failure is reported, not just the first — that is what makes this
    /// batched rather than fail-fast.
    #[test]
    fn every_failure_is_reported_in_order() {
        let results = vec![
            bad("a", "first failure"),
            ok("b"),
            bad("c", "second failure"),
            bad("d", "third failure"),
        ];

        let err = aggregate_failures(&results).expect("expected an error");
        let msg = err.to_string();
        let lines: Vec<&str> = msg.lines().collect();

        assert_eq!(lines.len(), 3, "one line per failure: {msg}");
        assert!(lines[0].contains("first failure"));
        assert!(lines[1].contains("second failure"));
        assert!(lines[2].contains("third failure"));
    }

    /// A failure carrying no message must not silently reduce the error to
    /// nothing when other failures do have messages.
    #[test]
    fn failure_without_a_message_does_not_mask_others() {
        let mut messageless = bad("a", "");
        messageless.error_message = None;

        let results = vec![messageless, bad("b", "real failure")];

        let err = aggregate_failures(&results).expect("expected an error");
        assert!(err.to_string().contains("real failure"));
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::path::{Path, PathBuf};

    /// A scratch directory under the system temp dir, removed on drop.
    pub(crate) struct TempDir(PathBuf);

    impl TempDir {
        pub(crate) fn new(tag: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "gpudb_kifs_{tag}_{}_{}",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        pub(crate) fn path(&self) -> &Path {
            &self.0
        }

        pub(crate) fn str(&self) -> String {
            self.0.to_string_lossy().to_string()
        }

        /// Writes `contents` to `relative` under this directory, creating any
        /// parent directories, and returns the file's path.
        pub(crate) fn write(&self, relative: &str, contents: &[u8]) -> PathBuf {
            let path = self.0.join(relative);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("create parent dir");
            }
            std::fs::write(&path, contents).expect("write file");
            path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

#[cfg(test)]
mod local_file_tests {
    use super::test_support::TempDir;
    use super::*;

    fn options(split: i64) -> KifsFileHandlerOptions {
        KifsFileHandlerOptions::new().with_file_size_to_split(split).unwrap()
    }

    fn batch_locals(batch: &FileBatch) -> Vec<&str> {
        batch.files.iter().map(|(l, _)| l.as_str()).collect()
    }

    // ---- glob detection ----

    #[test]
    fn glob_characters_are_recognised() {
        for s in ["*.csv", "a?.csv", "{a,b}.csv", "[ab].csv", "a\\b.csv"] {
            assert!(is_glob_pattern(s), "{s} should be a pattern");
        }
        assert!(!is_glob_pattern("/data/plain_file.csv"));
    }

    // `local_file_exists` skips the check for `* ? { [` but not for a
    // backslash, so a backslash-only name must really exist.
    #[test]
    fn local_file_exists_does_not_treat_a_backslash_as_a_pattern() {
        let dir = TempDir::new("exists");
        assert!(local_file_exists("/no/such/dir/*.csv"));
        assert!(local_file_exists("/no/such/dir/[ab].csv"));
        assert!(!local_file_exists(&format!("{}/no\\such.csv", dir.str())));

        let real = dir.write("back\\slash.csv", b"x");
        assert!(local_file_exists(&real.to_string_lossy()));
    }

    #[test]
    fn local_file_exists_rejects_blank_names() {
        assert!(!local_file_exists(""));
        assert!(!local_file_exists("   "));
    }

    #[tokio::test]
    async fn local_dir_exists_checks_for_a_directory() {
        let dir = TempDir::new("direxists");
        let file = dir.write("f.txt", b"x");
        assert!(local_dir_exists(&dir.str()).await);
        assert!(!local_dir_exists(&file.to_string_lossy()).await);
        assert!(!local_dir_exists(&format!("{}/missing", dir.str())).await);
        assert!(!local_dir_exists("  ").await);
    }

    // ---- batching ----

    #[test]
    fn batches_keep_insertion_order() {
        let mut manager = FullFileBatcher::new(&options(1000));
        for name in ["z", "a", "m", "b", "y"] {
            manager.add_file(name, &format!("dir/{name}"), 1);
        }
        manager.create_batches();

        assert_eq!(manager.number_of_batches(), 1);
        let batch = manager.nth_batch(0).unwrap();
        assert_eq!(batch_locals(batch), vec!["z", "a", "m", "b", "y"]);
        assert_eq!(batch.total_size, 5);
    }

    // Adding the same local file twice keeps its first position and takes
    // the latest remote name.
    #[test]
    fn a_file_added_twice_is_batched_once() {
        let mut manager = FullFileBatcher::new(&options(1000));
        manager.add_file("a", "dir/a1", 1);
        manager.add_file("b", "dir/b", 1);
        manager.add_file("a", "dir/a2", 2);
        manager.create_batches();

        let batch = manager.nth_batch(0).unwrap();
        assert_eq!(
            batch.files,
            vec![("a".to_string(), "dir/a2".to_string()), ("b".to_string(), "dir/b".to_string())]
        );
        assert_eq!(batch.total_size, 3);
    }

    #[test]
    fn batches_split_at_the_size_limit() {
        let mut manager = FullFileBatcher::new(&options(10));
        // 4+6 reaches the limit exactly and closes; 5 would take 7 past it, so
        // 7 is closed alone and 5 starts a new batch, which the 0-byte file
        // joins.
        for (name, size) in [("a", 4), ("b", 6), ("c", 7), ("d", 5), ("e", 0)] {
            manager.add_file(name, name, size);
        }
        manager.create_batches();

        let batches: Vec<Vec<&str>> = manager.batches().iter().map(batch_locals).collect();
        assert_eq!(batches, vec![vec!["a", "b"], vec!["c"], vec!["d", "e"]]);
    }

    // A batch that is filled exactly is closed, so a following 0-byte file
    // goes into the next batch — the one case where this split differs
    // from a plain "add while it fits".
    #[test]
    fn an_exactly_full_batch_is_closed() {
        let mut manager = FullFileBatcher::new(&options(10));
        manager.add_file("a", "a", 10);
        manager.add_file("b", "b", 0);
        manager.create_batches();

        let batches: Vec<Vec<&str>> = manager.batches().iter().map(batch_locals).collect();
        assert_eq!(batches, vec![vec!["a"], vec!["b"]]);
    }

    #[test]
    fn a_file_larger_than_the_limit_does_not_loop_forever() {
        let mut manager = FullFileBatcher::new(&options(10));
        manager.add_file("a", "a", 3);
        manager.add_file("big", "big", 50);
        manager.add_file("b", "b", 3);
        manager.create_batches();

        let batches: Vec<Vec<&str>> = manager.batches().iter().map(batch_locals).collect();
        assert_eq!(batches, vec![vec!["a"], vec!["big"], vec!["b"]]);
    }

    #[test]
    fn create_batches_can_be_called_again() {
        let mut manager = FullFileBatcher::new(&options(10));
        manager.add_file("a", "a", 3);
        manager.create_batches();
        manager.create_batches();
        assert_eq!(manager.number_of_batches(), 1);

        manager.clear_batches();
        manager.create_batches();
        assert_eq!(manager.number_of_batches(), 0);
    }

    // ---- glob syntax ----

    fn glob_matches(glob: &str, s: &str) -> bool {
        let pattern = nio_glob_to_regex(glob, false).expect("valid glob");
        regex::Regex::new(&pattern).expect("valid regex").is_match(s)
    }

    /// The regexes some patterns translate to, on Unix.
    #[test]
    fn globs_translate_to_regexes() {
        assert_eq!(nio_glob_to_regex("*.csv", false).unwrap(), "^[^/]*\\.csv$");
        assert_eq!(nio_glob_to_regex("**/x", false).unwrap(), "^.*/x$");
        assert_eq!(nio_glob_to_regex("a?c", false).unwrap(), "^a[^/]c$");
        assert_eq!(nio_glob_to_regex("*.{csv,json}", false).unwrap(), "^[^/]*\\.(?:(?:csv)|(?:json))$");
        assert_eq!(nio_glob_to_regex("[!a-c]", false).unwrap(), "^[[^/]&&[^a-c]]$");
        assert_eq!(nio_glob_to_regex("a\\*b", false).unwrap(), "^a\\*b$");
    }

    #[test]
    fn star_stays_within_a_component_and_double_star_crosses() {
        assert!(glob_matches("*.csv", "a.csv"));
        assert!(glob_matches("*.csv", ".hidden.csv"));
        assert!(!glob_matches("*.csv", "sub/a.csv"));
        assert!(glob_matches("**.csv", "sub/a.csv"));
        assert!(glob_matches("/b/**/*.csv", "/b/sub/a.csv"));
        // `**/` needs at least one directory, unlike the `glob` crate's.
        assert!(!glob_matches("/b/**/*.csv", "/b/a.csv"));
        assert!(!glob_matches("a?c", "a/c"));
    }

    #[test]
    fn groups_classes_and_escapes() {
        assert!(glob_matches("*.{csv,json}", "x.json"));
        assert!(!glob_matches("*.{csv,json}", "x.txt"));
        assert!(glob_matches("x{}y", "xy"));
        assert!(glob_matches("[ab].csv", "b.csv"));
        assert!(!glob_matches("[!ab].csv", "b.csv"));
        assert!(glob_matches("[-a]", "-"));
        assert!(glob_matches("[a-]", "-"));
        assert!(glob_matches("[{,]", ","));
        assert!(glob_matches("[a&&b]", "&"));
        assert!(glob_matches("a\\*b", "a*b"));
        assert!(!glob_matches("a\\*b", "axb"));
        assert!(glob_matches("a,b}", "a,b}"));
        assert!(glob_matches("(x)+.|$^", "(x)+.|$^"));
    }

    #[test]
    fn invalid_globs_are_rejected() {
        for glob in ["{a,{b,c}}", "{a,b", "[ab", "a\\", "[a/b]", "[z-a]", "[a--]"] {
            assert!(nio_glob_to_regex(glob, false).is_err(), "{glob} should be rejected");
        }
    }

    #[test]
    fn dos_globs_use_the_backslash_separator() {
        assert_eq!(nio_glob_to_regex("*/x", true).unwrap(), "^[^\\\\]*\\\\x$");
        assert!(nio_glob_to_regex("[a\\b]", true).is_err());
    }

    // ---- directory search ----

    fn search(
        dir: &TempDir,
        pattern: &str,
        recursive: bool,
    ) -> (Vec<String>, Vec<String>) {
        search_local_directories(&dir.str(), pattern, "kdir", recursive).expect("search")
    }

    fn fixture() -> TempDir {
        let dir = TempDir::new("search");
        dir.write("b.csv", b"1");
        dir.write("a.csv", b"1");
        dir.write("c.json", b"1");
        dir.write(".hidden.csv", b"1");
        dir.write("sub/d.csv", b"1");
        dir.write("sub/deeper/e.csv", b"1");
        dir.write("other/sub/f.csv", b"1");
        dir
    }

    #[test]
    fn a_name_pattern_matches_top_level_files_only_when_not_recursive() {
        let dir = fixture();
        let (_, remotes) = search(&dir, "*.csv", false);
        assert_eq!(remotes, vec!["kdir/.hidden.csv", "kdir/a.csv", "kdir/b.csv"]);
    }

    #[test]
    fn a_recursive_name_pattern_matches_at_any_depth_in_walk_order() {
        let dir = fixture();
        let (locals, remotes) = search(&dir, "*.csv", true);
        assert_eq!(
            remotes,
            vec![
                "kdir/.hidden.csv",
                "kdir/a.csv",
                "kdir/b.csv",
                "kdir/other/sub/f.csv",
                "kdir/sub/d.csv",
                "kdir/sub/deeper/e.csv",
            ]
        );
        // Local paths are absolute.
        assert!(locals.iter().all(|l| Path::new(l).is_absolute()));
    }

    /// With a separator the pattern is matched against the whole path, so
    /// `recursive` does not make `sub/*.csv` match deeper `sub` directories —
    /// the old `base/**/sub/*.csv` did.
    #[test]
    fn a_path_pattern_matches_only_below_the_base_directory() {
        let dir = fixture();
        let (_, remotes) = search(&dir, "sub/*.csv", true);
        assert_eq!(remotes, vec!["kdir/sub/d.csv"]);
    }

    #[test]
    fn a_double_star_path_pattern_recurses_even_when_not_recursive() {
        let dir = fixture();
        let (_, remotes) = search(&dir, "**/*.csv", false);
        assert_eq!(
            remotes,
            vec!["kdir/other/sub/f.csv", "kdir/sub/d.csv", "kdir/sub/deeper/e.csv"]
        );
    }

    #[test]
    fn a_brace_group_matches_each_alternative() {
        let dir = fixture();
        let (_, remotes) = search(&dir, "[ac].{csv,json}", false);
        assert_eq!(remotes, vec!["kdir/a.csv", "kdir/c.json"]);
    }

    #[test]
    fn a_plain_name_is_found_at_any_depth_when_recursive() {
        let dir = fixture();
        dir.write("sub/a.csv", b"1");
        let (_, remotes) = search(&dir, "a.csv", true);
        assert_eq!(remotes, vec!["kdir/a.csv", "kdir/sub/a.csv"]);
    }

    #[test]
    fn an_empty_target_dir_gives_relative_remote_names() {
        let dir = fixture();
        let (_, remotes) = search_local_directories(&dir.str(), "sub/*.csv", "", true).unwrap();
        assert_eq!(remotes, vec!["sub/d.csv"]);
    }

    // Without `recursive` the search looks only at the base directory's own
    // entries, so a pattern naming files one level down finds nothing: the
    // walk depth is 1 here.
    #[test]
    fn a_path_pattern_finds_nothing_when_not_recursive() {
        let dir = fixture();
        let (_, remotes) = search(&dir, "*/d.csv", false);
        assert!(remotes.is_empty(), "{remotes:?}");

        let (_, remotes) = search(&dir, "*/d.csv", true);
        assert_eq!(remotes, vec!["kdir/sub/d.csv"]);
    }

    #[test]
    fn a_missing_directory_yields_no_files() {
        let dir = TempDir::new("missing");
        let missing = format!("{}/nope", dir.str());
        let (locals, remotes) = search_local_directories(&missing, "*.csv", "kdir", true).unwrap();
        assert!(locals.is_empty() && remotes.is_empty());
    }

    #[test]
    fn a_relative_base_dir_is_made_absolute() {
        let dir = fixture();
        let relative = format!("{}/sub/..", dir.str());
        let (locals, _) = search_local_directories(&relative, "a.csv", "kdir", false).unwrap();
        assert_eq!(locals, vec![dir.path().join("a.csv").to_string_lossy().to_string()]);
    }

    #[test]
    fn an_invalid_pattern_is_an_error() {
        let dir = fixture();
        assert!(search_local_directories(&dir.str(), "{a,b", "kdir", false).is_err());
        assert!(search_local_directories(&dir.str(), "[", "kdir", false).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn links_to_directories_are_not_followed_or_reported() {
        let dir = fixture();
        std::os::unix::fs::symlink(dir.path().join("sub"), dir.path().join("link_to_sub")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("a.csv"), dir.path().join("link_to_a.csv")).unwrap();

        let (_, remotes) = search(&dir, "*.csv", true);
        assert!(remotes.contains(&"kdir/link_to_a.csv".to_string()), "{remotes:?}");
        assert!(!remotes.iter().any(|r| r.contains("link_to_sub")), "{remotes:?}");
    }

    // A start directory reached through a link is searched as the
    // directory, with the paths found under the link.
    #[cfg(unix)]
    #[test]
    fn a_start_directory_reached_through_a_link_is_searched() {
        let dir = fixture();
        let links = TempDir::new("search_link");
        let link = links.path().join("link");
        std::os::unix::fs::symlink(dir.path(), &link).unwrap();
        let link = link.to_string_lossy().to_string();

        let (locals, remotes) = search_local_directories(&link, "*.csv", "kdir", false).unwrap();
        assert_eq!(locals, [format!("{link}/.hidden.csv"), format!("{link}/a.csv"), format!("{link}/b.csv")]);
        assert_eq!(remotes, ["kdir/.hidden.csv", "kdir/a.csv", "kdir/b.csv"]);

        let (_, remotes) = search_local_directories(&link, "*.csv", "kdir", true).unwrap();
        assert!(remotes.contains(&"kdir/sub/deeper/e.csv".to_string()), "{remotes:?}");
    }

    // A file given without a pattern is named by its base directory, the
    // separator and its name, so a bare name is "./<name>".
    #[test]
    fn a_file_named_without_a_pattern_is_in_the_current_directory() {
        let sep = std::path::MAIN_SEPARATOR;
        assert_eq!(parse_file_name("a.csv").file_path(), format!(".{sep}a.csv"));
        assert_eq!(parse_file_name("/data/a.csv").file_path(), format!("/data{sep}a.csv"));
        assert_eq!(parse_file_name("rel/a.csv").file_path(), format!("rel{sep}a.csv"));
    }

    // ---- sizes and names ----

    #[test]
    fn files_whose_size_cannot_be_read_are_left_out() {
        let dir = TempDir::new("categorize");
        let small = dir.write("small.csv", b"123");
        let big = dir.write("big.csv", &[0u8; 20]);
        let missing = dir.path().join("gone.csv");

        let locals: Vec<String> = [&small, &missing, &big]
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        let remotes = vec!["k/small.csv".to_string(), "k/gone.csv".to_string(), "k/big.csv".to_string()];

        let (manager, multipart) = categorize_files_by_size(&locals, &remotes, &options(10));
        assert_eq!(manager.number_of_batches(), 1);
        assert_eq!(manager.nth_batch(0).unwrap().files, vec![(locals[0].clone(), remotes[0].clone())]);
        assert_eq!(multipart, vec![(locals[2].clone(), remotes[2].clone())]);
    }

    // A downloaded file keeps its path below the KiFS path asked for.
    #[test]
    fn a_downloaded_file_keeps_its_path_below_the_one_asked_for() {
        for (requested, name, local) in [
            ("dir", "dir/a.csv", "a.csv"),
            ("dir/", "dir/a.csv", "a.csv"),
            ("dir", "dir/a/x.csv", "a/x.csv"),
            ("dir", "dir/b/x.csv", "b/x.csv"),
            ("dir/a/x.csv", "dir/a/x.csv", "x.csv"),
            ("dir/*.csv", "dir/a.csv", "a.csv"),
            ("dir/*", "dir/sub/x.csv", "sub/x.csv"),
            ("dir/sub", "dir/sub/x.csv", "x.csv"),
            ("other", "dir/x.csv", "x.csv"),
            ("dir", "dir/./a.csv", "a.csv"),
        ] {
            assert_eq!(download_relative_path(requested, name).as_deref(), Ok(local), "{requested} {name}");
        }
        assert!(download_relative_path("dir", "dir/../../etc/x").is_err());
        assert!(download_relative_path("dir", "dir/a/../../x").is_err());
    }

    #[test]
    fn kifs_file_name_takes_the_last_component() {
        assert_eq!(kifs_file_name("dir/a/x.csv"), "x.csv");
        assert_eq!(kifs_file_name("dir/x.csv"), "x.csv");
        assert_eq!(kifs_file_name("x.csv"), "x.csv");
    }

    #[test]
    fn normalize_absolute_removes_dot_components() {
        let base = std::env::temp_dir();
        let messy = base.join("a").join(".").join("b").join("..").join("c");
        assert_eq!(normalize_absolute(&messy).unwrap(), base.join("a").join("c"));
        assert!(normalize_absolute(Path::new("rel")).unwrap().is_absolute());
    }
}

//! File download functionality for KiFS.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use log::{debug, error, info, warn};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::{Kinetica, KineticaError, DownloadFilesRequest, ShowFilesRequest};
use super::file_operation::{aggregate_failures, download_relative_path, io_error_message, kifs_file_name, normalize_absolute};
use super::listeners::FileDownloadListener;
use super::options::{DownloadOptions, KifsFileHandlerOptions};
use super::types::{KifsFileInfo, MultiPartDownloadInfo, OpMode, OperationResult};

/// Internal downloader for handling file downloads from KiFS.
pub struct FileDownloader<'a> {
    /// Kinetica client instance
    db: Arc<Kinetica>,
    /// List of KiFS file paths to download
    file_names: Vec<String>,
    /// Target local directory
    local_dir_name: String,
    /// Download options
    download_options: DownloadOptions,
    /// File handler options
    file_handler_options: KifsFileHandlerOptions,
    /// Optional callback listener
    callback: Option<&'a dyn FileDownloadListener>,
    /// Where each KiFS file found is saved (see [`download_relative_path`])
    local_paths: parking_lot::Mutex<HashMap<String, PathBuf>>,
}

impl<'a> FileDownloader<'a> {
    /// Creates a new file downloader.
    pub fn new(
        db: Arc<Kinetica>,
        file_names: Vec<String>,
        local_dir_name: String,
        download_options: DownloadOptions,
        file_handler_options: KifsFileHandlerOptions,
        callback: Option<&'a dyn FileDownloadListener>,
    ) -> Self {
        Self {
            db,
            file_names,
            local_dir_name,
            download_options,
            file_handler_options,
            callback,
            local_paths: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Performs the download operation with error aggregation.
    ///
    /// This method:
    /// 1. Gets file info from KiFS
    /// 2. Categorizes files into full-file and multi-part downloads
    /// 3. Downloads multi-part files one at a time
    /// 4. Downloads full files in batches
    /// 5. Aggregates all errors and returns them at the end
    ///
    /// There is a result per file, named by its KiFS path. A failure does not
    /// stop the rest; their messages are joined, a line each, into the error
    /// returned.
    pub async fn download(&self) -> Result<Vec<OperationResult>, KineticaError> {
        // Categorize files by size
        let (full_files, multipart_files, refused) = self.categorize_files_by_size().await?;

        debug!(
            "Found {} full files and {} multi-part files to download",
            full_files.len(),
            multipart_files.len()
        );

        let mut all_results = refused;
        all_results.extend(self.download_multi_part_files(&multipart_files).await);
        all_results.extend(self.download_full_files(&full_files).await);

        if full_files.is_empty() && multipart_files.is_empty() {
            warn!("No files found to download ...");
        }

        // Report every failure together, rather than the first one only.
        if let Some(e) = aggregate_failures(&all_results) {
            return Err(e);
        }

        Ok(all_results)
    }

    /// Sorts the files the given KiFS paths name into those downloaded whole
    /// and those downloaded in parts, by size.
    ///
    /// Each file is planned to its local path (see
    /// [`download_relative_path`]), and once: a file named again, by another
    /// of the paths given, is not downloaded twice. A file whose local path
    /// is another's, or would leave the local directory, is not downloaded,
    /// and gets a failed result.
    async fn categorize_files_by_size(&self) -> Result<(Vec<String>, Vec<String>, Vec<OperationResult>), KineticaError> {
        let mut full_files = Vec::new();
        let mut multipart_files = Vec::new();
        let mut refused = Vec::new();
        let mut local_paths: HashMap<String, PathBuf> = HashMap::new();
        let mut saved_from: HashMap<PathBuf, String> = HashMap::new();

        for kifs_path in &self.file_names {
            let request = ShowFilesRequest {
                paths: vec![kifs_path.clone()],
                options: HashMap::new(),
            };
            let response = self.db.show_files_raw(&request).await?;

            for (i, file_name) in response.file_names.iter().enumerate() {
                if local_paths.contains_key(file_name) {
                    continue;
                }
                let local = match download_relative_path(kifs_path, file_name) {
                    Ok(relative) => self.under_local_dir(&relative),
                    Err(message) => {
                        error!("{message}");
                        refused.push(OperationResult::failure(file_name.clone(), OpMode::Download, &message)
                            .with_error(KineticaError::Config(message.clone())));
                        continue;
                    }
                };
                if let Some(other) = saved_from.get(&local) {
                    let message = format!(
                        "KiFS file <{file_name}> was not downloaded: its local file <{}> is the download of KiFS file <{other}>",
                        local.display()
                    );
                    error!("{message}");
                    refused.push(OperationResult::failure(file_name.clone(), OpMode::Download, &message)
                        .with_error(KineticaError::Config(message.clone())));
                    continue;
                }
                saved_from.insert(local.clone(), file_name.clone());
                local_paths.insert(file_name.clone(), local);
                if response.sizes.get(i).copied().unwrap_or(0) > self.file_handler_options.file_size_to_split() {
                    multipart_files.push(file_name.clone());
                } else {
                    full_files.push(file_name.clone());
                }
            }
        }

        *self.local_paths.lock() = local_paths;
        Ok((full_files, multipart_files, refused))
    }

    /// The statistics of the files `path` names.
    async fn file_info_from_server(&self, path: &str) -> Result<Vec<KifsFileInfo>, KineticaError> {
        let request = ShowFilesRequest { paths: vec![path.to_string()], options: HashMap::new() };
        let response = self.db.show_files_raw(&request).await?;
        Ok(response
            .file_names
            .iter()
            .enumerate()
            .map(|(i, file_name)| KifsFileInfo {
                file_name: file_name.clone(),
                file_size: response.sizes.get(i).copied().unwrap_or(0),
                created_by: response.users.get(i).cloned().unwrap_or_default(),
                creation_time: response.creation_times.get(i).copied().unwrap_or(0),
                info: HashMap::new(),
            })
            .collect())
    }

    /// Downloads the multi-part files, one at a time, each looked up again
    /// for its size first.
    async fn download_multi_part_files(&self, kifs_names: &[String]) -> Vec<OperationResult> {
        let mut results = Vec::with_capacity(kifs_names.len());
        for kifs_name in kifs_names {
            let mut result = OperationResult {
                file_name: kifs_name.clone(),
                multi_part: true,
                op_mode: Some(OpMode::Download),
                ..Default::default()
            };

            // A file the server no longer lists fails on its own
            let info = match self.file_info_from_server(kifs_name).await {
                Ok(infos) => infos
                    .into_iter()
                    .next()
                    .ok_or_else(|| KineticaError::Api("the server lists no such file".to_string())),
                Err(e) => Err(e),
            };
            // The message, and the error the result holds
            let outcome = match info {
                Ok(info) => self.download_multipart_file(&info).await.map_err(|message| {
                    (format!("Failed to download multi-part file <{kifs_name}>: {message}"), KineticaError::Api(message))
                }),
                Err(e) => Err((format!("Failed to get file info for multi-part download <{kifs_name}>: {}", e.message()), e)),
            };

            match outcome {
                Ok(()) => {
                    info!("Downloaded multi-part file: {kifs_name}");
                    result.successful = true;
                }
                Err((error_message, error)) => {
                    error!("{error_message}");
                    result.error_message = Some(error_message);
                    result.error = Some(error);
                }
            }
            results.push(result);
        }
        results
    }

    /// Downloads the full files in batches.
    async fn download_full_files(&self, kifs_names: &[String]) -> Vec<OperationResult> {
        let mut results = Vec::new();
        for batch in self.create_download_batches(kifs_names).await {
            results.extend(self.download_full_file_batch(&batch).await);
        }
        results
    }

    /// Groups the full files into batches of up to the split size, each
    /// file's size looked up again.
    ///
    /// A batch is closed once its files reach the split size exactly, or
    /// before a file that would take it past. A file whose size cannot be
    /// looked up counts as empty, the failure logged.
    async fn create_download_batches(&self, kifs_names: &[String]) -> Vec<Vec<String>> {
        let mut sizes = Vec::with_capacity(kifs_names.len());
        for kifs_name in kifs_names {
            let request = ShowFilesRequest { paths: vec![kifs_name.clone()], options: HashMap::new() };
            let size = match self.db.show_files_raw(&request).await {
                Ok(response) => response.sizes.first().copied().unwrap_or(0),
                Err(e) => {
                    error!("{}", e.message());
                    0
                }
            };
            sizes.push(size);
        }
        batch_by_size(kifs_names, &sizes, self.file_handler_options.file_size_to_split())
    }

    /// Downloads a batch of full files in a single API call.
    ///
    /// A file that cannot be written locally is recorded as a failure and the
    /// rest of the batch is still saved. If the request itself fails, every
    /// file in the batch fails with it. The listener hears of the files
    /// saved, if any.
    async fn download_full_file_batch(&self, batch: &[String]) -> Vec<OperationResult> {
        let request = DownloadFilesRequest {
            file_names: batch.to_vec(),
            read_offsets: vec![],
            read_lengths: vec![],
            options: HashMap::new(),
        };

        let mut results = Vec::new();

        let response = match self.db.download_files_raw(&request).await {
            Ok(response) => response,
            Err(e) => {
                let error_message = format!("Failed to download file batch: {}", e.message());
                error!("{error_message}");
                for kifs_name in batch {
                    results.push(
                        OperationResult::failure(kifs_name.clone(), OpMode::Download, &error_message).with_error(e.clone()),
                    );
                }
                return results;
            }
        };

        let mut saved_names: Vec<String> = Vec::new();
        for (i, kifs_name) in response.file_names.iter().enumerate() {
            // A name with no matching payload would otherwise drop out of the
            // report entirely — neither saved nor failed. Record it.
            let Some(data) = response.file_data.get(i) else {
                let error_message = format!("Server returned no data for file '{kifs_name}'; it was not saved");
                error!("{error_message}");
                results.push(
                    OperationResult::failure(kifs_name.clone(), OpMode::Download, &error_message)
                        .with_error(KineticaError::Api(error_message.clone())),
                );
                continue;
            };

            match self.save_full_file(kifs_name, data).await {
                // A file skipped because it exists and overwriting is off also
                // lands here, and is reported as downloaded — to the caller
                // and to the listener — as `save_full_file` returns normally
                // after its warning. The risk is that the caller cannot tell a
                // stale local copy from a fresh download.
                Ok(()) => {
                    saved_names.push(kifs_name.clone());
                    results.push(OperationResult::success(kifs_name.clone(), OpMode::Download));
                }
                Err(message) => {
                    let error_message = format!("Failed to save downloaded file <{kifs_name}>: {message}");
                    error!("{error_message}");
                    results.push(
                        OperationResult::failure(kifs_name.clone(), OpMode::Download, &error_message)
                            .with_error(KineticaError::Api(message)),
                    );
                }
            }
        }

        if let Some(callback) = self.callback {
            if !saved_names.is_empty() {
                callback.on_full_file_download(saved_names);
            }
        }

        results
    }

    /// Where a KiFS file is saved: at its planned path below the local
    /// directory (see `categorize_files_by_size`), made absolute.
    fn local_path(&self, kifs_name: &str) -> PathBuf {
        self.local_paths.lock().get(kifs_name).cloned().unwrap_or_else(|| self.under_local_dir(kifs_file_name(kifs_name)))
    }

    /// `relative`, a `/`-separated path, in the local directory, made
    /// absolute.
    fn under_local_dir(&self, relative: &str) -> PathBuf {
        let mut path = PathBuf::from(&self.local_dir_name);
        path.extend(relative.split('/'));
        normalize_absolute(&path).unwrap_or(path)
    }

    /// Saves a whole file downloaded. Returns, on failure, the message to
    /// report.
    ///
    /// Nothing is written when the file exists and overwriting is off; a
    /// warning says so.
    async fn save_full_file(&self, kifs_name: &str, data: &[u8]) -> Result<(), String> {
        let path = self.local_path(kifs_name);
        debug!("Writing downloaded KiFS file <{}> to <{}>", kifs_file_name(kifs_name), path.display());

        // A file whose existence cannot be determined is treated as existing.
        let exists = tokio::fs::try_exists(&path).await.unwrap_or(true);
        if exists && !self.download_options.overwrite_existing {
            warn!(
                "File <{kifs_name}> exists in local directory <{}>; Use 'overwrite_existing' to download and overwrite ",
                self.local_dir_name
            );
            return Ok(());
        }

        let write_failed = |e: std::io::Error| {
            format!(
                "Error writing downloaded KiFS file <{kifs_name}> to <{}>:  {}",
                path.display(),
                io_error_message(&path.to_string_lossy(), &e)
            )
        };
        create_parent_dirs(&path).await?;
        let mut staged = StagedFile::create(&path).await.map_err(write_failed)?;
        staged.file.write_all(data).await.map_err(write_failed)?;
        staged.commit().await.map_err(write_failed)
    }

    /// Downloads a large file in multiple parts. Returns, on failure, the
    /// message to report.
    ///
    /// Nothing is downloaded when the local file exists and overwriting is
    /// off. Otherwise the parts, numbered from 0, are written in order as
    /// they arrive to a temporary file beside the local one (see
    /// [`StagedFile`]), which takes its place once every part is in; the
    /// listener hears of each, and of the download's completion. If a part
    /// fails, the temporary file is deleted and an existing local copy is left
    /// as it was. The deletion is done by a guard, so it also happens if this
    /// future is dropped mid-download.
    ///
    /// Each part asks for the bytes left in the file, at most the split size.
    async fn download_multipart_file(&self, file_info: &KifsFileInfo) -> Result<(), String> {
        let file_size = file_info.file_size;
        // At least 1, as the options only accept a positive size; `max` keeps
        // the division safe regardless.
        let part_size = self.file_handler_options.file_size_to_split().max(1);
        // The number of parts actually requested.
        let num_parts = (file_size + part_size - 1) / part_size;

        let local_path = self.local_path(&file_info.file_name);

        debug!(
            "Starting multi-part download: {} -> {} ({} parts)",
            file_info.file_name,
            local_path.display(),
            num_parts
        );

        // A file whose existence cannot be determined is treated as existing.
        // A file kept is reported as downloaded, without a warning.
        let local_exists = tokio::fs::try_exists(&local_path).await.unwrap_or(true);
        if local_exists && !self.download_options.overwrite_existing {
            return Ok(());
        }

        create_parent_dirs(&local_path).await.inspect_err(|message| error!("{message}"))?;
        // A file that cannot be written is left as it is. From here on, any
        // early return — or the future being dropped — deletes the temporary
        // file.
        let mut staged = StagedFile::create(&local_path).await.map_err(|e| {
            let message = io_error_message(&local_path.to_string_lossy(), &e);
            error!("{message}");
            message
        })?;

        let mut result_list: Vec<OperationResult> = Vec::new();

        for part_num in 0..num_parts {
            let offset = part_num * part_size;
            let length = std::cmp::min(file_size - offset, part_size);
            let part_failed =
                |message: String| format!("Could not complete download part #{part_num} : {message}");

            let request = DownloadFilesRequest {
                file_names: vec![file_info.file_name.clone()],
                read_offsets: vec![offset],
                read_lengths: vec![length],
                options: HashMap::new(),
            };

            let response = match self.db.download_files_raw(&request).await {
                Ok(response) => response,
                Err(e) => {
                    error!("{}", e.message());
                    let message = format!(
                        "Could not complete download part #{part_num} for file <{}> : {}",
                        file_info.file_name,
                        e.message()
                    );
                    error!("{message}");
                    return Err(message);
                }
            };

            // A response with no data for the part fails the download.
            // Skipping it instead would leave a gap and report the file as
            // downloaded.
            let Some(data) = response.file_data.first() else {
                return Err(part_failed(
                    "the server sent no data for the part".to_string(),
                ));
            };

            // A part of another length than asked for, from a file changed
            // since it was listed, would leave the parts after it out of
            // place.
            if data.len() as i64 != length {
                return Err(part_failed(format!(
                    "the server sent {} bytes where {length} were asked for",
                    data.len()
                )));
            }

            // Written where the previous part ended.
            staged.file.write_all(data)
                .await
                .map_err(|e| part_failed(io_error_message(&local_path.to_string_lossy(), &e)))?;

            let download_info = MultiPartDownloadInfo {
                file_name: file_info.file_name.clone(),
                read_offset: offset,
                read_length: length,
                download_part_number: part_num,
                total_parts: num_parts,
            };

            let result = OperationResult::multipart_download_success(&file_info.file_name, download_info);
            result_list.push(result.clone());

            if let Some(callback) = self.callback {
                callback.on_part_download(result);
            }
        }

        // Flush the file and put it in place. A tokio `File` completes its
        // writes in the background, so a write error can surface only here;
        // it is a failed write, and the temporary file is deleted as for any
        // other.
        staged.commit().await.map_err(|e| io_error_message(&local_path.to_string_lossy(), &e))?;

        if let Some(callback) = self.callback {
            callback.on_multi_part_download_complete(result_list);
        }

        Ok(())
    }
}

/// Groups whole files to download into batches: each file joins the batch
/// being filled; one that takes the batch past `split` starts the next
/// batch, and a batch that reaches `split` exactly is closed with it.
fn batch_by_size(names: &[String], sizes: &[i64], split: i64) -> Vec<Vec<String>> {
    let mut batches = Vec::new();
    let mut batch: Vec<String> = Vec::new();
    let mut sum: i64 = 0;
    for (name, &size) in names.iter().zip(sizes) {
        if !batch.is_empty() && sum + size > split {
            batches.push(std::mem::take(&mut batch));
            sum = 0;
        }
        batch.push(name.clone());
        sum += size;
        if sum >= split {
            batches.push(std::mem::take(&mut batch));
            sum = 0;
        }
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    batches
}

/// Deletes a partially downloaded file when dropped, unless
/// [`keep`](Self::keep) was called.
///
/// Dropping happens on every early return and when the download's future is
/// dropped, so a failed or abandoned multi-part download never leaves a
/// truncated file behind. The removal uses blocking `std::fs::remove_file`,
/// since `Drop` cannot await; deleting one file is brief.
struct PartialFileGuard {
    /// The file to delete, or `None` once kept
    path: Option<String>,
}

impl PartialFileGuard {
    fn new(path: &str) -> Self {
        Self { path: Some(path.to_string()) }
    }

    /// Keeps the file: the download completed.
    fn keep(&mut self) {
        self.path = None;
    }
}

impl Drop for PartialFileGuard {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        match std::fs::remove_file(&path) {
            Ok(()) => debug!("Cleaned up partial download file: {path}"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => error!("Failed to clean up partial download file <{path}>: {}", io_error_message(&path, &e)),
        }
    }
}

/// A downloaded file being written: a temporary file beside the local file
/// it is for, which takes the local file's place, by a rename, once it is
/// complete. Until then an existing local copy is left as it was, and the
/// temporary file is deleted if the download fails or is dropped.
struct StagedFile {
    file: tokio::fs::File,
    temp: PathBuf,
    target: PathBuf,
    guard: PartialFileGuard,
}

impl StagedFile {
    /// Creates the temporary file for `target`. Fails, as writing to it
    /// would, where `target` exists and cannot be written: a read-only local
    /// file is not replaced.
    async fn create(target: &std::path::Path) -> std::io::Result<Self> {
        if tokio::fs::try_exists(target).await.unwrap_or(false) {
            OpenOptions::new().write(true).open(target).await?;
        }
        let name = target.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
        let temp = target.with_file_name(format!(".{name}.{}.download", uuid::Uuid::new_v4().simple()));
        let file = OpenOptions::new().write(true).create_new(true).open(&temp).await?;
        let guard = PartialFileGuard::new(&temp.to_string_lossy());
        Ok(StagedFile { file, temp, target: target.to_path_buf(), guard })
    }

    /// Finishes the file and puts it in place of the local file.
    async fn commit(mut self) -> std::io::Result<()> {
        self.file.flush().await?;
        drop(self.file);
        tokio::fs::rename(&self.temp, &self.target).await?;
        self.guard.keep();
        Ok(())
    }
}

/// Makes the directories a file downloaded to `path` goes in, below the
/// local directory, where the KiFS file's path has subdirectories.
async fn create_parent_dirs(path: &std::path::Path) -> Result<(), String> {
    let Some(parent) = path.parent() else { return Ok(()) };
    tokio::fs::create_dir_all(parent).await.map_err(|e| {
        format!("Error creating local directory <{}>: {}", parent.display(), io_error_message(&parent.to_string_lossy(), &e))
    })
}

#[cfg(test)]
mod tests {
    use super::super::file_operation::test_support::TempDir;
    use super::*;

    // A batch closes on reaching the split size exactly, or before a file
    // that would take it past.
    #[test]
    fn files_are_batched_by_size() {
        let names: Vec<String> = ["a", "b", "c", "d", "e"].iter().map(|n| n.to_string()).collect();
        let batches = batch_by_size(&names, &[4, 0, 3, 2, 4], 4);
        assert_eq!(batches, [vec!["a"], vec!["b", "c"], vec!["d"], vec!["e"]]);
        assert_eq!(batch_by_size(&names[..2], &[1, 1], 4), [vec!["a", "b"]]);
    }

    /// Each /download/files request's names, offsets and lengths.
    type Downloads = Arc<parking_lot::Mutex<Vec<(Vec<String>, Vec<i64>, Vec<i64>)>>>;

    /// The KiFS files a stand-in server holds, and what it was asked for.
    struct Kifs {
        server: crate::gpudb::fake_server::FakeServer,
        k: Arc<Kinetica>,
        downloads: Downloads,
    }

    async fn kifs(host: &str, files: &'static [(&'static str, &'static [u8])], fail_offset: Option<i64>) -> Kifs {
        kifs_shrunk(host, files, fail_offset, None).await
    }

    /// [`kifs`], with each file's content cut to `shrink_to` bytes for
    /// /download/files, as if the files had shrunk since /show/files listed
    /// them.
    async fn kifs_shrunk(
        host: &str,
        files: &'static [(&'static str, &'static [u8])],
        fail_offset: Option<i64>,
        shrink_to: Option<usize>,
    ) -> Kifs {
        use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
        use crate::{DownloadFilesResponse, ShowFilesResponse};
        #[derive(serde::Deserialize)]
        struct Paths {
            paths: Vec<String>,
        }
        #[derive(serde::Deserialize)]
        struct Download {
            file_names: Vec<String>,
            read_offsets: Vec<i64>,
            read_lengths: Vec<i64>,
        }
        let server = fake_server::start(host).await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let downloads: Downloads = Default::default();
        let log = downloads.clone();
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            match path {
                "/show/files" => {
                    let request: Paths =
                        serde_avro_fast::from_datum_slice(body, &crate::generated::SHOW_FILES_REQUEST_SCHEMA).unwrap();
                    let matched: Vec<_> = files.iter().filter(|(name, _)| name.starts_with(&request.paths[0])).collect();
                    Some(Ok(encode(
                        &crate::generated::SHOW_FILES_RESPONSE_SCHEMA,
                        &ShowFilesResponse {
                            file_names: matched.iter().map(|(n, _)| n.to_string()).collect(),
                            sizes: matched.iter().map(|(_, d)| d.len() as i64).collect(),
                            users: matched.iter().map(|_| "u".to_string()).collect(),
                            creation_times: matched.iter().map(|_| 0).collect(),
                            ..Default::default()
                        },
                    )))
                }
                "/download/files" => {
                    let request: Download =
                        serde_avro_fast::from_datum_slice(body, &crate::generated::DOWNLOAD_FILES_REQUEST_SCHEMA).unwrap();
                    log.lock().push((request.file_names.clone(), request.read_offsets.clone(), request.read_lengths.clone()));
                    if fail_offset.is_some() && request.read_offsets.first() == fail_offset.as_ref() {
                        return Some(Err("no such part".to_string()));
                    }
                    let data = request
                        .file_names
                        .iter()
                        .enumerate()
                        .map(|(i, name)| {
                            let content = files.iter().find(|(n, _)| n == name).unwrap().1;
                            let content = &content[..shrink_to.unwrap_or(content.len()).min(content.len())];
                            let data = match (request.read_offsets.get(i), request.read_lengths.get(i)) {
                                (Some(&offset), Some(&length)) => {
                                    &content[offset as usize..((offset + length) as usize).min(content.len())]
                                }
                                _ => content,
                            };
                            serde_bytes::ByteBuf::from(data.to_vec())
                        })
                        .collect();
                    Some(Ok(encode(
                        &crate::generated::DOWNLOAD_FILES_RESPONSE_SCHEMA,
                        &DownloadFilesResponse { file_names: request.file_names, file_data: data, ..Default::default() },
                    )))
                }
                _ => None,
            }
        }));
        let mut options = crate::KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![server.url.to_string()], options).await.expect("client"));
        Kifs { server, k, downloads }
    }

    /// What a listener heard, in order.
    #[derive(Default)]
    struct Heard(parking_lot::Mutex<Vec<String>>);

    impl FileDownloadListener for Heard {
        fn on_multi_part_download_complete(&self, results: Vec<OperationResult>) {
            self.0.lock().push(format!("complete {}", results.len()));
        }
        fn on_part_download(&self, result: OperationResult) {
            let info = result.download_info.unwrap();
            self.0.lock().push(format!(
                "part {} of {} at {} for {}",
                info.download_part_number, info.total_parts, info.read_offset, info.read_length
            ));
        }
        fn on_full_file_download(&self, file_names: Vec<String>) {
            self.0.lock().push(format!("full {file_names:?}"));
        }
    }

    fn downloader<'a>(k: &Arc<Kinetica>, names: &[&str], dir: &TempDir, options: DownloadOptions, heard: &'a Heard) -> FileDownloader<'a> {
        FileDownloader::new(
            k.clone(),
            names.iter().map(|n| n.to_string()).collect(),
            dir.str(),
            options,
            KifsFileHandlerOptions::new().with_file_size_to_split(4).unwrap(),
            Some(heard),
        )
    }

    // Each whole file's size looked up again for batching, the files saved
    // named to the listener, and a large file's parts numbered from 0.
    #[tokio::test]
    async fn files_are_downloaded_whole_or_in_parts() {
        static FILES: &[(&str, &[u8])] = &[("d/a.txt", b"12"), ("d/b.txt", b"34"), ("d/c.txt", b"5"), ("d/big.bin", b"0123456789")];
        let kifs = kifs("127.0.0.151", FILES, None).await;
        let dir = TempDir::new("download_parity");
        dir.write("c.txt", b"local");
        let heard = Heard::default();

        let results = downloader(&kifs.k, &["d"], &dir, DownloadOptions::default().with_overwrite_existing(false), &heard).download().await.unwrap();
        let names: Vec<&str> = results.iter().map(|r| r.file_name.as_str()).collect();
        assert_eq!(names, ["d/big.bin", "d/a.txt", "d/b.txt", "d/c.txt"], "multi-part first");
        assert!(results.iter().all(|r| r.successful));

        assert_eq!(std::fs::read(dir.path().join("big.bin")).unwrap(), b"0123456789");
        assert_eq!(std::fs::read(dir.path().join("c.txt")).unwrap(), b"local", "new options do not overwrite");
        // One lookup for the path, one for the large file, one per whole file
        assert_eq!(kifs.server.behavior.count("/show/files"), 1 + 1 + 3);
        let downloads = kifs.downloads.lock().clone();
        assert_eq!(
            downloads,
            [
                (vec!["d/big.bin".to_string()], vec![0], vec![4]),
                (vec!["d/big.bin".to_string()], vec![4], vec![4]),
                (vec!["d/big.bin".to_string()], vec![8], vec![2]),
                (vec!["d/a.txt".to_string(), "d/b.txt".to_string()], vec![], vec![]),
                (vec!["d/c.txt".to_string()], vec![], vec![]),
            ]
        );
        assert_eq!(
            *heard.0.lock(),
            [
                "part 0 of 3 at 0 for 4",
                "part 1 of 3 at 4 for 4",
                "part 2 of 3 at 8 for 2",
                "complete 3",
                r#"full ["d/a.txt", "d/b.txt"]"#,
                r#"full ["d/c.txt"]"#,
            ]
        );
    }

    // A part that fails is reported, and the partial file is deleted.
    #[tokio::test]
    async fn a_failed_part_fails_the_file_and_removes_it() {
        static FILES: &[(&str, &[u8])] = &[("d/big.bin", b"0123456789")];
        let kifs = kifs("127.0.0.152", FILES, Some(4)).await;
        let dir = TempDir::new("download_part_failure");
        let heard = Heard::default();

        let err = downloader(&kifs.k, &["d/big.bin"], &dir, DownloadOptions::default(), &heard)
            .download()
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "API error: Failed to download multi-part file <d/big.bin>: Could not complete download part #1 for file <d/big.bin> : no such part"
        );
        assert!(!dir.path().join("big.bin").exists());
        assert!(names_in(&dir).is_empty(), "no temporary file left");
        assert_eq!(*heard.0.lock(), ["part 0 of 3 at 0 for 4"]);
    }

    /// The names in `dir`, sorted.
    fn names_in(dir: &TempDir) -> Vec<String> {
        let mut names: Vec<String> =
            std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
        names.sort();
        names
    }

    // A download that fails leaves an existing local copy as it was; one
    // that completes replaces it. No temporary file is left either way.
    #[tokio::test]
    async fn a_failed_download_leaves_the_local_copy_as_it_was() {
        static FILES: &[(&str, &[u8])] = &[("d/big.bin", b"0123456789")];
        let failing = kifs("127.0.0.156", FILES, Some(4)).await;
        let dir = TempDir::new("download_keeps_copy");
        let existing = dir.write("big.bin", b"old copy");
        let heard = Heard::default();

        downloader(&failing.k, &["d/big.bin"], &dir, DownloadOptions::default(), &heard)
            .download()
            .await
            .unwrap_err();
        assert_eq!(std::fs::read(&existing).unwrap(), b"old copy");
        assert_eq!(names_in(&dir), ["big.bin"], "no temporary file left");

        let working = kifs("127.0.0.157", FILES, None).await;
        downloader(&working.k, &["d/big.bin"], &dir, DownloadOptions::default(), &heard)
            .download()
            .await
            .unwrap();
        assert_eq!(std::fs::read(&existing).unwrap(), b"0123456789");
        assert_eq!(names_in(&dir), ["big.bin"], "no temporary file left");
    }

    // A directory's files keep their paths below it, so two of one name in
    // different subdirectories do not collide.
    // A file named twice is downloaded once, and one whose local path
    // another already has is refused rather than written over it.
    #[tokio::test]
    async fn a_directorys_files_keep_their_paths_below_it() {
        static FILES: &[(&str, &[u8])] =
            &[("d/a/x.csv", b"ax"), ("d/b/x.csv", b"bx"), ("d/top.csv", b"top"), ("e/top.csv", b"other")];
        let kifs = kifs("127.0.0.155", FILES, None).await;
        let dir = TempDir::new("download_structure");
        let heard = Heard::default();

        let err = downloader(&kifs.k, &["d", "e", "d/top.csv"], &dir, DownloadOptions::default(), &heard)
            .download()
            .await
            .unwrap_err();
        assert_eq!(std::fs::read(dir.path().join("a").join("x.csv")).unwrap(), b"ax");
        assert_eq!(std::fs::read(dir.path().join("b").join("x.csv")).unwrap(), b"bx");
        assert_eq!(std::fs::read(dir.path().join("top.csv")).unwrap(), b"top", "not written over by e/top.csv");
        assert!(!dir.path().join("x.csv").exists(), "not flattened");
        let message = err.to_string();
        assert!(message.contains("KiFS file <e/top.csv> was not downloaded: its local file <"), "{message}");
        assert!(message.contains("is the download of KiFS file <d/top.csv>"), "{message}");
        let downloaded: Vec<String> = kifs.downloads.lock().iter().flat_map(|(names, _, _)| names.clone()).collect();
        assert_eq!(downloaded.iter().filter(|n| *n == "d/top.csv").count(), 1, "named twice, downloaded once");
        assert!(!downloaded.contains(&"e/top.csv".to_string()));
    }

    // A part shorter than asked for, from a file that shrank after it was
    // listed, fails the file and removes it.
    #[tokio::test]
    async fn a_short_part_fails_the_file_and_removes_it() {
        static FILES: &[(&str, &[u8])] = &[("d/big.bin", b"0123456789")];
        let kifs = kifs_shrunk("127.0.0.154", FILES, None, Some(9)).await;
        let dir = TempDir::new("download_short_part");
        let heard = Heard::default();

        let err = downloader(&kifs.k, &["d/big.bin"], &dir, DownloadOptions::default(), &heard)
            .download()
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "API error: Failed to download multi-part file <d/big.bin>: Could not complete download part #2 : the server \
             sent 1 bytes where 2 were asked for"
        );
        assert!(!dir.path().join("big.bin").exists());
        assert_eq!(*heard.0.lock(), ["part 0 of 3 at 0 for 4", "part 1 of 3 at 4 for 4"]);
    }

    // A local file that cannot be opened is left as it is.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_file_that_cannot_be_opened_is_not_deleted() {
        use std::os::unix::fs::PermissionsExt;
        static FILES: &[(&str, &[u8])] = &[("d/big.bin", b"0123456789")];
        let kifs = kifs("127.0.0.153", FILES, None).await;
        let dir = TempDir::new("download_open_failure");
        let existing = dir.write("big.bin", b"keep me");
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o444)).unwrap();
        // Running as root, permissions are not enforced; nothing to test.
        if std::fs::OpenOptions::new().write(true).open(&existing).is_ok() {
            return;
        }
        let heard = Heard::default();

        let err = downloader(&kifs.k, &["d/big.bin"], &dir, DownloadOptions::default(), &heard)
            .download()
            .await
            .unwrap_err();
        assert!(err.to_string().ends_with("big.bin"), "{err}");
        assert_eq!(std::fs::read(&existing).unwrap(), b"keep me");
    }

    #[test]
    fn a_partial_file_is_deleted_unless_kept() {
        let dir = TempDir::new("partial");
        let dropped = dir.write("dropped.bin", b"partial");
        let kept = dir.write("kept.bin", b"complete");

        drop(PartialFileGuard::new(&dropped.to_string_lossy()));

        let mut guard = PartialFileGuard::new(&kept.to_string_lossy());
        guard.keep();
        drop(guard);

        assert!(!dropped.exists());
        assert!(kept.exists());
    }

    #[test]
    fn a_partial_file_already_gone_is_not_an_error() {
        let dir = TempDir::new("partial_gone");
        let missing = dir.path().join("never_written.bin");
        drop(PartialFileGuard::new(&missing.to_string_lossy()));
    }

    /// Dropping the future that owns the guard — as happens when a download
    /// is cancelled or times out — deletes the partial file.
    #[tokio::test]
    async fn a_dropped_download_future_deletes_its_partial_file() {
        let dir = TempDir::new("partial_future");
        let path = dir.write("in_progress.bin", b"part one");
        let path_str = path.to_string_lossy().to_string();

        let download = async move {
            let _partial = PartialFileGuard::new(&path_str);
            std::future::pending::<()>().await;
        };
        let _ = tokio::time::timeout(std::time::Duration::from_millis(10), download).await;

        assert!(!path.exists());
    }
}

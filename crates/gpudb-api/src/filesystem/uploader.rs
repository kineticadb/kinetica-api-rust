//! File upload functionality for KiFS.

use std::collections::HashMap;
use std::sync::Arc;
use log::{debug, error, info, warn};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{Kinetica, KineticaError, UploadFilesRequest};
use super::file_operation::{
    aggregate_failures, categorize_files_by_size, is_glob_pattern, io_error_message, local_file_exists,
    parse_file_name, search_local_directories, FileBatch, FullFileBatcher, SearchError,
};
use super::listeners::FileUploadListener;
use super::options::{KifsFileHandlerOptions, UploadOptions};
use super::types::{MultiPartOperation, MultiPartUploadInfo, OpMode, OperationResult};
use super::KIFS_PATH_SEPARATOR;

/// Internal uploader for handling file uploads to KiFS.
pub struct FileUploader<'a> {
    /// Kinetica client instance
    db: Arc<Kinetica>,
    /// List of local file paths to upload
    file_names: Vec<String>,
    /// Target KiFS directory
    remote_dir_name: String,
    /// Upload options
    upload_options: UploadOptions,
    /// File handler options
    file_handler_options: KifsFileHandlerOptions,
    /// Optional callback listener
    callback: Option<&'a dyn FileUploadListener>,
}

/// The readable files of one full-file batch, ready to send.
struct PreparedBatch {
    /// Remote names of the files read
    file_names: Vec<String>,
    /// Their contents, in the same order
    file_data: Vec<serde_bytes::ByteBuf>,
    /// Total payload bytes
    bytes: i64,
}

impl<'a> FileUploader<'a> {
    /// Creates a new file uploader.
    pub fn new(
        db: Arc<Kinetica>,
        file_names: Vec<String>,
        remote_dir_name: String,
        upload_options: UploadOptions,
        file_handler_options: KifsFileHandlerOptions,
        callback: Option<&'a dyn FileUploadListener>,
    ) -> Self {
        Self {
            db,
            file_names,
            remote_dir_name,
            upload_options,
            file_handler_options,
            callback,
        }
    }

    /// Performs the upload operation with error aggregation.
    ///
    /// This method:
    /// 1. Resolves glob patterns to actual files
    /// 2. Categorizes files into full-file and multi-part uploads
    /// 3. Uploads multi-part files one at a time
    /// 4. Uploads full files in batches, several batches at a time
    /// 5. Aggregates all errors and returns them at the end
    ///
    /// There is a result per multi-part file, named by its local path; one
    /// per batch of whole files sent, listing them in `full_file_names`; and
    /// one per file that could not be read. A failure does not stop the
    /// rest; their messages are joined, a line each, into the error returned.
    pub async fn upload(&self) -> Result<Vec<OperationResult>, KineticaError> {
        let (local_files, remote_files) = self.resolve().await?;
        self.upload_resolved(&local_files, &remote_files).await
    }

    /// Uploads the files resolved, each to its remote path, as
    /// [`upload`](Self::upload) does.
    pub(crate) async fn upload_resolved(
        &self,
        local_files: &[String],
        remote_files: &[String],
    ) -> Result<Vec<OperationResult>, KineticaError> {
        // Categorize the files by size
        let (batch_manager, multipart_files) = self.categorize(local_files, remote_files).await?;

        debug!(
            "Found {} full-file batches and {} multi-part files to upload",
            batch_manager.number_of_batches(),
            multipart_files.len()
        );

        let mut all_results = self.upload_multi_part_files(&multipart_files).await;
        all_results.extend(self.upload_full_files(batch_manager.batches()).await);

        // A pattern that matched nothing (or a recursive search that found
        // nothing) is only a warning, and the upload returns successfully
        // having uploaded nothing. Names that are not patterns were already
        // checked to exist by the handler. The risk is that a typo in a
        // pattern passes silently; callers who need at least one file should
        // check the returned results.
        if batch_manager.number_of_batches() == 0 && multipart_files.is_empty() {
            warn!("No files found to upload ...");
        }

        // Report every failure together, rather than the first one only.
        if let Some(e) = aggregate_failures(&all_results) {
            return Err(e);
        }

        Ok(all_results)
    }

    /// Resolves the file names to upload into the local files found and the
    /// remote path of each, in the order found.
    ///
    /// This walks directories, which is blocking I/O, so it runs on the
    /// runtime's blocking pool, where it does not stall the async worker
    /// threads.
    pub(crate) async fn resolve(&self) -> Result<(Vec<String>, Vec<String>), KineticaError> {
        let file_names = self.file_names.clone();
        let remote_dir_name = self.remote_dir_name.clone();
        let recursive = self.upload_options.recursive;

        tokio::task::spawn_blocking(move || resolve_files(&file_names, &remote_dir_name, recursive))
            .await
            .map_err(|e| KineticaError::Config(format!("Local file search failed: {e}")))?
    }

    /// Sorts the files resolved into full-file batches and multi-part files,
    /// by size, reading their metadata on the blocking pool.
    async fn categorize(
        &self,
        local_files: &[String],
        remote_files: &[String],
    ) -> Result<(FullFileBatcher, Vec<(String, String)>), KineticaError> {
        let (local_files, remote_files) = (local_files.to_vec(), remote_files.to_vec());
        let options = self.file_handler_options.clone();
        tokio::task::spawn_blocking(move || categorize_files_by_size(&local_files, &remote_files, &options))
            .await
            .map_err(|e| KineticaError::Config(format!("Local file search failed: {e}")))
    }

    /// Uploads the multi-part files, one at a time. Each gets a result named
    /// by its local path.
    async fn upload_multi_part_files(&self, files: &[(String, String)]) -> Vec<OperationResult> {
        let mut results = Vec::with_capacity(files.len());
        for (local_path, remote_path) in files {
            let mut result = OperationResult {
                file_name: local_path.clone(),
                multi_part: true,
                op_mode: Some(OpMode::Upload),
                ..Default::default()
            };
            match self.upload_multipart_file(local_path, remote_path).await {
                Ok(()) => {
                    info!("Uploaded multi-part file: {local_path} -> {remote_path}");
                    result.successful = true;
                }
                Err(message) => {
                    let error_message = format!("Failed to upload multi-part file <{local_path}>: {message}");
                    error!("{error_message}");
                    result.error_message = Some(error_message);
                    result.error = Some(KineticaError::Api(message));
                }
            }
            results.push(result);
        }
        results
    }

    /// Uploads the full-file batches, several at a time.
    ///
    /// Each batch is read and its request started at once. The requests
    /// started are waited for once as many batches as the thread pool size
    /// have been started, or once the payload bytes they hold reach
    /// `max_upload_bytes_in_flight`, whichever comes first. The byte ceiling
    /// is the one that bounds memory, since every payload stays resident
    /// until its request has finished.
    ///
    /// As each request finishes, its batch's result goes to the listener, in
    /// the order they finish. If the upload's future is dropped, the
    /// `JoinSet` aborts the requests still running.
    async fn upload_full_files(&self, batches: &[FileBatch]) -> Vec<OperationResult> {
        let pool_size = self.file_handler_options.thread_pool_size().max(1);
        let max_bytes_in_flight = self.file_handler_options.max_upload_bytes_in_flight();

        let mut results = Vec::new();
        let mut requests: JoinSet<Result<Vec<String>, KineticaError>> = JoinSet::new();
        let mut count = 0;
        let mut bytes_in_flight: i64 = 0;

        for batch in batches {
            let prepared = self.read_full_file_batch(batch, &mut results).await;

            bytes_in_flight += prepared.bytes;
            // Where every file in the batch was unreadable, there is nothing
            // to send, but the batch is still counted.
            if !prepared.file_names.is_empty() {
                let request = UploadFilesRequest {
                    file_names: prepared.file_names.clone(),
                    file_data: prepared.file_data,
                    options: self.ttl_options(),
                };
                let names = prepared.file_names;
                let db = self.db.clone();
                requests.spawn(async move { db.upload_files_raw(&request).await.map(|_| names) });
            }
            count += 1;

            if let Some(ceiling) = wave_ceiling(count, pool_size, bytes_in_flight, max_bytes_in_flight) {
                debug!(
                    "Upload full-file batches: {ceiling} ceiling reached ({count} batches, {bytes_in_flight} bytes in flight)"
                );
                self.collect_full_file_batches(&mut requests, &mut results).await;
                count = 0;
                bytes_in_flight = 0;
            }
        }

        self.collect_full_file_batches(&mut requests, &mut results).await;
        results
    }

    /// Reads the files of one full-file batch.
    ///
    /// A file that cannot be read locally gets a failed result, reported to
    /// the listener's `on_full_file_upload`, and the rest of the batch is
    /// still uploaded.
    async fn read_full_file_batch(&self, batch: &FileBatch, results: &mut Vec<OperationResult>) -> PreparedBatch {
        let mut prepared = PreparedBatch {
            file_names: Vec::new(),
            file_data: Vec::new(),
            bytes: 0,
        };

        for (local_path, remote_path) in &batch.files {
            match tokio::fs::read(local_path).await {
                Ok(data) => {
                    prepared.bytes += data.len() as i64;
                    prepared.file_names.push(remote_path.clone());
                    // ByteBuf::from is a move of the Vec, not a copy of the bytes.
                    prepared.file_data.push(serde_bytes::ByteBuf::from(data));
                }
                Err(e) => {
                    let reason = io_error_message(local_path, &e);
                    let error_message = format!("Failed to read source file <{local_path}>: {reason}");
                    error!("{error_message}");

                    let result = OperationResult {
                        file_name: local_path.clone(),
                        error_message: Some(error_message),
                        error: Some(KineticaError::Config(reason)),
                        ..Default::default()
                    };
                    if let Some(callback) = self.callback {
                        callback.on_full_file_upload(result.clone());
                    }
                    results.push(result);
                }
            }
        }

        prepared
    }

    /// Waits for the full-file batch requests started, reporting each as it
    /// finishes.
    ///
    /// A batch sent gets a successful result listing its files; a failed one
    /// gets a failed result with the request's error, which names no file.
    async fn collect_full_file_batches(
        &self,
        requests: &mut JoinSet<Result<Vec<String>, KineticaError>>,
        results: &mut Vec<OperationResult>,
    ) {
        while let Some(finished) = requests.join_next().await {
            let outcome = finished.unwrap_or_else(|e| Err(KineticaError::Api(e.to_string())));
            let result = match outcome {
                Ok(file_names) => {
                    info!("Uploaded batch of {} files", file_names.len());
                    OperationResult { full_file_names: file_names, successful: true, ..Default::default() }
                }
                Err(e) => {
                    let message = e.message();
                    error!("Error in full file upload batch: {message}");
                    OperationResult { error_message: Some(message), error: Some(e), ..Default::default() }
                }
            };
            if let Some(callback) = self.callback {
                callback.on_full_file_upload(result.clone());
            }
            results.push(result);
        }
    }

    /// Uploads a large file in multiple parts. Returns, on failure, the
    /// message to report.
    ///
    /// The job is initiated first. Then the file is opened and its parts are
    /// read and uploaded in turn, and the job is completed. If anything after
    /// the initiation fails before the completion is sent -- opening or
    /// reading the file, or uploading a part -- a cancel is sent for the job.
    /// A failed initiation is not cancelled, since there is no job yet, and a
    /// failed completion is not cancelled either, which leaves the server to
    /// discard the uncommitted job on its own. A failed cancel is logged and
    /// the original error returned.
    ///
    /// The listener hears of each part uploaded, and of the job's completion
    /// or cancellation with the results of every stage so far.
    ///
    /// If this future is dropped mid-upload, no cancel is sent: there is no
    /// way to await a request from `Drop`. That leaves the job uncommitted on
    /// the server.
    async fn upload_multipart_file(&self, local_path: &str, remote_path: &str) -> Result<(), String> {
        let job = MultiPartJob { id: Uuid::new_v4().to_string(), remote_path };
        let mut stages: Vec<OperationResult> = Vec::new();

        self.send_stage(&job, MultiPartOperation::Init, 0, 0, None, &mut stages).await?;

        let mut completing = false;
        let outcome = self.upload_parts_and_complete(&job, local_path, &mut stages, &mut completing).await;

        if !completing {
            if let Err(message) = self.send_stage(&job, MultiPartOperation::Cancel, 0, 0, None, &mut stages).await {
                // Logged, not returned: the error that caused the cancel is the
                // one the caller needs.
                error!("Failed to cancel upload job: {message}");
            }
        }
        outcome
    }

    /// Opens the file, uploads its parts, and completes the job, setting
    /// `completing` once the completion is about to be sent.
    async fn upload_parts_and_complete(
        &self,
        job: &MultiPartJob<'_>,
        local_path: &str,
        stages: &mut Vec<OperationResult>,
        completing: &mut bool,
    ) -> Result<(), String> {
        // A failure to open or read the file is reported without its cause
        let io_failure = |_| format!("Error uploading multi-part file <{local_path}> to <{}>", job.remote_path);

        let mut file = File::open(local_path).await.map_err(io_failure)?;
        let file_size = tokio::fs::metadata(local_path).await.map_err(io_failure)?.len() as i64;
        // At least 1, as the options only accept a positive size; `max` keeps
        // the division safe regardless.
        let part_size = self.file_handler_options.file_size_to_split().max(1);
        // The number of parts actually sent.
        let num_parts = (file_size + part_size - 1) / part_size;

        debug!("Starting multi-part upload: {local_path} -> {} ({num_parts} parts)", job.remote_path);

        for part_num in 0..num_parts {
            let offset = part_num * part_size;
            let this_part_size = std::cmp::min(file_size - offset, part_size) as usize;

            // `read_exact` fails on a short read, rather than sending a short
            // part silently.
            let mut buffer = vec![0u8; this_part_size];
            file.seek(SeekFrom::Start(offset as u64)).await.map_err(io_failure)?;
            file.read_exact(&mut buffer).await.map_err(io_failure)?;

            self.send_stage(job, MultiPartOperation::Part, part_num + 1, num_parts, Some(buffer), stages).await?;
        }

        *completing = true;
        self.send_stage(job, MultiPartOperation::Complete, 0, 0, None, stages).await
    }

    /// Returns the `ttl` option, if one is set.
    fn ttl_options(&self) -> HashMap<String, String> {
        let mut options = HashMap::new();
        if self.upload_options.ttl > 0 {
            options.insert("ttl".to_string(), self.upload_options.ttl.to_string());
        }
        options
    }

    /// Sends one stage of a multi-part upload, records its result and tells
    /// the listener.
    ///
    /// A data part carries its part number; the data-less stages (init,
    /// complete, cancel) carry the `ttl` option when one is set. On failure,
    /// the server's error is logged and a message naming the stage and the
    /// file returned.
    async fn send_stage(
        &self,
        job: &MultiPartJob<'_>,
        operation: MultiPartOperation,
        part_number: i64,
        total_parts: i64,
        data: Option<Vec<u8>>,
        stages: &mut Vec<OperationResult>,
    ) -> Result<(), String> {
        let remote_path = job.remote_path;
        let mut options = HashMap::new();
        options.insert("multipart_operation".to_string(), operation.as_str().to_string());
        options.insert("multipart_upload_uuid".to_string(), job.id.clone());
        let file_data = match data {
            Some(data) => {
                options.insert("multipart_upload_part_number".to_string(), part_number.to_string());
                vec![serde_bytes::ByteBuf::from(data)]
            }
            None => {
                options.extend(self.ttl_options());
                Vec::new()
            }
        };
        let request = UploadFilesRequest { file_names: vec![remote_path.to_string()], file_data, options };

        if let Err(e) = self.db.upload_files_raw(&request).await {
            error!("{}", e.message());
            let message = format!("Could not complete upload stage <{}> for file <{remote_path}>", operation.as_str());
            error!("{message}");
            return Err(message);
        }

        let upload_info = MultiPartUploadInfo {
            uuid: job.id.clone(),
            file_name: remote_path.to_string(),
            part_operation: Some(operation),
            upload_part_number: part_number,
            total_parts,
        };
        let mut result = OperationResult::multipart_upload_success(remote_path, upload_info);
        // Only the completing stage names the uploaded file.
        if operation == MultiPartOperation::Complete {
            result.full_file_names = vec![remote_path.to_string()];
        }
        stages.push(result.clone());

        // The initiating stage is recorded but not reported.
        if let Some(callback) = self.callback {
            match operation {
                MultiPartOperation::Part => callback.on_part_upload(result),
                MultiPartOperation::Cancel => callback.on_multi_part_upload_cancel(stages.clone()),
                MultiPartOperation::Complete => callback.on_multi_part_upload_complete(stages.clone()),
                MultiPartOperation::Init => {}
            }
        }
        Ok(())
    }
}

/// A multi-part upload job: its ID and the file it writes.
struct MultiPartJob<'a> {
    id: String,
    remote_path: &'a str,
}

/// Decides whether a wave of full-file batches is ready to send, naming the
/// ceiling reached: `"thread"` once it holds `pool_size` batches, `"byte"`
/// once its payloads reach `max_bytes_in_flight` (reported in preference).
fn wave_ceiling(
    count: usize,
    pool_size: usize,
    bytes_in_flight: i64,
    max_bytes_in_flight: i64,
) -> Option<&'static str> {
    let thread_ceiling = count.is_multiple_of(pool_size.max(1));
    let byte_ceiling = bytes_in_flight >= max_bytes_in_flight;

    if byte_ceiling {
        Some("byte")
    } else if thread_ceiling {
        Some("thread")
    } else {
        None
    }
}

/// Resolves file names/patterns to actual file paths.
///
/// Blocking; see [`FileUploader::resolve_and_categorize`].
fn resolve_files(
    file_names: &[String],
    remote_dir_name: &str,
    recursive: bool,
) -> Result<(Vec<String>, Vec<String>), KineticaError> {
    let mut local_files = Vec::new();
    let mut remote_files = Vec::new();

    for file_name in file_names {
        let parsed = parse_file_name(file_name);

        // `recursive` decides how deep a pattern searches, and how deep a
        // directory given by name is uploaded. A file given by name is that
        // file alone, even when `recursive` is set (the default); same-named
        // files below its directory are not uploaded.
        let directory = std::path::Path::new(&parsed.file_path()).is_dir();
        let search = if is_glob_pattern(&parsed.pattern) {
            Some((parsed.base_dir.clone(), parsed.pattern.clone()))
        } else if directory {
            // Its files, as `<directory>/*` gives them
            Some((parsed.file_path(), "*".to_string()))
        } else {
            None
        };
        if let Some((base_dir, pattern)) = search {
            // Use glob pattern matching
            match search_local_directories(&base_dir, &pattern, remote_dir_name, recursive) {
                Ok((mut locals, mut remotes)) => {
                    local_files.append(&mut locals);
                    remote_files.append(&mut remotes);
                }
                Err(SearchError::StartDirUnreadable(e)) => {
                    // The walk's error is logged and the upload goes ahead
                    // without that name's files. The names after this one are
                    // still resolved.
                    error!("Error resolving local files for upload: {e}");
                    continue;
                }
                Err(SearchError::Failed(e)) => return Err(e),
            }
        } else {
            // Simple file - check if it exists
            let full_path = parsed.file_path();

            if local_file_exists(&full_path) {
                local_files.push(full_path);
                let remote_path = format!(
                    "{}{}{}",
                    remote_dir_name,
                    KIFS_PATH_SEPARATOR,
                    parsed.pattern
                );
                remote_files.push(remote_path);
            } else {
                // Unreachable through the handler, which rejects a missing
                // plain file before the upload starts.
                warn!("Local file not found, skipping: {full_path}");
            }
        }
    }

    Ok((local_files, remote_files))
}

#[cfg(test)]
mod tests {
    use super::super::file_operation::test_support::TempDir;
    use super::*;

    fn remotes(dir: &TempDir, names: &[&str], recursive: bool) -> Vec<String> {
        let names: Vec<String> = names.iter().map(|n| format!("{}/{n}", dir.str())).collect();
        resolve_files(&names, "kdir", recursive).expect("resolve").1
    }

    /// What a stand-in server saw of each `/upload/files` request: its file
    /// names, the sizes of its data, and its options.
    type Seen = Arc<parking_lot::Mutex<Vec<(Vec<String>, Vec<usize>, HashMap<String, String>)>>>;

    /// A server taking uploads, failing the requests `fail` picks.
    async fn server(
        host: &str,
        fail: impl Fn(&HashMap<String, String>, &[String]) -> bool + Send + Sync + 'static,
    ) -> (crate::gpudb::fake_server::FakeServer, Arc<Kinetica>, Seen) {
        use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
        #[derive(serde::Deserialize)]
        struct Request {
            file_names: Vec<String>,
            file_data: Vec<serde_bytes::ByteBuf>,
            options: HashMap<String, String>,
        }
        let server = fake_server::start(host).await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let seen: Seen = Default::default();
        let log = seen.clone();
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            if path != "/upload/files" {
                return None;
            }
            let request: Request =
                serde_avro_fast::from_datum_slice(body, &crate::generated::UPLOAD_FILES_REQUEST_SCHEMA).unwrap();
            let failed = fail(&request.options, &request.file_names);
            log.lock().push((
                request.file_names,
                request.file_data.iter().map(|d| d.len()).collect(),
                request.options,
            ));
            Some(if failed {
                Err("boom".to_string())
            } else {
                Ok(encode(&crate::generated::UPLOAD_FILES_RESPONSE_SCHEMA, &crate::UploadFilesResponse::default()))
            })
        }));
        let mut options = crate::KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![server.url.to_string()], options).await.expect("client"));
        (server, k, seen)
    }

    /// What a listener heard, in order.
    #[derive(Default)]
    struct Heard(parking_lot::Mutex<Vec<String>>);

    fn stages(results: &[OperationResult]) -> String {
        results
            .iter()
            .map(|r| r.upload_info.as_ref().and_then(|i| i.part_operation).map_or("?", |o| o.as_str()))
            .collect::<Vec<_>>()
            .join(",")
    }

    impl FileUploadListener for Heard {
        fn on_multi_part_upload_complete(&self, results: Vec<OperationResult>) {
            self.0.lock().push(format!("complete {}", stages(&results)));
        }
        fn on_multi_part_upload_cancel(&self, results: Vec<OperationResult>) {
            self.0.lock().push(format!("cancel {}", stages(&results)));
        }
        fn on_part_upload(&self, result: OperationResult) {
            let info = result.upload_info.unwrap();
            self.0.lock().push(format!("part {} of {}", info.upload_part_number, info.total_parts));
        }
        fn on_full_file_upload(&self, result: OperationResult) {
            self.0.lock().push(format!(
                "full {} {:?} {:?} {:?} {:?}",
                result.successful,
                result.file_name,
                result.full_file_names,
                result.error_message,
                result.error.map(|e| e.to_string())
            ));
        }
    }

    fn uploader<'a>(
        k: &Arc<Kinetica>,
        names: Vec<String>,
        split: i64,
        ttl: i32,
        heard: &'a Heard,
    ) -> FileUploader<'a> {
        FileUploader::new(
            k.clone(),
            names,
            "kdir".to_string(),
            UploadOptions::new().with_recursive(false).with_ttl(ttl),
            KifsFileHandlerOptions::new().with_file_size_to_split(split).unwrap(),
            Some(heard),
        )
    }

    // Init, the parts numbered from 1, complete; the TTL on the data-less
    // stages only; the listener told of each part and of the completion.
    #[tokio::test]
    async fn a_large_file_is_uploaded_in_stages() {
        let (_server, k, seen) = server("127.0.0.141", |_, _| false).await;
        let dir = TempDir::new("multipart_stages");
        let local = dir.write("big.bin", b"0123456789").to_string_lossy().to_string();
        let heard = Heard::default();

        let results = uploader(&k, vec![local.clone()], 4, 60, &heard).upload().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].successful && results[0].multi_part);
        assert_eq!(results[0].file_name, local, "named by its local path");

        let seen = seen.lock();
        let operations: Vec<String> = seen
            .iter()
            .map(|(_, sizes, o)| {
                let part = o.get("multipart_upload_part_number").map_or("-", String::as_str);
                let ttl = o.get("ttl").map_or("-", String::as_str);
                format!("{} part {part} ttl {ttl} data {sizes:?}", o["multipart_operation"])
            })
            .collect();
        assert_eq!(
            operations,
            [
                "init part - ttl 60 data []",
                "upload_part part 1 ttl - data [4]",
                "upload_part part 2 ttl - data [4]",
                "upload_part part 3 ttl - data [2]",
                "complete part - ttl 60 data []",
            ]
        );
        assert!(seen.iter().all(|(names, _, o)| names == &["kdir/big.bin"] && o["multipart_upload_uuid"] == seen[0].2["multipart_upload_uuid"]));
        assert_eq!(
            *heard.0.lock(),
            ["part 1 of 3", "part 2 of 3", "part 3 of 3", "complete init,upload_part,upload_part,upload_part,complete"]
        );
    }

    // A part that fails cancels the job, and the failure is reported.
    #[tokio::test]
    async fn a_failed_part_cancels_the_job() {
        let (_server, k, seen) =
            server("127.0.0.142", |o, _| o.get("multipart_upload_part_number").is_some_and(|n| n == "2")).await;
        let dir = TempDir::new("multipart_cancel");
        let local = dir.write("big.bin", b"0123456789").to_string_lossy().to_string();
        let heard = Heard::default();

        let err = uploader(&k, vec![local.clone()], 4, -1, &heard).upload().await.unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "API error: Failed to upload multi-part file <{local}>: Could not complete upload stage <upload_part> for file <kdir/big.bin>"
            )
        );
        let operations: Vec<String> = seen.lock().iter().map(|(_, _, o)| o["multipart_operation"].clone()).collect();
        assert_eq!(operations, ["init", "upload_part", "upload_part", "cancel"]);
        assert_eq!(*heard.0.lock(), ["part 1 of 3", "cancel init,upload_part,cancel"]);
    }

    // A result per batch, to the listener as it finishes; a failed batch
    // carries the server's message alone, and its error.
    #[tokio::test]
    async fn whole_files_are_reported_a_batch_at_a_time() {
        let (_server, k, _seen) = server("127.0.0.143", |_, names| names.contains(&"kdir/c.csv".to_string())).await;
        let dir = TempDir::new("full_batches");
        for name in ["a.csv", "b.csv", "c.csv"] {
            dir.write(name, b"12");
        }
        let heard = Heard::default();

        let err = uploader(&k, vec![format!("{}/*.csv", dir.str())], 4, -1, &heard).upload().await.unwrap_err();
        assert_eq!(err.to_string(), "API error: boom");
        let mut events = heard.0.lock().clone();
        events.sort();
        assert_eq!(
            events,
            [
                r#"full false "" [] Some("boom") Some("API error: boom")"#,
                r#"full true "" ["kdir/a.csv", "kdir/b.csv"] None None"#,
            ]
        );
    }

    #[test]
    fn a_wave_is_sent_at_the_thread_or_byte_ceiling() {
        // Below both ceilings.
        assert_eq!(wave_ceiling(1, 5, 10, 100), None);
        assert_eq!(wave_ceiling(4, 5, 99, 100), None);
        // The pool is full.
        assert_eq!(wave_ceiling(5, 5, 10, 100), Some("thread"));
        // The payload bytes reach the ceiling first.
        assert_eq!(wave_ceiling(2, 5, 100, 100), Some("byte"));
        assert_eq!(wave_ceiling(5, 5, 150, 100), Some("byte"));
        // A pool of one sends every batch on its own.
        assert_eq!(wave_ceiling(1, 1, 0, 100), Some("thread"));
    }

    #[test]
    fn a_plain_name_uploads_only_that_file_when_not_recursive() {
        let dir = TempDir::new("resolve_plain");
        dir.write("a.csv", b"1");
        dir.write("sub/a.csv", b"1");
        assert_eq!(remotes(&dir, &["a.csv"], false), vec!["kdir/a.csv"]);
    }

    // `recursive` applies to patterns: a file given by name is that file
    // alone, not the same-named files below it.
    #[test]
    fn a_plain_name_uploads_only_that_file_when_recursive() {
        let dir = TempDir::new("resolve_recursive");
        dir.write("a.csv", b"1");
        dir.write("sub/a.csv", b"1");
        let (locals, uploaded) = resolve_files(&[format!("{}/a.csv", dir.str())], "kdir", true).unwrap();
        assert_eq!(uploaded, vec!["kdir/a.csv"]);
        assert_eq!(locals, vec![format!("{}{}a.csv", dir.str(), std::path::MAIN_SEPARATOR)]);
        assert_eq!(remotes(&dir, &["*.csv"], true), vec!["kdir/a.csv", "kdir/sub/a.csv"], "a pattern still searches");
    }

    /// A directory given by name uploads its files, as `<directory>/*` does,
    /// and those below it when recursive.
    #[test]
    fn a_directory_given_by_name_uploads_its_files() {
        let dir = TempDir::new("resolve_directory");
        dir.write("data/a.csv", b"1");
        dir.write("data/sub/b.csv", b"1");
        assert_eq!(remotes(&dir, &["data"], false), vec!["kdir/a.csv"]);
        assert_eq!(remotes(&dir, &["data"], true), vec!["kdir/a.csv", "kdir/sub/b.csv"]);
    }

    #[test]
    fn a_pattern_that_matches_nothing_resolves_to_no_files() {
        let dir = TempDir::new("resolve_nothing");
        dir.write("a.csv", b"1");
        assert!(remotes(&dir, &["*.json"], false).is_empty());
    }

    #[test]
    fn several_names_resolve_in_order() {
        let dir = TempDir::new("resolve_order");
        dir.write("b.csv", b"1");
        dir.write("a.json", b"1");
        dir.write("a.csv", b"1");
        assert_eq!(
            remotes(&dir, &["*.json", "*.csv"], false),
            vec!["kdir/a.json", "kdir/a.csv", "kdir/b.csv"]
        );
    }

    #[test]
    fn an_invalid_pattern_fails_the_resolution() {
        let dir = TempDir::new("resolve_invalid");
        let names = vec![format!("{}/[", dir.str())];
        assert!(resolve_files(&names, "kdir", false).is_err());
    }

    // An unreadable start directory leaves its own files out, logged; the
    // names after it are still resolved.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_directory_leaves_out_only_its_own_files() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new("resolve_unreadable");
        dir.write("a.csv", b"1");
        dir.write("locked/b.csv", b"1");
        dir.write("c.csv", b"1");
        let locked = dir.path().join("locked");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();

        // Running as root, permissions are not enforced; nothing to test.
        let enforced = std::fs::read_dir(&locked).is_err();

        let names: Vec<String> = [
            format!("{}/a.csv", dir.str()),
            format!("{}/locked/*.csv", dir.str()),
            format!("{}/c.csv", dir.str()),
        ]
        .to_vec();
        let result = resolve_files(&names, "kdir", false);

        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();

        if enforced {
            assert_eq!(result.expect("resolve").1, vec!["kdir/a.csv", "kdir/c.csv"]);
        }
    }

    #[test]
    fn categorized_batches_keep_the_resolved_order() {
        let dir = TempDir::new("resolve_batches");
        for name in ["c.csv", "a.csv", "b.csv"] {
            dir.write(name, b"12");
        }
        let names = vec![format!("{}/*.csv", dir.str())];
        let (locals, remotes) = resolve_files(&names, "kdir", false).unwrap();
        let options = KifsFileHandlerOptions::new().with_file_size_to_split(4).unwrap();
        let (manager, multipart) = categorize_files_by_size(&locals, &remotes, &options);

        assert!(multipart.is_empty());
        let batches: Vec<Vec<&str>> = manager
            .batches()
            .iter()
            .map(|b| b.files.iter().map(|(_, r)| r.as_str()).collect())
            .collect();
        assert_eq!(batches, vec![vec!["kdir/a.csv", "kdir/b.csv"], vec!["kdir/c.csv"]]);
    }
}

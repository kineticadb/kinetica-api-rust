//! Callback listeners for upload and download progress tracking.

use super::types::OperationResult;

/// Listener trait for file upload progress callbacks.
///
/// Implement this trait to receive notifications about upload progress,
/// including per-part completion for multi-part uploads.
///
/// # Example
///
/// ```
/// use gpudb_api::filesystem::{FileUploadListener, OperationResult};
///
/// struct MyUploadListener;
///
/// impl FileUploadListener for MyUploadListener {
///     fn on_multi_part_upload_complete(&self, results: Vec<OperationResult>) {
///         println!("Multi-part upload complete with {} parts", results.len());
///     }
///
///     fn on_part_upload(&self, result: OperationResult) {
///         if let Some(info) = &result.upload_info {
///             println!("Uploaded part {} of {}", info.upload_part_number, info.total_parts);
///         }
///     }
///
///     fn on_full_file_upload(&self, result: OperationResult) {
///         if result.successful {
///             println!("Uploaded: {:?}", result.full_file_names);
///         } else {
///             println!("Upload error: {:?}", result.error_message);
///         }
///     }
/// }
/// ```
pub trait FileUploadListener: Send + Sync {
    /// Called when all parts of a multi-part upload have been completed.
    ///
    /// The list holds a result for every stage of the job, in order: the
    /// initiating stage, one per uploaded part, and the completing stage.
    /// Only the completing stage names the uploaded file in
    /// `full_file_names`. Use `upload_info` to tell the stages apart.
    ///
    /// # Arguments
    ///
    /// * `results` - List of results for each stage of the upload
    fn on_multi_part_upload_complete(&self, results: Vec<OperationResult>);

    /// Called when a multi-part upload was aborted because one of its stages
    /// failed after it had been initiated.
    ///
    /// The list holds a result for every stage that completed before the
    /// failure, followed by the cancelling stage. It is informational only:
    /// the failure is also reported in the upload's returned error. Not called
    /// when the cancel request itself fails. By default it does nothing.
    ///
    /// # Arguments
    ///
    /// * `results` - List of results for each stage, ending with the cancel
    fn on_multi_part_upload_cancel(&self, results: Vec<OperationResult>) {
        let _ = results;
    }

    /// Called whenever a single part of a multi-part upload has been completed.
    ///
    /// This is useful for tracking progress during large file uploads. It is
    /// called for the data parts only, not for the initiating or completing
    /// stages.
    ///
    /// # Arguments
    ///
    /// * `result` - Result of the part upload
    fn on_part_upload(&self, result: OperationResult);

    /// Called for each batch of whole files sent in one request, as it
    /// finishes, and for each file that could not be read to be sent.
    ///
    /// For a batch sent, `full_file_names` lists its files and `file_name` is
    /// empty; if the request failed, `successful` is false and
    /// `error_message` says why.
    /// For a file that could not be read, `file_name` is its local name and
    /// `error_message` says why.
    ///
    /// # Arguments
    ///
    /// * `result` - Result of the upload operation
    fn on_full_file_upload(&self, result: OperationResult);
}

/// Listener trait for file download progress callbacks.
///
/// Implement this trait to receive notifications about download progress,
/// including per-part completion for multi-part downloads.
///
/// # Example
///
/// ```
/// use gpudb_api::filesystem::{FileDownloadListener, OperationResult};
///
/// struct MyDownloadListener;
///
/// impl FileDownloadListener for MyDownloadListener {
///     fn on_multi_part_download_complete(&self, results: Vec<OperationResult>) {
///         println!("Multi-part download complete with {} parts", results.len());
///     }
///
///     fn on_part_download(&self, result: OperationResult) {
///         if let Some(info) = &result.download_info {
///             println!("Downloaded part {} of {}", info.download_part_number, info.total_parts);
///         }
///     }
///
///     fn on_full_file_download(&self, file_names: Vec<String>) {
///         println!("Full files downloaded: {file_names:?}");
///     }
/// }
/// ```
pub trait FileDownloadListener: Send + Sync {
    /// Called when all parts of a multi-part download have been completed.
    ///
    /// # Arguments
    ///
    /// * `results` - List of results for each part of the download
    fn on_multi_part_download_complete(&self, results: Vec<OperationResult>);

    /// Called whenever a single part of a multi-part download has been completed.
    ///
    /// This is useful for tracking progress during large file downloads.
    ///
    /// # Arguments
    ///
    /// * `result` - Result of the part download
    fn on_part_download(&self, result: OperationResult);

    /// Called once per batch of whole files fetched in one request, with the
    /// KiFS names of those saved, when at least one was. A file skipped
    /// because it already existed locally and overwriting is off counts as
    /// saved.
    ///
    /// # Arguments
    ///
    /// * `file_names` - The KiFS names of the files saved
    fn on_full_file_download(&self, file_names: Vec<String>);
}

/// A simple upload listener that wraps callback functions.
///
/// This provides a convenient way to create listeners using closures
/// without implementing the full trait.
///
/// # Example
///
/// ```
/// use gpudb_api::filesystem::{FnUploadListener, OperationResult};
///
/// let listener = FnUploadListener::new(
///     |results| println!("Multi-part complete: {} parts", results.len()),
///     |result| println!("Part {} uploaded", result.file_name),
///     |result| println!("Full files uploaded: {:?}", result.full_file_names),
/// );
/// ```
pub struct FnUploadListener<F1, F2, F3>
where
    F1: Fn(Vec<OperationResult>) + Send + Sync,
    F2: Fn(OperationResult) + Send + Sync,
    F3: Fn(OperationResult) + Send + Sync,
{
    on_multi_part_complete: F1,
    on_part: F2,
    on_full: F3,
}

impl<F1, F2, F3> FnUploadListener<F1, F2, F3>
where
    F1: Fn(Vec<OperationResult>) + Send + Sync,
    F2: Fn(OperationResult) + Send + Sync,
    F3: Fn(OperationResult) + Send + Sync,
{
    /// Creates a new function-based upload listener.
    pub fn new(on_multi_part_complete: F1, on_part: F2, on_full: F3) -> Self {
        Self {
            on_multi_part_complete,
            on_part,
            on_full,
        }
    }
}

impl<F1, F2, F3> FileUploadListener for FnUploadListener<F1, F2, F3>
where
    F1: Fn(Vec<OperationResult>) + Send + Sync,
    F2: Fn(OperationResult) + Send + Sync,
    F3: Fn(OperationResult) + Send + Sync,
{
    fn on_multi_part_upload_complete(&self, results: Vec<OperationResult>) {
        (self.on_multi_part_complete)(results);
    }

    fn on_part_upload(&self, result: OperationResult) {
        (self.on_part)(result);
    }

    fn on_full_file_upload(&self, result: OperationResult) {
        (self.on_full)(result);
    }
}

/// A simple download listener that wraps callback functions.
///
/// This provides a convenient way to create listeners using closures
/// without implementing the full trait.
///
/// # Example
///
/// ```
/// use gpudb_api::filesystem::{FnDownloadListener, OperationResult};
///
/// let listener = FnDownloadListener::new(
///     |results| println!("Multi-part complete: {} parts", results.len()),
///     |result| println!("Part {} downloaded", result.file_name),
///     |file_names| println!("Full files downloaded: {file_names:?}"),
/// );
/// ```
pub struct FnDownloadListener<F1, F2, F3>
where
    F1: Fn(Vec<OperationResult>) + Send + Sync,
    F2: Fn(OperationResult) + Send + Sync,
    F3: Fn(Vec<String>) + Send + Sync,
{
    on_multi_part_complete: F1,
    on_part: F2,
    on_full: F3,
}

impl<F1, F2, F3> FnDownloadListener<F1, F2, F3>
where
    F1: Fn(Vec<OperationResult>) + Send + Sync,
    F2: Fn(OperationResult) + Send + Sync,
    F3: Fn(Vec<String>) + Send + Sync,
{
    /// Creates a new function-based download listener.
    pub fn new(on_multi_part_complete: F1, on_part: F2, on_full: F3) -> Self {
        Self {
            on_multi_part_complete,
            on_part,
            on_full,
        }
    }
}

impl<F1, F2, F3> FileDownloadListener for FnDownloadListener<F1, F2, F3>
where
    F1: Fn(Vec<OperationResult>) + Send + Sync,
    F2: Fn(OperationResult) + Send + Sync,
    F3: Fn(Vec<String>) + Send + Sync,
{
    fn on_multi_part_download_complete(&self, results: Vec<OperationResult>) {
        (self.on_multi_part_complete)(results);
    }

    fn on_part_download(&self, result: OperationResult) {
        (self.on_part)(result);
    }

    fn on_full_file_download(&self, file_names: Vec<String>) {
        (self.on_full)(file_names);
    }
}

/// A no-op upload listener that ignores all callbacks.
/// Useful for operations where progress tracking is not needed.
///
/// Passing it behaves exactly like passing `None`; it is for code that
/// needs a `&dyn FileUploadListener` in hand regardless.
pub struct NoOpUploadListener;

impl FileUploadListener for NoOpUploadListener {
    fn on_multi_part_upload_complete(&self, _results: Vec<OperationResult>) {}
    fn on_part_upload(&self, _result: OperationResult) {}
    fn on_full_file_upload(&self, _result: OperationResult) {}
}

/// A no-op download listener that ignores all callbacks.
/// Useful for operations where progress tracking is not needed.
///
/// Passing it behaves exactly like passing `None`; it is for code that
/// needs a `&dyn FileDownloadListener` in hand regardless.
pub struct NoOpDownloadListener;

impl FileDownloadListener for NoOpDownloadListener {
    fn on_multi_part_download_complete(&self, _results: Vec<OperationResult>) {}
    fn on_part_download(&self, _result: OperationResult) {}
    fn on_full_file_download(&self, _file_names: Vec<String>) {}
}

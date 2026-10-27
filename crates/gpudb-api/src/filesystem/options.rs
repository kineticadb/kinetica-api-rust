//! Configuration options for file operations.

use crate::KineticaError;

/// Options for file upload operations.
///
/// # Example
///
/// ```
/// use gpudb_api::filesystem::UploadOptions;
///
/// // Create default options (recursive search, no TTL)
/// let options = UploadOptions::default();
///
/// // Create custom options
/// let options = UploadOptions::new()
///     .with_recursive(true)
///     .with_ttl(3600); // Files expire in 1 hour
/// ```
#[derive(Debug, Clone)]
pub struct UploadOptions {
    /// Whether to search subdirectories for the files to upload.
    /// Default: `true`
    ///
    /// This applies to glob patterns and to a directory given by name, which
    /// uploads its files as `<directory>/*` does. A file given by name is
    /// uploaded alone either way: `upload("/data/file.csv")` uploads that
    /// file, not also `/data/sub/file.csv`. Files found below a pattern's or
    /// directory's base go to the same path below the remote directory.
    pub recursive: bool,

    /// Time-to-live value for uploaded files in seconds.
    /// A value of -1 or 0 means no expiration.
    /// Default: `-1`
    pub ttl: i32,
}

impl Default for UploadOptions {
    fn default() -> Self {
        Self {
            recursive: true,
            ttl: -1,
        }
    }
}

impl UploadOptions {
    /// Creates a new `UploadOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether patterns and directories given by name are searched
    /// below their own level. See [`UploadOptions::recursive`].
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Sets the TTL (time-to-live) in seconds for uploaded files.
    /// Use -1 or 0 for no expiration.
    pub fn with_ttl(mut self, ttl: i32) -> Self {
        self.ttl = ttl;
        self
    }

    /// Returns whether recursive search is enabled.
    pub fn is_recursive(&self) -> bool {
        self.recursive
    }

    /// Returns the TTL value.
    pub fn ttl(&self) -> i32 {
        self.ttl
    }
}

/// Options for file download operations.
///
/// The default options, which a download given none uses, overwrite
/// existing files.
///
/// # Example
///
/// ```
/// use gpudb_api::filesystem::DownloadOptions;
///
/// // The default options (overwrite existing files)
/// let options = DownloadOptions::default();
/// assert!(options.is_overwrite_existing());
///
/// // Options that keep existing files
/// let options = DownloadOptions::default().with_overwrite_existing(false);
/// assert!(!options.is_overwrite_existing());
/// ```
#[derive(Debug, Clone)]
pub struct DownloadOptions {
    /// Whether to overwrite existing files on the local file system.
    pub overwrite_existing: bool,
}

impl Default for DownloadOptions {
    /// The default options: overwriting existing files.
    fn default() -> Self {
        Self {
            overwrite_existing: true,
        }
    }
}

impl DownloadOptions {
    /// Sets whether to overwrite existing local files.
    pub fn with_overwrite_existing(mut self, overwrite: bool) -> Self {
        self.overwrite_existing = overwrite;
        self
    }

    /// Sets whether to overwrite existing local files.
    pub fn set_overwrite_existing(&mut self, overwrite: bool) {
        self.overwrite_existing = overwrite;
    }

    /// Returns whether overwriting existing files is enabled.
    pub fn is_overwrite_existing(&self) -> bool {
        self.overwrite_existing
    }
}

/// Default size threshold for multi-part transfers, and the largest one
/// allowed: 60 MB.
pub(crate) const DEFAULT_FILE_SIZE_TO_SPLIT: i64 = 60 * 1024 * 1024;

/// Default number of full-file upload batches sent concurrently.
pub(crate) const DEFAULT_FULL_FILE_DISPATCHER_THREADPOOL_SIZE: usize = 5;

/// Default thread pool termination timeout, in seconds.
pub(crate) const DEFAULT_THREAD_POOL_TERMINATION_TIMEOUT: u64 = 90;

/// Default ceiling on full-file upload payload bytes held in memory at once.
///
/// It is 128 MB, whatever memory the process has. Lower it with
/// [`KifsFileHandlerOptions::with_max_upload_bytes_in_flight`] where memory is
/// tight.
// A Rust process has no configured heap ceiling to derive a default from,
// so the default is a fixed 128 MB.
const DEFAULT_MAX_UPLOAD_BYTES_IN_FLIGHT: i64 = 128 * 1024 * 1024;

/// Options for the KiFS file handler.
///
/// These options control the behavior of file uploads and downloads,
/// including batch sizes and upload concurrency.
///
/// The fields are private and are set only through the validating `with_*`
/// methods. A public `file_size_to_split` could be set to 0 or a negative
/// number, which the multi-part part-count arithmetic divides by.
///
/// # Example
///
/// ```
/// use gpudb_api::filesystem::KifsFileHandlerOptions;
///
/// // Create default options
/// let options = KifsFileHandlerOptions::default();
///
/// // Create custom options
/// let options = KifsFileHandlerOptions::new()
///     .with_file_size_to_split(50 * 1024 * 1024)? // 50 MB
///     .with_thread_pool_size(1)?;
/// # Ok::<(), gpudb_api::KineticaError>(())
/// ```
#[derive(Debug, Clone)]
pub struct KifsFileHandlerOptions {
    /// Size threshold for splitting files into multiple parts (in bytes).
    /// Files larger than this size will be uploaded/downloaded in parts, and
    /// smaller files are uploaded/downloaded in batches of up to this size.
    /// Default: 60 MB (62914560 bytes)
    file_size_to_split: i64,

    /// Number of full-file upload batches sent to the server concurrently.
    /// Default: 5
    full_file_dispatcher_threadpool_size: usize,

    /// Ceiling on the full-file upload payload bytes held in memory at once,
    /// across the batches being sent concurrently.
    /// Default: 128 MB
    max_upload_bytes_in_flight: i64,

    /// Timeout in seconds for thread pool termination.
    /// Default: 90 seconds
    thread_pool_termination_timeout: u64,
}

impl Default for KifsFileHandlerOptions {
    fn default() -> Self {
        Self {
            file_size_to_split: DEFAULT_FILE_SIZE_TO_SPLIT,
            full_file_dispatcher_threadpool_size: DEFAULT_FULL_FILE_DISPATCHER_THREADPOOL_SIZE,
            max_upload_bytes_in_flight: DEFAULT_MAX_UPLOAD_BYTES_IN_FLIGHT,
            thread_pool_termination_timeout: DEFAULT_THREAD_POOL_TERMINATION_TIMEOUT,
        }
    }
}

impl KifsFileHandlerOptions {
    /// Creates a new `KifsFileHandlerOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the file size threshold for multi-part transfers.
    ///
    /// # Arguments
    ///
    /// * `size` - Size in bytes. Must be a positive value <= 60 MB.
    ///
    /// # Errors
    ///
    /// Fails if the size is <= 0 or > 60 MB.
    pub fn with_file_size_to_split(mut self, size: i64) -> Result<Self, KineticaError> {
        if size <= 0 || size > DEFAULT_FILE_SIZE_TO_SPLIT {
            return Err(KineticaError::Config(format!(
                "file_size_to_split must be positive and at most {DEFAULT_FILE_SIZE_TO_SPLIT}"
            )));
        }
        self.file_size_to_split = size;
        Ok(self)
    }

    /// Sets how many full-file upload batches are sent concurrently.
    ///
    /// Uploads proceed in waves: up to this many batches are sent at once, and
    /// the next wave starts when every batch of the current one has finished.
    ///
    /// # Arguments
    ///
    /// * `size` - Number of concurrent batches. Must be between 1 and the
    ///   number of available CPUs.
    ///
    /// # Errors
    ///
    /// Fails if the size is 0 or greater than the number of available CPUs.
    pub fn with_thread_pool_size(mut self, size: usize) -> Result<Self, KineticaError> {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        if size == 0 || size > cpus {
            return Err(KineticaError::Config(format!("Thread pool size must be between 1 and {cpus}")));
        }
        self.full_file_dispatcher_threadpool_size = size;
        Ok(self)
    }

    /// Sets the ceiling on full-file upload payload bytes held in memory at
    /// once.
    ///
    /// Lower it where memory is tight: an upload then waits for outstanding
    /// batches to finish more often, trading throughput for a smaller peak.
    /// Raise it for more throughput where memory is plentiful.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The ceiling in bytes. Must be positive.
    ///
    /// # Errors
    ///
    /// Fails if `bytes` is <= 0.
    pub fn with_max_upload_bytes_in_flight(mut self, bytes: i64) -> Result<Self, KineticaError> {
        if bytes <= 0 {
            return Err(KineticaError::Config(format!(
                "max_upload_bytes_in_flight must be positive; got {bytes}"
            )));
        }
        self.max_upload_bytes_in_flight = bytes;
        Ok(self)
    }

    /// Sets the timeout for thread pool termination.
    ///
    /// This has no effect. There are no thread pools to shut down: the upload
    /// tasks are awaited to completion, and aborted as soon as the
    /// operation's future is dropped.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Timeout in seconds.
    pub fn with_termination_timeout(mut self, timeout: u64) -> Self {
        self.thread_pool_termination_timeout = timeout;
        self
    }

    /// Returns the file size threshold for multi-part transfers.
    pub fn file_size_to_split(&self) -> i64 {
        self.file_size_to_split
    }

    /// Returns the number of full-file upload batches sent concurrently.
    pub fn thread_pool_size(&self) -> usize {
        self.full_file_dispatcher_threadpool_size
    }

    /// Returns the number of full-file upload batches sent concurrently.
    pub fn full_file_dispatcher_threadpool_size(&self) -> i32 {
        self.full_file_dispatcher_threadpool_size as i32
    }

    /// Returns the ceiling on full-file upload payload bytes held in memory
    /// at once.
    pub fn max_upload_bytes_in_flight(&self) -> i64 {
        self.max_upload_bytes_in_flight
    }

    /// Returns the termination timeout.
    pub fn termination_timeout(&self) -> u64 {
        self.thread_pool_termination_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_documented_values() {
        let options = KifsFileHandlerOptions::default();
        assert_eq!(options.file_size_to_split(), 62914560);
        assert_eq!(options.thread_pool_size(), 5);
        assert_eq!(options.max_upload_bytes_in_flight(), 128 * 1024 * 1024);
        assert_eq!(options.termination_timeout(), 90);
    }

    #[test]
    fn invalid_values_are_refused() {
        let message = |e: KineticaError| e.to_string();
        for size in [0, -1, DEFAULT_FILE_SIZE_TO_SPLIT + 1] {
            assert_eq!(
                message(KifsFileHandlerOptions::new().with_file_size_to_split(size).unwrap_err()),
                "Configuration error: file_size_to_split must be positive and at most 62914560"
            );
        }
        let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        for size in [0, cpus + 1] {
            assert_eq!(
                message(KifsFileHandlerOptions::new().with_thread_pool_size(size).unwrap_err()),
                format!("Configuration error: Thread pool size must be between 1 and {cpus}")
            );
        }
        assert_eq!(
            message(KifsFileHandlerOptions::new().with_max_upload_bytes_in_flight(0).unwrap_err()),
            "Configuration error: max_upload_bytes_in_flight must be positive; got 0"
        );
    }

    #[test]
    fn default_download_options_overwrite() {
        assert!(DownloadOptions::default().is_overwrite_existing());
        assert!(!DownloadOptions::default().with_overwrite_existing(false).is_overwrite_existing());
    }

    #[test]
    fn setters_round_trip() {
        let options = KifsFileHandlerOptions::new()
            .with_file_size_to_split(1024)
            .unwrap()
            .with_thread_pool_size(1)
            .unwrap()
            .with_max_upload_bytes_in_flight(4096)
            .unwrap()
            .with_termination_timeout(7);
        assert_eq!(options.file_size_to_split(), 1024);
        assert_eq!(options.thread_pool_size(), 1);
        assert_eq!(options.max_upload_bytes_in_flight(), 4096);
        assert_eq!(options.termination_timeout(), 7);
    }
}

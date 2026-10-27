//! Main KiFS file handler API.

use std::collections::HashMap;
use std::sync::Arc;
use log::error;

use crate::{
    Kinetica, KineticaError,
    CreateDirectoryRequest, DeleteDirectoryRequest, DeleteFilesRequest,
    ShowDirectoriesRequest, ShowFilesRequest,
};
use super::downloader::FileDownloader;
use super::file_operation::{local_dir_exists, local_file_exists};
use super::ingest::{FileIngestor, IngestOptions, IngestResult, TableCreationOptions};
use super::listeners::{FileDownloadListener, FileUploadListener};
use super::options::{
    DownloadOptions, KifsFileHandlerOptions, UploadOptions, DEFAULT_FULL_FILE_DISPATCHER_THREADPOOL_SIZE,
    DEFAULT_THREAD_POOL_TERMINATION_TIMEOUT,
};
use super::types::{KifsDirectoryInfo, KifsFileInfo, OperationResult};
use super::uploader::FileUploader;

/// Main class for interacting with the Kinetica File System (KiFS).
///
/// This handler provides a high-level API for file uploads, downloads,
/// directory management, and file operations.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::{Kinetica, KineticaOptions};
/// use gpudb_api::filesystem::{KifsFileHandler, KifsFileHandlerOptions};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let kinetica = Arc::new(
///     Kinetica::new(vec!["http://localhost:9191".to_string()], KineticaOptions::new()).await?
/// );
///
/// let handler = KifsFileHandler::new(kinetica, None);
///
/// // Create a directory
/// handler.create_directory("my_dir", true).await?;
///
/// // Upload files
/// handler.upload("/path/to/*.csv", "my_dir", None, None).await?;
///
/// // List files
/// let files = handler.show_files(vec!["my_dir".to_string()]).await?;
/// println!("Found {} files", files.len());
///
/// // Delete directory
/// handler.delete_directory("my_dir", true, true).await?;
/// # Ok(())
/// # }
/// ```
pub struct KifsFileHandler {
    /// Kinetica client instance
    db: Arc<Kinetica>,
    /// File handler options
    options: KifsFileHandlerOptions,
}

impl KifsFileHandler {
    /// Creates a new KiFS file handler.
    ///
    /// # Arguments
    ///
    /// * `db` - The Kinetica client instance
    /// * `options` - Optional file handler options (uses defaults if None)
    pub fn new(db: Arc<Kinetica>, options: Option<KifsFileHandlerOptions>) -> Self {
        Self {
            db,
            options: options.unwrap_or_default(),
        }
    }

    /// Returns the file handler options.
    pub fn options(&self) -> &KifsFileHandlerOptions {
        &self.options
    }

    /// The default thread pool termination timeout, in seconds.
    pub fn default_thread_pool_termination_timeout() -> i32 {
        DEFAULT_THREAD_POOL_TERMINATION_TIMEOUT as i32
    }

    /// The default number of full-file upload batches sent concurrently.
    pub fn default_full_file_dispatcher_threadpool_size() -> i32 {
        DEFAULT_FULL_FILE_DISPATCHER_THREADPOOL_SIZE as i32
    }

    // ==================== Upload Operations ====================

    /// Uploads a single file from a local path to a KiFS directory.
    ///
    /// # Arguments
    ///
    /// * `file_name` - Name and path of the file (or glob pattern) to upload
    /// * `remote_dir_name` - Name of the KiFS directory to upload to
    /// * `upload_options` - Optional upload options (uses defaults if None)
    /// * `callback` - Optional listener for progress callbacks
    ///
    /// # Returns
    ///
    /// A vector of `OperationResult` for each file processed (successful or failed).
    ///
    /// # Errors
    ///
    /// Returns an aggregated error containing all failure messages if any files fail to upload.
    /// The operation continues processing remaining files even when individual uploads fail.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use gpudb_api::filesystem::KifsFileHandler;
    /// # async fn example(handler: &KifsFileHandler) -> Result<(), Box<dyn std::error::Error>> {
    /// // Upload a single file
    /// let results = handler.upload("/path/to/file.csv", "my_dir", None, None).await?;
    ///
    /// // Upload with glob pattern
    /// let results = handler.upload("/data/**/*.json", "my_dir", None, None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn upload(
        &self,
        file_name: &str,
        remote_dir_name: &str,
        upload_options: Option<UploadOptions>,
        callback: Option<&dyn FileUploadListener>,
    ) -> Result<Vec<OperationResult>, KineticaError> {
        self.upload_multiple(vec![file_name.to_string()], remote_dir_name, upload_options, callback)
            .await
    }

    /// Uploads multiple files to a KiFS directory.
    ///
    /// # Arguments
    ///
    /// * `file_names` - Names and paths of the files (or glob patterns) to upload
    /// * `remote_dir_name` - Name of the KiFS directory to upload to
    /// * `upload_options` - Optional upload options (uses defaults if None)
    /// * `callback` - Optional listener for progress callbacks
    ///
    /// # Returns
    ///
    /// A vector of `OperationResult` for each file processed (successful or failed).
    ///
    /// # Errors
    ///
    /// Returns an aggregated error containing all failure messages if any files fail to upload.
    /// The operation continues processing remaining files even when individual uploads fail.
    pub async fn upload_multiple(
        &self,
        file_names: Vec<String>,
        remote_dir_name: &str,
        upload_options: Option<UploadOptions>,
        callback: Option<&dyn FileUploadListener>,
    ) -> Result<Vec<OperationResult>, KineticaError> {
        // Validate remote directory exists
        if !self.kifs_directory_exists(remote_dir_name).await {
            return Err(KineticaError::Api("Remote directory does not exist".to_string()));
        }

        // Validate that explicit files exist. A glob pattern passes this
        // check (`local_file_exists` returns true for it) and is resolved
        // later; a pattern that then matches nothing is only a warning, but a
        // plain name that does not exist fails the whole upload here, before
        // anything is sent.
        if file_names.is_empty() {
            return Err(KineticaError::Config(
                "List of local files to upload cannot be empty".to_string(),
            ));
        }

        for file_name in &file_names {
            if file_name.is_empty() {
                return Err(KineticaError::Config(
                    "Name of local file to upload cannot be empty".to_string(),
                ));
            }
        }

        let names = file_names.clone();
        let non_existent: Vec<String> = tokio::task::spawn_blocking(move || {
            names.into_iter().filter(|n| !local_file_exists(n)).collect()
        })
        .await
        .map_err(|e| KineticaError::Config(format!("Local file check failed: {e}")))?;

        if !non_existent.is_empty() {
            return Err(KineticaError::Config(format!(
                "Input files [[{}]] not found (patterns must match valid Glob syntax)",
                non_existent.join(", ")
            )));
        }

        let options = upload_options.unwrap_or_default();

        let uploader = FileUploader::new(
            self.db.clone(),
            file_names,
            remote_dir_name.to_string(),
            options,
            self.options.clone(),
            callback,
        );

        uploader.upload().await
    }

    // ==================== Download Operations ====================

    /// Downloads a single KiFS file to a local directory.
    ///
    /// # Arguments
    ///
    /// * `file_name` - Path of the KiFS file to download
    /// * `local_dir_name` - Path of the local directory to download to
    /// * `download_options` - Optional download options (uses defaults if None)
    /// * `callback` - Optional listener for progress callbacks
    ///
    /// # Returns
    ///
    /// A vector of `OperationResult` for each file processed (successful or failed).
    ///
    /// # Errors
    ///
    /// Returns an aggregated error containing all failure messages if any files fail to download.
    /// The operation continues processing remaining files even when individual downloads fail.
    pub async fn download(
        &self,
        file_name: &str,
        local_dir_name: &str,
        download_options: Option<DownloadOptions>,
        callback: Option<&dyn FileDownloadListener>,
    ) -> Result<Vec<OperationResult>, KineticaError> {
        self.download_multiple(
            vec![file_name.to_string()],
            local_dir_name,
            download_options,
            callback,
        )
        .await
    }

    /// Downloads multiple KiFS files to a local directory.
    ///
    /// # Arguments
    ///
    /// * `file_names` - Paths of the KiFS files to download
    /// * `local_dir_name` - Path of the local directory to download to
    /// * `download_options` - Optional download options (uses defaults if None)
    /// * `callback` - Optional listener for progress callbacks
    ///
    /// # Returns
    ///
    /// A vector of `OperationResult` for each file processed (successful or failed).
    ///
    /// # Errors
    ///
    /// Returns an aggregated error containing all failure messages if any files fail to download.
    /// The operation continues processing remaining files even when individual downloads fail.
    pub async fn download_multiple(
        &self,
        file_names: Vec<String>,
        local_dir_name: &str,
        download_options: Option<DownloadOptions>,
        callback: Option<&dyn FileDownloadListener>,
    ) -> Result<Vec<OperationResult>, KineticaError> {
        // Validate local directory
        if local_dir_name.is_empty() {
            return Err(KineticaError::Config(
                "Local directory name is empty".to_string(),
            ));
        }

        // Validate file names. Checked before the local directory.
        if file_names.is_empty() {
            return Err(KineticaError::Config(
                "List of KiFS file names cannot be empty".to_string(),
            ));
        }

        for file_name in &file_names {
            if file_name.is_empty() {
                return Err(KineticaError::Config(
                    "KiFS file name cannot be empty".to_string(),
                ));
            }
        }

        if !local_dir_exists(local_dir_name).await {
            return Err(KineticaError::Config(format!(
                "Local directory {local_dir_name} does not exist"
            )));
        }

        let options = download_options.unwrap_or_default();

        let downloader = FileDownloader::new(
            self.db.clone(),
            file_names,
            local_dir_name.to_string(),
            options,
            self.options.clone(),
            callback,
        );

        downloader.download().await
    }

    /// Downloads all files from a KiFS directory to a local directory.
    ///
    /// Each file keeps its path below the KiFS directory: `dir/a.csv` is saved
    /// as `<local dir>/a.csv`, and `dir/sub/x.csv` as `<local dir>/sub/x.csv`,
    /// its subdirectory made as needed.
    ///
    /// # Arguments
    ///
    /// * `remote_dir_name` - Path of the KiFS directory to download
    /// * `local_dir_name` - Path of the local directory to download to
    /// * `download_options` - Optional download options (uses defaults if None)
    /// * `callback` - Optional listener for progress callbacks
    ///
    /// # Returns
    ///
    /// A vector of `OperationResult` for each file processed (successful or failed).
    ///
    /// # Errors
    ///
    /// Returns an aggregated error containing all failure messages if any files fail to download.
    /// The operation continues processing remaining files even when individual downloads fail.
    pub async fn download_dir(
        &self,
        remote_dir_name: &str,
        local_dir_name: &str,
        download_options: Option<DownloadOptions>,
        callback: Option<&dyn FileDownloadListener>,
    ) -> Result<Vec<OperationResult>, KineticaError> {
        if !local_dir_exists(local_dir_name).await {
            return Err(KineticaError::Config(format!(
                "Local directory {local_dir_name} does not exist"
            )));
        }

        // Get files in the directory
        let request = ShowFilesRequest {
            paths: vec![remote_dir_name.to_string()],
            options: HashMap::new(),
        };

        let response = self.db.show_files_raw(&request).await?;

        if !response.file_names.is_empty() {
            return self.download_multiple(response.file_names, local_dir_name, download_options, callback)
                .await;
        }

        Ok(Vec::new())
    }

    // ==================== Ingest ====================

    /// Uploads local files and inserts their records into `table_name`.
    ///
    /// The files, which may be glob patterns, are uploaded to a directory of
    /// their own under the user's KiFS home directory, inserted from there
    /// with `/insert/records/fromfiles` and the options given, and the
    /// directory is deleted afterwards.
    ///
    /// # Errors
    ///
    /// Fails if no file is named, a name is empty, or a file named (not a
    /// pattern) does not exist; and with the upload's or the insert's error
    /// if either fails.
    pub async fn ingest(
        &self,
        file_names: Vec<String>,
        table_name: &str,
        ingest_options: Option<IngestOptions>,
        create_table_options: Option<TableCreationOptions>,
    ) -> Result<IngestResult, KineticaError> {
        if file_names.is_empty() {
            return Err(KineticaError::Config("List of files to ingest cannot be empty".to_string()));
        }
        if file_names.iter().any(String::is_empty) {
            return Err(KineticaError::Config("Name of file to ingest cannot be empty".to_string()));
        }

        let names = file_names.clone();
        let non_existent: Vec<String> =
            tokio::task::spawn_blocking(move || names.into_iter().filter(|n| !local_file_exists(n)).collect())
                .await
                .map_err(|e| KineticaError::Config(format!("Local file check failed: {e}")))?;
        if !non_existent.is_empty() {
            return Err(KineticaError::Config(format!(
                "Input file names [[{}]] to ingest are not found ",
                non_existent.join(", ")
            )));
        }

        let result = FileIngestor::new(self.db.clone(), table_name, file_names, ingest_options, create_table_options)
            .ingest_from_files()
            .await;
        match result.error_message() {
            Some(message) if result.error().is_some() => Err(KineticaError::Api(message.to_string())),
            _ => Ok(result),
        }
    }

    // ==================== File Operations ====================

    /// Deletes the given list of KiFS file paths.
    ///
    /// # Arguments
    ///
    /// * `file_names` - List of files in KiFS to delete
    /// * `no_error_if_not_exists` - Whether to suppress errors if files don't exist
    pub async fn delete_files(
        &self,
        file_names: Vec<String>,
        no_error_if_not_exists: bool,
    ) -> Result<(), KineticaError> {
        let mut options = HashMap::new();
        options.insert(
            "no_error_if_not_exists".to_string(),
            if no_error_if_not_exists { "true" } else { "false" }.to_string(),
        );

        let request = DeleteFilesRequest {
            file_names,
            options,
        };

        self.db.delete_files_raw(&request).await?;
        Ok(())
    }

    /// Deletes files in the given KiFS directory.
    ///
    /// The files found are deleted with one `/delete/files` request, sent
    /// even when there are none.
    ///
    /// # Arguments
    ///
    /// * `remote_dir_name` - Name of the KiFS directory containing files to delete
    /// * `no_error_if_not_exists` - Whether to suppress errors if the directory doesn't exist
    pub async fn delete_files_in_dir(
        &self,
        remote_dir_name: &str,
        no_error_if_not_exists: bool,
    ) -> Result<(), KineticaError> {
        // Get files in the directory
        let show_request = ShowFilesRequest {
            paths: vec![remote_dir_name.to_string()],
            options: HashMap::new(),
        };

        let response = self.db.show_files_raw(&show_request).await?;
        self.delete_files(response.file_names, no_error_if_not_exists).await
    }

    /// Returns statistics about the given KiFS files and/or directories.
    ///
    /// # Arguments
    ///
    /// * `remote_paths` - List of KiFS files and/or directory names
    ///
    /// # Returns
    ///
    /// List of `KifsFileInfo` objects containing statistics about the files.
    pub async fn show_files(&self, remote_paths: Vec<String>) -> Result<Vec<KifsFileInfo>, KineticaError> {
        if remote_paths.is_empty() {
            return Err(KineticaError::Config(
                "List of KiFS directory names cannot be empty".to_string(),
            ));
        }

        let request = ShowFilesRequest {
            paths: remote_paths,
            options: HashMap::new(),
        };

        let response = self.db.show_files_raw(&request).await?;

        let mut results = Vec::new();
        for (i, file_name) in response.file_names.iter().enumerate() {
            results.push(KifsFileInfo {
                file_name: file_name.clone(),
                file_size: response.sizes.get(i).copied().unwrap_or(0),
                created_by: response.users.get(i).cloned().unwrap_or_default(),
                creation_time: response.creation_times.get(i).copied().unwrap_or(0),
                info: response.info.clone(),
            });
        }

        Ok(results)
    }

    // ==================== Directory Operations ====================

    /// Creates a KiFS directory with the given name.
    ///
    /// # Arguments
    ///
    /// * `remote_dir_name` - Name of the KiFS directory to create
    /// * `no_error_if_exists` - Whether to suppress an error if the directory already exists
    pub async fn create_directory(
        &self,
        remote_dir_name: &str,
        no_error_if_exists: bool,
    ) -> Result<(), KineticaError> {
        let mut options = HashMap::new();
        options.insert(
            "no_error_if_exists".to_string(),
            if no_error_if_exists { "true" } else { "false" }.to_string(),
        );

        let request = CreateDirectoryRequest {
            directory_name: remote_dir_name.to_string(),
            options,
        };

        self.db.create_directory_raw(&request).await?;
        Ok(())
    }

    /// Deletes the given KiFS directory.
    ///
    /// # Arguments
    ///
    /// * `remote_dir_name` - Name of the KiFS directory to delete
    /// * `recursive` - Whether to delete all files and subdirectories
    /// * `no_error_if_not_exists` - Whether to suppress an error if the directory doesn't exist
    pub async fn delete_directory(
        &self,
        remote_dir_name: &str,
        recursive: bool,
        no_error_if_not_exists: bool,
    ) -> Result<(), KineticaError> {
        let mut options = HashMap::new();
        options.insert(
            "recursive".to_string(),
            if recursive { "true" } else { "false" }.to_string(),
        );
        options.insert(
            "no_error_if_not_exists".to_string(),
            if no_error_if_not_exists { "true" } else { "false" }.to_string(),
        );

        let request = DeleteDirectoryRequest {
            directory_name: remote_dir_name.to_string(),
            options,
        };

        self.db.delete_directory_raw(&request).await?;
        Ok(())
    }

    /// Returns statistics about the given KiFS directories: one entry for
    /// each.
    ///
    /// # Arguments
    ///
    /// * `remote_dir_names` - List of KiFS directory names
    pub async fn show_directories(
        &self,
        remote_dir_names: Vec<String>,
    ) -> Result<Vec<KifsDirectoryInfo>, KineticaError> {
        self.show_directories_with_options(remote_dir_names, HashMap::new()).await
    }

    /// Returns statistics about the given KiFS directories, one entry for
    /// each, passing `options` to each `/show/directories`.
    pub async fn show_directories_with_options(
        &self,
        remote_dir_names: Vec<String>,
        options: HashMap<String, String>,
    ) -> Result<Vec<KifsDirectoryInfo>, KineticaError> {
        if remote_dir_names.is_empty() {
            return Err(KineticaError::Config(
                "Set of KiFS directory names cannot be empty".to_string(),
            ));
        }

        let mut results = Vec::new();

        for dir_name in &remote_dir_names {
            if dir_name.trim_matches(|c: char| c <= ' ').is_empty() {
                return Err(KineticaError::Config(
                    "Set of KiFS directory names cannot contain an empty directory name".to_string(),
                ));
            }

            let request = ShowDirectoriesRequest {
                directory_name: dir_name.clone(),
                options: options.clone(),
            };

            let response = self.db.show_directories_raw(&request).await?;

            fn first<T: Clone>(list: &[T]) -> Result<T, KineticaError> {
                list.first()
                    .cloned()
                    .ok_or_else(|| KineticaError::Api("The /show/directories response is missing an entry".to_string()))
            }
            results.push(KifsDirectoryInfo {
                kifs_path: first(&response.directories)?,
                created_by: first(&response.users)?,
                permission: first(&response.permissions)?,
                creation_time: first(&response.creation_times)?,
                info: HashMap::new(),
            });
        }

        Ok(results)
    }

    /// Returns statistics about a single KiFS directory.
    ///
    /// # Arguments
    ///
    /// * `remote_dir_name` - Name of the KiFS directory
    ///
    /// # Returns
    ///
    /// List of `KifsDirectoryInfo` objects (may include subdirectories).
    pub async fn show_directory(&self, remote_dir_name: &str) -> Result<Vec<KifsDirectoryInfo>, KineticaError> {
        self.show_directory_with_options(remote_dir_name, HashMap::new()).await
    }

    /// Returns statistics about a single KiFS directory, passing `options`
    /// to `/show/directories`.
    pub async fn show_directory_with_options(
        &self,
        remote_dir_name: &str,
        options: HashMap<String, String>,
    ) -> Result<Vec<KifsDirectoryInfo>, KineticaError> {
        if remote_dir_name.is_empty() {
            return Err(KineticaError::Config(
                "KiFS directory name [remote_dir_name] cannot be empty".to_string(),
            ));
        }
        self.directories(remote_dir_name, options).await
    }

    /// Returns statistics about all KiFS directories.
    ///
    /// # Returns
    ///
    /// List of `KifsDirectoryInfo` objects for all directories.
    pub async fn show_all_directories(&self) -> Result<Vec<KifsDirectoryInfo>, KineticaError> {
        self.show_all_directories_with_options(HashMap::new()).await
    }

    /// Returns statistics about all KiFS directories, passing `options` to
    /// `/show/directories`.
    pub async fn show_all_directories_with_options(
        &self,
        options: HashMap<String, String>,
    ) -> Result<Vec<KifsDirectoryInfo>, KineticaError> {
        self.directories("", options).await
    }

    /// Every entry `/show/directories` gives for `directory_name`.
    async fn directories(
        &self,
        directory_name: &str,
        options: HashMap<String, String>,
    ) -> Result<Vec<KifsDirectoryInfo>, KineticaError> {
        let request = ShowDirectoriesRequest {
            directory_name: directory_name.to_string(),
            options,
        };

        let response = self.db.show_directories_raw(&request).await?;

        let mut results = Vec::new();
        for (i, directory) in response.directories.iter().enumerate() {
            results.push(KifsDirectoryInfo {
                kifs_path: directory.clone(),
                created_by: response.users.get(i).cloned().unwrap_or_default(),
                creation_time: response.creation_times.get(i).copied().unwrap_or(0),
                permission: response.permissions.get(i).cloned().unwrap_or_default(),
                info: HashMap::new(),
            });
        }

        Ok(results)
    }

    // ==================== Existence Checks ====================

    /// Checks whether the given KiFS directory exists.
    ///
    /// # Arguments
    ///
    /// * `dir_name` - The full KiFS directory path to check
    ///
    /// # Returns
    ///
    /// `true` if the directory exists, `false` otherwise.
    pub async fn kifs_directory_exists(&self, dir_name: &str) -> bool {
        if dir_name.is_empty() {
            error!("KiFS directory name cannot be empty");
            return false;
        }

        let request = ShowDirectoriesRequest {
            directory_name: dir_name.to_string(),
            options: HashMap::new(),
        };

        match self.db.show_directories_raw(&request).await {
            Ok(response) => response.directories.contains(&dir_name.to_string()),
            Err(e) => {
                error!("{e}");
                false
            }
        }
    }

    /// Checks whether the given KiFS directories exist.
    ///
    /// # Arguments
    ///
    /// * `dir_names` - The set of full KiFS directory paths to check
    ///
    /// # Returns
    ///
    /// `true` if all directories exist, `false` otherwise.
    pub async fn kifs_directories_exist(&self, dir_names: &[String]) -> bool {
        if dir_names.is_empty() {
            error!("List of Directory names cannot be empty");
            return false;
        }

        let request = ShowDirectoriesRequest {
            directory_name: String::new(),
            options: HashMap::new(),
        };

        match self.db.show_directories_raw(&request).await {
            Ok(response) => {
                let existing: std::collections::HashSet<_> = response.directories.iter().collect();
                dir_names.iter().all(|name| existing.contains(name))
            }
            Err(e) => {
                error!("{e}");
                false
            }
        }
    }

    /// Checks whether the given KiFS file exists.
    ///
    /// # Arguments
    ///
    /// * `file_name` - The full KiFS file path to check
    ///
    /// # Returns
    ///
    /// `true` if the file exists, `false` otherwise.
    pub async fn kifs_file_exists(&self, file_name: &str) -> bool {
        if file_name.is_empty() {
            error!("KiFS file name cannot be empty");
            return false;
        }

        let request = ShowFilesRequest {
            paths: vec![file_name.to_string()],
            options: HashMap::new(),
        };

        match self.db.show_files_raw(&request).await {
            Ok(response) => response.file_names.contains(&file_name.to_string()),
            Err(e) => {
                error!("{e}");
                false
            }
        }
    }
}

/// The handler's checks and requests, against a stand-in server.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
    use crate::{DeleteFilesResponse, KineticaOptions, ShowDirectoriesResponse, ShowFilesResponse};

    /// A handler on a server knowing the directories `dirs`, each holding no
    /// files, and recording what it is asked to delete.
    async fn handler(host: &str, dirs: &'static [&'static str]) -> (fake_server::FakeServer, KifsFileHandler) {
        let server = fake_server::start(host).await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, _: &[u8]| {
            let reply = match path {
                "/show/directories" => encode(
                    &crate::generated::SHOW_DIRECTORIES_RESPONSE_SCHEMA,
                    &ShowDirectoriesResponse {
                        directories: dirs.iter().map(|d| d.to_string()).collect(),
                        users: dirs.iter().map(|_| "u".to_string()).collect(),
                        creation_times: dirs.iter().map(|_| 7).collect(),
                        data_usages: dirs.iter().map(|_| 0).collect(),
                        data_limits: dirs.iter().map(|_| 0).collect(),
                        permissions: dirs.iter().map(|_| "write".to_string()).collect(),
                        ..Default::default()
                    },
                ),
                "/show/files" => encode(&crate::generated::SHOW_FILES_RESPONSE_SCHEMA, &ShowFilesResponse::default()),
                "/delete/files" => encode(&crate::generated::DELETE_FILES_RESPONSE_SCHEMA, &DeleteFilesResponse::default()),
                _ => return None,
            };
            Some(Ok(reply))
        }));
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0);
        let k = Arc::new(Kinetica::new(vec![server.url.to_string()], options).await.expect("client"));
        (server, KifsFileHandler::new(k, None))
    }

    // One entry for each directory: the first entry of its answer.
    #[tokio::test]
    async fn directories_are_shown_one_entry_each() {
        let (_server, handler) = handler("127.0.0.131", &["a", "a/b"]).await;
        let dirs = handler.show_directories(vec!["a".into(), "x".into()]).await.unwrap();
        let paths: Vec<&str> = dirs.iter().map(|d| d.kifs_path.as_str()).collect();
        assert_eq!(paths, ["a", "a"]);
        assert_eq!(dirs[0].permission, "write");
        assert_eq!(handler.show_directory("a").await.unwrap().len(), 2, "a single directory's every entry");

        let message = |e: KineticaError| e.to_string();
        assert_eq!(
            message(handler.show_directories(Vec::new()).await.unwrap_err()),
            "Configuration error: Set of KiFS directory names cannot be empty"
        );
        assert_eq!(
            message(handler.show_directories(vec![" \t".into()]).await.unwrap_err()),
            "Configuration error: Set of KiFS directory names cannot contain an empty directory name"
        );
        assert_eq!(
            message(handler.show_directory("").await.unwrap_err()),
            "Configuration error: KiFS directory name [remote_dir_name] cannot be empty"
        );
        assert_eq!(
            message(handler.show_files(Vec::new()).await.unwrap_err()),
            "Configuration error: List of KiFS directory names cannot be empty"
        );
    }

    // The files found are deleted, even when there are none.
    #[tokio::test]
    async fn the_files_of_an_empty_directory_are_still_deleted() {
        let (server, handler) = handler("127.0.0.132", &["a"]).await;
        handler.delete_files_in_dir("a", true).await.unwrap();
        assert_eq!(server.behavior.count("/delete/files"), 1);
    }

    // What upload and download refuse.
    #[tokio::test]
    async fn invalid_transfers_are_refused() {
        let (_server, handler) = handler("127.0.0.133", &["a"]).await;
        let message = |e: KineticaError| e.to_string();
        assert_eq!(
            message(handler.upload("/no/such/file.txt", "missing", None, None).await.unwrap_err()),
            "API error: Remote directory does not exist"
        );
        assert_eq!(
            message(
                handler
                    .upload_multiple(vec!["/no/such/1.txt".into(), "/no/such/2.txt".into()], "a", None, None)
                    .await
                    .unwrap_err()
            ),
            "Configuration error: Input files [[/no/such/1.txt, /no/such/2.txt]] not found (patterns must match valid Glob syntax)"
        );
        assert_eq!(
            message(handler.download("a/f.txt", "/no/such/dir", None, None).await.unwrap_err()),
            "Configuration error: Local directory /no/such/dir does not exist"
        );
        assert_eq!(KifsFileHandler::default_thread_pool_termination_timeout(), 90);
        assert_eq!(KifsFileHandler::default_full_file_dispatcher_threadpool_size(), 5);
    }
}

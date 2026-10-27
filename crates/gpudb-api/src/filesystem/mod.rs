//! KiFS (Kinetica File System) API module.
//!
//! This module provides a high-level API for interacting with Kinetica's distributed
//! file system (KiFS). It supports file uploads, downloads, directory management,
//! and file operations with support for both single-file and multi-part transfers.
//!
//! # Features
//!
//! - **File Upload**: Upload single files or multiple files with glob pattern support
//! - **File Download**: Download single files or entire directories
//! - **Directory Management**: Create, delete, and list KiFS directories
//! - **Progress Callbacks**: Optional listeners for tracking upload/download progress
//! - **Multi-part Transfers**: Automatic handling of large files using chunked transfers
//! - **Glob Pattern Support**: Use patterns like `*.csv` or `data/**/*.json` for batch operations
//!
//! # Example
//!
//! ```no_run
//! use gpudb_api::{Kinetica, KineticaOptions};
//! use gpudb_api::filesystem::{KifsFileHandler, KifsFileHandlerOptions, UploadOptions};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let kinetica = Arc::new(
//!     Kinetica::new(vec!["http://localhost:9191".to_string()], KineticaOptions::new()).await?
//! );
//!
//! // Create a file handler
//! let handler = KifsFileHandler::new(kinetica, None);
//!
//! // Create a directory
//! handler.create_directory("my_directory", true).await?;
//!
//! // Upload a file
//! handler.upload("/path/to/file.csv", "my_directory", None, None).await?;
//!
//! // Upload multiple files with glob pattern
//! handler.upload("/data/*.csv", "my_directory", None, None).await?;
//!
//! // Download a file
//! handler.download("my_directory/file.csv", "/local/path", None, None).await?;
//!
//! // List files in a directory
//! let files = handler.show_files(vec!["my_directory".to_string()]).await?;
//! for file in files {
//!     println!("File: {} ({} bytes)", file.file_name, file.file_size);
//! }
//!
//! // Delete a file
//! handler.delete_files(vec!["my_directory/file.csv".to_string()], true).await?;
//!
//! // Delete a directory
//! handler.delete_directory("my_directory", true, true).await?;
//! # Ok(())
//! # }
//! ```

mod types;
mod options;
mod listeners;
mod file_operation;
mod uploader;
mod downloader;
mod handler;
mod ingest;

// Re-export public types
pub use types::{
    KifsFileInfo,
    KifsDirectoryInfo,
    OperationResult,
    MultiPartUploadInfo,
    MultiPartDownloadInfo,
    MultiPartOperation,
    OpMode,
};

pub use options::{
    UploadOptions,
    DownloadOptions,
    KifsFileHandlerOptions,
};

pub use listeners::{
    FileUploadListener,
    FileDownloadListener,
    FnUploadListener,
    FnDownloadListener,
    NoOpUploadListener,
    NoOpDownloadListener,
};

pub use handler::KifsFileHandler;

pub use ingest::{
    ingest_option, table_creation_option, ErrorHandlingMode, FileType, IngestOptions, IngestResult, PartitionType,
    TableCreationOptions, TypeInferenceMode,
};

/// Separator character between a KiFS directory and KiFS file name.
/// This can also be used within a file name to create a virtual directory within a path.
pub const KIFS_PATH_SEPARATOR: &str = "/";

/// Prefix to use when referencing KiFS files; e.g., for purposes of file ingest.
pub const KIFS_PATH_PREFIX: &str = "kifs://";

/// Alias for a user's home directory.
pub const REMOTE_USER_HOME_DIR_PREFIX: &str = "~";

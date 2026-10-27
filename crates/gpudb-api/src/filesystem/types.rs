//! Core types for the KiFS filesystem API.

use std::collections::HashMap;
use std::fmt;

use crate::KineticaError;

/// Indicates whether an operation is an upload or download.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpMode {
    /// File upload operation
    Upload,
    /// File download operation
    Download,
}

impl fmt::Display for OpMode {
    /// The mode's name in capitals: `UPLOAD` or `DOWNLOAD`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OpMode::Upload => "UPLOAD",
            OpMode::Download => "DOWNLOAD",
        })
    }
}

/// Multi-part operation stages for upload/download. Where there is no
/// stage, an `Option<MultiPartOperation>` is `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MultiPartOperation {
    /// Initialize a multi-part transfer
    Init,
    /// Upload/download a single part
    Part,
    /// Complete the multi-part transfer
    Complete,
    /// Cancel the multi-part transfer
    Cancel,
}

impl MultiPartOperation {
    /// Returns the string value of the operation for API calls.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::Part => "upload_part",
            Self::Complete => "complete",
            Self::Cancel => "cancel",
        }
    }
}

impl fmt::Display for MultiPartOperation {
    /// The stage's name in capitals, such as `UPLOAD_PART`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Init => "INIT",
            Self::Part => "UPLOAD_PART",
            Self::Complete => "COMPLETE",
            Self::Cancel => "CANCEL",
        })
    }
}

/// Information about a multi-part upload operation.
#[derive(Debug, Clone, Default)]
pub struct MultiPartUploadInfo {
    /// UUID identifying this upload job
    pub uuid: String,
    /// Name of the file being uploaded
    pub file_name: String,
    /// Current operation being performed
    pub part_operation: Option<MultiPartOperation>,
    /// Current part number (1-indexed)
    pub upload_part_number: i64,
    /// Total number of parts in this upload
    pub total_parts: i64,
}

impl fmt::Display for MultiPartUploadInfo {
    /// `MultiPartUploadInfo{uuid='..', part_operation=.., upload_part_number=..,
    /// total_parts=.., file_name='..'}`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MultiPartUploadInfo{{uuid='{}', part_operation={}, upload_part_number={}, total_parts={}, file_name='{}'}}",
            self.uuid,
            or_none(&self.part_operation),
            self.upload_part_number,
            self.total_parts,
            self.file_name
        )
    }
}

/// Information about a part of a multi-part download.
#[derive(Debug, Clone, Default)]
pub struct MultiPartDownloadInfo {
    /// Name of the file being downloaded
    pub file_name: String,
    /// Offset in the file of this part
    pub read_offset: i64,
    /// Length of this part
    pub read_length: i64,
    /// Number of this part, counting from 0
    pub download_part_number: i64,
    /// Total number of parts in this download
    pub total_parts: i64,
}

/// Information about a KiFS file.
#[derive(Debug, Clone, Default)]
pub struct KifsFileInfo {
    /// Size of the file in bytes
    pub file_size: i64,
    /// Full KiFS file name (including path)
    pub file_name: String,
    /// Username of the user who created the file
    pub created_by: String,
    /// Unix timestamp of when the file was created
    pub creation_time: i64,
    /// Additional information about the file
    pub info: HashMap<String, String>,
}

impl fmt::Display for KifsFileInfo {
    /// `KifsFileInfo{file_size=.., file_name='..', created_by='..',
    /// creation_time=.., info={..}}`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KifsFileInfo{{file_size={}, file_name='{}', created_by='{}', creation_time={}, info={}}}",
            self.file_size,
            self.file_name,
            self.created_by,
            self.creation_time,
            sorted_map(&self.info)
        )
    }
}

/// Information about a KiFS directory.
#[derive(Debug, Clone, Default)]
pub struct KifsDirectoryInfo {
    /// Full KiFS path of the directory
    pub kifs_path: String,
    /// Username of the user who created the directory
    pub created_by: String,
    /// Unix timestamp of when the directory was created
    pub creation_time: i64,
    /// Permission string for the directory
    pub permission: String,
    /// Additional information about the directory
    pub info: HashMap<String, String>,
}

impl fmt::Display for KifsDirectoryInfo {
    /// `KifsDirectoryInfo{kifs_path='..', created_by='..', creation_time=..,
    /// permission='..', info={..}}`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KifsDirectoryInfo{{kifs_path='{}', created_by='{}', creation_time={}, permission='{}', info={}}}",
            self.kifs_path,
            self.created_by,
            self.creation_time,
            self.permission,
            sorted_map(&self.info)
        )
    }
}

/// A value's text, or `None`.
fn or_none(value: &Option<impl fmt::Display>) -> String {
    value.as_ref().map_or_else(|| "None".to_string(), ToString::to_string)
}

/// A text quoted, or `None`.
fn quoted_or_none(value: &Option<String>) -> String {
    value.as_ref().map_or_else(|| "None".to_string(), |v| format!("'{v}'"))
}

/// A map as `{k=v, k2=v2}`, its keys sorted so that the text is the same
/// every time.
fn sorted_map(map: &HashMap<String, String>) -> String {
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort();
    let entries: Vec<String> = entries.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{{{}}}", entries.join(", "))
}

/// Result of an upload or download operation.
///
/// This struct encapsulates information about the outcome of a file transfer,
/// including success/failure status, error information, and multi-part details.
///
/// Results compare and hash by `successful`, `multi_part`, `file_name`,
/// `error_message` and `op_mode`.
#[derive(Debug, Clone, Default)]
pub struct OperationResult {
    /// Name of the file uploaded/downloaded
    pub file_name: String,
    /// Names of all files in case of full file upload
    pub full_file_names: Vec<String>,
    /// Whether the upload/download was successful
    pub successful: bool,
    /// The error the operation failed with, where there is one: the
    /// server's or the multi-part job's, or the local file's
    pub error: Option<KineticaError>,
    /// Error message if the operation failed
    pub error_message: Option<String>,
    /// Indicates whether it is an upload or download
    pub op_mode: Option<OpMode>,
    /// Whether this result is for a multi-part operation
    pub multi_part: bool,
    /// Information about a multi-part upload (if applicable)
    pub upload_info: Option<MultiPartUploadInfo>,
    /// Information about a multi-part download (if applicable)
    pub download_info: Option<MultiPartDownloadInfo>,
}

impl PartialEq for OperationResult {
    fn eq(&self, other: &Self) -> bool {
        self.successful == other.successful
            && self.multi_part == other.multi_part
            && self.file_name == other.file_name
            && self.error_message == other.error_message
            && self.op_mode == other.op_mode
    }
}

impl Eq for OperationResult {}

impl std::hash::Hash for OperationResult {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (&self.file_name, self.successful, &self.error_message, self.op_mode, self.multi_part).hash(state);
    }
}

impl OperationResult {
    /// The result, with the error it failed with.
    pub fn with_error(mut self, error: KineticaError) -> Self {
        self.error = Some(error);
        self
    }

    /// Orders results by their download part number, for `sort_by`. A
    /// result that is not a download part comes first.
    pub fn compare_by_download_part_number(&self, other: &Self) -> std::cmp::Ordering {
        let part = |r: &Self| r.download_info.as_ref().map(|info| info.download_part_number);
        part(self).cmp(&part(other))
    }

    /// Creates a new successful operation result.
    pub fn success(file_name: impl Into<String>, op_mode: OpMode) -> Self {
        Self {
            file_name: file_name.into(),
            successful: true,
            op_mode: Some(op_mode),
            ..Default::default()
        }
    }

    /// Creates a new failed operation result.
    pub fn failure(file_name: impl Into<String>, op_mode: OpMode, error: impl Into<String>) -> Self {
        Self {
            file_name: file_name.into(),
            successful: false,
            error_message: Some(error.into()),
            op_mode: Some(op_mode),
            ..Default::default()
        }
    }

    /// Creates a new successful multi-part upload result.
    pub fn multipart_upload_success(
        file_name: impl Into<String>,
        upload_info: MultiPartUploadInfo,
    ) -> Self {
        Self {
            file_name: file_name.into(),
            successful: true,
            op_mode: Some(OpMode::Upload),
            multi_part: true,
            upload_info: Some(upload_info),
            ..Default::default()
        }
    }

    /// Creates a new successful multi-part download result.
    pub fn multipart_download_success(
        file_name: impl Into<String>,
        download_info: MultiPartDownloadInfo,
    ) -> Self {
        Self {
            file_name: file_name.into(),
            successful: true,
            op_mode: Some(OpMode::Download),
            multi_part: true,
            download_info: Some(download_info),
            ..Default::default()
        }
    }
}

impl fmt::Display for OperationResult {
    /// `OperationResult{file_name='..', successful=.., error=..,
    /// error_message=.., op_mode=.., multi_part=.., upload_info=..}`, with
    /// `None` for what is absent.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OperationResult{{file_name='{}', successful={}, error={}, error_message={}, op_mode={}, \
             multi_part={}, upload_info={}}}",
            self.file_name,
            self.successful,
            or_none(&self.error),
            quoted_or_none(&self.error_message),
            or_none(&self.op_mode),
            self.multi_part,
            or_none(&self.upload_info)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_and_infos_print_their_fields() {
        let upload = MultiPartUploadInfo {
            uuid: "u-1".into(),
            file_name: "kdir/big.bin".into(),
            part_operation: Some(MultiPartOperation::Part),
            upload_part_number: 2,
            total_parts: 3,
        };
        assert_eq!(
            upload.to_string(),
            "MultiPartUploadInfo{uuid='u-1', part_operation=UPLOAD_PART, upload_part_number=2, total_parts=3, \
             file_name='kdir/big.bin'}"
        );
        assert!(MultiPartUploadInfo::default().to_string().contains("part_operation=None"));

        let ok = OperationResult::multipart_upload_success("/d/big.bin", upload);
        assert_eq!(
            ok.to_string(),
            "OperationResult{file_name='/d/big.bin', successful=true, error=None, error_message=None, \
             op_mode=UPLOAD, multi_part=true, upload_info=MultiPartUploadInfo{uuid='u-1', part_operation=UPLOAD_PART, \
             upload_part_number=2, total_parts=3, file_name='kdir/big.bin'}}"
        );
        let batch = OperationResult { error_message: Some("boom".into()), ..Default::default() }
            .with_error(KineticaError::Api("boom".into()));
        assert_eq!(
            batch.to_string(),
            "OperationResult{file_name='', successful=false, error=API error: boom, error_message='boom', \
             op_mode=None, multi_part=false, upload_info=None}"
        );

        let file = KifsFileInfo {
            file_size: 12,
            file_name: "d/a.csv".into(),
            created_by: "u".into(),
            creation_time: 7,
            info: HashMap::from([("b".into(), "2".into()), ("a".into(), "1".into())]),
        };
        assert_eq!(file.to_string(), "KifsFileInfo{file_size=12, file_name='d/a.csv', created_by='u', creation_time=7, info={a=1, b=2}}");
        let dir = KifsDirectoryInfo { kifs_path: "d".into(), created_by: "u".into(), creation_time: 7, permission: "write".into(), ..Default::default() };
        assert_eq!(dir.to_string(), "KifsDirectoryInfo{kifs_path='d', created_by='u', creation_time=7, permission='write', info={}}");
    }

    // Results compare and hash by success, multi-part, file name, error
    // message and mode, whatever the error and the part details.
    #[test]
    fn results_compare_by_outcome_name_and_mode() {
        use std::collections::HashSet;
        let failed = |e: &str| OperationResult::failure("f", OpMode::Download, "bad").with_error(KineticaError::Api(e.into()));
        assert_eq!(failed("one"), failed("two"));
        assert_eq!(HashSet::from([failed("one"), failed("two")]).len(), 1);
        assert_ne!(failed("one"), OperationResult::failure("f", OpMode::Upload, "bad"));
        assert_ne!(failed("one"), OperationResult::failure("g", OpMode::Download, "bad"));

        let part = |n| OperationResult::multipart_download_success("f", MultiPartDownloadInfo { download_part_number: n, ..Default::default() });
        let mut parts = [part(2), part(0), part(1)];
        parts.sort_by(OperationResult::compare_by_download_part_number);
        let numbers: Vec<i64> = parts.iter().map(|r| r.download_info.as_ref().unwrap().download_part_number).collect();
        assert_eq!(numbers, [0, 1, 2]);
    }
}

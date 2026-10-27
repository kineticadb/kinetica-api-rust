//! KiFS File System API demonstration.
//!
//! This example demonstrates the KiFS file handling capabilities:
//! - Creating directories
//! - Uploading files
//! - Listing files and directories
//! - Downloading files
//! - Deleting files and directories
//!
//! Run with: cargo run -p gpudb-examples --bin kifs_demo

use gpudb_api::filesystem::{
    KifsFileHandler,
    FnUploadListener, FnDownloadListener,
};
use gpudb_api::Kinetica;
use gpudb_examples::connection_config;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")
    ).init();

    println!("=== KiFS File System API Demo ===\n");

    // Get connection config from environment
    let (urls, options) = connection_config();

    // Connect to Kinetica
    println!("Connecting to Kinetica...");
    let kinetica = Arc::new(Kinetica::new(urls, options).await?);
    println!("Connected!\n");

    // Create file handler
    let handler = KifsFileHandler::new(kinetica.clone(), None);

    // Test directory name
    let test_dir = "rust_kifs_demo_test";

    // 1. Create a directory
    println!("1. Creating directory '{test_dir}'...");
    handler.create_directory(test_dir, true).await?;
    println!("   Directory created!\n");

    // 2. Check if directory exists
    println!("2. Checking if directory exists...");
    let exists = handler.kifs_directory_exists(test_dir).await;
    println!("   Directory '{test_dir}' exists: {exists}\n");

    // 3. Show all directories
    println!("3. Listing all KiFS directories...");
    let dirs = handler.show_all_directories().await?;
    println!("   Found {} directories:", dirs.len());
    for dir in &dirs {
        println!("   - {} (created by: {})", dir.kifs_path, dir.created_by);
    }
    println!();

    // 4. Create a test file locally and upload it
    println!("4. Creating and uploading a test file...");
    let test_content = "Hello from Rust KiFS API!\nThis is a test file.\n";
    let local_test_file = "/tmp/kifs_rust_test.txt";
    std::fs::write(local_test_file, test_content)?;
    println!("   Created local file: {local_test_file}");

    // Upload with progress callback
    let upload_listener = FnUploadListener::new(
        |_results| println!("   Multi-part upload complete!"),
        |result| {
            if let Some(info) = &result.upload_info {
                println!("   Uploaded part {} of {}", info.upload_part_number, info.total_parts);
            }
        },
        |result| {
            if result.successful {
                println!("   Full file upload complete: {:?}", result.full_file_names);
            } else {
                println!("   Upload error: {:?}", result.error_message);
            }
        },
    );

    handler.upload(local_test_file, test_dir, None, Some(&upload_listener)).await?;
    println!("   File uploaded!\n");

    // 5. List files in the directory
    println!("5. Listing files in '{test_dir}'...");
    let files = handler.show_files(vec![test_dir.to_string()]).await?;
    println!("   Found {} files:", files.len());
    for file in &files {
        println!("   - {} ({} bytes, created by: {})",
            file.file_name, file.file_size, file.created_by);
    }
    println!();

    // 6. Download the file
    println!("6. Downloading files...");
    let download_dir = "/tmp/kifs_download_test";
    std::fs::create_dir_all(download_dir)?;

    let download_listener = FnDownloadListener::new(
        |_results| println!("   Multi-part download complete!"),
        |result| {
            if let Some(info) = &result.download_info {
                // Download parts are numbered from 0
                println!("   Downloaded part {} of {}", info.download_part_number + 1, info.total_parts);
            }
        },
        |file_names| println!("   Full file download complete: {} files", file_names.len()),
    );

    handler.download_dir(test_dir, download_dir, None, Some(&download_listener)).await?;

    // Verify downloaded content
    let downloaded_file = format!("{download_dir}/kifs_rust_test.txt");
    if std::path::Path::new(&downloaded_file).exists() {
        let downloaded_content = std::fs::read_to_string(&downloaded_file)?;
        println!("   Downloaded file content matches: {}", downloaded_content == test_content);
    }
    println!();

    // 7. Delete files in the directory
    println!("7. Deleting files in '{test_dir}'...");
    handler.delete_files_in_dir(test_dir, true).await?;
    println!("   Files deleted!\n");

    // 8. Delete the directory
    println!("8. Deleting directory '{test_dir}'...");
    handler.delete_directory(test_dir, true, true).await?;
    println!("   Directory deleted!\n");

    // 9. Verify directory is gone
    println!("9. Verifying directory is deleted...");
    let still_exists = handler.kifs_directory_exists(test_dir).await;
    println!("   Directory '{test_dir}' exists: {still_exists}\n");

    // Cleanup local files
    std::fs::remove_file(local_test_file).ok();
    std::fs::remove_dir_all(download_dir).ok();

    println!("=== Demo Complete ===");
    Ok(())
}

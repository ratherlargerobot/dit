use crate::fsutil;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// queue sizes
// the queues that are small refer to files that might be in the OS page cache
pub const CHANNEL_SIZE_TRANSFER_REQUEST: usize = 10_000;
pub const CHANNEL_SIZE_HASH_REQUEST: usize = 10_000;
pub const CHANNEL_SIZE_HASH_RESPONSE: usize = 3;
pub const CHANNEL_SIZE_COPY_TO_DEST_REQUEST: usize = 10_000;
pub const CHANNEL_SIZE_COPY_FILE_REQUEST: usize = 3;

/**
 * Represents a dit error.
 */
#[derive(Debug)]
pub struct DitError {
    pub error: String,
}

impl DitError {
    fn new(s: &str) -> DitError {
        DitError {
            error: String::from(s),
        }
    }
}

impl Error for DitError {}

impl Display for DitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

/**
 * Convenience error function that matches the return type of a lot of other internal functions.
 */
pub fn dit_error<T>(s: &str) -> Result<T, Box<dyn Error>> {
    Err(Box::new(DitError::new(s)))
}

/**
 * Represents the paths to read and write.
 */
pub struct ReadWritePaths {
    pub read_paths: Vec<PathBuf>,
    pub write_paths: Vec<PathBuf>,
}

/**
 * Represents the result of a merge or copy operation.
 */
#[derive(PartialEq)]
pub enum MergeResult {
    Ok,
    Conflict,
    Error,
}

/**
 * Represents the current state of a running thread.
 */
#[derive(Clone)]
pub struct ThreadRunContext {
    // should the thread be running?
    running: Arc<AtomicBool>,

    // once the thread stops running, should the thread finish any pending work before it stops?
    clean: Arc<AtomicBool>,
}

impl ThreadRunContext {
    pub fn new() -> ThreadRunContext {
        ThreadRunContext {
            running: Arc::new(AtomicBool::new(true)),
            clean: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn from(parent: &ThreadRunContext) -> ThreadRunContext {
        let clean = parent.clean.clone();

        ThreadRunContext {
            running: Arc::new(AtomicBool::new(true)),
            clean,
        }
    }

    pub fn is_running(&self) -> bool {
        let atomic_boolean = self.running.clone();
        return atomic_boolean.load(Ordering::Relaxed);
    }

    pub fn is_clean(&self) -> bool {
        let atomic_boolean = self.clean.clone();
        return atomic_boolean.load(Ordering::Relaxed);
    }

    pub fn shutdown(&self) {
        let atomic_boolean = self.running.clone();
        atomic_boolean.store(false, Ordering::Relaxed);
    }

    pub fn unclean_shutdown(&self) {
        let atomic_boolean = self.clean.clone();
        atomic_boolean.store(false, Ordering::Relaxed);

        let atomic_boolean = self.running.clone();
        atomic_boolean.store(false, Ordering::Relaxed);
    }
}

/**
 * Are all of the read and write paths valid, and actually exist on the filesystem?
 *
 * Read paths are verified, to make sure they exist and are directories.
 *
 * Write paths are verified, to make sure they exist and are directories. If write paths do not
 * exist, this function attempts to non-recursively create them.
 *
 * If anything goes wrong, an Error is returned.
 */
pub fn ensure_valid_read_write_paths(
    read_write_paths: &ReadWritePaths,
) -> Result<(), Box<dyn Error>> {
    // make sure all read paths exists, and are valid directories
    for read_path in &read_write_paths.read_paths {
        // special case: disallow root path
        if "/".eq(read_path.to_str().unwrap()) {
            return dit_error("can not use '/' as read path");
        }

        // does read path exist?
        if !read_path.exists() {
            let err_msg = format!(
                "read path does not exist: '{}'",
                read_path.to_str().unwrap()
            );
            return dit_error(&err_msg);
        }

        // is read path a directory?
        if !read_path.is_dir() {
            let err_msg = format!(
                "read path is not a directory: '{}'",
                read_path.to_str().unwrap()
            );
            return dit_error(&err_msg);
        }
    }

    // make sure all write paths exist, creating them (non-recursively) if necessary
    for write_path in &read_write_paths.write_paths {
        // special case: disallow root path
        if "/".eq(write_path.to_str().unwrap()) {
            return dit_error("can not use '/' as write path");
        }

        if write_path.exists() {
            // if the write path exists, make sure it is a directory
            if !write_path.is_dir() {
                let err_msg = format!(
                    "write path exists, but is not a directory: '{}'",
                    write_path.to_str().unwrap()
                );
                return dit_error(&err_msg);
            }
        } else {
            // if the write path doesn't exist, create it
            match fsutil::mkdir(write_path) {
                Ok(_) => {}
                Err(e) => {
                    return dit_error(&e.to_string());
                }
            }
        }
    }

    Ok(())
}

/**
 * Does the given set of source path and write paths have a write merge conflict?
 *
 * Checks filesystem metadata on the source path, and all existing destination paths, to see if
 * the files are all the same size.
 */
pub fn has_write_merge_conflict(
    write_paths: &Vec<String>,
    src_path: &Path,
    sub_path: &str,
) -> bool {
    match src_path.metadata() {
        Ok(src_metadata) => {
            for write_path in write_paths {
                let mut dest_path = PathBuf::from(write_path);
                dest_path.push(sub_path);

                if dest_path.exists() {
                    match dest_path.metadata() {
                        Ok(dest_metadata) => {
                            if src_metadata.len() != dest_metadata.size() {
                                // file sizes differ between src and dest,
                                // it's a write merge conflict
                                return true;
                            }
                        }
                        Err(_) => {
                            // if the dest path exists, but we can't read its metadata,
                            // assume the worst
                            return true;
                        }
                    }
                }
            }
        }
        Err(_) => {
            // if we can't read the source path metadata, assume the worst
            return true;
        }
    }

    // no write merge conflict
    false
}

/**
 * Does the file metadata for all source and destination paths match closely enough that we can
 * skip copying the files?
 */
pub fn all_files_match(
    read_paths: &Vec<&str>,
    write_paths: &Vec<&str>,
    sub_path_plus_dirent: &str,
) -> bool {
    let mut found_read_file = false;
    let mut file_size = 0;

    for read_path in read_paths {
        let mut path_buf = PathBuf::from(read_path);
        path_buf.push(sub_path_plus_dirent);

        if path_buf.exists() {
            match path_buf.metadata() {
                Ok(metadata) => {
                    if !found_read_file {
                        // set the comparison file size to the first file size we see
                        file_size = metadata.size();
                        found_read_file = true;
                    } else if metadata.size() != file_size {
                        // read file sizes differ
                        return false;
                    }
                }
                Err(_) => {
                    return false;
                }
            }
        }
    }

    // no read files, something is wrong
    if !found_read_file {
        panic!();
    }

    for write_path in write_paths {
        let mut path_buf = PathBuf::from(write_path);
        path_buf.push(sub_path_plus_dirent);

        // file must exist in all destination paths to be considered a match
        if !path_buf.exists() {
            return false;
        }

        match path_buf.metadata() {
            Ok(metadata) => {
                if metadata.size() != file_size {
                    // this write file is not the same size as the read files
                    return false;
                }
            }
            Err(_) => {
                return false;
            }
        }
    }

    // we have at least one read file, and all write files, and they're all the same size
    true
}

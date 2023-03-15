use std::path::PathBuf;

/**
 * A request that simply indicates what the next type of transfer should be.
 */
#[derive(PartialEq)]
pub enum TransferRequest {
    Copy,
    Merge,
}

/**
 * Request to hash the contents of the given source file.
 */
pub struct HashRequest {
    pub sub_path: String,
    pub src_path: PathBuf,
}

/**
 * The result of hashing the contents of a source file.
 */
pub struct HashResult {
    pub sub_path: String,
    pub src_path: PathBuf,
    pub hash: String,
}

/**
 * Copy a single source file to its corresponding location in all of the destination directories.
 */
pub struct CopyToDestRequest {
    pub sub_path: String,
    pub src_path: PathBuf,
}

/**
 * Request to copy a single source file to a single destination file.
 */
pub struct CopyFileRequest {
    pub src_path: PathBuf,
    pub dest_path: PathBuf,
}

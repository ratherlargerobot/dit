use crate::common::ThreadRunContext;
use crate::message::{
    CopyFileRequest, CopyToDestRequest, HashRequest, HashResult, TransferRequest,
};
use crate::{common, discover, fsutil, MergeResult};
use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender};
use std::time::Duration;

const RECV_TIMEOUT: Duration = Duration::from_millis(100);

//////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS                                                         //
//////////////////////////////////////////////////////////////////////////////

/**
 * Discover thread.
 */
pub fn discover(
    thread_run_ctx: ThreadRunContext,
    xfer_req_tx: &SyncSender<TransferRequest>,
    copy_to_dest_req_tx: &SyncSender<CopyToDestRequest>,
    hash_req_channels_tx: Vec<SyncSender<Option<HashRequest>>>,
    read_paths: Vec<String>,
    write_paths: Vec<String>,
) {
    // create vec of *references* to hash request tx channels
    let mut hash_req_tx_vec = vec![];
    for i in 0..hash_req_channels_tx.len() {
        hash_req_tx_vec.push(hash_req_channels_tx.get(i).unwrap());
    }

    let mut read_paths_str = vec![];
    for read_path in &read_paths {
        read_paths_str.push(read_path.as_str());
    }

    let mut write_paths_str = vec![];
    for write_path in &write_paths {
        write_paths_str.push(write_path.as_str());
    }

    // discover files
    let _ = discover::discover_files(
        &thread_run_ctx,
        &xfer_req_tx,
        &copy_to_dest_req_tx,
        &hash_req_tx_vec,
        &read_paths_str,
        &write_paths_str,
    );
}

/**
 * Hash thread.
 */
pub fn hash(
    thread_run_ctx: ThreadRunContext,
    log_warn: fn(&str),
    hash_req_rx: Receiver<Option<HashRequest>>,
    hash_res_tx: SyncSender<Option<HashResult>>,
) {
    while thread_run_ctx.is_running() {
        match hash_req_rx.recv_timeout(RECV_TIMEOUT) {
            Ok(option_hash_req) => {
                if thread_run_ctx.is_clean() {
                    let option_hash_res =
                        handle_hash_req(&thread_run_ctx, log_warn, option_hash_req);
                    match hash_res_tx.send(option_hash_res) {
                        Ok(_) => {}
                        Err(e) => {
                            let err = format!("error sending to hash request queue: {}", e);
                            log_warn(&err);
                            thread_run_ctx.unclean_shutdown();
                        }
                    }
                }
            }
            Err(_) => {
                // timeout, ignore
            }
        }
    }

    // if we stopped early because of an error, don't drain the queue
    if !thread_run_ctx.is_clean() {
        return;
    }

    loop {
        match hash_req_rx.recv_timeout(RECV_TIMEOUT) {
            Ok(option_hash_req) => {
                if thread_run_ctx.is_clean() {
                    let option_hash_res =
                        handle_hash_req(&thread_run_ctx, log_warn, option_hash_req);
                    match hash_res_tx.send(option_hash_res) {
                        Ok(_) => {}
                        Err(e) => {
                            let err = format!("error sending to hash request queue: {}", e);
                            log_warn(&err);
                            thread_run_ctx.unclean_shutdown();
                        }
                    }
                }
            }
            Err(_) => {
                // timeout, queue is empty
                break;
            }
        }
    }
}

/**
 * Merge thread.
 */
pub fn merge(
    thread_run_ctx: ThreadRunContext,
    log_info: fn(&str),
    log_warn: fn(&str),
    write_paths: Vec<String>,
    xfer_req_rx: Receiver<TransferRequest>,
    hash_res_channels_rx: Vec<Receiver<Option<HashResult>>>,
    copy_to_dest_rx: Receiver<CopyToDestRequest>,
    copy_file_req_channels_tx: Vec<SyncSender<CopyFileRequest>>,
) -> MergeResult {
    let mut merge_result = MergeResult::Ok;

    while thread_run_ctx.is_running() {
        match xfer_req_rx.recv_timeout(RECV_TIMEOUT) {
            Ok(xfer_req) => {
                let cur_result = handle_xfer_req(
                    &thread_run_ctx,
                    log_info,
                    log_warn,
                    &write_paths,
                    &hash_res_channels_rx,
                    &copy_to_dest_rx,
                    &copy_file_req_channels_tx,
                    xfer_req,
                );
                merge_result = max_merge_result(&merge_result, &cur_result);
            }
            Err(_) => {
                // timeout, ignore
            }
        }
    }

    // if we stopped early because of an error, don't drain the queue
    if !thread_run_ctx.is_clean() {
        return merge_result;
    }

    loop {
        match xfer_req_rx.recv_timeout(RECV_TIMEOUT) {
            Ok(xfer_req) => {
                let cur_result = handle_xfer_req(
                    &thread_run_ctx,
                    log_info,
                    log_warn,
                    &write_paths,
                    &hash_res_channels_rx,
                    &copy_to_dest_rx,
                    &copy_file_req_channels_tx,
                    xfer_req,
                );
                merge_result = max_merge_result(&merge_result, &cur_result);
            }
            Err(_) => {
                // timeout, queue is empty
                break;
            }
        }
    }

    merge_result
}

/**
 * Copy thread.
 */
pub fn copy(
    thread_run_ctx: ThreadRunContext,
    log_warn: fn(&str),
    copy_file_req_rx: Receiver<CopyFileRequest>,
) {
    while thread_run_ctx.is_running() {
        match copy_file_req_rx.recv_timeout(RECV_TIMEOUT) {
            Ok(copy_file_req) => {
                handle_copy(&thread_run_ctx, log_warn, copy_file_req);
            }
            Err(_) => {
                // timeout, ignore
            }
        }
    }

    // if we stopped early because of an error, don't drain the queue
    if !thread_run_ctx.is_clean() {
        return;
    }

    loop {
        match copy_file_req_rx.recv_timeout(RECV_TIMEOUT) {
            Ok(copy_file_req) => {
                handle_copy(&thread_run_ctx, log_warn, copy_file_req);
            }
            Err(_) => {
                // timeout, queue is empty
                break;
            }
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS                                                        //
//////////////////////////////////////////////////////////////////////////////

/**
 * Handle a TransferRequest for the merge thread.
 */
fn handle_xfer_req(
    thread_run_ctx: &ThreadRunContext,
    log_info: fn(&str),
    log_warn: fn(&str),
    write_paths: &Vec<String>,
    hash_res_channels_rx: &Vec<Receiver<Option<HashResult>>>,
    copy_to_dest_rx: &Receiver<CopyToDestRequest>,
    copy_file_req_channels_tx: &Vec<SyncSender<CopyFileRequest>>,
    xfer_req: TransferRequest,
) -> MergeResult {
    match xfer_req {
        TransferRequest::Copy => handle_copy_to_dest(
            &thread_run_ctx,
            log_info,
            log_warn,
            &write_paths,
            &copy_to_dest_rx,
            &copy_file_req_channels_tx,
        ),
        TransferRequest::Merge => handle_hash_merge(
            &thread_run_ctx,
            log_info,
            log_warn,
            &write_paths,
            &hash_res_channels_rx,
            &copy_file_req_channels_tx,
        ),
    }
}

/**
 * Handle a HashRequest for a hash thread.
 */
fn handle_hash_req(
    thread_run_ctx: &ThreadRunContext,
    log_warn: fn(&str),
    option_hash_req: Option<HashRequest>,
) -> Option<HashResult> {
    match option_hash_req {
        Some(hash_req) => match crate::fsutil::hash_file(hash_req.src_path.as_path()) {
            Ok(hash) => {
                let sub_path = hash_req.sub_path;
                let src_path = hash_req.src_path;
                let hash_result = HashResult {
                    sub_path,
                    src_path,
                    hash,
                };

                return Some(hash_result);
            }
            Err(_) => {
                let err = format!(
                    "error hashing file: {}",
                    hash_req.src_path.to_str().unwrap()
                );
                log_warn(&err);
                thread_run_ctx.unclean_shutdown();
            }
        },
        None => {}
    }

    None
}

/**
 * Get the full merge conflict destination file path for the given file and write path.
 */
fn get_merge_conflict_dest_file_path(
    write_path: &str,
    src_path: &Path,
    sub_path: &str,
    hash: Option<&str>,
    conflict_type: &str,
) -> String {
    // construct merge conflict filename to write (without a path)
    // <file_stem>.__<conflict_type>__<hash>.<extension>
    let mut file_name = String::new();

    let path_parent_path_buf = PathBuf::from(sub_path);

    /*
    Returns the Path without its final component, if there is one.
    Returns None if the path terminates in a root or prefix.
     */
    let path_parent = path_parent_path_buf.parent();

    /*
    None, if there is no file name;
    The entire file name if there is no embedded .;
    The entire file name if the file name begins with . and has no other .s within;
    Otherwise, the portion of the file name before the final .
     */
    let file_stem = src_path.file_stem();

    /*
    None, if there is no file name;
    None, if there is no embedded .;
    None, if the file name begins with . and has no other .s within;
    Otherwise, the portion of the file name after the final .
     */
    let extension = src_path.extension();

    if file_stem.is_some() {
        file_name.push_str(file_stem.unwrap().to_str().unwrap());
        file_name.push('.');
    }

    file_name.push_str("__");
    file_name.push_str(&conflict_type);
    file_name.push_str("__");

    match hash {
        Some(hash) => {
            file_name.push_str(hash);
        }
        None => match fsutil::hash_file(src_path) {
            Ok(hash) => {
                file_name.push_str(&hash);
            }
            Err(_) => {
                // we'll still have a merge conflict filename, but without a hash
            }
        },
    }

    if extension.is_some() {
        file_name.push('.');
        file_name.push_str(extension.unwrap().to_str().unwrap());
    }

    // construct full destination path to write
    let mut dest_path = PathBuf::from(write_path);
    if path_parent.is_some() {
        dest_path.push(path_parent.unwrap());
    }
    dest_path.push(file_name);

    String::from(dest_path.to_str().unwrap())
}

/**
 * Handle a hashed file merge for the merge thread.
 */
fn handle_hash_merge(
    thread_run_ctx: &ThreadRunContext,
    log_info: fn(&str),
    log_warn: fn(&str),
    write_paths: &Vec<String>,
    hash_res_channels_rx: &Vec<Receiver<Option<HashResult>>>,
    copy_file_req_channels_tx: &Vec<SyncSender<CopyFileRequest>>,
) -> MergeResult {
    // map of hash -> HashResult
    let mut map = BTreeMap::new();

    // build a map of each unique copy of this file sub path
    for hash_res_rx in hash_res_channels_rx {
        match hash_res_rx.recv() {
            Ok(option_hash_res) => match option_hash_res {
                Some(hash_res) => {
                    if !map.contains_key(&hash_res.hash) {
                        map.insert(String::from(&hash_res.hash), hash_res);
                    }
                }
                None => {}
            },
            Err(e) => {
                if thread_run_ctx.is_clean() {
                    let err = format!("error reading from hash result queue: {}", e);
                    log_warn(&err);
                    thread_run_ctx.unclean_shutdown();
                }
                return MergeResult::Error;
            }
        }
    }

    if 0 == map.len() {
        if thread_run_ctx.is_clean() {
            let err = format!("error reading from hash result queue (0 records from all queues)");
            log_warn(&err);
            thread_run_ctx.unclean_shutdown();
        }
        return MergeResult::Error;
    } else if 1 == map.len() {
        // common case: everything matched the same hash value, just copy one of the files

        // this outer loop should only execute once, because map.len() == 1
        for (_, hash_res) in &map {
            let msg = format!("{}", &hash_res.sub_path);
            log_info(&msg);

            let has_write_merge_conflict = common::has_write_merge_conflict(
                write_paths,
                &hash_res.src_path,
                &hash_res.sub_path,
            );

            let mut i = 0;
            for write_path in write_paths {
                // get copy file request tx handle
                let copy_file_req_tx = copy_file_req_channels_tx.get(i).unwrap();

                if has_write_merge_conflict {
                    // special case: write merge conflict
                    let dest_path_str = get_merge_conflict_dest_file_path(
                        write_path,
                        &hash_res.src_path,
                        &hash_res.sub_path,
                        Some(&hash_res.hash),
                        "WRITE_MERGE_CONFLICT",
                    );

                    let err = format!(
                        "{} -> {}",
                        &hash_res.src_path.to_str().unwrap(),
                        dest_path_str
                    );
                    log_warn(&err);

                    let copy_file_req = CopyFileRequest {
                        src_path: PathBuf::from(&hash_res.src_path),
                        dest_path: PathBuf::from(dest_path_str),
                    };

                    if thread_run_ctx.is_clean() {
                        match copy_file_req_tx.send(copy_file_req) {
                            Ok(_) => {}
                            Err(_) => {
                                if thread_run_ctx.is_clean() {
                                    let err = format!("error writing copy file request");
                                    log_warn(&err);
                                    thread_run_ctx.unclean_shutdown();
                                }
                                return MergeResult::Error;
                            }
                        }
                    }
                } else {
                    // common case: no write merge conflict
                    let mut dest_path_buf = PathBuf::from(write_path);
                    dest_path_buf.push(&hash_res.sub_path);

                    let copy_file_req = CopyFileRequest {
                        src_path: PathBuf::from(&hash_res.src_path),
                        dest_path: dest_path_buf,
                    };

                    if thread_run_ctx.is_clean() {
                        match copy_file_req_tx.send(copy_file_req) {
                            Ok(_) => {}
                            Err(_) => {
                                if thread_run_ctx.is_clean() {
                                    let err = format!("error writing copy file request");
                                    log_warn(&err);
                                    thread_run_ctx.unclean_shutdown();
                                }
                                return MergeResult::Error;
                            }
                        }
                    }
                }

                i += 1;
            }

            if has_write_merge_conflict {
                return MergeResult::Conflict;
            }
        }

        return MergeResult::Ok;
    } else {
        // merge conflict: source files have different contents, copy each one with different names

        // print out the sub path
        let mut hash_count = 0;
        for (_, hash_res) in &map {
            if 0 == hash_count {
                let msg = format!("{}", &hash_res.sub_path);
                log_info(&msg);
            }

            hash_count += 1;
        }

        let mut i = 0;
        for write_path in write_paths {
            let copy_file_req_tx = copy_file_req_channels_tx.get(i).unwrap();

            for (_, hash_res) in &map {
                // get read merge conflict destination file path
                let dest_path = get_merge_conflict_dest_file_path(
                    write_path,
                    &hash_res.src_path,
                    &hash_res.sub_path,
                    Some(&hash_res.hash),
                    "READ_MERGE_CONFLICT",
                );

                let err = format!("{} -> {}", &hash_res.src_path.to_str().unwrap(), dest_path);
                log_warn(&err);

                // send copy file request for one specific write destination
                // e.g. /path/to/disk1/foo.__READ_MERGE_CONFLICT__<hash>.jpg
                let copy_file_req = CopyFileRequest {
                    src_path: PathBuf::from(&hash_res.src_path),
                    dest_path: PathBuf::from(&dest_path),
                };

                match copy_file_req_tx.send(copy_file_req) {
                    Ok(_) => {}
                    Err(_) => {
                        if thread_run_ctx.is_clean() {
                            let err = format!("error writing copy file request");
                            log_warn(&err);
                            thread_run_ctx.unclean_shutdown();
                        }
                        return MergeResult::Error;
                    }
                }
            }

            i += 1;
        }

        return MergeResult::Conflict;
    }
}

/**
 * Handle copying a source file to all destinations for the merge thread.
 */
fn handle_copy_to_dest(
    thread_run_ctx: &ThreadRunContext,
    log_info: fn(&str),
    log_warn: fn(&str),
    write_paths: &Vec<String>,
    copy_to_dest_rx: &Receiver<CopyToDestRequest>,
    copy_file_req_channels_tx: &Vec<SyncSender<CopyFileRequest>>,
) -> MergeResult {
    match copy_to_dest_rx.recv() {
        Ok(copy_to_dest_req) => {
            let msg = format!("{}", &copy_to_dest_req.sub_path);
            log_info(&msg);

            let has_write_merge_conflict = common::has_write_merge_conflict(
                write_paths,
                &copy_to_dest_req.src_path,
                &copy_to_dest_req.sub_path,
            );

            let mut i = 0;
            for write_path in write_paths {
                let copy_file_req_tx = copy_file_req_channels_tx.get(i).unwrap();

                if has_write_merge_conflict {
                    // special case: write merge conflict
                    let dest_path_str = get_merge_conflict_dest_file_path(
                        write_path,
                        &copy_to_dest_req.src_path,
                        &copy_to_dest_req.sub_path,
                        None,
                        "WRITE_MERGE_CONFLICT",
                    );

                    let err = format!(
                        "{} -> {}",
                        &copy_to_dest_req.src_path.to_str().unwrap(),
                        dest_path_str
                    );
                    log_warn(&err);

                    let copy_file_req = CopyFileRequest {
                        src_path: PathBuf::from(&copy_to_dest_req.src_path),
                        dest_path: PathBuf::from(dest_path_str),
                    };

                    if thread_run_ctx.is_clean() {
                        match copy_file_req_tx.send(copy_file_req) {
                            Ok(_) => {}
                            Err(_) => {
                                if thread_run_ctx.is_clean() {
                                    let err = format!("error writing copy file request");
                                    log_warn(&err);
                                    thread_run_ctx.unclean_shutdown();
                                }
                                return MergeResult::Error;
                            }
                        }
                    }
                } else {
                    // common case: no write merge conflict
                    let mut dest_path_buf = PathBuf::from(write_path);
                    dest_path_buf.push(&copy_to_dest_req.sub_path);

                    let copy_file_req = CopyFileRequest {
                        src_path: PathBuf::from(&copy_to_dest_req.src_path),
                        dest_path: dest_path_buf,
                    };

                    if thread_run_ctx.is_clean() {
                        match copy_file_req_tx.send(copy_file_req) {
                            Ok(_) => {}
                            Err(_) => {
                                if thread_run_ctx.is_clean() {
                                    let err = format!("error writing copy file request");
                                    log_warn(&err);
                                    thread_run_ctx.unclean_shutdown();
                                }
                                return MergeResult::Error;
                            }
                        }
                    }
                }

                i += 1;
            }
        }
        Err(e) => {
            if thread_run_ctx.is_clean() {
                let err = format!("{}", e.to_string());
                log_warn(&err);
                thread_run_ctx.unclean_shutdown();
            }
            return MergeResult::Error;
        }
    }

    MergeResult::Ok
}

/**
 * Get the "maximum" merge result.
 *
 * Success is the "lowest" value, a merge conflict is in the middle, and an error is the "highest"
 * value.
 *
 * This lets us ratchet up so we can remember the most serious error we've seen.
 */
fn max_merge_result(a: &MergeResult, b: &MergeResult) -> MergeResult {
    if MergeResult::Error.eq(a) || MergeResult::Error.eq(b) {
        return MergeResult::Error;
    }

    if MergeResult::Conflict.eq(a) || MergeResult::Conflict.eq(b) {
        return MergeResult::Conflict;
    }

    MergeResult::Ok
}

/**
 * Handle a file copy request for a copy thread.
 */
fn handle_copy(
    thread_run_ctx: &ThreadRunContext,
    log_warn: fn(&str),
    copy_file_req: CopyFileRequest,
) {
    // if the destination path already exists, don't copy the file again
    // we are trusting that the destination file is correct, because if it was copied
    // by this program last time, it would have been written atomically
    if copy_file_req.dest_path.exists() {
        return;
    }

    // get the destination directory
    let dest_parent_path = copy_file_req.dest_path.parent();
    if dest_parent_path.is_none() {
        let err = format!(
            "invalid destination path: '{}'",
            copy_file_req.dest_path.to_str().unwrap()
        );
        log_warn(&err);
        thread_run_ctx.unclean_shutdown();
        return;
    }
    let dest_parent_path = dest_parent_path.unwrap();

    // try to create the destination directory if it doesn't already exist
    if !dest_parent_path.exists() {
        match fsutil::mkdir_p(&dest_parent_path) {
            Ok(_) => {}
            Err(e) => {
                let err = format!(
                    "error creating destination directory: '{}': '{}'",
                    &dest_parent_path.to_str().unwrap(),
                    e
                );
                log_warn(&err);
                thread_run_ctx.unclean_shutdown();
                return;
            }
        }
    }

    // create temp file to write into
    let mkstemp_result = fsutil::mkstemp(&dest_parent_path);
    if mkstemp_result.is_err() {
        let err = format!(
            "error creating temp file in directory: '{}'",
            &dest_parent_path.to_str().unwrap(),
        );
        log_warn(&err);
        thread_run_ctx.unclean_shutdown();
        return;
    }

    // files are automatically closed when they go out of scope
    // the block below wraps around the parts of the code that operate on the temp file
    // while it still needs to be open
    let mut tmp_path_buf = PathBuf::new();
    {
        let (tmp_file, path_buf) = mkstemp_result.unwrap();
        tmp_path_buf.push(path_buf);

        // open source file for reading
        match File::open(&copy_file_req.src_path) {
            // copy the source file to the tmp destination file
            Ok(src_file) => match fsutil::copy_file(&src_file, &tmp_file) {
                Ok(_) => {}
                Err(e) => {
                    let err = format!(
                        "error copying '{}' to '{}': '{}'",
                        &copy_file_req.src_path.to_str().unwrap(),
                        &copy_file_req.dest_path.to_str().unwrap(),
                        e
                    );
                    log_warn(&err);
                    thread_run_ctx.unclean_shutdown();
                    return;
                }
            },
            Err(e) => {
                let err = format!(
                    "error opening source file for reading: '{}': {}",
                    &copy_file_req.src_path.to_str().unwrap(),
                    e
                );
                log_warn(&err);

                thread_run_ctx.unclean_shutdown();

                // if we had an error while the temp file was open, try to remove it
                match fs::remove_file(&tmp_path_buf) {
                    Ok(_) => {}
                    Err(_) => {
                        let err = format!(
                            "error removing temp file: '{}'",
                            &tmp_path_buf.to_str().unwrap()
                        );
                        log_warn(&err);
                    }
                }

                return;
            }
        }
    }

    // copy file time metadata from the source file to the dest file
    match fsutil::copy_file_time_metadata(&copy_file_req.src_path, &tmp_path_buf.as_path()) {
        Ok(_) => {}
        Err(e) => {
            let err = format!(
                "error copying file time metadata from '{}' to '{}': '{}'",
                &copy_file_req.src_path.to_str().unwrap(),
                &tmp_path_buf.to_str().unwrap(),
                e
            );
            log_warn(&err);
            thread_run_ctx.unclean_shutdown();
            return;
        }
    }

    // chmod the destination file 0644
    match fsutil::chmod(&tmp_path_buf.as_path()) {
        Ok(_) => {}
        Err(e) => {
            let err = format!(
                "error changing file permissions on '{}': '{}'",
                &tmp_path_buf.to_str().unwrap(),
                e
            );
            log_warn(&err);
            thread_run_ctx.unclean_shutdown();
            return;
        }
    }

    // atomically rename the temp file into place in the final destination file path
    match fsutil::atomic_rename(&tmp_path_buf.as_path(), &copy_file_req.dest_path) {
        Ok(_) => {}
        Err(e) => {
            let err = format!(
                "error renaming file from '{}' to '{}': '{}'",
                &tmp_path_buf.to_str().unwrap(),
                &copy_file_req.dest_path.to_str().unwrap(),
                e
            );
            log_warn(&err);
            thread_run_ctx.unclean_shutdown();
            return;
        }
    }
}

use crate::common::MergeResult;
use crate::common::{dit_error, ThreadRunContext};
use crate::message::{
    CopyFileRequest, CopyToDestRequest, HashRequest, HashResult, TransferRequest,
};
use crate::{common, ReadWritePaths};
use crate::threads;
use std::error::Error;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, SyncSender};
use std::thread;

/**
 * Main public API.
 */

/**
 * Accepts the command-line arguments (excluding args[0], which is the program name).
 *
 * Excepts to find arguments of the form: ["read", <read-paths...>, "write", <write-paths...>]
 *
 * Does not check paths for validity
 *
 * Returns ReadWritePaths, or Err if at least one read and write path are provided
 */
pub fn get_cli_read_write_paths(args: &[String]) -> Result<ReadWritePaths, Box<dyn Error>> {
    let mut read_paths: Vec<PathBuf> = vec![];
    let mut write_paths: Vec<PathBuf> = vec![];

    let mut set_read = false;
    let mut set_write = false;

    for s in args {
        if "read".eq(s) {
            set_read = true;
            set_write = false;
            continue;
        }
        if "write".eq(s) {
            set_read = false;
            set_write = true;
            continue;
        }

        // strip trailing slash from path, if present
        //
        // as a special case, allow the root path "/" to pass through
        // (so we can detect and block it elsewhere)
        let mut tmp_s = String::from(s);
        if s.ends_with("/") && (s.len() > 1) {
            tmp_s.pop();
        }
        let s = &tmp_s;

        // set read path
        if set_read {
            read_paths.push(PathBuf::from(s));
        }

        // set write path
        if set_write {
            write_paths.push(PathBuf::from(s));
        }
    }

    if read_paths.len() < 1 {
        return dit_error("must have at least one read path");
    }
    if write_paths.len() < 1 {
        return dit_error("must have at least one write path");
    }

    Ok(ReadWritePaths {
        read_paths,
        write_paths,
    })
}

/**
 * Top-level entry point to copy files from N sources to M destinations.
 *
 * Returns a MergeResult indicating how it went.
 */
pub fn copy(
    log_info: fn(&str),
    log_warn: fn(&str),
    read_write_paths: &ReadWritePaths,
) -> Result<MergeResult, Box<dyn Error>> {
    // ensure we have valid read and write paths, creating the write paths if necessary
    match common::ensure_valid_read_write_paths(read_write_paths) {
        Ok(_) => {}
        Err(e) => {
            return dit_error(&e.to_string());
        }
    }

    let mut read_paths = vec![];
    for read_path in &read_write_paths.read_paths {
        read_paths.push(read_path.to_str().unwrap());
    }

    let mut write_paths = vec![];
    for write_path in &read_write_paths.write_paths {
        write_paths.push(write_path.to_str().unwrap());
    }

    // transfer request channel
    let (xfer_req_tx, xfer_req_rx): (SyncSender<TransferRequest>, Receiver<TransferRequest>) =
        mpsc::sync_channel(crate::common::CHANNEL_SIZE_TRANSFER_REQUEST);

    // copy to dest channel
    let (copy_to_dest_tx, copy_to_dest_rx): (
        SyncSender<CopyToDestRequest>,
        Receiver<CopyToDestRequest>,
    ) = mpsc::sync_channel(crate::common::CHANNEL_SIZE_COPY_TO_DEST_REQUEST);

    // hash request/response channels
    let mut hash_req_channels_tx = vec![];
    let mut hash_req_channels_rx = vec![];
    let mut hash_res_channels_tx = vec![];
    let mut hash_res_channels_rx = vec![];
    for _ in &read_write_paths.read_paths {
        // hash request channel
        let (hash_req_tx, hash_req_rx): (
            SyncSender<Option<HashRequest>>,
            Receiver<Option<HashRequest>>,
        ) = mpsc::sync_channel(crate::common::CHANNEL_SIZE_HASH_REQUEST);

        // hash response channel
        let (hash_res_tx, hash_res_rx): (
            SyncSender<Option<HashResult>>,
            Receiver<Option<HashResult>>,
        ) = mpsc::sync_channel(crate::common::CHANNEL_SIZE_HASH_RESPONSE);

        // add channels to vectors
        hash_req_channels_tx.push(hash_req_tx);
        hash_req_channels_rx.push(hash_req_rx);

        hash_res_channels_tx.push(hash_res_tx);
        hash_res_channels_rx.push(hash_res_rx);
    }

    // copy file request channels
    let mut copy_file_req_channels_tx = vec![];
    let mut copy_file_req_channels_rx = vec![];
    for _ in &read_write_paths.write_paths {
        // copy file request channel
        let (copy_file_req_tx, copy_file_req_rx): (
            SyncSender<CopyFileRequest>,
            Receiver<CopyFileRequest>,
        ) = mpsc::sync_channel(crate::common::CHANNEL_SIZE_COPY_FILE_REQUEST);

        // add channel to vectors
        copy_file_req_channels_tx.push(copy_file_req_tx);
        copy_file_req_channels_rx.push(copy_file_req_rx);
    }

    let root_run_ctx = ThreadRunContext::new();

    let discovery_run_ctx = ThreadRunContext::from(&root_run_ctx);
    let discovery_run_ctx_clone = discovery_run_ctx.clone();

    let mut hash_run_ctx_vec = vec![];
    let mut hash_run_ctx_clone_vec = vec![];
    for _ in &read_paths {
        let hash_run_ctx = ThreadRunContext::from(&root_run_ctx);
        let hash_run_ctx_clone = hash_run_ctx.clone();

        hash_run_ctx_vec.push(hash_run_ctx);
        hash_run_ctx_clone_vec.push(hash_run_ctx_clone);
    }

    let merge_run_ctx = ThreadRunContext::from(&root_run_ctx);
    let merge_run_ctx_clone = merge_run_ctx.clone();

    let mut copy_run_ctx_vec = vec![];
    let mut copy_run_ctx_clone_vec = vec![];
    for _ in &read_write_paths.write_paths {
        let copy_run_ctx = ThreadRunContext::from(&root_run_ctx);
        let copy_run_ctx_clone = copy_run_ctx.clone();

        copy_run_ctx_vec.push(copy_run_ctx);
        copy_run_ctx_clone_vec.push(copy_run_ctx_clone);
    }

    let mut read_paths_copy = vec![];
    for read_path in &read_paths {
        read_paths_copy.push(String::from(*read_path));
    }

    let mut write_paths_discover_copy = vec![];
    for write_path in &write_paths {
        write_paths_discover_copy.push(String::from(*write_path));
    }

    let mut write_paths_copy = vec![];
    for write_path in &write_paths {
        write_paths_copy.push(String::from(*write_path));
    }

    let discovery_thread = thread::spawn(move || {
        threads::discover(
            discovery_run_ctx_clone,
            &xfer_req_tx,
            &copy_to_dest_tx,
            hash_req_channels_tx,
            read_paths_copy,
            write_paths_discover_copy,
        )
    });

    let mut hash_threads = vec![];
    for _ in &read_paths {
        let hash_run_ctx_clone = hash_run_ctx_clone_vec.pop().unwrap();
        let hash_req_channel_rx = hash_req_channels_rx.pop().unwrap();
        let hash_res_channel_tx = hash_res_channels_tx.pop().unwrap();

        let hash_thread = thread::spawn(move || {
            threads::hash(
                hash_run_ctx_clone,
                log_warn,
                hash_req_channel_rx,
                hash_res_channel_tx,
            );
        });
        hash_threads.push(hash_thread);
    }

    let merge_thread = thread::spawn(move || {
        threads::merge(
            merge_run_ctx_clone,
            log_info,
            log_warn,
            write_paths_copy,
            xfer_req_rx,
            hash_res_channels_rx,
            copy_to_dest_rx,
            copy_file_req_channels_tx,
        )
    });

    let mut copy_threads = vec![];
    for _ in &read_write_paths.write_paths {
        let copy_run_ctx_clone = copy_run_ctx_clone_vec.pop().unwrap();
        let copy_file_req_channel_rx = copy_file_req_channels_rx.pop().unwrap();

        let copy_thread = thread::spawn(move || {
            threads::copy(copy_run_ctx_clone, log_warn, copy_file_req_channel_rx);
        });
        copy_threads.push(copy_thread);
    }

    match discovery_thread.join() {
        Ok(_) => {}
        Err(_) => {
            if root_run_ctx.is_clean() {
                let err = format!("error in discovery_thread.join()");
                log_warn(&err);
                root_run_ctx.unclean_shutdown();
            }
        }
    }

    for hash_run_ctx in hash_run_ctx_vec {
        hash_run_ctx.shutdown();
    }

    for hash_thread in hash_threads {
        match hash_thread.join() {
            Ok(_) => {}
            Err(_) => {
                if root_run_ctx.is_clean() {
                    let err = format!("error in hash_thread.join()");
                    log_warn(&err);
                    root_run_ctx.unclean_shutdown();
                }
            }
        }
    }

    merge_run_ctx.shutdown();
    let merge_result = merge_thread.join();

    for copy_run_ctx in copy_run_ctx_vec {
        copy_run_ctx.shutdown();
    }

    for copy_thread in copy_threads {
        match copy_thread.join() {
            Ok(_) => {}
            Err(_) => {
                if root_run_ctx.is_clean() {
                    let err = format!("error in copy_thread.join()");
                    log_warn(&err);
                    root_run_ctx.unclean_shutdown();
                }
            }
        }
    }

    if !root_run_ctx.is_clean() {
        return Ok(MergeResult::Error);
    }

    Ok(merge_result.unwrap())
}

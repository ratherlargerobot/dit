use crate::common;
use crate::common::{dit_error, ThreadRunContext};
use crate::message::{CopyToDestRequest, HashRequest, TransferRequest};
use std::collections::{BTreeSet, HashSet};
use std::error::Error;
use std::path::PathBuf;
use std::sync::mpsc::SyncSender;

/**
 * Recursively discover all of the files in the read paths that we want to copy, and send them
 * to the right channels in other threads.
 *
 * If a single source file is found for a sub path, a CopyToDestRequest is sent for that file.
 *
 * If multiple source files are found for a sub path in more than one read path, then a HashRequest
 * is sent to each hash request queue (or None if no file was found for the corresponding read
 * path.
 *
 * If a CopyToDestRequest was sent, a TransferRequest of type Copy is also sent.
 *
 * If a HashRequest (or None) was sent to each hash request queue, a TransferRequest of type Merge
 * is also sent.
 */
pub fn discover_files(
    thread_run_ctx: &ThreadRunContext,
    xfer_req_tx: &SyncSender<TransferRequest>,
    copy_to_dest_req_tx: &SyncSender<CopyToDestRequest>,
    hash_req_tx_vec: &Vec<&SyncSender<Option<HashRequest>>>,
    read_paths: &Vec<&str>,
    write_paths: &Vec<&str>,
) -> Result<(), Box<dyn Error>> {
    __discover_files(
        thread_run_ctx,
        xfer_req_tx,
        copy_to_dest_req_tx,
        hash_req_tx_vec,
        read_paths,
        write_paths,
        "",
    )
}

// recursive implementation of public discover_files() function
fn __discover_files(
    thread_run_ctx: &ThreadRunContext,
    xfer_req_tx: &SyncSender<TransferRequest>,
    copy_to_dest_req_tx: &SyncSender<CopyToDestRequest>,
    hash_req_tx_vec: &Vec<&SyncSender<Option<HashRequest>>>,
    read_paths: &Vec<&str>,
    write_paths: &Vec<&str>,
    sub_path: &str,
) -> Result<(), Box<dyn Error>> {
    // if the program is supposed to shut down, stop discovering files
    // this returns Ok because if we get to this point, something else already shut down
    if !thread_run_ctx.is_running() {
        return Ok(());
    }

    // map of all dirents found in any of the read_paths[*]/sub_path directories
    let mut all_dirent_maps: BTreeSet<String> = BTreeSet::new();

    // vector of hash sets of dirents, with one hash set for each read_path
    // the list order of read_paths is the index into the hash maps
    // e.g. read_paths[i] matches up with read_path_dirent_maps[i]
    let mut read_path_dirent_maps: Vec<HashSet<String>> = vec![];

    // go through each read path
    for read_path in read_paths {
        // create a dirent map for this read path, to capture all the dirents in this directory
        let mut dirent_map: HashSet<String> = HashSet::new();

        // create the new read_paths with sub paths appended
        let mut read_path_with_sub_path = String::new();
        read_path_with_sub_path.push_str(read_path);
        if !"".eq(sub_path) {
            read_path_with_sub_path.push('/');
            read_path_with_sub_path.push_str(sub_path);
        }
        let read_path_with_sub_path = read_path_with_sub_path;

        // create a read path PathBuf, so we can read from it in a minute
        let read_path_buf = PathBuf::from(&read_path_with_sub_path);

        // if this read path has this subpath
        if read_path_buf.is_dir() {
            // get all of the dirents for this read path, and stick them in the maps
            for dirent in read_path_buf.read_dir()? {
                let dirent = dirent?;

                // file name is the bare name of the dirent inside of this directory (e.g. "foo")
                let file_name = String::from(dirent.file_name().to_str().unwrap());

                // skip hidden files and directories
                if file_name.starts_with('.') {
                    continue;
                }

                // add to combined map for all read paths
                all_dirent_maps.insert(String::from(&file_name));

                // add to specific map for this read path
                dirent_map.insert(file_name);
            }
        }

        // add the specific map for this read path to the vector of per-read-path dirent maps
        read_path_dirent_maps.push(dirent_map);
    }

    // go through each dirent that we found across all of the read paths with sub paths
    for dirent_str in &all_dirent_maps {
        // keep track of if this dirent is a file or a directory (or both or neither across dirs)
        let mut is_file = false;
        let mut is_dir = false;

        // list of full paths to all the files that we found
        let mut files_found_or_placeholders: Vec<Option<PathBuf>> = vec![];

        // assemble the next sub path, based on the sub path we received, plus the dirent
        let mut next_sub_path = String::new();
        if !"".eq(sub_path) {
            next_sub_path.push_str(&sub_path);
            next_sub_path.push('/');
        }
        next_sub_path.push_str(dirent_str);
        let sub_path_plus_dirent = next_sub_path;

        // figure out whether each instance of this dirent that exists is a file/directory/etc
        let mut i = 0;
        let mut actual_files_found = 0;
        for read_path in read_paths {
            let mut found_file_this_time = false;
            let dirent_map = &read_path_dirent_maps.get(i).unwrap();
            if dirent_map.contains(dirent_str) {
                // assemble a path buf with the full path to this particular file in one read path
                let mut full_path_buf = PathBuf::from(&read_path);
                full_path_buf.push(&sub_path_plus_dirent);
                let full_path_buf = full_path_buf;

                // if the full path file dirent exists, figure out if it's a file or directory
                if full_path_buf.exists() {
                    if full_path_buf.is_dir() {
                        is_dir = true;
                    } else if full_path_buf.is_file() {
                        // if it's a file, we'll probably need to refer to it again soon
                        files_found_or_placeholders.push(Some(full_path_buf));
                        is_file = true;
                        found_file_this_time = true;
                        actual_files_found += 1;
                    }
                }
            }

            // if we didn't find a file, add a None placeholder
            if !found_file_this_time {
                files_found_or_placeholders.push(None);
            }

            i += 1;
        }

        // alias previously mutable variables to immutable equivalents
        let is_file = is_file;
        let is_dir = is_dir;

        // file and directory
        if is_file && is_dir {
            let err_str = format!(
                "path must be a file or directory, not both: '{}'",
                &sub_path_plus_dirent
            );
            return dit_error(&err_str);
        }

        // neither file nor directory
        if (!is_file) && (!is_dir) {
            let err_str = format!(
                "path must be a file or directory: '{}'",
                &sub_path_plus_dirent
            );
            return dit_error(&err_str);
        }

        // file
        if is_file {
            // if the metadata for all src and dest files match, we can avoid hashing and copying
            if common::all_files_match(read_paths, write_paths, &sub_path_plus_dirent) {
                continue;
            }

            if actual_files_found > 1 {
                let mut i = 0;
                for file_name in files_found_or_placeholders {
                    // if we're supposed to shut down, stop discovering new files
                    if !thread_run_ctx.is_running() {
                        return Ok(());
                    }

                    match file_name {
                        Some(file_name) => {
                            // add hash request to queue
                            let hash_request = HashRequest {
                                sub_path: String::from(&sub_path_plus_dirent),
                                src_path: file_name,
                            };
                            hash_req_tx_vec.get(i).unwrap().send(Some(hash_request))?;
                        }
                        None => {
                            // add placeholder to hash request queue
                            hash_req_tx_vec.get(i).unwrap().send(None)?;
                        }
                    }

                    i += 1;
                }

                // if we're supposed to shut down, stop discovering new files
                if !thread_run_ctx.is_running() {
                    return Ok(());
                }

                // send a merge transfer request
                xfer_req_tx.send(TransferRequest::Merge)?;
            } else {
                for file_name in files_found_or_placeholders {
                    // if we're supposed to shut down, stop discovering new files
                    if !thread_run_ctx.is_running() {
                        return Ok(());
                    }

                    match file_name {
                        Some(file_name) => {
                            // send copy to dest request
                            let copy_to_dest_request = CopyToDestRequest {
                                sub_path: String::from(&sub_path_plus_dirent),
                                src_path: file_name,
                            };
                            copy_to_dest_req_tx.send(copy_to_dest_request)?;

                            // send copy transfer request
                            xfer_req_tx.send(TransferRequest::Copy)?;

                            break;
                        }
                        None => {}
                    }
                }
            }
        }

        // directory
        if is_dir {
            match __discover_files(
                thread_run_ctx,
                xfer_req_tx,
                copy_to_dest_req_tx,
                hash_req_tx_vec,
                read_paths,
                write_paths,
                &sub_path_plus_dirent,
            ) {
                Ok(()) => {}
                Err(e) => return Err(e),
            }
        }
    }

    Ok(())
}

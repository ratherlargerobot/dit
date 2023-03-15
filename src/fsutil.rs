use crate::common;
use crate::common::dit_error;
use libc::timespec;
use nix::sys::stat::UtimensatFlags;
use nix::sys::time::TimeSpec;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::ffi::CString;
use std::fs;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};

const BUF_SIZE: usize = 8192;

/**
 * Wrapper around the POSIX rename() function.
 *
 * Guaranteed to be atomic on UNIX and Linux systems.
 */
pub fn atomic_rename(src: &Path, dest: &Path) -> Result<(), Box<dyn Error>> {
    // N.B. Rust std::fs::rename() currently happens to be a wrapper around POSIX rename(),
    //      but makes no guarantee that this will continue to be the case in the future

    let c_src = CString::new(src.to_str().unwrap()).unwrap();
    let c_dest = CString::new(dest.to_str().unwrap()).unwrap();

    unsafe {
        let result = libc::rename(c_src.as_ptr(), c_dest.as_ptr());
        if 0 == result {
            return Ok(());
        }
    };

    let err_str = format!(
        "could not rename '{}' to '{}'",
        &src.to_str().unwrap(),
        &dest.to_str().unwrap()
    );

    dit_error(&err_str)
}

/**
 * Analyze the given file, and return a string with an sha256 hex digest hash.
 */
pub fn hash_file(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut hasher = Sha256::new();

    let mut f = File::open(path)?;
    let mut buf = [0; BUF_SIZE];

    loop {
        match f.read(&mut buf) {
            Ok(bytes_read) => {
                if 0 == bytes_read {
                    break;
                }
                hasher.update(&buf[..bytes_read]);
            }
            Err(e) => match e.kind() {
                ErrorKind::UnexpectedEof => break,
                ErrorKind::Interrupted => continue,
                _ => {
                    let err_str = format!("error reading file: '{}'", path.to_str().unwrap());
                    return common::dit_error(&err_str);
                }
            },
        }
    }

    let hash_result = hasher.finalize();

    let hex_digest_str = format!("{:x}", hash_result);

    Ok(hex_digest_str)
}

/**
 * Copy the access time and modification time from the source file to the destination file.
 */
pub fn copy_file_time_metadata(src: &Path, dest: &Path) -> Result<(), Box<dyn Error>> {
    let src_metadata = src.metadata()?;

    nix::sys::stat::utimensat(
        None,
        dest,
        &TimeSpec::from(timespec {
            tv_sec: src_metadata.atime(),
            tv_nsec: src_metadata.atime_nsec(),
        }),
        &TimeSpec::from(timespec {
            tv_sec: src_metadata.mtime(),
            tv_nsec: src_metadata.mtime_nsec(),
        }),
        UtimensatFlags::NoFollowSymlink,
    )?;

    Ok(())
}

/**
 * Create the given directory.
 */
pub fn mkdir(path: &Path) -> Result<(), Box<dyn Error>> {
    match fs::create_dir(path) {
        Ok(_) => Ok(()),
        Err(e) => {
            let err_msg = format!(
                "could not create directory: '{}': '{}'",
                path.to_str().unwrap(),
                e
            );
            return dit_error(&err_msg);
        }
    }
}

/**
 * Create the given directory, recursively.
 */
pub fn mkdir_p(path: &Path) -> Result<(), Box<dyn Error>> {
    match fs::create_dir_all(path) {
        Ok(_) => Ok(()),
        Err(e) => {
            let err_msg = format!(
                "could not recursively create directory: '{}': '{}'",
                path.to_str().unwrap(),
                e
            );
            return dit_error(&err_msg);
        }
    }
}

/**
 * Create a temp file, securely, in the given directory.
 *
 * Returns a newly-created File, opened for writing.
 */
pub fn mkstemp(base_dir: &Path) -> Result<(File, PathBuf), Box<dyn Error>> {
    let mut template = String::new();

    match base_dir.to_str() {
        Some(base_dir_str) => {
            template.push_str(&base_dir_str);
            template.push_str("/");
            template.push_str("__tmp_dit_");
            template.push_str("XXXXXX");
        }
        None => {
            return common::dit_error("could not convert base_dir path to string");
        }
    }

    match nix::unistd::mkstemp(Path::new(&template)) {
        Ok((raw_fd, path_buf)) => {
            // from_raw_fd() is unsafe because the caller needs to be the exclusive owner of the fd
            // in this case we are the exclusive owner of the fd since we just created it
            // using mkstemp inside of this function
            let file = unsafe { File::from_raw_fd(raw_fd) };
            Ok((file, path_buf))
        }
        Err(e) => Err(Box::new(e)),
    }
}

/**
 * Basic chmod 644 operation for files.
 */
pub fn chmod(path: &Path) -> Result<(), Box<dyn Error>> {
    let path_str = String::from(path.to_str().unwrap());
    let c_str_path = CString::new(path_str)?;

    let result = unsafe { libc::chmod(c_str_path.as_ptr(), 0o644) };
    if 0 != result {
        let err = format!(
            "could not chmod file: '{}', chmod() returned {}",
            path.to_str().unwrap(),
            result
        );
        return dit_error(&err);
    }

    Ok(())
}

/**
 * Copy a source file to a destination file, creating or overwriting the destination file.
 */
#[cfg(any(target_os = "android", target_os = "linux"))]
pub fn copy_file(src: &File, dest: &File) -> Result<(), Box<dyn Error>> {
    use std::os::unix::io::AsRawFd;

    let src_fd = src.as_raw_fd();
    let dest_fd = dest.as_raw_fd();
    let mut offset: libc::off_t = 0;

    let count = src.metadata()?.len() as libc::size_t;

    let n = unsafe { libc::sendfile(dest_fd, src_fd, &mut offset, count) };
    if -1 == n {
        return Err(Box::new(std::io::Error::last_os_error()));
    }

    Ok(())
}

#[cfg(not(any(target_os = "android", target_os = "linux")))]
pub fn copy_file(mut src: &File, mut dest: &File) -> Result<(), Box<dyn Error>> {
    use std::io::Write;

    let mut buf = [0; BUF_SIZE];

    loop {
        match src.read(&mut buf) {
            Ok(bytes_read) => {
                if 0 == bytes_read {
                    break;
                }

                match dest.write_all(&buf[..bytes_read]) {
                    Ok(_) => {}
                    Err(_) => {
                        return common::dit_error("write error");
                    }
                }
            }
            Err(e) => match e.kind() {
                ErrorKind::UnexpectedEof => break,
                ErrorKind::Interrupted => continue,
                _ => {
                    return common::dit_error("read error");
                }
            },
        }
    }

    Ok(())
}

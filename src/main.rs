use dit::MergeResult;
use std::env;
use std::process;

const PROGRAM_NAME: &str = "dit";
const EXIT_OK: i32 = 0;
const EXIT_FAIL: i32 = 1;
const EXIT_WARN: i32 = 2;

fn log_info(s: &str) {
    println!("{}", s);
}

fn log_warn(s: &str) {
    eprintln!("{}: {}", PROGRAM_NAME, s);
}

pub fn show_usage() {
    eprintln!("{}", PROGRAM_NAME);
    eprintln!("Usage: {} read <src...> write <dest...>", PROGRAM_NAME);
    process::exit(EXIT_FAIL);
}

fn main() {
    // get all command-line arguments
    let args: Vec<String> = env::args().collect();

    // extract read/write paths from the command-line arguments
    let read_write_paths = match dit::get_cli_read_write_paths(&args[1..]) {
        Ok(read_write_paths) => read_write_paths,
        Err(_) => {
            show_usage();
            // can't happen, show_usage() quits the program
            panic!();
        }
    };

    // copy the files, and exit the program with a suitable exit code
    match dit::copy(log_info, log_warn, &read_write_paths) {
        Ok(merge_result) => match merge_result {
            MergeResult::Ok => {
                process::exit(EXIT_OK);
            }
            MergeResult::Conflict => {
                log_warn("merge conflicts encountered");
                process::exit(EXIT_WARN);
            }
            MergeResult::Error => {
                log_warn("fatal error");
                process::exit(EXIT_FAIL);
            }
        },
        Err(e) => {
            log_warn(&e.to_string());
            process::exit(EXIT_FAIL);
        }
    }
}

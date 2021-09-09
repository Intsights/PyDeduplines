use ahash::AHashSet;
use bstr::ByteVec;
use bstr::io::BufReadExt;
use crossbeam_deque::{Steal, Worker};
use crossbeam_utils::thread as crossbeam_thread;
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::path::{PathBuf, Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::thread;
use std::time;

fn split_file(
    working_directory: &Path,
    input_file_path: &Path,
    prefix: String,
    num_parts: usize,
    should_stop: &AtomicBool,
) -> PyResult<()> {
    let mut output_files = Vec::with_capacity(num_parts);
    for i in 0..num_parts {
        let part_output_file_path = working_directory.join(format!("{}{}", prefix, i));
        let output_file = File::create(part_output_file_path)
            .map_err(|err| PyRuntimeError::new_err(format!("Could not create part_output_file_path: {:?}", err)))?;
        output_files.push(BufWriter::new(output_file));
    }

    let input_file = File::open(input_file_path)
        .map_err(|err| PyRuntimeError::new_err(format!("Could not open input_file_path: {:?}", err)))?;
    let input_file = BufReader::new(input_file);

    input_file.for_byte_line(
        |line| {
            let mut hash = 0;
            for char in line {
                hash += *char as usize;
            }
            let index = hash % num_parts;

            output_files[index].write_all(line)
                .map_err(|err| PyRuntimeError::new_err(format!("Could not write to output_files[index]: {:?}", err)))?;
            output_files[index].write_all(b"\n")
                .map_err(|err| PyRuntimeError::new_err(format!("Could not write to output_files[index]: {:?}", err)))?;

            Ok(!should_stop.load(Ordering::Relaxed))
        }
    )?;

    Ok(())
}

fn compute_part_added_lines(
    first_file_path: &Path,
    second_file_path: &Path,
    output_file: Arc<Mutex<BufWriter<File>>>,
    should_stop: &AtomicBool,
) -> PyResult<()> {
    let first_file = File::open(first_file_path)
        .map_err(|err| PyRuntimeError::new_err(format!("Could not open first_file_path: {:?}", err)))?;

    let metadata = fs::metadata(first_file_path)
        .map_err(|err| PyRuntimeError::new_err(format!("Could not get first_file_path metadata: {:?}", err)))?;
    let number_of_bytes: usize = metadata.len() as usize + 1;

    let mut number_of_lines: usize = 0;
    let mut lines_text: Vec<u8> = Vec::with_capacity(number_of_bytes);
    let first_file_buf_reader = BufReader::new(first_file);
    first_file_buf_reader.for_byte_line_with_terminator(
        |line| {
            lines_text.extend_from_slice(line);
            number_of_lines += 1;

            Ok(!should_stop.load(Ordering::Relaxed))
        }
    )?;

    let mut lines_set: AHashSet<&[u8]> = AHashSet::with_capacity(number_of_lines);
    let mut current_offset: usize = 0;
    lines_text.for_byte_line_with_terminator(
        |line| {
            lines_set.insert(&lines_text.as_slice()[current_offset..current_offset + line.len() - 1]);
            current_offset += line.len();

            Ok(!should_stop.load(Ordering::Relaxed))
        }
    )?;

    let second_file = File::open(second_file_path)
        .map_err(|err| PyRuntimeError::new_err(format!("Could not open second_file_path: {:?}", err)))?;
    let second_file_buf_reader = BufReader::new(second_file);
    second_file_buf_reader.for_byte_line(
        |line| {
            if !lines_set.contains(line) {
                let mut output_file_locked = output_file.lock();
                output_file_locked.write_all(line)
                    .map_err(|err| PyRuntimeError::new_err(format!("Could not write to output_file_locked: {:?}", err)))?;
                output_file_locked.write_all(b"\n")
                    .map_err(|err| PyRuntimeError::new_err(format!("Could not write to output_file_locked: {:?}", err)))?;
            }

            Ok(!should_stop.load(Ordering::Relaxed))
        }
    )?;

    Ok(())
}

fn compute_part_unique_lines(
    file_paths: Vec<PathBuf>,
    output_file: Arc<Mutex<BufWriter<File>>>,
    should_stop: &AtomicBool,
) -> PyResult<()> {
    let mut total_number_of_bytes: usize = 0;
    for file_path in file_paths.iter() {
        let metadata = fs::metadata(file_path)
        .map_err(|err| PyRuntimeError::new_err(format!("Could not get file_path metadata: {:?}", err)))?;
        total_number_of_bytes += metadata.len() as usize + 1;
    }

    let mut lines_text: Vec<u8> = Vec::with_capacity(total_number_of_bytes);
    let mut total_number_of_lines: usize = 0;
    for file_path in file_paths.iter() {
        let file = File::open(file_path)
            .map_err(|err| PyRuntimeError::new_err(format!("Could not open file_path: {:?}", err)))?;
            let file_buf_reader = BufReader::new(file);

            file_buf_reader.for_byte_line_with_terminator(
                |line| {
                    lines_text.extend_from_slice(line);
                    total_number_of_lines += 1;

                    Ok(!should_stop.load(Ordering::Relaxed))
                }
            )?;
            if !lines_text.ends_with(b"\n") {
                lines_text.push_char('\n');
            }
        }

    let mut lines_set: AHashSet<&[u8]> = AHashSet::with_capacity(total_number_of_lines);
    let mut current_offset: usize = 0;
    lines_text.for_byte_line_with_terminator(
        |line| {
            lines_set.insert(&lines_text.as_slice()[current_offset..current_offset + line.len() - 1]);
            current_offset += line.len();

            Ok(!should_stop.load(Ordering::Relaxed))
        }
    )?;

    for line in lines_set.into_iter() {
        let mut output_file_locked = output_file.lock();
        output_file_locked.write_all(line)
            .map_err(|err| PyRuntimeError::new_err(format!("Could not write to output_file_locked: {:?}", err)))?;
        output_file_locked.write_all(b"\n")
            .map_err(|err| PyRuntimeError::new_err(format!("Could not write to output_file_locked: {:?}", err)))?;
    }

    Ok(())
}

#[pyfunction]
fn compute_added_lines(
    py: Python,
    working_directory: PathBuf,
    first_file_path: PathBuf,
    second_file_path: PathBuf,
    output_file_path: PathBuf,
    number_of_splits: usize,
    number_of_threads: usize,
) -> PyResult<()> {
    let num_parts = number_of_threads * number_of_splits;

    let mut python_signal_result = Ok(());
    let results = Arc::new(Mutex::new(Vec::new()));
    let should_stop = AtomicBool::new(false);
    let working_threads = AtomicUsize::new(2);

    crossbeam_thread::scope(
        |s| {
            s.spawn(
                |_| {
                    let result = split_file(
                        working_directory.as_path(),
                        first_file_path.as_path(),
                        "first_".to_string(),
                        num_parts,
                        &should_stop,
                    );
                    results.lock().push(result);
                    working_threads.fetch_sub(1, Ordering::Relaxed);
                }
            );
            s.spawn(
                |_| {
                    let result = split_file(
                        working_directory.as_path(),
                        second_file_path.as_path(),
                        "second_".to_string(),
                        num_parts,
                        &should_stop,
                    );
                    results.lock().push(result);
                    working_threads.fetch_sub(1, Ordering::Relaxed);

                }
            );
            while working_threads.load(Ordering::Relaxed) != 0 {
                python_signal_result = py.check_signals();
                if python_signal_result.is_err() {
                    should_stop.store(true, Ordering::Relaxed);

                    break;
                }

                thread::sleep(time::Duration::from_millis(100));
            }
        }
    ).map_err(|err| PyRuntimeError::new_err(format!("Splitting thread pool has paniced: {:?}", err)))?;
    python_signal_result?;
    for result in results.lock().drain(..) {
        result?;
    }

    let mut python_signal_result = Ok(());
    let working_threads = AtomicUsize::new(num_parts);
    let output_file = File::create(output_file_path)
        .map_err(|err| PyRuntimeError::new_err(format!("Could not create output_file_path: {:?}", err)))?;
    let output_file = Arc::new(Mutex::new(BufWriter::new(output_file)));

    crossbeam_thread::scope(
        |s| {
            let worker = Worker::new_lifo();
            let stealer = worker.stealer();

            for i in 0..num_parts {
                worker.push(
                    (
                        i,
                        output_file.clone(),
                        &should_stop,
                        &working_threads,
                        &working_directory,
                    )
                );
            }

            for _ in 0..number_of_threads {
                let stealer = stealer.clone();
                let results = results.clone();
                s.spawn(
                    move |_| {
                        while let Steal::Success(
                            (
                                i,
                                output_file,
                                should_stop,
                                working_threads,
                                working_directory,
                            )
                        ) = stealer.steal() {
                            let result = compute_part_added_lines(
                                working_directory.join(format!("first_{}", i)).as_path(),
                                working_directory.join(format!("second_{}", i)).as_path(),
                                output_file,
                                should_stop,
                            );
                            results.lock().push(result);
                            working_threads.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
                );
            }

            while working_threads.load(Ordering::Relaxed) != 0 {
                python_signal_result = py.check_signals();
                if python_signal_result.is_err() {
                    should_stop.store(true, Ordering::Relaxed);

                    break;
                }

                thread::sleep(time::Duration::from_millis(100));
            }
        }
    ).map_err(|err| PyRuntimeError::new_err(format!("Computing added lines thread pool has paniced: {:?}", err)))?;
    python_signal_result?;
    for result in results.lock().drain(..) {
        result?;
    }

    Ok(())
}

#[pyfunction]
fn compute_unique_lines(
    py: Python,
    working_directory: PathBuf,
    file_paths: Vec<PathBuf>,
    output_file_path: PathBuf,
    number_of_splits: usize,
    number_of_threads: usize,
) -> PyResult<()> {
    let num_parts = number_of_threads * number_of_splits;

    let mut python_signal_result = Ok(());
    let results = Arc::new(Mutex::new(Vec::new()));
    let should_stop = AtomicBool::new(false);
    let working_threads = AtomicUsize::new(file_paths.len());

    crossbeam_thread::scope(
        |s| {
            let file_paths = file_paths.to_vec();
            for (i, file_path) in file_paths.into_iter().enumerate() {
                let working_directory = &working_directory;
                let working_threads = &working_threads;
                let should_stop = &should_stop;
                let results = results.clone();
                s.spawn(
                    move |_| {
                        let result = split_file(
                            working_directory.as_path(),
                            file_path.as_path(),
                            format!("{}_", i),
                            num_parts,
                            should_stop,
                        );
                        results.lock().push(result);
                        working_threads.fetch_sub(1, Ordering::Relaxed);
                    }
                );
            }

            while working_threads.load(Ordering::Relaxed) != 0 {
                python_signal_result = py.check_signals();
                if python_signal_result.is_err() {
                    should_stop.store(true, Ordering::Relaxed);

                    break;
                }

                thread::sleep(time::Duration::from_millis(100));
            }
        }
    ).map_err(|err| PyRuntimeError::new_err(format!("Splitting thread pool has paniced: {:?}", err)))?;
    python_signal_result?;
    for result in results.lock().drain(..) {
        result?;
    }

    let mut python_signal_result = Ok(());
    let working_threads = AtomicUsize::new(num_parts);
    let output_file = File::create(output_file_path)
        .map_err(|err| PyRuntimeError::new_err(format!("Could not create output_file_path: {:?}", err)))?;
    let output_file = Arc::new(Mutex::new(BufWriter::new(output_file)));

    crossbeam_thread::scope(
        |s| {
            let file_paths = file_paths.to_vec();
            let worker = Worker::new_lifo();
            let stealer = worker.stealer();

            for part_number in 0..num_parts {
                let mut part_file_paths = Vec::new();
                for file_path_index in 0..file_paths.len() {
                    part_file_paths.push(
                        working_directory.join(format!("{}_{}", file_path_index, part_number))
                    );
                }
                worker.push(
                    (
                        part_file_paths,
                        output_file.clone(),
                        &should_stop,
                        &working_threads,
                    )
                );
            }

            for _ in 0..number_of_threads {
                let stealer = stealer.clone();
                let results = results.clone();
                s.spawn(
                    move |_| {
                        while let Steal::Success(
                            (
                                part_file_paths,
                                output_file,
                                should_stop,
                                working_threads,
                            )
                        ) = stealer.steal() {
                            let result = compute_part_unique_lines(
                                part_file_paths,
                                output_file,
                                should_stop,
                            );
                            results.lock().push(result);
                            working_threads.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
                );
            }

            while working_threads.load(Ordering::Relaxed) != 0 {
                python_signal_result = py.check_signals();
                if python_signal_result.is_err() {
                    should_stop.store(true, Ordering::Relaxed);

                    break;
                }

                thread::sleep(time::Duration::from_millis(100));
            }
        }
    ).map_err(|err| PyRuntimeError::new_err(format!("Computing unique lines thread pool has paniced: {:?}", err)))?;
    python_signal_result?;
    for result in results.lock().drain(..) {
        result?;
    }

    Ok(())
}

#[pymodule]
fn pydeduplines(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compute_added_lines, m)?)?;
    m.add_function(wrap_pyfunction!(compute_unique_lines, m)?)?;

    Ok(())
}

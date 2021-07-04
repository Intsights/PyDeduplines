use bstr::ByteVec;
use bstr::io::BufReadExt;
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::prelude::*;
use std::path::{PathBuf, Path};
use std::sync::Arc;

fn split_file(
    working_directory: &Path,
    input_file_path: &Path,
    prefix: String,
    num_parts: usize,
) -> PyResult<()> {
    let mut output_files = Vec::with_capacity(num_parts);
    for i in 0..num_parts {
        let part_output_file_path = working_directory.join(format!("{}{}", prefix, i));
        let output_file = File::create(part_output_file_path)?;
        output_files.push(BufWriter::new(output_file));
    }

    let input_file = File::open(input_file_path)?;
    let input_file = BufReader::new(input_file);

    input_file.for_byte_line(
        |line| {
            let mut hash = 0;
            for char in line {
                hash += *char as usize;
            }
            let index = hash % num_parts;

            output_files[index].write_all(line)?;
            output_files[index].write_all(b"\n")?;

            Ok(true)
        }
    ).map_err(|_| PyRuntimeError::new_err("Could not split the file"))
}

fn compute_part_added_lines(
    first_file_path: &Path,
    second_file_path: &Path,
    output_file: Arc<Mutex<BufWriter<File>>>,
) -> PyResult<()> {
    let first_file = File::open(first_file_path)?;
    let first_file_buf_reader = BufReader::new(first_file);

    let mut lines_vec: Vec<Vec<u8>> = Vec::new();
    first_file_buf_reader.for_byte_line(
        |line| {
            lines_vec.push(Vec::from_slice(line));

            Ok(true)
        }
    )?;

    let mut lines_set = HashSet::with_capacity(lines_vec.len());
    for line in lines_vec.iter() {
        lines_set.insert(line.as_slice());
    }

    let second_file = File::open(second_file_path)?;
    let second_file_buf_reader = BufReader::new(second_file);
    second_file_buf_reader.for_byte_line(
        |line| {
            if !lines_set.contains(line) {
                let mut output_file_locked = output_file.lock();
                output_file_locked.write_all(line)?;
                output_file_locked.write_all(b"\n")?;
            }

            Ok(true)
        }
    ).map_err(|_| PyRuntimeError::new_err("Could not search for unique lines"))
}

fn compute_part_unique_lines(
    file_paths: Vec<PathBuf>,
    output_file: Arc<Mutex<BufWriter<File>>>,
) -> PyResult<()> {
    let mut lines_vec: Vec<Vec<u8>> = Vec::new();
    for file_path in file_paths.iter() {
        let file = File::open(file_path)?;
        let file_buf_reader = BufReader::new(file);

        file_buf_reader.for_byte_line(
            |line| {
                lines_vec.push(Vec::from_slice(line));

                Ok(true)
            }
        )?;
    }

    let mut lines_set: HashSet<&[u8]> = HashSet::with_capacity(lines_vec.len());
    for line in lines_vec.iter() {
        if lines_set.insert(line.as_slice()) {
            let mut output_file_locked = output_file.lock();
            output_file_locked.write_all(line)?;
            output_file_locked.write_all(b"\n")?;
        }
    }

    Ok(())
}

#[pyfunction]
fn compute_added_lines(
    working_directory: PathBuf,
    first_file_path: PathBuf,
    second_file_path: PathBuf,
    output_file_path: PathBuf,
    number_of_splits: usize,
    number_of_threads: usize,
) -> PyResult<()> {
    let num_parts = number_of_threads * number_of_splits;

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(number_of_threads)
        .build()
        .map_err(|_| PyRuntimeError::new_err("Could not create a thread pool"))?;

    pool.scope(
        |s| {
            s.spawn(
                |_| {
                    split_file(
                        working_directory.as_path(),
                        first_file_path.as_path(),
                        "first_".to_string(),
                        num_parts,
                    ).unwrap_or(());
                }
            );
            s.spawn(
                |_| {
                    split_file(
                        working_directory.as_path(),
                        second_file_path.as_path(),
                        "second_".to_string(),
                        num_parts,
                    ).unwrap_or(());
                }
            );
        }
    );

    let output_file = File::create(output_file_path)?;
    let output_file = Arc::new(Mutex::new(BufWriter::new(output_file)));
    pool.scope(
        |s| {
            for i in 0..num_parts {
                let output_file = output_file.clone();
                let working_directory = working_directory.clone();
                s.spawn(
                    move |_| {
                        compute_part_added_lines(
                            working_directory.join(format!("first_{}", i)).as_path(),
                            working_directory.join(format!("second_{}", i)).as_path(),
                            output_file,
                        ).unwrap_or(());
                    }
                );
            }
        }
    );

    Ok(())
}

#[pyfunction]
fn compute_unique_lines(
    working_directory: PathBuf,
    file_paths: Vec<PathBuf>,
    output_file_path: PathBuf,
    number_of_splits: usize,
    number_of_threads: usize,
) -> PyResult<()> {
    let num_parts = number_of_threads * number_of_splits;

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(number_of_threads)
        .build()
        .map_err(|_| PyRuntimeError::new_err("Could not create a thread pool"))?;

    pool.scope(
        |s| {
            for (i, file_path) in file_paths.iter().enumerate() {
                let working_directory = working_directory.clone();
                s.spawn(
                    move |_| {
                        split_file(
                            working_directory.as_path(),
                            file_path.as_path(),
                            format!("{}_", i),
                            num_parts,
                        ).unwrap_or(());
                    }
                );
            }
        }
    );

    let output_file = File::create(output_file_path)?;
    let output_file = Arc::new(Mutex::new(BufWriter::new(output_file)));
    pool.scope(
        |s| {
            for part_number in 0..num_parts {
                let part_file_paths = (0..file_paths.len()).map(
                    |file_path_index| {
                        PathBuf::from(&working_directory).join(format!("{}_{}", file_path_index, part_number))
                    }
                ).collect();
                let output_file = output_file.clone();
                s.spawn(
                    move |_| {
                        compute_part_unique_lines(
                            part_file_paths,
                            output_file,
                        ).unwrap_or(());
                    }
                );
            }
        }
    );

    Ok(())
}

#[pymodule]
fn pydeduplines(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compute_added_lines, m)?)?;
    m.add_function(wrap_pyfunction!(compute_unique_lines, m)?)?;

    Ok(())
}

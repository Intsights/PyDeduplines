<p align="center">
    <a href="https://github.com/intsights/PyDeduplines">
        <img src="https://raw.githubusercontent.com/intsights/PyDeduplines/master/images/logo.png" alt="Logo">
    </a>
    <h3 align="center">
        Python library for a duplicate lines removal written in Rust
    </h3>
</p>

![license](https://img.shields.io/badge/MIT-License-blue)
![Python](https://img.shields.io/badge/Python-3.7%20%7C%203.8%20%7C%203.9-blue)
![OS](https://img.shields.io/badge/OS-Mac%20%7C%20Linux%20%7C%20Windows-blue)
![Build](https://github.com/intsights/PyDeduplines/workflows/Build/badge.svg)
[![PyPi](https://img.shields.io/pypi/v/PyDeduplines.svg)](https://pypi.org/project/PyDeduplines/)

## Table of Contents

- [Table of Contents](#table-of-contents)
- [About The Project](#about-the-project)
  - [Built With](#built-with)
  - [Performance](#performance)
    - [Deduplicating](#deduplicating)
    - [Added Lines](#added-lines)
  - [Installation](#installation)
- [Documentation](#documentation)
- [Usage](#usage)
- [License](#license)
- [Contact](#contact)


## About The Project

This library is used to manipulate the lines of files. To achieve speed and efficiency, the library is written in Rust.

There are two functions in the library:
- `compute_unique_lines` - This function takes a list of input file paths and an output file path, iterates over the input file paths and writes unique lines to the output file.
- `compute_added_lines` - This function takes three arguments `first_file_path`, `second_file_path` and `output_file_path`, and writes to the output file only lines that appeared in the second file but not in the first.


### Built With

* [pyo3](https://github.com/PyO3/pyo3)
* [crossbeam](https://github.com/crossbeam-rs/crossbeam)
* [ahash](https://github.com/tkaitchuck/aHash)
* [parking_lot](https://github.com/Amanieu/parking_lot)
* [memchr](https://github.com/BurntSushi/memchr)
* [bytecount](https://github.com/llogiq/bytecount)


### Performance

#### Deduplicating
| Library  | Function | Time | Peak Memory |
| ------------- | ------------- | ------------- | ------------- |
| [GNU Sort](https://www.gnu.org/software/coreutils/) | sort -u -o output 500mb_one 500mb_two | 37.35s | 8,261mb |
| [PyDeduplines](https://github.com/intsights/PyDeduplines) | compute_unique_lines('./workdir', ['500mb_one', '500mb_two'], 'output', 16) | 4.55s | 685mb |

#### Added Lines
| Library  | Function | Time | Peak Memory |
| ------------- | ------------- | ------------- | ------------- |
| [GNU Sort](https://www.gnu.org/software/coreutils/) | comm -1 -3 <(sort 500mb_one) <(sort 500mb_two) > output.txt | 26.53s | 4,132mb |
| [PyDeduplines](https://github.com/intsights/PyDeduplines) | compute_added_lines('./workdir', '500mb_one', '500mb_two', 'output', 16) | 3.95s | 314mb |


### Installation

```sh
pip3 install PyDeduplines
```


## Documentation

```python
def compute_unique_lines(
    working_directory: str,
    file_paths: typing.List[str],
    output_file_path: str,
    number_of_splits: int,
    number_of_threads: int = 0,
) -> None: ...
```
- `working_directory` - A file path of a directory to work in. Each split file would be created in this directory.
- `file_paths` - A list of strings containing the input file paths to iterate over and to calculate unique values for.
- `output_file_path` - The path where the unique lines will be written.
- `number_of_splits` - This parameter specifies how many smaller splits are to be made from each input file based on the number of splits. The idea behind this library is defined by this parameter. The more splits, the lower the peak memory consumption. Remember that the more splits you have, the more files you have open.
- `number_of_threads` - Number of parallel threads. Using *0* means to use as many cores as possible. The number of threads greater than *1* would cause multiple splits on each input file.

```python
def compute_added_lines(
    working_directory: str,
    first_file_path: str,
    second_file_path: str,
    output_file_path: str,
    number_of_splits: int,
    number_of_threads: int = 0,
) -> None: ...
```
- `working_directory` - A file path of a directory to work in. Each split file would be created in this directory.
- `first_file_path` - A path to the first file to be iterated over.
- `second_file_path` - A file path to iterate over and find lines that do not exist in the first file.
- `output_file_path` - A path to the output file that contains the lines that appeared in the second file but not in the first.
- `number_of_splits` - This parameter specifies how many smaller splits are to be made from each input file based on the number of splits. The idea behind this library is defined by this parameter. The more splits, the lower the peak memory consumption. Remember that the more splits you have, the more files you have open.
- `number_of_threads` - Number of parallel threads. Using *0* means to use as many cores as possible. The number of threads greater than *1* would cause multiple splits on each input file.

## Usage

```python
import pydeduplines


pydeduplines.compute_unique_lines(
    working_directory='tmp',
    file_paths=[
        '500mb_one',
        '500mb_two',
    ],
    output_file_path='output',
    number_of_splits=4,
)

pydeduplines.compute_added_lines(
    working_directory='tmp',
    first_file_path='500mb_one',
    second_file_path='500mb_two',
    output_file_path='output',
    number_of_splits=4,
)
```


## License

Distributed under the MIT License. See `LICENSE` for more information.


## Contact

Gal Ben David - gal@intsights.com

Project Link: [https://github.com/intsights/PyDeduplines](https://github.com/intsights/PyDeduplines)

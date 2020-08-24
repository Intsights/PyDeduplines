<p align="center">
    <a href="https://github.com/intsights/PyDeduplines">
        <img src="https://raw.githubusercontent.com/intsights/PyDeduplines/master/images/logo.png" alt="Logo">
    </a>
    <h3 align="center">
        Python library for a duplicate lines removal written in C++
    </h3>
</p>

![license](https://img.shields.io/badge/MIT-License-blue)
![Python](https://img.shields.io/badge/Python-3.6%20%7C%203.7%20%7C%203.8%20%7C%20pypy3-blue)
![Build](https://github.com/intsights/PyDeduplines/workflows/Build/badge.svg)
[![PyPi](https://img.shields.io/pypi/v/PyDeduplines.svg)](https://pypi.org/project/PyDeduplines/)

## Table of Contents

- [Table of Contents](#table-of-contents)
- [About The Project](#about-the-project)
  - [Built With](#built-with)
  - [Performance](#performance)
    - [Deduplicating](#deduplicating)
    - [Added Lines](#added-lines)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Documentation](#documentation)
- [Usage](#usage)
- [License](#license)
- [Contact](#contact)


## About The Project

PyDeduplines is a library intended for manipulating files' lines. The library is written in C++ to achieve speed and efficiency. For the deduplication, the library uses a specific hash set implementation called [Parallel Hashmap](https://github.com/greg7mdp/parallel-hashmap) which is fast and memory efficient.
The library consists of two functions:
- `compute_unique_lines` - This function takes a list of input files paths and an output file path, iterates over each of the input files paths and writes to the output file unique lines.
- `compute_added_lines` - This function take three arguments `first_file_path`, `second_file_path` and `output_file_path`, and writes to the output file only lines that appeared in the second file but not in the first.


### Built With

* [Parallel Hashmap](https://github.com/greg7mdp/parallel-hashmap)
* [taskflow](https://github.com/taskflow/taskflow)
* [pybind11](https://github.com/pybind/pybind11)


### Performance

#### Deduplicating
| Library  | Function | Time | Peak Memory |
| ------------- | ------------- | ------------- | ------------- |
| [GNU Sort](https://www.gnu.org/software/coreutils/) | sort -u -o output 500mb_one 500mb_two | 53.35s | 9,376mb |
| [PyDeduplines](https://github.com/intsights/PyDeduplines) | compute_unique_lines(['500mb_one', '500mb_two'], 'output', 4) | 17.31s | 1,040mb |

#### Added Lines
| Library  | Function | Time | Peak Memory |
| ------------- | ------------- | ------------- | ------------- |
| [GNU Sort](https://www.gnu.org/software/coreutils/) | comm -13 <(sort 500mb_one -u) <(sort 500mb_two -u) | 52.04s | 9,376mb |
| [PyDeduplines](https://github.com/intsights/PyDeduplines) | compute_added_lines('500mb_one', '500mb_two', 'output', 4) | 6.91s | 681mb |


### Prerequisites

In order to compile this package you should have GCC & Python development package installed.
* Fedora
```sh
sudo dnf install python3-devel gcc-c++
```
* Ubuntu 20.04
```sh
sudo apt install python3-dev build-essential
```

### Installation

```sh
pip3 install PyDeduplines
```


## Documentation

```python
class FilesDeduplicator:
  def __init__(
      self,
      working_directory: str,
      number_of_threads: int,
  ) -> None
```
- `working_directory` - A file path of a directory to work with. Every splitted file would be created in this directory.
- `number_of_threads` - The number of threads to execute in parallel. `0` means the mumber of available cpu cores. Every number of threads greater than `1` would produce multiple splits on each input file.


```python
def compute_unique_lines(
    self,
    file_paths: typing.List[str],
    output_file_path: str,
    number_of_splits: int,
) -> None
```
- `file_paths` - A list of strings containing the inputs file paths to iterate over and to compute unique lines.
- `output_file_path` - An output file path that will be filled with the unique lines.
- `number_of_splits` - Each input file would be split into multiple smaller splits according to this parameter. This parameter is the while idea of this library. The more splits the less peak memory consumption. One should remember that the more splits the more disk io.


```python
def compute_added_lines(
    self,
    first_file_path: str,
    second_file_path: str,
    output_file_path: str,
    number_of_splits: int,
) -> None
```
- `first_file_path` - A file path to iterate over.
- `second_file_path` - A file path to iterate over and look for lines that do not exist in the first file.
- `output_file_path` - An output file path that will be filled with the lines that appeared in the second file but not in the first.
- `number_of_splits` - Each input file would be split into multiple smaller splits according to this parameter. This parameter is the while idea of this library. The more splits the less peak memory consumption. One should remember that the more splits the more disk io.


## Usage

```python
import pydeduplines

file_deduplicator = pydeduplines.FilesDeduplicator(
  working_directory='/home/wavenator/work/PyDeduplines/tmp',
  number_of_threads=0,
)
file_deduplicator.compute_unique_lines(
    file_paths=[
        '500mb_one',
        '500mb_two',
    ],
    output_file_path='output',
    number_of_splits=4,
)

file_deduplicator.compute_added_lines(
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

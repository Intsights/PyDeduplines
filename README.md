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
    - [CPU](#cpu)
    - [Memory](#memory)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [License](#license)
- [Contact](#contact)


## About The Project

PyDeduplines is a library intended for deduplicating multiple files, line by line. The library is written in C++ to achieve speed and efficiency. The library also uses [mimalloc](https://github.com/microsoft/mimalloc) allocator written by `Microsoft` for memory allocation efficiency. For the deduplication, the library uses a specific hash set implementation called [Parallel Hashmap](https://github.com/greg7mdp/parallel-hashmap) which is fast and memory efficient. The library is twice as fast as sort-uniq and twice as memory efficient as sort-uniq.


### Built With

* [mimalloc](https://github.com/microsoft/mimalloc)
* [Parallel Hashmap](https://github.com/greg7mdp/parallel-hashmap)


### Performance

#### CPU
| Library  | Text Size | Function | Time | Improvement Factor |
| ------------- | ------------- | ------------- | ------------- | ------------- |
| [ripgrepy](https://pypi.org/project/ripgrepy/) | 500mb | sort -u -o output input | 40.37s | 1.0x |
| [PyDeduplines](https://github.com/intsights/PyDeduplines) | 500mb | pydeduplines.deduplicate_lines(['input'], 'output') | 18.54s | 2.17x |

#### Memory
| Library  | Text Size | Function | Peak RSS Memory (bytes) | Improvement Factor |
| ------------- | ------------- | ------------- | ------------- | ------------- |
| [ripgrepy](https://pypi.org/project/ripgrepy/) | 500mb | sort -u -o output input | 4802100 | 1.0x |
| [PyDeduplines](https://github.com/intsights/PyDeduplines) | 500mb | pydeduplines.deduplicate_lines(['input'], 'output') | 2345932 | 2.05x |


### Prerequisites

In order to compile this package you should have GCC & Python development package installed.
* Fedora
```sh
sudo dnf install python3-devel gcc-c++
```
* Ubuntu 18.04
```sh
sudo apt install python3-dev g++-9
```

### Installation

```sh
pip3 install PyDeduplines
```


## Usage

```python
import PyDeduplines

# reads files line by line as writes them into a new file only if they
# were found for the first time.
pydeduplines.deduplicate_lines(['input'], 'output')
```


## License

Distributed under the MIT License. See `LICENSE` for more information.


## Contact

Gal Ben David - gal@intsights.com

Project Link: [https://github.com/intsights/PyDeduplines](https://github.com/intsights/PyDeduplines)

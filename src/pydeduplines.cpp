#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string_view>
#include <string>
#include <thread>
#include <vector>

#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "parallel_hashmap/phmap.h"
#include "taskflow/taskflow.hpp"


class FilesDeduplicator {
    public:
    FilesDeduplicator(
        std::string working_directory,
        std::uint8_t number_of_threads,
        std::uint64_t max_memory_bytes
    ) {
        this->working_directory = std::filesystem::path(working_directory);
        std::filesystem::create_directories(this->working_directory);

        if (number_of_threads == 0) {
            number_of_threads = std::thread::hardware_concurrency();
        }

        this->number_of_threads = number_of_threads;
        this->max_memory_bytes = max_memory_bytes;

        this->taskflow_executor = std::make_unique<tf::Executor>(number_of_threads);
    }

    ~FilesDeduplicator() {
        // std::filesystem::remove_all(working_directory);
        this->taskflow_executor.reset();
    }

    int compute_num_parts(
        std::string first_file_path,
        std::string second_file_path
    ) {
        auto first_file_size = std::filesystem::file_size(first_file_path);
        auto second_file_size = std::filesystem::file_size(second_file_path);

        std::ifstream first_file(first_file_path);
        auto first_file_num_lines = std::count(
            std::istreambuf_iterator<char>(first_file),
            std::istreambuf_iterator<char>(),
            '\n'
        );

        long hashtable_memory = first_file_num_lines * 23.7;

        long total_memory = first_file_size + second_file_size + hashtable_memory;
        long num_parts = this->number_of_threads * total_memory / this->max_memory_bytes;

        return num_parts;
    }

    void split_file(
        std::filesystem::path input_file_path,
        std::string prefix,
        std::uint32_t num_parts
    ) {
        std::vector<std::ofstream> output_files(num_parts);
        for(std::uint32_t i = 0; i < num_parts; i++) {
            std::string part_output_file_path = (this->working_directory / (prefix + std::to_string(i))).string();
            output_files[i] = std::ofstream(part_output_file_path);
        }

        std::ifstream input_file(input_file_path);
        if(!input_file.is_open()) {
            throw std::runtime_error("failed to open input file: " + input_file_path.string());
        }

        std::string line;
        while(std::getline(input_file, line)) {
            unsigned long hash = 5381;

            for(const char & c: line) {
                hash = ((hash << 5) + hash) + c;
            }

            unsigned int index = (unsigned int)hash % num_parts;
            output_files[index] << line << '\n';
        }
    }

    void compute_part_added_lines(
        std::filesystem::path first_file_path,
        std::filesystem::path second_file_path,
        std::ofstream & output_file
    ) {
        std::ifstream first_file(first_file_path, std::ios::binary);
        std::vector<char> first_file_data;
        first_file_data.reserve(
            std::filesystem::file_size(first_file_path)
        );
        std::copy(
            std::istreambuf_iterator<char>(first_file),
            std::istreambuf_iterator<char>(),
            std::back_inserter(first_file_data)
        );
        std::uint32_t number_of_lines = std::count(
            first_file_data.begin(),
            first_file_data.end(),
            '\n'
        );

        phmap::parallel_flat_hash_set<std::string_view> lines_set;
        lines_set.reserve(number_of_lines);

        if (first_file_data.size() != 0) {
            auto start = first_file_data.begin();
            while(true) {
                auto next_newline_pos = std::find(start, first_file_data.end(), '\n');
                lines_set.emplace(std::string_view(&*start, next_newline_pos - start));

                if(next_newline_pos == first_file_data.end()) {
                    break;
                }

                start = next_newline_pos + 1;
            }
        }

        std::ifstream second_file(second_file_path.string());
        if(!second_file.is_open()) {
            throw std::runtime_error("could not open part file: " + second_file_path.string());
        }

        std::string line;
        while(std::getline(second_file, line)) {
            if(!lines_set.contains(line)) {
                this->output_file_mutex.lock();
                output_file << line << '\n';
                this->output_file_mutex.unlock();
            }
        }
    }

    void compute_part_deduped_lines(
        std::filesystem::path first_file_path,
        std::filesystem::path second_file_path,
        std::ofstream & output_file
    ) {
        std::ifstream first_file(first_file_path, std::ios::binary);
        std::vector<char> first_file_data;
        first_file_data.reserve(
            std::filesystem::file_size(first_file_path)
        );
        std::copy(
            std::istreambuf_iterator<char>(first_file),
            std::istreambuf_iterator<char>(),
            std::back_inserter(first_file_data)
        );
        std::uint32_t first_file_number_of_lines = std::count(
            first_file_data.begin(),
            first_file_data.end(),
            '\n'
        );

        std::ifstream second_file(second_file_path, std::ios::binary);
        std::vector<char> second_file_data;
        second_file_data.reserve(
            std::filesystem::file_size(second_file_path)
        );
        std::copy(
            std::istreambuf_iterator<char>(second_file),
            std::istreambuf_iterator<char>(),
            std::back_inserter(second_file_data)
        );
        std::uint32_t second_file_number_of_lines = std::count(
            second_file_data.begin(),
            second_file_data.end(),
            '\n'
        );

        phmap::parallel_flat_hash_set<std::string_view> lines_set;
        lines_set.reserve(first_file_number_of_lines + second_file_number_of_lines);

        if (first_file_data.size() != 0) {
            auto start = first_file_data.begin();
            while(true) {
                auto next_newline_pos = std::find(start, first_file_data.end(), '\n');
                lines_set.emplace(std::string_view(&*start, next_newline_pos - start));

                if(next_newline_pos == first_file_data.end()) {
                    break;
                }

                start = next_newline_pos + 1;
            }
        }

        if (second_file_data.size() != 0) {
            auto start = second_file_data.begin();
            while(true) {
                auto next_newline_pos = std::find(start, second_file_data.end(), '\n');
                lines_set.emplace(std::string_view(&*start, next_newline_pos - start));

                if(next_newline_pos == second_file_data.end()) {
                    break;
                }

                start = next_newline_pos + 1;
            }
        }

        for (const auto & line : lines_set) {
            this->output_file_mutex.lock();
            output_file << line << '\n';
            this->output_file_mutex.unlock();
        }
    }

    void compute_added_lines(
        std::filesystem::path first_file_path,
        std::filesystem::path second_file_path,
        std::filesystem::path output_file_path
    ) {
        std::ofstream output_file(output_file_path.c_str());
        if (!output_file.is_open()) {
            throw std::runtime_error("could not open output file: " + output_file_path.string());
        }

        int num_parts = this->compute_num_parts(
            first_file_path,
            second_file_path
        );

        tf::Taskflow split_files_tf;
        split_files_tf.emplace(
            [this, first_file_path, num_parts] {
                this->split_file(first_file_path, "first_", num_parts);
            },
            [this, second_file_path, num_parts] {
                this->split_file(second_file_path, "second_", num_parts);
            }
        );

        tf::Taskflow compute_added_lines_tf;
        compute_added_lines_tf.for_each_index(
            0,
            num_parts,
            1,
            [this, &output_file] (int i) {
                std::filesystem::path first_file_part_path(
                    (this->working_directory / ("first_" + std::to_string(i))).string()
                );
                std::filesystem::path second_file_part_path(
                    (this->working_directory / ("second_" + std::to_string(i))).string()
                );
                compute_part_added_lines(first_file_part_path, second_file_part_path, output_file);
            }
        );

        this->taskflow_executor->run(split_files_tf).wait();
        this->taskflow_executor->run(compute_added_lines_tf).wait();
    }

    void compute_deduped_lines(
        std::filesystem::path first_file_path,
        std::filesystem::path second_file_path,
        std::filesystem::path output_file_path
    ) {
        std::ofstream output_file(output_file_path.c_str());
        if (!output_file.is_open()) {
            throw std::runtime_error("could not open output file: " + output_file_path.string());
        }

        int num_parts = this->compute_num_parts(
            first_file_path,
            second_file_path
        );

        tf::Taskflow split_files_tf;
        split_files_tf.emplace(
            [this, first_file_path, num_parts] {
                this->split_file(first_file_path, "first_", num_parts);
            },
            [this, second_file_path, num_parts] {
                this->split_file(second_file_path, "second_", num_parts);
            }
        );

        tf::Taskflow compute_added_lines_tf;
        compute_added_lines_tf.for_each_index(
            0,
            num_parts,
            1,
            [this, &output_file] (int i) {
                std::filesystem::path first_file_part_path(
                    (this->working_directory / ("first_" + std::to_string(i))).string()
                );
                std::filesystem::path second_file_part_path(
                    (this->working_directory / ("second_" + std::to_string(i))).string()
                );
                compute_part_deduped_lines(first_file_part_path, second_file_part_path, output_file);
            }
        );

        this->taskflow_executor->run(split_files_tf).wait();
        this->taskflow_executor->run(compute_added_lines_tf).wait();
    }

    std::filesystem::path working_directory;
    std::uint8_t number_of_threads;
    std::uint64_t max_memory_bytes;
    std::unique_ptr<tf::Executor> taskflow_executor;
    std::mutex output_file_mutex;
};


PYBIND11_MODULE(pydeduplines, m) {
    pybind11::class_<std::filesystem::path>(m, "Path")
    .def(pybind11::init<std::string>());
    pybind11::implicitly_convertible<std::string, std::filesystem::path>();

    pybind11::class_<FilesDeduplicator>(m, "FilesDeduplicator")
        .def(
            pybind11::init<std::string, std::uint8_t, std::uint64_t>(),
            "FilesDeduplicator object that handles searches over an index file",
            pybind11::arg("working_directory"),
            pybind11::arg("number_of_threads"),
            pybind11::arg("max_memory_bytes")
        )
        .def(
            "compute_added_lines",
            &FilesDeduplicator::compute_added_lines,
            "search over an index file for a substring",
            pybind11::arg("first_file_path"),
            pybind11::arg("second_file_path"),
            pybind11::arg("output_file_path")
        )
        .def(
            "compute_deduped_lines",
            &FilesDeduplicator::compute_deduped_lines,
            "search over an index file for a substring",
            pybind11::arg("first_file_path"),
            pybind11::arg("second_file_path"),
            pybind11::arg("output_file_path")
        );
}

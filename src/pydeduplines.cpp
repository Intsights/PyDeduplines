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
        std::uint8_t number_of_threads
    ) {
        this->working_directory = std::filesystem::path(working_directory);
        std::filesystem::create_directories(this->working_directory);

        if (number_of_threads == 0) {
            number_of_threads = std::thread::hardware_concurrency();
        }

        this->number_of_threads = number_of_threads;
        this->taskflow_executor = std::make_unique<tf::Executor>(number_of_threads);
    }

    ~FilesDeduplicator() {
        std::filesystem::remove_all(working_directory);
        this->taskflow_executor.reset();
    }

    inline void load_file_lines_to_vector(
        std::filesystem::path file_path,
        std::vector<std::string> & output_vector
    ) {
        std::ifstream file(file_path, std::ios::binary);

        std::string line;
        while (std::getline(file, line)) {
            output_vector.push_back(line);
        }
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
        std::vector<std::string> first_file_lines;
        this->load_file_lines_to_vector(first_file_path, first_file_lines);

        phmap::parallel_flat_hash_set<std::string_view> lines_set;
        lines_set.reserve(first_file_lines.size());

        std::string line;
        for (const auto & line : first_file_lines) {
            lines_set.emplace(line);
        }

        std::ifstream second_file(second_file_path.string());
        if(!second_file.is_open()) {
            throw std::runtime_error("could not open part file: " + second_file_path.string());
        }

        while(std::getline(second_file, line)) {
            if(!lines_set.contains(line)) {
                this->output_file_mutex.lock();
                output_file << line << '\n';
                this->output_file_mutex.unlock();
            }
        }
    }

    void compute_part_unique_lines(
        std::vector<std::filesystem::path> file_paths,
        std::ofstream & output_file
    ) {
        phmap::parallel_flat_hash_set<std::string_view> lines_set;

        std::vector<std::string> file_lines;
        std::string line;
        for (const auto & file_path : file_paths) {
            std::ifstream file(file_path);

            while (std::getline(file, line)) {
                file_lines.push_back(line);
            }
        }

        for (const auto & line : file_lines) {
            const auto & [it, inserted] = lines_set.emplace(line);
            if (inserted) {
                this->output_file_mutex.lock();
                output_file << line << '\n';
                this->output_file_mutex.unlock();
            }
        }
    }

    void compute_added_lines(
        std::filesystem::path first_file_path,
        std::filesystem::path second_file_path,
        std::filesystem::path output_file_path,
        std::uint8_t number_of_splits
    ) {
        std::ofstream output_file(output_file_path);
        if (!output_file.is_open()) {
            throw std::runtime_error("could not open output file: " + output_file_path.string());
        }

        int num_parts = this->number_of_threads * number_of_splits;

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

    void compute_unique_lines(
        std::vector<std::filesystem::path> file_paths,
        std::filesystem::path output_file_path,
        std::uint8_t number_of_splits
    ) {
        std::ofstream output_file(output_file_path);
        if (!output_file.is_open()) {
            throw std::runtime_error("could not open output file: " + output_file_path.string());
        }

        int num_parts = this->number_of_threads * number_of_splits;

        tf::Taskflow split_files_tf;
        for (std::size_t file_index = 0; file_index < file_paths.size(); ++file_index) {
            split_files_tf.emplace(
                [this, &file_paths, file_index, num_parts] {
                    this->split_file(file_paths[file_index], std::to_string(file_index) + "_", num_parts);
                }
            );
        }

        tf::Taskflow compute_added_lines_tf;
        compute_added_lines_tf.for_each_index(
            0,
            num_parts,
            1,
            [this, file_paths, &output_file] (int part_number) {
                std::vector<std::filesystem::path> part_file_paths;
                for (std::size_t file_index = 0; file_index < file_paths.size(); ++file_index) {
                    std::filesystem::path part_file_path(
                        this->working_directory / (std::to_string(file_index) + "_" + std::to_string(part_number))
                    );
                    part_file_paths.push_back(part_file_path);
                }
                compute_part_unique_lines(part_file_paths, output_file);
            }
        );

        this->taskflow_executor->run(split_files_tf).wait();
        this->taskflow_executor->run(compute_added_lines_tf).wait();
    }

    std::filesystem::path working_directory;
    std::uint8_t number_of_threads;
    std::unique_ptr<tf::Executor> taskflow_executor;
    std::mutex output_file_mutex;
};


PYBIND11_MODULE(pydeduplines, m) {
    pybind11::class_<std::filesystem::path>(m, "Path")
    .def(pybind11::init<std::string>());
    pybind11::implicitly_convertible<std::string, std::filesystem::path>();

    pybind11::class_<FilesDeduplicator>(m, "FilesDeduplicator")
        .def(
            pybind11::init<std::string, std::uint8_t>(),
            "FilesDeduplicator file manipulations methods such as deduplicating lines and find additional lines",
            pybind11::arg("working_directory"),
            pybind11::arg("number_of_threads")
        )
        .def(
            "compute_added_lines",
            &FilesDeduplicator::compute_added_lines,
            "Compute the added lines that exist in second file and not in first file",
            pybind11::arg("first_file_path"),
            pybind11::arg("second_file_path"),
            pybind11::arg("output_file_path"),
            pybind11::arg("number_of_splits")
        )
        .def(
            "compute_unique_lines",
            &FilesDeduplicator::compute_unique_lines,
            "Iterate over the input files and writes unique lines into the output file",
            pybind11::arg("file_paths"),
            pybind11::arg("output_file_path"),
            pybind11::arg("number_of_splits")
        );
}

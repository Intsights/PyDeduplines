#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <fstream>
#include <string>
#include <vector>

#include "mimalloc/static.c"
#include "mimalloc/include/mimalloc-override.h"
#include "mimalloc/include/mimalloc-new-delete.h"
#include "parallel_hashmap/phmap.h"


void deduplicate_lines(
    std::vector<std::string> input_files_paths,
    std::string output_file_path
) {
    phmap::parallel_flat_hash_set<std::string_view> lines_set;
    std::ofstream output_file(output_file_path);

    for (const auto & input_file_path : input_files_paths) {
        std::ifstream input_file(input_file_path);
        std::string line;
        while (std::getline(input_file, line)) {
            char * tmp_line = new char[line.size()];
            std::copy(line.begin(), line.end(), tmp_line);
            std::string_view line_sv(tmp_line, line.size());
            const auto & [it, inserted] = lines_set.emplace(line_sv);
            if (inserted == false) {
                delete [] tmp_line;
            } else {
                output_file << line << '\n';
            }
        }

        input_file.close();
    }

    for (auto & line : lines_set) {
        delete [] line.data();
    }
    lines_set.clear();
}


PYBIND11_MODULE(pydeduplines, m) {
    m.def(
        "deduplicate_lines",
        &deduplicate_lines,
        "Iterates through the files list and appends each distinct line into the new output file",
        pybind11::arg("input_files_paths"),
        pybind11::arg("output_file_path")
    );
}

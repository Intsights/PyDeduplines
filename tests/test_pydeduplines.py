import tempfile
import contextlib
import pytest
import random

import pydeduplines


@pytest.mark.parametrize('number_of_threads', [0, 1, 2])
@pytest.mark.parametrize('number_of_splits', [1, 2])
def test_compute_unique_lines_one_file(
    number_of_threads,
    number_of_splits,
):
    tempdir = tempfile.TemporaryDirectory()
    file_deduplicator = pydeduplines.FilesDeduplicator(
        working_directory=tempdir.name,
        number_of_threads=number_of_threads,
    )

    with contextlib.ExitStack() as stack:
        test_input_file_one = stack.enter_context(
            tempfile.NamedTemporaryFile('w')
        )
        test_output_file = stack.enter_context(
            tempfile.NamedTemporaryFile('r')
        )

        lines = [
            f'line{i}'
            for i in range(11000)
        ]
        random.shuffle(lines)

        test_input_file_one.file.write('\n'.join(lines * 2))
        test_input_file_one.file.flush()

        file_deduplicator.compute_unique_lines(
            file_paths=[
                test_input_file_one.name,
            ],
            output_file_path=test_output_file.name,
            number_of_splits=number_of_splits,
        )
        unique_file_data = test_output_file.read()

        assert sorted(unique_file_data.split('\n')) == sorted(lines + [''])


@pytest.mark.parametrize('number_of_threads', [0, 1, 2])
@pytest.mark.parametrize('number_of_splits', [1, 2])
def test_compute_unique_lines_two_files(
    number_of_threads,
    number_of_splits,
):
    tempdir = tempfile.TemporaryDirectory()
    file_deduplicator = pydeduplines.FilesDeduplicator(
        working_directory=tempdir.name,
        number_of_threads=number_of_threads,
    )

    with contextlib.ExitStack() as stack:
        test_input_file_one = stack.enter_context(
            tempfile.NamedTemporaryFile('w')
        )
        test_input_file_two = stack.enter_context(
            tempfile.NamedTemporaryFile('w')
        )
        test_output_file = stack.enter_context(
            tempfile.NamedTemporaryFile('r')
        )

        lines = [
            f'line{i}'
            for i in range(11000)
        ]
        random.shuffle(lines)

        test_input_file_one.file.write('\n'.join(lines[:10000]))
        test_input_file_one.file.flush()

        test_input_file_two.file.write('\n'.join(lines[:11000]))
        test_input_file_two.file.flush()

        file_deduplicator.compute_unique_lines(
            file_paths=[
                test_input_file_one.name,
                test_input_file_two.name,
            ],
            output_file_path=test_output_file.name,
            number_of_splits=number_of_splits,
        )
        unique_file_data = test_output_file.read()

        assert sorted(unique_file_data.split('\n')) == sorted(lines + [''])


@pytest.mark.parametrize('number_of_splits', [1, 2])
def test_compute_added_lines(
    number_of_splits,
):
    tempdir = tempfile.TemporaryDirectory()
    file_deduplicator = pydeduplines.FilesDeduplicator(
        working_directory=tempdir.name,
        number_of_threads=0,
    )

    with contextlib.ExitStack() as stack:
        test_input_file_one = stack.enter_context(
            tempfile.NamedTemporaryFile('w')
        )
        test_input_file_two = stack.enter_context(
            tempfile.NamedTemporaryFile('w')
        )
        test_output_file = stack.enter_context(
            tempfile.NamedTemporaryFile('r')
        )

        lines = [
            f'line{i}'
            for i in range(11000)
        ]
        random.shuffle(lines)

        test_input_file_one.file.write('\n'.join(lines[:10000]))
        test_input_file_one.file.flush()

        test_input_file_two.file.write('\n'.join(lines[:11000]))
        test_input_file_two.file.flush()

        file_deduplicator.compute_added_lines(
            first_file_path=test_input_file_one.name,
            second_file_path=test_input_file_two.name,
            output_file_path=test_output_file.name,
            number_of_splits=number_of_splits,
        )
        added_lines_file_data = test_output_file.read()

        assert sorted(added_lines_file_data.split('\n')) == sorted(lines[10000:] + [''])

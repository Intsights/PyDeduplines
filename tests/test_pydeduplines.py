import tempfile
import contextlib
import pytest
import random

import pydeduplines


@pytest.mark.parametrize(
    'number_of_threads',
    [
        0,
        1,
        2,
    ]
)
@pytest.mark.parametrize(
    'number_of_splits',
    [
        1,
        2,
    ]
)
def test_compute_unique_lines_one_file(
    number_of_threads,
    number_of_splits,
):
    with contextlib.ExitStack() as stack:
        test_input_file_one = stack.enter_context(
            tempfile.NamedTemporaryFile('wb')
        )
        test_output_file = stack.enter_context(
            tempfile.NamedTemporaryFile('rb')
        )

        lines = [
            f'line{i}'.encode()
            for i in range(11000)
        ]
        random.shuffle(lines)

        test_input_file_one.file.write(b'\n'.join(lines * 2))
        test_input_file_one.file.flush()

        tempdir = tempfile.mkdtemp()
        pydeduplines.compute_unique_lines(
            working_directory=tempdir,
            file_paths=[
                test_input_file_one.name,
            ],
            output_file_path=test_output_file.name,
            number_of_splits=number_of_splits,
            number_of_threads=number_of_threads,
        )
        unique_file_data = test_output_file.read()

        assert sorted(unique_file_data.split(b'\n')[:-1]) == sorted(lines)


@pytest.mark.parametrize(
    'number_of_threads',
    [
        0,
        1,
        2,
    ]
)
@pytest.mark.parametrize(
    'number_of_splits',
    [
        1,
        2,
    ]
)
def test_compute_unique_lines_two_files(
    number_of_threads,
    number_of_splits,
):
    with contextlib.ExitStack() as stack:
        test_input_file_one = stack.enter_context(
            tempfile.NamedTemporaryFile('wb')
        )
        test_input_file_two = stack.enter_context(
            tempfile.NamedTemporaryFile('wb')
        )
        test_output_file = stack.enter_context(
            tempfile.NamedTemporaryFile('rb')
        )

        lines = [
            f'line{i}'.encode()
            for i in range(11000)
        ]
        random.shuffle(lines)

        test_input_file_one.file.write(b'\n'.join(lines[:10000]))
        test_input_file_one.file.flush()

        test_input_file_two.file.write(b'\n'.join(lines[:11000]))
        test_input_file_two.file.flush()

        tempdir = tempfile.mkdtemp()
        pydeduplines.compute_unique_lines(
            working_directory=tempdir,
            file_paths=[
                test_input_file_one.name,
                test_input_file_two.name,
            ],
            output_file_path=test_output_file.name,
            number_of_splits=number_of_splits,
            number_of_threads=number_of_threads,
        )
        unique_file_data = test_output_file.read()

        assert sorted(unique_file_data.split(b'\n')[:-1]) == sorted(lines)


@pytest.mark.parametrize(
    'number_of_threads',
    [
        0,
        1,
        2,
    ]
)
@pytest.mark.parametrize(
    'number_of_splits',
    [
        1,
        2,
    ]
)
def test_compute_added_lines(
    number_of_threads,
    number_of_splits,
):
    with contextlib.ExitStack() as stack:
        test_input_file_one = stack.enter_context(
            tempfile.NamedTemporaryFile('wb')
        )
        test_input_file_two = stack.enter_context(
            tempfile.NamedTemporaryFile('wb')
        )
        test_output_file = stack.enter_context(
            tempfile.NamedTemporaryFile('rb')
        )

        lines = [
            f'line{i}'.encode()
            for i in range(11000)
        ]
        random.shuffle(lines)

        test_input_file_one.file.write(b'\n'.join(lines[:10000]))
        test_input_file_one.file.flush()

        test_input_file_two.file.write(b'\n'.join(lines[:11000]))
        test_input_file_two.file.flush()

        tempdir = tempfile.mkdtemp()
        pydeduplines.compute_added_lines(
            working_directory=tempdir,
            first_file_path=test_input_file_one.name,
            second_file_path=test_input_file_two.name,
            output_file_path=test_output_file.name,
            number_of_splits=number_of_splits,
            number_of_threads=number_of_threads,
        )
        added_lines_file_data = test_output_file.read()
        assert sorted(added_lines_file_data.split(b'\n')[:-1]) == sorted(lines[10000:])

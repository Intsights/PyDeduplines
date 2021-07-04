import multiprocessing
import os
import pathlib
import typing
import shutil

from . import pydeduplines


def compute_unique_lines(
    working_directory: str,
    file_paths: typing.List[str],
    output_file_path: str,
    number_of_splits: int,
    number_of_threads: int = 0,
) -> None:
    try:
        os.makedirs(
            name=working_directory,
            exist_ok=True,
        )

        for file_path in file_paths:
            if not os.path.exists(
                path=file_path,
            ):
                raise FileNotFoundError(f'Could not find file: {file_path}')

        output_file_folder = pathlib.Path(output_file_path).parent
        if not os.access(
            path=output_file_folder,
            mode=os.W_OK,
        ):
            raise PermissionError(f'Could not write to the output file folder: {output_file_folder}')

        if number_of_threads <= 0:
            number_of_threads = multiprocessing.cpu_count()

        return pydeduplines.compute_unique_lines(
            working_directory,
            file_paths,
            output_file_path,
            number_of_splits,
            number_of_threads,
        )
    finally:
        shutil.rmtree(
            path=working_directory,
        )


def compute_added_lines(
    working_directory: str,
    first_file_path: str,
    second_file_path: str,
    output_file_path: str,
    number_of_splits: int,
    number_of_threads: int = 0,
) -> None:
    try:
        os.makedirs(
            name=working_directory,
            exist_ok=True,
        )

        for file_path in [
            first_file_path,
            second_file_path,
        ]:
            if not os.path.exists(
                path=file_path,
            ):
                raise FileNotFoundError(f'Could not find file: {file_path}')

        output_file_folder = pathlib.Path(output_file_path).parent
        if not os.access(
            path=output_file_folder,
            mode=os.W_OK,
        ):
            raise PermissionError(f'Could not write to the output file folder: {output_file_folder}')

        if number_of_threads <= 0:
            number_of_threads = multiprocessing.cpu_count()

        return pydeduplines.compute_added_lines(
            working_directory,
            first_file_path,
            second_file_path,
            output_file_path,
            number_of_splits,
            number_of_threads,
        )
    finally:
        shutil.rmtree(
            path=working_directory,
        )

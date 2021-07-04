import typing


def compute_unique_lines(
    working_directory: str,
    file_paths: typing.List[str],
    output_file_path: str,
    number_of_splits: int,
    number_of_threads: int = 0,
) -> None: ...


def compute_added_lines(
    working_directory: str,
    first_file_path: str,
    second_file_path: str,
    output_file_path: str,
    number_of_splits: int,
    number_of_threads: int = 0,
) -> None: ...

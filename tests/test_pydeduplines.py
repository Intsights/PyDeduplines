import tempfile
import unittest
import unittest.mock
import contextlib

import pydeduplines


class PyDeduplinesTestCase(
    unittest.TestCase,
):
    def test_pydeduplines(
        self,
    ):
        with contextlib.ExitStack() as stack:
            test_input_file_one = stack.enter_context(tempfile.NamedTemporaryFile('w'))
            test_input_file_two = stack.enter_context(tempfile.NamedTemporaryFile('w'))
            test_output_file_one = stack.enter_context(tempfile.NamedTemporaryFile('r'))
            test_output_file_two = stack.enter_context(tempfile.NamedTemporaryFile('r'))

            test_input_file_one.file.write(
                'line1\n'
                'line2\n'
                'line3\n'
                'line1\n'
                'line3\n'
                'line4\n'
                'line1\n'
            )
            test_input_file_one.file.flush()
            test_input_file_two.file.write(
                'line1\n'
                'line2\n'
                'line3\n'
                'line5\n'
                'line1\n'
                'line3\n'
                'line4\n'
                'line1\n'
            )
            test_input_file_two.file.flush()

            pydeduplines.deduplicate_lines(
                input_files_paths=[
                    test_input_file_one.name,
                ],
                output_file_path=test_output_file_one.name,
            )

            deduped_file_data = test_output_file_one.read()
            self.assertEqual(
                first=deduped_file_data,
                second=(
                    'line1\n'
                    'line2\n'
                    'line3\n'
                    'line4\n'
                ),
            )

            pydeduplines.deduplicate_lines(
                input_files_paths=[
                    test_input_file_one.name,
                    test_input_file_two.name,
                ],
                output_file_path=test_output_file_two.name,
            )

            deduped_file_data = test_output_file_two.read()
            self.assertEqual(
                first=deduped_file_data,
                second=(
                    'line1\n'
                    'line2\n'
                    'line3\n'
                    'line4\n'
                    'line5\n'
                ),
            )

import pytest
import os

json_file_path = "output/result_1.json"
xml_file_path = "output/result_1.xml"


@pytest.fixture(scope="module")
def check_files():
    files = [json_file_path, xml_file_path]
    return files


def test_files_exist(check_files):
    for file_path in check_files:
        assert os.path.exists(file_path), f"File {file_path} does not exist."


def test_files_not_empty(check_files):
    for file_path in check_files:
        assert os.path.getsize(file_path) > 0, f"File {file_path} is empty."

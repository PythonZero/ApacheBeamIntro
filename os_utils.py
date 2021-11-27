"""Code that interacts with the OS"""
import glob
import os

from constants import ROOT_DIR


def clear_output(file_type: str = "csv") -> None:
    """Deletes all files of the file_type

    :param file_type: the extension, e.g. "csv" "jsonl" to look for
    """
    files_to_delete = glob.glob(os.path.join(ROOT_DIR, "output", f"*.{file_type}"))
    for file in files_to_delete:
        print(f"Deleting {file}")
        os.remove(file)
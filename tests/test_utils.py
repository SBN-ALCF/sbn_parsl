import pytest
import pathlib

from sbn_parsl.utils import create_provider_by_hostname, subrun_dir


def test_create_provider():
    pass

def test_create_executor():
    pass

def test_create_useropts():
    pass

def test_subrun_dir():
    # no args
    prefix = pathlib.Path('my/path/base')
    subrun_path = subrun_dir(prefix, 1234)
    assert subrun_path == prefix / '001200' / '001234'

def test_subrun_dir_args():
    prefix = pathlib.Path('my/path/base')
    subrun_path = subrun_dir(prefix, 1234, 1, 1, 8)
    assert subrun_path == prefix / '00001234'

def test_subrun_dir_args2():
    prefix = pathlib.Path('my/path/base')
    subrun_path = subrun_dir(prefix, 1234, 3, 2, 8)
    assert subrun_path == prefix / '00001000' / '00001234'

def test_subrun_dir_except():
    prefix = pathlib.Path('my/path/base')
    with pytest.raises(RuntimeError):
        subrun_path = subrun_dir(prefix, 1234, 3, 0, 8)

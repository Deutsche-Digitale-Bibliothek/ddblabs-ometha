import pytest
from ometha.cli import parseargs
import yaml
from ometha.harvester import read_yaml_file


def test_parseargs():
    args = parseargs()
    assert isinstance(args, dict)
    assert "timeout" in args
    assert "n_procs" in args
    assert "out_f" in args
    assert "debug" in args
    assert "exp_type" in args


def test_read_yaml_file(tmpdir):
    # Create a temporary YAML file for testing
    file_path = tmpdir.join("test.yml")
    data = {"key1": "value1", "key2": "value2"}
    with open(file_path, "w") as f:
        yaml.dump(data, f)

    # Test read_yaml_file function
    result = read_yaml_file(file_path, ["key1", "key2"])
    assert result == ["value1", "value2"]

    # Test with a key that does not exist in the file
    # with pytest.raises(SystemExit):
    #     read_yaml_file(file_path, ["nonexistent_key"])

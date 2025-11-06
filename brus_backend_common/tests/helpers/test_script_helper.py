from brus_backend_common.helpers import script_helper


def test_flatten_json():
    """Test flattening jsons"""
    test_json = {"a": [1, 2, 3], "b": [2, 3, 4]}
    result = {"a_0": 1, "a_1": 2, "a_2": 3, "b_0": 2, "b_1": 3, "b_2": 4}
    assert script_helper.flatten_json(test_json) == result

    test_json = [{"a": [1, 2, 3]}, {"b": [2, 3, 4]}]
    result = {"0_a_0": 1, "0_a_1": 2, "0_a_2": 3, "1_b_0": 2, "1_b_1": 3, "1_b_2": 4}
    assert script_helper.flatten_json(test_json) == result


def test_trim_nested_obj():
    """Test trimming nested objects"""
    test_json = [{" a ": ["1", "         2", "3     "], "b": ["  2   ", "  3", "4"]}]
    # note: it doesn't trim the keys, only the values
    result = [{" a ": ["1", "2", "3"], "b": ["2", "3", "4"]}]
    assert script_helper.trim_nested_obj(test_json) == result

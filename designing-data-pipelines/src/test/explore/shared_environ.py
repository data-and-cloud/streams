from os import environ


def test_that__env_settings_are_shared_across_tests():
    environ['shared_prop'] = 'shared_value'


def test_that__env_settings_from_previous_test_is_still_visible():
    assert environ['shared_prop'] == 'shared_value'

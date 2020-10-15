from iguazioig.apiv0_1 import format_pip_libraries


def test_format_pip_libraries_no_input():
    function = {'no pip': 1}
    pip_string = format_pip_libraries(function)
    assert pip_string == ['pip install v3io==0.5.0']


def test_format_pip_libraries_normal_input():
    function = {'pip': ['nuclio-jupyter', 'marshmallow', 'requests']}
    pip_string = format_pip_libraries(function)
    assert pip_string == ['pip install nuclio-jupyter marshmallow requests v3io==0.5.0']


def test_format_pip_libraries_v3io_competing_input():
    function = {'pip': ['nuclio-jupyter', 'marshmallow', 'requests', 'v3io==0.4.0']}
    pip_string = format_pip_libraries(function)
    assert pip_string == ['pip install nuclio-jupyter marshmallow requests v3io==0.5.0']
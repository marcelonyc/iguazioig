import pytest
import logging
import json

from nuclio import Context, Event

from iguazioig.templates.processing_template_0_2_0 import init_context, handler


class LogTester:

    def __init__(self, logger=None):

        # make sure this works if the user has a logger in their class/module
        object_logger = logging.getLogger(__name__)
        self.logger = logger if logger is not None else object_logger

    def process(self, context, message):
        _ = context
        _ = message
        self.logger.info('Logger got passed through')


@pytest.fixture
def step_config(monkeypatch):

    step_config = {
        'module_paths': [
            'tests'
        ],
        'class_module': 'tests.api_0_2_0.test_template',
        'class_name': 'LogTester',
        'methods': ['process']
    }

    monkeypatch.setenv('STEP_CONFIG', json.dumps(step_config))

    return monkeypatch


def test_init_context_sets_logger_correctly(caplog, step_config):

    context = Context()
    init_context(context)
    # logger injected into init from init context
    assert context.user_data.instance.logger == context.logger, "logger instance is not the context logger"

    handler(context, Event(json.dumps({"hi": "Hello"})))
    captured = caplog.records
    for records in captured:
        assert 'Logger got passed through' in records.message, "logger failed to log to standard out"

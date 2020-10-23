import ast
import base64
import json
import os
import requests
import sys

MODULE_PATHS = ast.literal_eval(os.getenv('STEP_CONFIG'))['MODULE_PATHS']
module_paths = MODULE_PATHS or None
if module_paths is not None:
    for module_path in module_paths:
        sys.path.append(module_path)

IMPORT_MODULES = ast.literal_eval(os.getenv('STEP_CONFIG'))['IMPORT_MODULES']
import_modules = IMPORT_MODULES or None
if import_modules is not None:
    imports = {}
    import importlib

    for module in import_modules:
        imports[module] = importlib.import_module(module)

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def output_stream(context, message, details):
    Records = []
    # message must be a Python dictionary
    record = json.dumps(message).encode('utf-8')
    messageb64 = base64.b64encode(record)
    Records.append({
        "Data": messageb64.decode('utf-8'),
        "PartitionKey": message['PartitionKey'],
    })
    #     context.user_data.v3io_client.stream.put_records(container=context.user_data.step_config['OUTPUT_STREAM_CONTAINER'],
    #                                              stream_path=details['output_stream'],records=Records)
    url = "http://%s/%s/%s/" % ('v3io-webapi:8081',
                                context.user_data.step_config[
                                    'OUTPUT_STREAM_CONTAINER'],
                                details['output_stream'])
    payload = {"Records": Records}
    try:
        requests.post(url, headers=context.user_data.headers, json=payload,
                      verify=False)
    except Exception as e:
        context.logger.error("Stream post failed: %s" % e)

    return


def igz_stream_init(context):
    setattr(context.user_data, 'headers', {
        "Content-Type": "application/json",
        "X-v3io-function": "PutRecords",
        "X-v3io-session-key": os.getenv("V3IO_ACCESS_KEY")
    })


def output_function(context, message, details):
    _module, _function = details['post_process_function'].split(".")
    call_function = "imports['%s'].%s(context,message)" % (_module, _function)
    try:
        eval(call_function)
    except Exception as e:
        context.logger.error("output_function failed: %s" % e)
    return


def output_http(context, message, details):
    context.logger.info("If I was working I would post to %s the key %s" % (
        details['url'], message['PartitionKey']))
    return


def post_process(context, message):
    for output in context.user_data.outputs:
        if 'condition' in output:
            try:
                if not eval(output['condition']):
                    continue
            except:
                context.logger.debug(
                    "Invalid condition spec %s" % output['condition'])
                continue

        call_function = "output_%s(context,message,output)" % output['kind']
        try:
            eval(call_function)
        except Exception as e:
            context.logger.error("post_process failed: %s" % e)


def process(context, message):
    # message is dictionary loaded from the stream event
    _function = "context.user_data.processing_class.%s(context,message)" % \
                context.user_data.step_config['PROCESSING_FUNCTION']
    try:
        post_message = eval(_function)
    except Exception as e:
        context.logger.error("process failed: %s" % e)
        return
    post_process(context, post_message)

    return


def init_context(context):
    setattr(context.user_data, 'step_config',
            ast.literal_eval(os.getenv('STEP_CONFIG')))
    _class_init = os.getenv('CLASS_INIT')
    _module, _class = context.user_data.step_config[
        'CLASS_LOAD_FUNCTION'].split(".")
    if _class_init is not None:
        _class_init = json.loads(_class_init)
        _load_class = "imports['%s'].%s(**_class_init)" % (_module, _class)
    else:
        _load_class = "imports['%s'].%s()" % (_module, _class)

    try:
        setattr(context.user_data, 'processing_class', eval(_load_class))
    except Exception as e:
        context.logger.error("Init class failed: %s" % e)
        raise

    setattr(context.user_data, 'outputs',
            context.user_data.step_config['OUTPUTS'])
    _stream_output = False
    for output in context.user_data.outputs:
        if output['kind'] == 'stream':
            _stream_output = True

    if _stream_output:
        igz_stream_init(context)

    return


def handler(context, event):
    # context.logger.debug(event.body)
    try:
        message = json.loads(event.body.decode('utf-8'))
    except Exception as e:
        context.logger.debug("Malformed Json %s - Error %s" % (str(event.body),e))
        raise

    process(context, message)

    return

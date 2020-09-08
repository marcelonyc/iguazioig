import yaml
import v3io_frames as v3f
from mlrun import code_to_function, mount_v3io, mlconf
import os

from iguazioig.composer import composer 

def create_streams_v1alpha1(project_graph=''):
    for stream in project_graph['project']['v3io_streams']:
        try:
            client = v3f.Client("framesd:8081",container=stream['container'])
            client.create("stream",
                      table=stream['path'],
                      shards=stream['shards'],
                      retention_hours=stream['retention'],
                      if_exists=0)
        except:
            print("Failed to create stream",stream)
            raise

def _deploy_v1alpha1(project_graph=''):
    for function in project_graph['project']['functions']:
        fn = composer(project_graph['apiVersion'],
                                    function['function_name'],
                                    project=project_graph['project']['name'])
        
        fn.with_http(workers=1).apply(mount_v3io())

        GPU = bool(function['gpu'])
        if GPU:
            fn.spec.base_spec['spec']['build']['baseImage'] = function['docker_image']
        fn.spec.build.commands = ['pip install requests']

        fn.spec.min_replicas = function['minReplicas']
        fn.spec.max_replicas = function['maxReplicas']        

        if GPU:
            fn.spec.base_spec['spec']['resources'] = {}
            fn.spec.base_spec['spec']['resources']['limits']={'nvidia.com/gpu' : function['num_gpus']}

        fn.set_env('V3IO_ACCESS_KEY',os.getenv('V3IO_ACCESS_KEY'))
        INPUT_STREAM = function['input_stream']
        consumer_group=function['function_name']
        #consumer_group='inferencegrp'

        maxWorkers = function['maxWorkers'] 

        trigger_spec={
              'kind': 'v3ioStream',
              'url' : "http://%s/%s/%s"% ('v3io-webapi:8081',function['input_stream_container'],f'{INPUT_STREAM}@{consumer_group}'),
            "password": os.getenv('V3IO_ACCESS_KEY'),  
            "maxWorkers" : maxWorkers,
            'attributes': {"pollingIntervalMs": 500,
                "seekTo": "latest",
                "readBatchSize": 100,
              }
            }
        fn.add_trigger('input-stream',trigger_spec)

        # These should in your Yaml
        fn.set_env('MODULE_PATHS',function['module_paths'])
        fn.set_env('IMPORT_MODULES',function['import_modules'])
        fn.set_env('CLASS_LOAD_FUNCTION',function['class_load_function'])
        fn.set_env('PROCESSING_FUNCTION',function['processing_function'])
        fn.set_env('STEP_NAME',function['function_name'])
        fn.set_env('POST_PROCESS_FUNCTION',function['post_process_function'])
        fn.set_env('OUTPUT_STREAM',function['output_stream'])
        fn.set_env('OUTPUT_STREAM_CONTAINER',function['output_stream_container'])

        if 'env_custom' in function:
            for env_var in function['env_custom']:
                fn.set_env(env_var['name'],env_var['value'])

        fn.apply(mount_v3io())

        addr = fn.deploy(project=project_graph['project']['name'])



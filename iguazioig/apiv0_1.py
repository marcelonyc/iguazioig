import yaml
import v3io_frames as v3f
from mlrun import code_to_function, mount_v3io, mlconf
import os
import json

from iguazioig.composer import composer 

def create_streams_v0_1(project_graph=''):
    _streams  =  project_graph['project']['v3io_streams']
    for stream in _streams.keys():
        try:
            client = v3f.Client("framesd:8081",container=_streams[stream]['container'])
            client.create("stream",
                      table=_streams[stream]['path'],
                      shards=_streams[stream]['shards'],
                      retention_hours=_streams[stream]['retention'],
                      if_exists=0)
        except:
            print("Failed to create stream",stream)
            raise

def _deploy_v0_1(project_graph=''):
    for function in project_graph['project']['functions']:
        fn = composer(project_graph['apiVersion'],
                                    function['function_name'],
                                    project=project_graph['project']['name'])
        
        #fn.with_http(workers=1)

        fn.spec.base_spec['spec']['build']['baseImage'] = function['docker_image']
        fn.spec.build.commands = ['pip install v3io==0.5.0']

        fn.spec.min_replicas = function['minReplicas']
        fn.spec.max_replicas = function['maxReplicas']        
        
        GPU = bool(function['gpu'])
        
        if GPU:
            fn.spec.base_spec['spec']['resources'] = {}
            fn.spec.base_spec['spec']['resources']['limits']={'nvidia.com/gpu' : function['num_gpus']}

        fn.set_env('V3IO_ACCESS_KEY',os.getenv('V3IO_ACCESS_KEY'))
        consumer_group=function['function_name'].replace('-','_')
        #consumer_group='inferencegrp'

        _input_streams = function['input_streams']
        for _stream in _input_streams.keys():
            _container = project_graph['project']['v3io_streams'][_stream]['container']
            _stream_path = project_graph['project']['v3io_streams'][_stream]['path']
            
            _maxWorkers = _input_streams[_stream]['maxWorkers']
            
            try:
                _v3io_access_key = _input_streams[_stream]['v3io_access_key']
            except:
                print("Using default v3io_access_key from environment")
                _v3io_access_key = os.getenv('V3IO_ACCESS_KEY')
                
            try: 
                _pollingIntervalMs = _input_streams[_stream]['pollingIntervalMs']
            except:
                print('Using default pollingIntervalMs')
                _pollingIntervalMs = 500
                
            try:
                _seekTo = _input_streams[_stream]['seekTo']
            except:
                print('Using default seek to latest')
                _seekTo = 'latest'
                
            try:
                _readBatchSize  = _input_streams[_stream]['readBatchSize']
            except:
                print('Using default readBatchSize 100')
                _readBatchSize = 100
                
            trigger_spec={
                  'kind': 'v3ioStream',
                  'url' : "http://%s/%s/%s"% ('v3io-webapi:8081',_container,f'{_stream_path}@{consumer_group}'),
                "password": _v3io_access_key,  
                "maxWorkers" : _maxWorkers,
                'attributes': {"pollingIntervalMs": _pollingIntervalMs,
                    "seekTo": _seekTo,
                    "readBatchSize": _readBatchSize,
                  }
                }
            fn.add_trigger(_stream,trigger_spec)

        # These should in your Yaml
        _step_config = {}
        _step_config['MODULE_PATHS'] = function['module_paths']
        _step_config['IMPORT_MODULES'] = function['import_modules']
        _step_config['CLASS_LOAD_FUNCTION'] = function['class_load_function']
        _step_config['PROCESSING_FUNCTION'] = function['processing_function']
        _step_config['STEP_NAME'] = function['function_name']
        _step_config['OUTPUT_STREAM_CONTAINER'] = function['output_stream_container']
        _step_config['OUTPUTS'] = function['outputs']
        
        fn.set_env("STEP_CONFIG", json.dumps(_step_config))
        if 'env_custom' in function:
            for env_var in function['env_custom']:
                fn.set_env(env_var['name'],env_var['value'])
                
        # MOunt v3io volumes
        if 'v3io_volumes' in project_graph['project']:
            _volumes = project_graph['project']['v3io_volumes']
            for volume in _volumes.keys():
                fn.apply(mount_v3io(name=volume,remote=_volumes[volume]['remote'],mount_path=_volumes[volume]['mount_path']))
         
        if 'class_init' in function:
            fn.set_env("CLASS_INIT",json.dumps(function['class_init'])) 
        
        if 'loggerSinks' in function:
            fn.spec.base_spec['spec']['loggerSinks'] = function['loggerSinks']
            
        addr = fn.deploy(project=project_graph['project']['name'])



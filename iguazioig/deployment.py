import yaml
import v3io_frames as v3f
from mlrun import code_to_function, mount_v3io, mlconf
import os

from iguazioig.composer import composer

def deploy(yaml_file=''):
    project_graph = yaml.load(open(yaml_file,'rb'),Loader=yaml.FullLoader)
    apiversion=project_graph['apiVersion']
    try:
        deploy_streams = "create_streams_%s(project_graph=project_graph)"% apiversion
        eval(deploy_streams)
    except:
        print("Failed to create streams")
        raise

    try:
        deploy_function = "_deploy_%s(project_graph=project_graph)"% apiversion
        eval(deploy_function)
    except:
        print("Failed to invoke deployment function")
        raise

    project = project_graph['project']['name']
    print ("Project {project} Deployed")

import os
from mlrun import code_to_function, mount_v3io

def composer(apiversion='v1alpha1',name='',project='default'):
    """Run a task on function/code (.py, .ipynb or .yaml) locally,
    e.g.:
       # define template
       task = get_template(name='myfunction', project='myproject')

    :param name:     function name
    :param project:  function project (none for 'default')
    :return: run mlrun function object
    """

    import iguazioig
    _module_path = os.path.dirname(iguazioig.__file__)
    templatefile = "%s/templates/processing_template_%s.ipynb"% (_module_path,apiversion)

    return code_to_function(name, project=project,
                         filename=templatefile, kind='nuclio')

    
        

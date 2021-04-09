import os
from mlrun import code_to_function


def composer(apiversion='v3', name='', project='iguazioig'):
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

    template_file = f"{_module_path}/templates/processing_template_{apiversion}.py"
    if not os.path.isfile(template_file):
        template_file = f"{_module_path}/templates/processing_template_{apiversion}.ipynb"

    return code_to_function(name, project=project,
                            filename=template_file, kind='nuclio')

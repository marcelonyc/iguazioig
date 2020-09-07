def composer(apiversion=apiversion,name='',project='default'):
    """Run a task on function/code (.py, .ipynb or .yaml) locally,
    e.g.:
       # define template
       task = get_template(name='myfunction', project='myproject')

    :param name:     function name
    :param project:  function project (none for 'default')
    :return: run mlrun function object
    """

    import iguazioig
    self.apiversion = apiversion
    _module_path = os.path.dirname(iguazioig.__file__)
    templatefile = "%s/processing_template_%s"% (_module_path,apiversion)

    return code_to_function(name, project=project,
                         filename=templatefile, kind='nuclio')

    
        

class Composer:
    
    def __init__(self,apiversion):
        import iguazioig
        self.apiversion = apiversion
        _module_path = os.path.dirname(iguazioig.__file__)
        self.templatefile = "{_module_path}/processing_template_{self.apiversion}"

    def get_template(self,name='',project='default'):
        """Run a task on function/code (.py, .ipynb or .yaml) locally,
        e.g.:
           # define template
           task = get_template(name='myfunction', project='myproject')
        
        :param name:     function name
        :param project:  function project (none for 'default')
        :return: run mlrun function object
        """
        return code_to_function(name, project=project,
                         filename=self.templatefile, kind='nuclio')

    
        

class ExternalClass:
    
    def invoke_ec(self,context,message):
        context.logger.debug("INVOKE EXT %s"% message)
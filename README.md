# Project Summary

This project provides a framework to construct model inference execution pipelines in Iguazio. Using Nuclio for serverless functions and v3io streams to distribute the workloads. 

To build the inference pipeline, you construct a yaml file with all the code dependencies and step layout. 

We use Jupyter Notebooks as templates for Nuclio functions code. This templates simplify the deployment. There is only one template in this version. It has these requirements for the application code. 

* Python code needs to conform to this template. Meaning: 
    * Taking parameters from environment variables
    * Single python class
    * One processing function in the class
    * An optional post processing functions
* Python code takes the full stream message as a dictionary and outputs a dictionary the output stream.
* For the execution of multiple data processing streams that need merged results, the "stream converge logic" needs to be built into one of the functions.

# Code templates

## Application Class
<code>
import mymodule
class MyModelClass:

      def __init__(self):
          self.model = os.getenv("MODEL_PATH")
          
      def load_model(self):
          mymodule.load_model(self.model)
          
      def postprocess(self):
          print("Process done")          
</code>

### In the examples directory:

My example has a last step that keeps a dictionary in memory and waits for two messages for a partition key before writing to a file. For this to work, the "converge" runs with one stream worker  per replica. Scale with replicas.

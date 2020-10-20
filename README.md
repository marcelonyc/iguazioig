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
```python
import mymodule
class MyModelClass:

      def __init__(self):
          self.model = os.getenv("MODEL_PATH")
          
      def load_model(self):
          mymodule.load_model(self.model)
          
      def postprocess(self):
          print("Process done")          
```

### In the examples directory:

My example has a last step that keeps a dictionary in memory and waits for two messages for a partition key before writing to a file. For this to work, the "converge" runs with one stream worker  per replica. Scale with replicas.


# API 0.2.0

## Usage

Significant changes were made in this API. For inference graph reference 
please look under tests/utils/api020/inference_graph.yml.

A few things to note when using:

* You must specify a class module and class name
* You must specify a list of methods (at least one) to be called on your 
  class sequentially (output of previous method is input to next)
* Arbitrary class init may be specified for a class 
* You may specify a "dry run" deployment which will tell you what will take place if deployed

To deploy:

```python
import iguazioig
deployment = iguazioig.Deployment(api_version='0.2.0')
deployment.deploy(inference_graph='<path to your yaml>')
```

## Development

The key concept is now that of a Deployer. To make your deployer API, subclass BaseDeployer in the 
base_deployer module. Also, add your api version and deployer to the Deployment class. If you need a different template, 
then add it under the templates directory. Critically, deployers do work externally through their client. Two clients 
currently exist in the base_deployer module, and others can easily be made by subclassing from the abstract base
class DeployerClient. Where appropriate, doc strings were added only to the base class.

Mor testing needs to be added, as there is only one real test right now. When testing, it is recommended to specify
dry run and use the returned list of commands to verify functionality.
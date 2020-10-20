import os
import yaml
from pathlib import Path
from typing import Union, List

from iguazioig.api_0_2_0 import Deployer as Deployer0_2_0


class Deployment:

    def __init__(self, api_version: str) -> None:
        self.api_version = api_version
        self.deployer_class = self._set_deployer()

    def _set_deployer(self):
        """
        Sets the deployer class using the api_version attribute

        Returns
        -------
        Deployer
        """

        if self.api_version == '0.2.0':
            return Deployer0_2_0
        else:
            raise Exception(f'Unsupported Deployment API version {self.api_version}')

    def deploy(self,
               inference_graph: Union[str, Path],
               recreate_data: bool = False,
               include_functions: List[str] = None,
               dry_run: bool = False,
               credentials_path: Union[str, Path] = None,
               *args,
               **kwargs) -> Union[None, str]:
        """
        Deploys inference graph from yaml specification

        Parameters
        ----------
        inference_graph : str or pathlib.Path
            Path to a yaml file describing deployment
        recreate_data : bool, optional
            Boolean indicating whether to delete the project data directory before deployment (default is False)
        include_functions: list, optional
            List of functions in the inference graph to selectively deploy (default is None deploying all functions)
        dry_run: bool, optional
            Boolean indicating if the deployment should describe the deployment plan rather than deploy
        credentials_path: str or pathlib.Path, optional
            Path to a yaml file with two fields: user_name and password with iguazio credentials to delete functions

        Returns
        -------
        None or str
        """

        if recreate_data and credentials_path is None:
            raise Exception('Recreating data requires function deletion first - specify a path to credentials')
        elif recreate_data:
            with open(credentials_path, 'r') as f:
                credentials = yaml.load(f, Loader=yaml.SafeLoader)

        deployer = self.deployer_class(inference_graph=inference_graph,
                                       recreate_data=recreate_data,
                                       include_functions=include_functions,
                                       credentials=credentials)

        deployment = deployer(dry_run=dry_run, *args, **kwargs)

        return deployment

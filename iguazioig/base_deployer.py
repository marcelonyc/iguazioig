import os
import yaml
import warnings
import requests
import urllib3
from abc import ABC
from pathlib import Path
from typing import Dict, Union, List, Any
from v3io.dataplane import Client


class BaseDeployer:

    """
    Deployers handle the actual yaml parsing and deployment - they should be named through semantic versioning
    """
    def __init__(self):
        self.client: Union[DeployerClient, None] = None
        self.recreate_data = False
        self.include_functions = None
        self.credentials = None

    def setup_streams(self) -> Dict[str, Any]:
        raise NotImplementedError

    def setup_functions(self, stream_specs: Dict) -> None:
        raise NotImplementedError

    def __call__(self, dry_run: bool = False, *args, **kwargs) -> Union[None, List[str]]:
        """
        Invoke a deployment, optionally describe deployment plan

        Parameters
        ----------
        dry_run: bool, optional
            Boolean indicating if the deployment should return a list of commands describing the deployment plan

        Returns
        -------
        None or list of deployment steps if dry_run

        """
        if self.recreate_data:
            warnings.warn('Recreating data will delete ALL functions before the data is recreated')
            if self.include_functions is not None:
                warnings.warn(f'Only the following functions will be restored {self.include_functions}')

        self.client = IguazioClient(self.credentials) if not dry_run else DryRunClient(self.credentials)
        stream_specs = self.setup_streams()
        self.setup_functions(stream_specs)
        print('Deployment complete')
        return self.client.dry_run if dry_run else None

    @staticmethod
    def _read_inference_graph(path: Union[str, Path]) -> Dict:
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    @staticmethod
    def _sluggify_name(name: str) -> str:
        return name.lower().strip().replace(" ", "-").replace("_", "-")

    @staticmethod
    def _snakeify_name(name: str) -> str:
        return name.lower().strip().replace(" ", "_").replace("-", "_")

    def _make_stream_path(self, project_name, stream_name):
        return f'/{project_name}/streams/{self._sluggify_name(stream_name)}'


class DeployerClient(ABC):

    def create_stream(self, stream_name: str, stream_spec: Dict) -> None:
        pass

    def delete_stream(self, stream_name: str, stream_spec: Dict) -> None:
        pass

    def create_function(self,  name: str, project_name: str, mlrun_function) -> None:
        pass

    def delete_function(self,  name: str, project_name: str) -> None:
        pass


class IguazioClient(DeployerClient):

    def __init__(self, credentials):
        self.client = Client()
        self.credentials = credentials

    def create_stream(self, stream_name: str, stream_spec: Dict) -> None:
        try:
            self.client.stream.create(
                container=stream_spec['container'],
                stream_path=stream_spec['path'],
                shard_count=stream_spec['shards'],
                access_key=os.environ['V3IO_ACCESS_KEY'],
                retention_period_hours=stream_spec['retention'],
                raise_for_status=[200, 204, 409]  # 409 is stream already exists
            )
        except Exception as e:
            raise Exception(f'Stream creation for stream {stream_name} failed with error: {e}')

    def delete_stream(self, stream_name: str, stream_spec: Dict) -> None:

        try:
            self.client.stream.delete(
                container=stream_spec['container'],
                stream_path=stream_spec['path'],
                access_key=os.environ['V3IO_ACCESS_KEY'],
                raise_for_status=[200, 204, 404, 400]  # don't worry if nothing to delete
            )
        except Exception as e:
            raise Exception(f'Stream deletion for stream {stream_name} failed with error: {e}')

    def create_function(self, name: str, project_name: str, mlrun_function) -> None:
        try:
            address = mlrun_function.deploy(project=project_name)
            print(f'Function {name} was deployed into project {project_name} at {address}')
        except Exception as e:
            raise Exception(f'Function {name} failed to deploy with error: {e}')

    def delete_function(self, name: str, project_name: str) -> None:
        warnings.warn('Deleting functions is reserved for on cluster due to internal DNS requirements')

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        session = requests.Session()
        try:
            session.auth = (self.credentials['username'], self.credentials['password'])
        except KeyError as e:
            raise Exception(f'To delete functions, which is required for stream recreation, you must set credentials '
                            f'in the environment as IGUAZIO_USERNAME and IGUAZIO_PASSWORD: {e} is missing')

        api_url = 'https://datanode-dashboard/api'
        auth = session.post(f'{api_url}/sessions', verify=False)
        function_name = f'{project_name}-{name}'
        response = session.delete(f'{api_url}/functions/{function_name}')

        if not response.ok:
            raise Exception(f'Function deletion failed with code {response.status_code} and message {response.text}')


class DryRunClient(DeployerClient):

    def __init__(self, credentials):
        _ = credentials
        self.dry_run = []

    def add_command(self, command: str) -> None:
        print(command)
        self.dry_run.append(command)

    def create_stream(self, stream_name: str, stream_spec: Dict) -> None:

        cmd = (f'Create stream {stream_name} - '
               f'container: {stream_spec["container"]} '
               f'path: {stream_spec["path"]} '
               f'shards: {stream_spec["shards"]} '
               f'retention: {stream_spec["retention"]}')

        self.add_command(cmd)

    def delete_stream(self, stream_name: str, stream_spec: Dict) -> None:

        cmd = (f'Delete stream {stream_name} - '
               f'container: {stream_spec["container"]} '
               f'path: {stream_spec["path"]} '
               f'shards: {stream_spec["shards"]} '
               f'retention: {stream_spec["retention"]}')

        self.add_command(cmd)

    def create_function(self, name: str, project_name: str, mlrun_function) -> None:

        cmd = f'Create function {name} in project {project_name} - {mlrun_function.to_dict()}'
        self.add_command(cmd)

    def delete_function(self, name: str, project_name: str) -> None:
        cmd = f'Delete function {name} in project {project_name}'
        self.add_command(cmd)

import asyncio, logging
from typing import List, Any, Optional
import aiohttp

class TowerApiError(Exception):
    pass

RECOGNIZED_PLATFORMS = {'aws-batch', 'google-lifesciences', 'google-batch', 'azure-batch', 'k8s-platform', 'eks-platform', 
                             'gke-platform', 'uge-platform', 'slurm-platform', 'lsf-platform', 'altair-platform', 'moab-platform'}

'''
Simplified Nextflow Tower API, does not provide control over all parameters.
In particular, compute environment fields beyond the platform, username and hostname.
The Tower CLI offers a powerful alternative for uploading entire configuration objects.
BUG: calling "put" on credentials when migrating between ssh/agent methods fails silently
BUG: putting config_text globally on compute env doesn't seem to apply to pipelines
'''
class TowerApi:
    def __init__(self, endpoint: str, bearer: str, workspace: Optional[str]):
        self._endpoint = endpoint
        self._headers = {'Authorization': f'Bearer {bearer}'}
        self._params = {'workspaceId': workspace} if workspace else {}
        self._semaphore = asyncio.Semaphore(20)

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_):
        await self._session.close()

    async def _throttled(self):
        await asyncio.sleep(1/20)

    async def _handle_get_json(self, subpath, headers={}, params={}) -> dict:
        async with self._semaphore:
            await self._throttled()
            logging.debug(f'get /{subpath}')
            async with self._session.get(
                f'{self._endpoint}/{subpath}', headers={**self._headers, **headers}, params={**self._params, **params}
            ) as response:
                if round(response.status / 100) != 2:
                    raise TowerApiError(
                        f'request to "{response.url}" returned a non 200 status code of {response.status}'
                    )
                return await response.json()

    async def _handle_json_post_json(self, subpath, json, headers={}, params={}, expected_status_code=204) -> Optional[dict]:
        async with self._semaphore:
            await self._throttled()
            logging.debug(f'post /{subpath}')
            headers = {**self._headers, **headers}
            async with self._session.post(
                f'{self._endpoint}/{subpath}', json=json, headers=headers, params={**self._params, **params}
            ) as response:
                if response.status != expected_status_code:
                    raise TowerApiError(
                        f'post request to "{response.url}" returned unexpected status code '
                        f'{response.status}. {expected_status_code} was expected'
                    )
                if expected_status_code == 200: 
                    return await response.json()

    async def _handle_json_delete_json(self, subpath, headers={}, params={}, expected_status_code=204) -> Optional[dict]:
        async with self._semaphore:
            await self._throttled()
            logging.debug(f'delete /{subpath}')
            headers = {**self._headers, **headers}
            async with self._session.delete(
                f'{self._endpoint}/{subpath}', headers=headers, params={**self._params, **params}
            ) as response:
                if response.status != expected_status_code:
                    raise TowerApiError(
                        f'delete request to "{response.url}" returned unexpected status code '
                        f'{response.status}. {expected_status_code} was expected'
                    )
                if expected_status_code == 200: 
                    return await response.json()

    async def _handle_json_put_json(self, subpath, json, headers={}, params={}, expected_status_code=204) -> Optional[dict]:
        async with self._semaphore:
            await self._throttled()
            logging.debug(f'put /{subpath}')
            headers = {**self._headers, **headers}
            async with self._session.put(
                f'{self._endpoint}/{subpath}', json=json, headers=headers, params={**self._params, **params}
            ) as response:
                if response.status != expected_status_code:
                    raise TowerApiError(
                        f'put request to "{response.url}" returned unexpected status code '
                        f'{response.status}. {expected_status_code} was expected'
                    )
                if expected_status_code == 200: 
                    return await response.json()
                
    async def get_user_id(self) -> str:
        return (await self._handle_get_json('user-info'))['user']['userName']

    async def get_workspace_id_by_name(self, user_id: str, workspace_name: str) -> Optional[str]:
        orgs_and_workspaces = (await self._handle_get_json(f'user/{user_id}/workspaces'))['orgsAndWorkspaces']
        return next((x for x in orgs_and_workspaces if x['workspaceName'] == workspace_name), {}).get('workspaceId')

    async def get_compute(self) -> List[Any]:
        return (await self._handle_get_json('compute-envs'))['computeEnvs']

    def get_compute_id_by_primary(self, compute_envs: List[Any]):
         return next((x for x in compute_envs if x['primary'] == True), {}).get('id')

    def get_compute_id_by_name(self, compute_envs: List[Any], compute_name: str) -> Optional[str]:
        return next((x for x in compute_envs if x['name'] == compute_name), {}).get('id')

    async def get_credentials(self) -> List[Any]:
        return (await self._handle_get_json('credentials'))['credentials']

    def get_credentials_id_by_name(self, credentials: List[Any], credentials_name: str) -> Optional[str]:
        return next((x for x in credentials if x['name'] == credentials_name), {}).get('id')

    async def get_credentials_id_by_compute(self, compute_id: str) -> Optional[str]:
        return (await self._handle_get_json(f'compute-envs/{compute_id}'))['credentialsId']

    async def add_credentials_ssh(self, credentials_name: str, credentials_description: str, existing_id: str, ssh_key: str) -> str:
        credential_data = {
            'credentials': {
                **({'id': existing_id} if existing_id else {}),
                'name': credentials_name,
                'description': credentials_description,
                'provider': 'ssh',
                'keys': {
                    'privateKey': ssh_key,
                    'passphrase': ''
                }
            }
        }
        if existing_id:
            await self._handle_json_put_json(f'credentials/{existing_id}', credential_data, expected_status_code=204)
            return existing_id
        return (await self._handle_json_post_json('credentials', credential_data, expected_status_code=200))['credentialsId']
        
    async def add_credentials_agent(self, credentials_name: str, credentials_description: str, workdir: str, existing_id: str, agent_connection_id: str) -> str:
        credential_data = {
            'credentials': {
                **({'id': existing_id} if existing_id else {}),
                'name': credentials_name,
                'description': credentials_description,
                'provider': 'tw-agent',
                'keys': {
                    'connectionId': agent_connection_id,
                    'workDir': workdir,
                    'shared': False
                }
            }
        }
        if existing_id:
            await self._handle_json_put_json(f'credentials/{existing_id}', credential_data, expected_status_code=204)
            return existing_id
        return (await self._handle_json_post_json('credentials', credential_data, expected_status_code=200))['credentialsId']

    async def remove_credentials(self, credentials_id: str):
        await self._handle_json_delete_json(f'credentials/{credentials_id}')
 
    async def add_compute(self, compute_name: str, compute_description: str, compute_platform: str, compute_host: str, compute_user: str, credentials_id: str, workdir: str, existing_id: str) -> str:
        compute_data = {
            'computeEnv': {
                **({'id': existing_id} if existing_id else {}),
                'name': compute_name,
                'description': compute_description,
                'credentialsId': credentials_id,
                'platform': compute_platform,
                'config' : {
                    'workDir': workdir,
                    'userName': compute_user,
                    'hostName': compute_host,
                }
            }
        }
        if existing_id:
            await self._handle_json_put_json(f'compute-envs/{existing_id}', compute_data, expected_status_code=204)
            return existing_id
        return (await self._handle_json_post_json('compute-envs', compute_data, expected_status_code=200))['computeEnvId']

    async def make_compute_primary(self, compute_id: str) -> bool:
        await self._handle_json_post_json(f'compute-envs/{compute_id}/primary', {})

    async def remove_compute(self, compute_id: str) -> bool:
        await self._handle_json_delete_json(f'compute-envs/{compute_id}')

    async def add_pipeline(self, pipeline_name: str, pipeline_description: str, pipeline_icon: str, pipeline_url: str, compute_id: str, label_ids: List[str],
                    config_text: str, prerun_text: str, workdir: str, profiles: List[str] = [], existing_id: Optional[str]=None) -> str:
        pipeline_data = {
            'name': pipeline_name,
            'description': pipeline_description,
            'icon': pipeline_icon,
            'launch': {
                'computeEnvId': compute_id,
                'pipeline': pipeline_url,
                'workDir': workdir,
                'pullLatest': True,
                'configText': config_text,
                'configProfiles': profiles,
                'preRunScript': prerun_text,
            },
            'labelIds': label_ids
        }

        if existing_id:
            await self._handle_json_put_json(f'pipelines/{existing_id}', pipeline_data, expected_status_code=200)
            return existing_id
        return (await self._handle_json_post_json('pipelines', pipeline_data, expected_status_code=200))['pipeline']['pipelineId']

    async def remove_pipeline(self, pipeline_id: str):
        await self._handle_json_delete_json(f'pipelines/{pipeline_id}')

    async def get_pipelines(self, attributes: List[str]=[]) -> List[Any]:
        return (await self._handle_get_json('pipelines', params={'attributes': ','.join(attributes)}))['pipelines']

    async def get_labels(self) -> List[Any]:
        return (await self._handle_get_json('labels', params={'max':9999}))['labels']

    async def add_label(self,name: str) -> str:
        label_data = {
            'id': None,
            'name': name
        }
        return (await self._handle_json_post_json('labels', label_data, expected_status_code=200))['id']

    async def remove_label(self, label_id: str):
        await self._handle_json_delete_json(f'labels/{label_id}')

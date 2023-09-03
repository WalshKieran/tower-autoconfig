import asyncio, logging, urllib, json
from typing import List, Optional

from tower_autoconfig.api import TowerApi, TowerApiError
from tower_autoconfig.utils import create_ssh_key, delete_ssh_key
from tower_autoconfig.agent import TowerAgentTimeout

class TowerAutoconfigError(Exception):
    pass

'''
Provides methods for composite Tower API operations - setting up and tearing down ssh/agent compute environments and pipelines
Can self-authenticate using a time-limited SSH key or by starting the Tower Agent (requires Java).
Can add user-provided nf-core pipelines concurrently with icons, descriptions and labels.
Could simplify futher by enforcing name==id for compute/credentials, since names are already considered unique.
'''
class TowerAutoconfig:
    def __init__(self, server: str, bearer: str, workspace_id: Optional[str]=None):
        self.server = server
        self.endpoint = f'https://{server}/api'
        self.workspace_id = workspace_id
        self.api = TowerApi(self.endpoint, bearer, workspace_id)
        self.ssh_key_comment = f'mykey:{self.server}'

    async def __aenter__(self):
        await self.api.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.api.__aexit__(exc_type, exc_value, traceback)

    async def prepare(self, compute_name: str, credentials_name: str, new_pipelines: List[str], pipeline_suffix: str, shouldForce: bool):
        
        # Fetch everything that is needed, concurrently
        skipPipe = (new_pipelines is None)
        user, compute_envs, credentials, pipelines, labels = (
                await asyncio.gather(
                    *[self.api.get_user_id(), self.api.get_compute(), self.api.get_credentials()], 
                    *([] if skipPipe else [self.api.get_pipelines(['labels']), self.api.get_labels()])
                )
            ) + ([None, None] if skipPipe else [])
        
        compute_id = self.api.get_compute_id_by_name(compute_envs, compute_name)
        compute_primary_id = self.api.get_compute_id_by_primary(compute_envs)
        credentials_id = self.api.get_credentials_id_by_name(credentials, credentials_name)

        # Optional pipeline metadata
        pipeline_ret = [None] * 7
        if not skipPipe:
            pipeline_suffix_len = len(pipeline_suffix)
                
            remote_pipelines = json.loads(urllib.request.urlopen('https://nf-co.re/pipelines.json').read().decode())['remote_workflows']
            remote_pipelines_dict = {p['full_name']: p for p in remote_pipelines}

            new_pipeline_dict = {}
            for p in new_pipelines:
                revisionless = p.split('@', 1)[0]
                if revisionless in remote_pipelines_dict:
                    new_pipeline_dict[revisionless] = p
                else:
                    logging.warn(f'Pipeline "{p}" skipped, not valid nf-core pipeline')                

            pipeline_name_to_id = dict()
            for p in pipelines:
                if p['name'].endswith(pipeline_suffix):
                    fixed_name = p['fixed_name'] = 'nf-core/' + p['name'][:-pipeline_suffix_len]
                    pipeline_name_to_id[fixed_name] = p['pipelineId']

            label_name_to_id = {l['name']: l['id'] for l in labels}
        
            pipelines_add_revisionless = new_pipeline_dict.keys() if shouldForce else (new_pipeline_dict.keys() - pipeline_name_to_id.keys())
            pipelines_add = [new_pipeline_dict[p] for p in pipelines_add_revisionless]
            pipelines_remove = pipeline_name_to_id.keys() - new_pipeline_dict.keys()

            old_label_set = {label['name'] for p in pipelines if 'fixed_name' in p and p['fixed_name'] in pipelines_remove for label in p['labels']}
            new_label_set = {topic for p in new_pipeline_dict.keys() for topic in remote_pipelines_dict[p]['topics']}
            labels_remove = old_label_set - new_label_set
            labels_add = new_label_set - label_name_to_id.keys()

            pipeline_ret = (
                pipelines_add, list(pipelines_remove), pipeline_name_to_id, remote_pipelines_dict,
                list(labels_add), list(labels_remove), label_name_to_id
            )
        
        return (user, compute_id, compute_primary_id, credentials_id, *pipeline_ret)

    def clean_ssh(self):
        delete_ssh_key(self.ssh_key_comment)

    async def setup_ssh(self, compute_name: str, compute_description: str, compute_platform: str, compute_host: str, compute_user: str, compute_queue_options: str, workdir: str,
        credentials_name: str, credentials_description: str, compute_id: Optional[str]=None, credentials_id: Optional[str]=None, shouldForce: bool=False, ssh_restrictions: str='') -> str:

        if not compute_id or shouldForce:
            if not credentials_id or shouldForce:
                ssh_key = create_ssh_key(self.ssh_key_comment, ssh_restrictions)
                credentials_id = await self.api.add_credentials_ssh(credentials_name, credentials_description, credentials_id, ssh_key)

            try:
                compute_id = await self.api.add_compute(compute_name, compute_description, compute_platform, compute_host, compute_user, compute_queue_options, credentials_id, workdir, compute_id)
            except TowerApiError as e:
                self.clean_ssh()
                await self.api.remove_credentials(credentials_id)
                raise TowerAutoconfigError("Failed to create compute environment - this usually means this machine isn't open to the internet. Try specifying --node to the main login node, or use the agent method.") from e

        return compute_id, credentials_id

    async def setup_agent(self, compute_name: str, compute_description: str, compute_platform: str, compute_host: str, compute_user: str, compute_queue_options: str, workdir: str,
        credentials_name: str, credentials_description: str, compute_id: Optional[str]=None, credentials_id: Optional[str]=None, shouldForce: bool=False, agent_connection_id: str='autoconfigured', bearer: str=None) -> str:

        if not compute_id or shouldForce:            
            # Start agent on hard-coded connection ID
            async with TowerAgentTimeout(agent_connection_id, workdir, self.endpoint, 300, bearer=bearer) as agent:
                # Create matching credential and/or compute while agent is running
                if not credentials_id or shouldForce:
                    credentials_id = await self.api.add_credentials_agent(credentials_name, credentials_description, workdir, credentials_id, agent_connection_id)
                compute_id = await self.api.add_compute(compute_name, compute_description, compute_platform, compute_host, compute_user, compute_queue_options, credentials_id, workdir, compute_id)

        return compute_id, credentials_id
    
    async def setup_pipelines(self, compute_id, pipelines_add, pipeline_name_to_id, label_name_to_id, remote_pipelines_dict, pipeline_suffix, config_text, prerun_text, workdir, profiles):
        tasks = []
        description_suffix = ' (NOTE: if you rename this, tower_autoconfig cannot clean it)'
        for p in pipelines_add:

            # Extract out revision from name
            split = p.split('@', 1)
            p = split[0]
            revision = split[1] if len(split) > 1 else None

            icon = 'https://avatars.githubusercontent.com/u/35520196?s=40&v=4' if p.startswith('nf-core/') else ''
            remote = remote_pipelines_dict[p]
            pipeline_url = 'https://github.com/' + remote['full_name']
            label_ids = [label_name_to_id[t] for t in remote['topics']]
            description = remote['description'] + description_suffix
            existing_id = pipeline_name_to_id.get(p)
            tasks.append(self.api.add_pipeline(p.split('/', 1)[-1] + pipeline_suffix, revision, description, icon, pipeline_url, compute_id, label_ids, config_text, prerun_text, workdir, profiles, existing_id))
        await asyncio.gather(*tasks)

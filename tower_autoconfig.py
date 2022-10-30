from genericpath import isfile
import os, sys, requests, subprocess, tempfile, shutil
import datetime
from pathlib import Path

class TowerApi:
    def __init__(self,endpoint : str, bearer: str, params: dict ):
        self._endpoint = endpoint
        self._headers = {'Authorization': f'Bearer {bearer}'}
        self._params = params

    def get_user_id(self):
        return requests.get(self._endpoint + f'/user-info', headers=self._headers).json()["user"]["id"]

    def get_workspace_id_by_name(self, user_id: str, workspace_name: str): #e.g. 'UNSW_Sydney'
        orgs_and_workspaces = requests.get(self._endpoint + f'/user/{user_id}/workspaces',
                                         headers=self._headers).json()["orgsAndWorkspaces"]
        return next((x for x in orgs_and_workspaces if x["workspaceName"] == workspace_name), {}).get("workspaceId", None)

    def get_compute_id_by_name(self, compute_name: str): #e.g. KATANA_COMPUTE_NAME
        compute_envs = requests.get(self._endpoint + "/compute-envs",
                                   headers=self._headers,
                                   params=self._params).json()["computeEnvs"]
        return next((x for x in compute_envs if x["name"] == compute_name), {}).get("id", None)

    def get_credentials_id_by_name(self, credentials_name: str): #e.g. KATANA_CREDENTIAL_NAME
        credentials = requests.get(self._endpoint + "/credentials",
                                   headers=self._headers,
                                   params=self._params).json()["credentials"]
        return next((x for x in credentials if x["name"] == credentials_name), {}).get("id", None)

    def get_credentials_id_by_compute(self, compute_id: str):
        return requests.get(self._endpoint + f'/compute-envs/{compute_id}',
                            headers=self._headers,
                            params=self._params).json()["credentialsId"]

    def add_credentials(self, credentials_name: str, private_key: str): #e.g. KATANA_CREDENTIAL_NAME
        credential_data = {
            "credentials": {
                "name": credentials_name,
                "description": "UNSW Sydney HPC personal compute credentials. Please do not edit.",
                "provider": "ssh",
                "keys": {
                    "privateKey": private_key,
                    "passphrase": ""
                }
            }
        }
        response = requests.post(self._endpoint + "/credentials",
                                 json=credential_data,
                                 headers=self._headers,
                                 params=self._params)

        if response.status_code != 200: raise Exception(response.json())
        return response.json()["credentialsId"]

    def remove_credentials(self, credentials_id: str):
        return requests.delete(self._endpoint + f'/credentials/{credentials_id}',
                               headers=self._headers,
                               params=self._params).status_code == 204

    def add_compute(self, compute_name: str, credentials_id: str, katana_username: str):
        #e.g. KATANA_COMPUTE_NAME
        compute_data = {
            "computeEnv": {
                "name": compute_name,
                "description": "UNSW Sydney HPC personal compute environment. Please do not edit.",
                "credentialsId": credentials_id,
                "platform": "altair-platform",
                "config" : {
                    "workDir": f'/srv/scratch/{katana_username}',
                    "userName": katana_username,
                    "hostName": "katana.unsw.edu.au",
                    "headQueue": "@kman",
                }
            }
        }

        response = requests.post(self._endpoint + "/compute-envs",
                                 json=compute_data,
                                 headers=self._headers,
                                 params=self._params)
        if response.status_code != 200:
            raise Exception(response.json())
        return response.json()["computeEnvId"]

    def make_compute_primary(self, compute_id: str):
        response = requests.post(self._endpoint + f'/compute-envs/{compute_id}/primary',
                                 headers=self._headers,
                                 params=self._params)
        return response.status_code == 204

    def remove_compute(self, compute_id: str):
        return requests.delete(self._endpoint + f'/compute-envs/{compute_id}',
                               headers=self._headers,
                               params=self._params).status_code == 204

    def add_workflow(self, compute_id: str, url: str, wf: dict, labels_id: str, config_text: str, katana_username: str):
        pipeline_data = {
            "name": wf["name"],
            "description": wf["description"],
            "icon": "https://avatars.githubusercontent.com/u/35520196?s=200&v=4",
            "launch": {
                "computeEnvId": compute_id,
                "pipeline": url,
                "workDir": f'/srv/scratch/{katana_username}',
                "pullLatest": True,
                "configText": config_text,
                #"configProfiles": ["test"],
            },
            "labelsIds": labels_id
        }

        response = requests.post(self._endpoint + "/pipelines",
                                 json=pipeline_data,
                                 headers=self._headers,
                                 params=self._params)

        if response.status_code != 200:
            raise Exception(response.json())
    
    def remove_workflows_if(self, filter_func, should_clean_labels: bool=True):
        label_counts = {}
        workflows = requests.get(self._endpoint + "/pipelines",
                                 headers=self._headers,
                                 params={**self._params,
                                         **{'attributes': 'labels'}}).json()["pipelines"]
        for wf in workflows:
            should_filter = filter_func(wf)

            if should_filter:
                requests.delete(self._endpoint + f'/pipelines/{wf["pipelineId"]}',
                                headers=self._headers,
                                params=self._params)

            if should_clean_labels:
                for label in wf["labels"]:
                    label_counts[label["id"]] = label_counts.get(label["id"], 0) + (0 if should_filter else 1)
    
        if should_clean_labels:
            for key, value in label_counts.items():
                if value == 0:
                    self.remove_label(key)

    def add_top_nfcore_workflows(self, compute_id: str, label_id: str, count: int =1):
        config_text = ""
        if os.path.isfile('nextflow.config'):
            with open('nextflow.config', 'r') as f:
                config_text = f.read()

        remote_workflows = requests.get("https://nf-co.re/pipelines.json").json()["remote_workflows"]
        remote_workflows_published = list(filter(lambda wf: wf.get("releases", False), remote_workflows))

        # NOTE: Annoyingly, pipeline names must be unique across organization
        # (e.g. A pipeline with name 'rnaseq' already exists in another workspace of this organization)
        for wf in remote_workflows_published: wf["name"] += "_unsw"

        top = sorted(remote_workflows_published,
                     reverse=True,
                     key=lambda x: x.get("stargazers_count", 0))[:count]
        names = { wf["name"] : True for wf in remote_workflows_published}

        # TODO: do put instead of replace to keep same pipelineIds, or re-add as same id
        self.remove_workflows_if(lambda tower_wf: tower_wf["name"] in names)

        topics_to_labels = {}
        for wf in top:
            topic_label_ids = [topics_to_labels.setdefault(t, self.get_label_id_by_name(t) or self.add_label(t)) for t in wf["topics"]]
            self.add_workflow(compute_id, wf["html_url"], wf, [label_id] + topic_label_ids, config_text)

    def get_label_id_by_name(self, label_name: str):
        # NOTE: all /labels endpoints are undocumented, but you can't get computeId for pipelines after they are defined so a label helps us track automanaged
        labels = requests.get(self._endpoint + f'/labels',
                              headers=self._headers,
                              params={**self._params, **{"search": label_name}}).json()["labels"]
        return labels[0]["id"] if len(labels) else None

    def add_label(self,name: str):
        label_data = {
            "id": None,
            "name": name
        }

        response = requests.post(self._endpoint + "/labels",
                                 json=label_data,
                                 headers=self._headers,
                                 params=self._params)
        if response.status_code != 200: raise Exception(response.json())
        return response.json()["id"]

    def remove_label(self, label_id: str):
        return requests.delete(self._endpoint + f'/labels/{label_id}',
                               headers=self._headers,
                               params=self._params).status_code == 204

    def get_or_create_credentials(self, credentials_name: str):
        return self.get_credentials_id_by_name(credentials_name) or \
               self.add_credentials(credentials_name, create_key('tower@biocommons'))

    def get_or_create_compute(self, compute_name: str, credentials_name):
        return self.get_compute_id_by_name(compute_name) or \
               self.add_compute(compute_name, self.get_or_create_credentials(credentials_name))

    def setup(self, compute_name: str, credentials_name: str, label_name: str):
        compute_id = self.get_or_create_compute(compute_name, credentials_name)
        self.make_compute_primary(compute_id)
        label_id = self.get_label_id_by_name(label_name) or self.add_label(label_name)
        self.add_top_nfcore_workflows(compute_id, label_id)

    def clean(self, compute_name: str, credentials_name: str, label_name: str):
        # Clean all pipelines with label
        # NOTE: Compute and pipelines have labels, could get credentials from compute.
        # Currently just used for pipelines
        label_id = self.get_label_id_by_name(label_name)
        if label_id is not None:
            self.remove_workflows_if(lambda wf: any(label["id"] == label_id for label in wf["labels"]))
            self.remove_label(label_id)

        # Clean compute
        compute_id = self.get_compute_id_by_name(compute_name)
        if compute_id is not None:
            self.remove_compute(compute_id)

        # Clean credentials
        credentials_id = self.get_credentials_id_by_name(credentials_name)
        if credentials_id is not None:
            self.remove_credentials(credentials_id)


def create_key(key_comment, dry_run=False): #e.g. 'tower@biocommons'
    global SSH_FROM_RESTRICTIONS

    privateKey = None
    originalAuthName = os.path.expanduser('~/.ssh/authorized_keys')

    # Copy existing authorized public keys into memory
    existing = []
    if os.path.isfile(originalAuthName):
        with open(originalAuthName, 'r') as originalAuth:
            for line in originalAuth:
                if line and not line.isspace() and not line.rstrip().endswith(' ' + key_comment):
                    existing.append(line)

    # Possibly safer to use same file system for temporary files over NFS
    same_fs_tmp = os.path.expanduser('~/.tower')
    Path(same_fs_tmp).mkdir(exist_ok=True, parents=True)

    # Add expiry date if configured
    if SSH_FROM_DAYS_LIMIT is not None: 
        SSH_FROM_RESTRICTIONS += ',expiry-time="%s"' % (datetime.datetime.today() + datetime.timedelta(days=SSH_FROM_DAYS_LIMIT)).strftime('%Y%m%d')

    # Rewrite authorized public keys (NOTE: sh-copy-id has different behaviour when replacing, /tmp and ~/ may be different NFS on HPC, suggestions welcome)
    with tempfile.TemporaryDirectory(dir=same_fs_tmp) as tmpdir:
        p = subprocess.run(['ssh-keygen', '-f', os.path.join(tmpdir, 'tower'), '-N', '', '-t', 'ed25519', '-C', key_comment], stdout=subprocess.DEVNULL)
        if p.returncode: raise Exception("Failed to create key pair")

        new_auth_path = os.path.join(tmpdir, 'authorized_keys')
        with open(new_auth_path, 'w') as newAuth:
            for line in existing: 
                newAuth.write(line)
            with open(os.path.join(tmpdir, 'tower.pub'), 'r') as pub:
                newAuth.write(SSH_FROM_RESTRICTIONS + ' ' + pub.read())

        with open(os.path.join(tmpdir, 'tower'), 'r') as priv: privateKey = priv.read()

        # Atomically update authorized keys
        os.chmod(new_auth_path, 0o644)
        if dry_run:
            with open(new_auth_path, 'r') as newAuth: print("Would have updated authorized keys to: " + newAuth.read())
        else: 
            os.rename(new_auth_path, originalAuthName)
    return privateKey


if __name__ == "__main__":
    MIN_PYTHON = (3, 6)
    if sys.version_info < MIN_PYTHON:
        sys.exit("Python %s.%s or later is required.\n" % MIN_PYTHON)

    if len(sys.argv) != 2 or sys.argv[1] == "--help":
        sys.exit("Usage: tower_autoconfig.py [setup|clean]")

    TOWER_HOST = os.environ.get("TOWER_HOST", "https://tower.nf")

    KATANA_USERNAME = os.environ.get("USER")
    BEARER = os.environ.get("TOWER_ACCESS_TOKEN")
    TOWER_WORKSPACE_ID = os.environ.get("TOWER_WORKSPACE_ID", None)

    KATANA_COMPUTE_NAME = "Katana"
    KATANA_CREDENTIAL_NAME = "katana-auto-" + KATANA_USERNAME
    KATANA_LABEL_NAME = "z-katana-auto-managed"  # Labels are sorted

    #TODO: Tower fails to connect when expiry specified?
    SSH_FROM_DAYS_LIMIT = None  # e.g. 30, None for unlimited -
    #TODO: Tower fails to connect when from specified?
    SSH_FROM_RESTRICTIONS = f'restrict,pty'  # f'restrict,pty,cert-authority,from="{TOWER_HOST}"'

    if BEARER is None:
        sys.exit("Please run export TOWER_ACCESS_TOKEN=<your-token> before setup")

    api = TowerApi("https://{TOWER_HOST}/api/",BEARER, {'workspaceId': TOWER_WORKSPACE_ID} )

    if sys.argv[1] == "setup": api.setup(KATANA_COMPUTE_NAME, KATANA_CREDENTIAL_NAME, KATANA_LABEL_NAME)
    elif sys.argv[1] == "clean": api.clean(KATANA_COMPUTE_NAME, KATANA_CREDENTIAL_NAME, KATANA_LABEL_NAME)
    else: sys.exit("Invalid command")

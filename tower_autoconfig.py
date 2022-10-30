from genericpath import isfile
import os, sys, requests, subprocess, tempfile, shutil
import datetime
from pathlib import Path

TOWER_HOST = os.environ.get("TOWER_HOST", "https://tower.nf")

KATANA_USERNAME = os.environ.get("USER")
BEARER = os.environ.get("TOWER_ACCESS_TOKEN")
TOWER_WORKSPACE_ID = os.environ.get("TOWER_WORKSPACE_ID", None)

KATANA_COMPUTE_NAME = "Katana"
KATANA_CREDENTIAL_NAME = "katana-auto-" + KATANA_USERNAME
KATANA_LABEL_NAME = "z-katana-auto-managed" #Labels are sorted

SSH_FROM_DAYS_LIMIT = None #e.g. 30, None for unlimited - TODO: Tower fails to connect when expiry specified?
SSH_FROM_RESTRICTIONS = f'restrict,pty' #f'restrict,pty,cert-authority,from="{TOWER_HOST}"' TODO: Tower fails to connect when from specified?


class TowerApi:
    def __init__(self,endpoint : str, bearer: str, params: dict ):
        self.ENDPOINT = endpoint
        self.HEADERS = {'Authorization': f'Bearer {bearer}'}
        self.PARAMS = params

    def get_user_id(self):
        return requests.get(self.ENDPOINT + f'/user-info', headers=self.HEADERS).json()["user"]["id"]

    def get_workspace_id_by_name(self, userId: str, workspaceName: str): #e.g. 'UNSW_Sydney'
        orgsAndWorkspaces = requests.get(self.ENDPOINT + f'/user/{userId}/workspaces', headers=self.HEADERS).json()["orgsAndWorkspaces"]
        return next((x for x in orgsAndWorkspaces if x["workspaceName"] == workspaceName), {}).get("workspaceId", None)

    def get_compute_id_by_name(self, computeName: str): #e.g. KATANA_COMPUTE_NAME
        computeEnvs = requests.get(self.ENDPOINT + "/compute-envs", headers=self.HEADERS, params=self.PARAMS).json()["computeEnvs"]
        return next((x for x in computeEnvs if x["name"] == computeName), {}).get("id", None)

    def get_credentials_id_by_name(self, credentialsName: str): #e.g. KATANA_CREDENTIAL_NAME
        credentials = requests.get(self.ENDPOINT + "/credentials", headers=self.HEADERS, params=self.PARAMS).json()["credentials"]
        return next((x for x in credentials if x["name"] == credentialsName), {}).get("id", None)

    def get_credentials_id_by_compute(self, computeId: str):
        return requests.get(self.ENDPOINT + f'/compute-envs/{computeId}', headers=self.HEADERS, params=self.PARAMS).json()["credentialsId"]

    def add_credentials(self, credentialsName: str, privateKey: str): #e.g. KATANA_CREDENTIAL_NAME
        credentialData = {
            "credentials": {
                "name": credentialsName,
                "description": "UNSW Sydney HPC personal compute credentials. Please do not edit.",
                "provider": "ssh",
                "keys": {
                    "privateKey": privateKey,
                    "passphrase": ""
                }
            }
        }
        response = requests.post(self.ENDPOINT + "/credentials", json=credentialData, headers=self.HEADERS, params=self.PARAMS)
        if response.status_code != 200: raise Exception(response.json())
        return response.json()["credentialsId"]

    def remove_credentials(self, credentialsId: str):
        return requests.delete(self.ENDPOINT + f'/credentials/{credentialsId}', headers=self.HEADERS, params=self.PARAMS).status_code == 204

    def add_compute(self, computeName: str, credentialsId: str): #e.g. KATANA_COMPUTE_NAME
        computeData = {
            "computeEnv": {
                "name": computeName,
                "description": "UNSW Sydney HPC personal compute environment. Please do not edit.",
                "credentialsId": credentialsId,
                "platform": "altair-platform",
                "config" : {
                    "workDir": f'/srv/scratch/{KATANA_USERNAME}',
                    "userName": KATANA_USERNAME,
                    "hostName": "katana.unsw.edu.au",
                    "headQueue": "@kman",
                }
            }
        }

        response = requests.post(self.ENDPOINT + "/compute-envs", json=computeData, headers=self.HEADERS, params=self.PARAMS)
        if response.status_code != 200:
            raise Exception(response.json())
        return response.json()["computeEnvId"]

    def make_compute_primary(self,computeId: str):
        response = requests.post(self.ENDPOINT + f'/compute-envs/{computeId}/primary', headers=self.HEADERS, params=self.PARAMS)
        return response.status_code == 204

    def remove_compute(self, computeId: str):
        return requests.delete(self.ENDPOINT + f'/compute-envs/{computeId}', headers=self.HEADERS, params=self.PARAMS).status_code == 204

    def add_workflow(self, computeId: str, url: str, wf: dict, labelsIds: str, configText: str):
        pipelineData = {
            "name": wf["name"],
            "description": wf["description"],
            "icon": "https://avatars.githubusercontent.com/u/35520196?s=200&v=4",
            "launch": {
                "computeEnvId": computeId,
                "pipeline": url,
                "workDir": f'/srv/scratch/{KATANA_USERNAME}',
                "pullLatest": True,
                "configText": configText,
                #"configProfiles": ["test"],
            },
            "labelsIds": labelsIds
        }

        response = requests.post(self.ENDPOINT + "/pipelines", json=pipelineData, headers=self.HEADERS, params=self.PARAMS)
        if response.status_code != 200:
            raise Exception(response.json())
    
    def remove_workflows_if(self,filterFunc, shouldCleanLabels: bool=True):
        labelCounts = {}
        workflows = requests.get(self.ENDPOINT + "/pipelines", headers=self.HEADERS, params={**self.PARAMS, **{'attributes': 'labels'}}).json()["pipelines"]
        for wf in workflows:
            shouldFilter = filterFunc(wf)

            if shouldFilter:
                requests.delete(self.ENDPOINT + f'/pipelines/{wf["pipelineId"]}', headers=self.HEADERS, params=self.PARAMS)

            if shouldCleanLabels:
                for label in wf["labels"]:
                    labelCounts[label["id"]] = labelCounts.get(label["id"], 0) + (0 if shouldFilter else 1)
    
        if shouldCleanLabels:
            for key, value in labelCounts.items():
                if value == 0:
                    self.remove_label(key)

    def add_top_nfcore_workflows(self, computeId: str, labelId: str, count: int =1):
        configText = ""
        if os.path.isfile('nextflow.config'):
            with open('nextflow.config', 'r') as f:
                configText = f.read()

        remote_workflows = requests.get("https://nf-co.re/pipelines.json").json()["remote_workflows"]
        remote_workflows_published = list(filter(lambda wf: wf.get("releases", False), remote_workflows))

        # NOTE: Annoyingly, pipeline names must be unique across organization (e.g. A pipeline with name 'rnaseq' already exists in another workspace of this organization)
        for wf in remote_workflows_published: wf["name"] += "_unsw"

        top = sorted(remote_workflows_published, reverse=True, key=lambda x: x.get("stargazers_count", 0))[:count]
        names = { wf["name"] : True for wf in remote_workflows_published}
    
        self.remove_workflows_if(lambda tower_wf: tower_wf["name"] in names) # TODO: do put instead of replace to keep same pipelineIds, or re-add as same id

        topics_to_labels = {}
        for wf in top:
            topicLabelIds = [topics_to_labels.setdefault(t, self.get_label_id_by_name(t) or self.add_label(t)) for t in wf["topics"]]
            self.add_workflow(computeId, wf["html_url"], wf, [labelId] + topicLabelIds, configText)

    def get_label_id_by_name(self, labelName: str):
        # NOTE: all /labels endpoints are undocumented, but you can't get computeId for pipelines after they are defined so a label helps us track automanaged
        labels = requests.get(self.ENDPOINT + f'/labels', headers=self.HEADERS, params={**self.PARAMS, **{"search": labelName}}).json()["labels"]
        return labels[0]["id"] if len(labels) else None

    def add_label(self,name: str):
        labelData = {
            "id": None,
            "name": name
        }

        response = requests.post(self.ENDPOINT + "/labels", json=labelData, headers=self.HEADERS, params=self.PARAMS)
        if response.status_code != 200: raise Exception(response.json())
        return response.json()["id"]

    def remove_label(self,labelId: str):
        return requests.delete(self.ENDPOINT + f'/labels/{labelId}', headers=self.HEADERS, params=self.PARAMS).status_code == 204

    def get_or_create_credentials(self, credentialsName: str):
        return self.get_credentials_id_by_name(credentialsName) or self.add_credentials(credentialsName, create_key('tower@biocommons'))

    def get_or_create_compute(self, computeName: str , credentialsName):
        return self.get_compute_id_by_name(computeName) or self.add_compute(computeName, self.get_or_create_credentials(credentialsName))

    def setup(self, computeName: str, credentialsName: str, labelName: str):
        computeId = self.get_or_create_compute(computeName, credentialsName)
        self.make_compute_primary(computeId)
        labelId = self.get_label_id_by_name(labelName) or self.add_label(labelName)
        self.add_top_nfcore_workflows(computeId, labelId)

    def clean(self, computeName: str, credentialsName: str, labelName: str):
        # Clean all pipelines with label
        # NOTE: Compute and pipelines have labels, could get credentials from compute. Currently just used for pipelines
        labelId = self.get_label_id_by_name(labelName)
        if labelId is not None:
            self.remove_workflows_if(lambda wf: any(label["id"] == labelId for label in wf["labels"]))
            self.remove_label(labelId)

        # Clean compute
        computeId = self.get_compute_id_by_name(computeName)
        if computeId is not None:
            self.remove_compute(computeId)

        # Clean credentials
        credentialsId = self.get_credentials_id_by_name(credentialsName)
        if credentialsId is not None:
            self.remove_credentials(credentialsId)


def create_key(keyComment, dryRun=False): #e.g. 'tower@biocommons'
    global SSH_FROM_RESTRICTIONS

    privateKey = None
    originalAuthName = os.path.expanduser('~/.ssh/authorized_keys')

    # Copy existing authorized public keys into memory
    existing = []
    if os.path.isfile(originalAuthName):
        with open(originalAuthName, 'r') as originalAuth:
            for line in originalAuth:
                if line and not line.isspace() and not line.rstrip().endswith(' ' + keyComment):
                    existing.append(line)

    # Possibly safer to use same file system for temporary files over NFS
    sameFSTmp = os.path.expanduser('~/.tower')
    Path(sameFSTmp).mkdir(exist_ok=True, parents=True)

    # Add expiry date if configured
    if SSH_FROM_DAYS_LIMIT is not None: 
        SSH_FROM_RESTRICTIONS += ',expiry-time="%s"' % (datetime.datetime.today() + datetime.timedelta(days=SSH_FROM_DAYS_LIMIT)).strftime('%Y%m%d')

    # Rewrite authorized public keys (NOTE: sh-copy-id has different behaviour when replacing, /tmp and ~/ may be different NFS on HPC, suggestions welcome)
    with tempfile.TemporaryDirectory(dir=sameFSTmp) as tmpdir:
        p = subprocess.run(['ssh-keygen', '-f', os.path.join(tmpdir, 'tower'), '-N', '', '-t', 'ed25519', '-C', keyComment], stdout=subprocess.DEVNULL)
        if p.returncode: raise Exception("Failed to create key pair")

        newAuthPath = os.path.join(tmpdir, 'authorized_keys')
        with open(newAuthPath, 'w') as newAuth:
            for line in existing: 
                newAuth.write(line)
            with open(os.path.join(tmpdir, 'tower.pub'), 'r') as pub:
                newAuth.write(SSH_FROM_RESTRICTIONS + ' ' + pub.read())

        with open(os.path.join(tmpdir, 'tower'), 'r') as priv: privateKey = priv.read()

        # Atomically update authorized keys
        os.chmod(newAuthPath, 0o644)
        if dryRun: 
            with open(newAuthPath, 'r') as newAuth: print("Would have updated authorized keys to: " + newAuth.read())
        else: 
            os.rename(newAuthPath, originalAuthName)
    return privateKey


if __name__ == "__main__":
    MIN_PYTHON = (3, 6)
    if sys.version_info < MIN_PYTHON:
        sys.exit("Python %s.%s or later is required.\n" % MIN_PYTHON)

    if len(sys.argv) != 2 or sys.argv[1] == "--help":
        sys.exit("Usage: tower_autoconfig.py [setup|clean]")
    
    if BEARER is None:
        sys.exit("Please run export TOWER_ACCESS_TOKEN=<your-token> before setup")

    api = TowerApi("https://{TOWER_HOST}/api/",BEARER, {'workspaceId': TOWER_WORKSPACE_ID} )

    if sys.argv[1] == "setup": api.setup(KATANA_COMPUTE_NAME, KATANA_CREDENTIAL_NAME, KATANA_LABEL_NAME)
    elif sys.argv[1] == "clean": api.clean(KATANA_COMPUTE_NAME, KATANA_CREDENTIAL_NAME, KATANA_LABEL_NAME)
    else: sys.exit("Invalid command")

## tower-autoconfig
Simple proof of concept package to automatically configure Nextflow Tower workspaces directly from the target HPC.

> **Note**
> This is a concept/test package only - don't test on workspaces with anything critical.

## Getting Started
### Installation
```bash
pip install git+https://github.com/WalshKieran/tower-autoconfig
```

### Typical Example
On most HPCs, node/platform should be automatically detected
```bash
# 1. Create two ready-to-run nf-core workflows on your Tower personal workspace
export TOWER_ACCESS_TOKEN=dhHg72...
tower_autoconfig setup agent --pipelines nf-core/rnaseq nf-core/sarek --config ./examples/nextflow.config --launchdir /your/scratch/dir

# 2. Run agent and launch pipelines (see setup output for your exact command)
tw-agent --url https://tower.nf/api --workdir /your/scratch/dir YOUR_HPC_GENERATED_NAME

# 3. In future, if you want to remove all auto credentials/compute/pipelines
tower_autoconfig clean
```

### All Subcommands
Full subcommand list, each has its own --help.
``` bash
tower-autoconfig setup agent
tower-autoconfig setup ssh
tower-autoconfig clean
```

## Usage (setup ssh)
```
usage: tower-autoconfig setup ssh [-h] [--server SERVER] --node NODE [-y] [-v]
                                  --platform PLATFORM --launchdir LAUNCHDIR
                                  [--pipelines PIPELINES [PIPELINES ...]]
                                  [--profiles PROFILES [PROFILES ...]]
                                  [--config CONFIG] [--prerun PRERUN] [-f]
                                  [--days DAYS]

optional arguments:
  -h, --help            show this help message and exit
  --server SERVER       API Tower server (default: tower.nf)
  --node NODE           full address to your HPC login node (guessed: katana3.restech.unsw.edu.au)
  -y, --yes             perform actions without confirming
  -v, --verbose         display all Tower API calls
  --platform PLATFORM   compute platform (guessed: altair-platform)
  --launchdir LAUNCHDIR
                        scratch directory used for launching workflows
  --pipelines PIPELINES [PIPELINES ...]
                        if provided, add/remove pipelines ending in cluster's
                        "friendly" generated name to match provided list
  --profiles PROFILES [PROFILES ...]
                        if provided, profiles assigned to any NEW/UPDATED
                        pipelines
  --config CONFIG       nextflow config assigned to NEW/UPDATED pipelines -
                        executor must match platform (default:
                        ./nextflow.config)
  --prerun PRERUN       script to prepare launch environment e.g load module
  -f, --force           force reload pipelines/compute if e.g. config has
                        changed - does not break existing runs
  --days DAYS           days SSH key will be valid (default: 30)

Environment variables: TOWER_ACCESS_TOKEN, TOWER_WORKSPACE_ID (optional)
```

### Bonus: Tower Agent Timeout
Probably not interesting, but if you want to use the internal Agent wrapper yourself (that automatically closes after e.g. 300 seconds of no running pipelines):
``` bash

curl https://raw.githubusercontent.com/WalshKieran/tower-autoconfig/master/src/tower_autoconfig/bin/tw-agent-timeout -O
bash ./tw-agent-timeout YOUR_HPC_GENERATED_NAME /tmp 300
```
Assumes `TOWER_ACCESS_TOKEN` is set - `YOUR_HPC_GENERATED_NAME` is used for documentation consistency, any configered agent connection ID will work 
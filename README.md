## tower-autoconfig
Simple proof of concept package to quickly test Nextflow Tower from the target HPC. Requires an internet-enabled queue for main job submission.

> **Note**
> This is a concept/test package to see what can be reliably automated and what cannot. Centrally administered, workspace-wide pipelines are still likely the best way forward. Please don't test on workspaces or HPC accounts with anything important - some files end up in ~/.

## Example 1 - SSH
For **illustrative purposes only** - please refer to the UNSW Katana official Nextflow docs.  
Here, the --node and --platform are detected automatically and all queues have internet access.
1. Configure
```bash
export TOWER_ACCESS_TOKEN=dhHg72...
LAUNCH_DIR="/srv/scratch/$USER/nextflow_launch"
mkdir "$LAUNCH_DIR"
module load python/3.11.3 java/11.0.17_8-openjdk

# Install tower-autoconfig
python3 -m pip install --user git+https://github.com/WalshKieran/tower-autoconfig

# Add nf-core/rnaseq SSH to your Tower personal workspace
tower-autoconfig setup ssh --profiles test \
--config https://raw.githubusercontent.com/WalshKieran/tower-autoconfig/master/example/nextflow.config \
--prerun <(echo "module load java/11.0.17_8-openjdk nextflow/22.10.7") \
--queue_options '"-lwalltime=12:00:00,select=1:ncpus=4:mem=4GB"' \
--launchdir "$LAUNCH_DIR" --pipelines nf-core/rnaseq
```

2. Clean
```bash
# Undo all previous automatic setup for this particular cluster
tower_autoconfig clean
```

## Example 2 - Agent
For **illustrative purposes only** - please refer to the NCI Gadi official Nextflow docs.  
In this case, Tower cannot log directly into the current login node, so --node must be provided. Furthermore, the default queue does not have internet access.

1. Configure
```bash
export TOWER_ACCESS_TOKEN=dhHg72...
LAUNCH_DIR="/scratch/$PROJECT/$USER/nextflow_launch"
mkdir "$LAUNCH_DIR"
module load python3/3.9.2 java/jdk-17.0.2 

# Install tower-autoconfig
python3 -m pip install --user git+https://github.com/WalshKieran/tower-autoconfig

# Add nf-core/rnaseq AGENT to your Tower personal workspace
tower-autoconfig setup ssh --node gadi.nci.org.au --profiles nci_gadi test \
--prerun <(echo "module load singularity nextflow/22.04.3") \
--queue_options '"-q copyq -lwalltime=10:00:00,ncpus=1,mem=4GB"' \
--launchdir "$LAUNCH_DIR" --pipelines nf-core/rnaseq@3.9
```

2. Run
```bash
# Start agent for at least 12 hours - See setup output for equivalent tw-agent command
tower-autoconfig-agent gadiauto "$LAUNCH_DIR" https://tower.nf/api 43200
```

3. Clean
```bash
# Undo all previous automatic setup for this particular cluster
tower_autoconfig clean --node gadi.nci.org.au
```

## Available Subcommands
``` bash
tower-autoconfig setup agent --help
tower-autoconfig setup ssh --help
tower-autoconfig clean --help

tower-autoconfig-agent --help # Bonus: agent wrapper with automatic shutdown
```

## Usage
Only the "setup ssh" subcommand is shown, but it shares most options with agent method:
```
usage: tower-autoconfig setup ssh [-h] [--server SERVER] [--node NODE] [-y] [-v] [--platform PLATFORM]
                                  [--queue_options QUEUE_OPTIONS] --launchdir LAUNCHDIR
                                  [--pipelines PIPELINES [PIPELINES ...]] [--profiles PROFILES [PROFILES ...]]
                                  [--config CONFIG] [--prerun PRERUN] [-f] [--days DAYS]

optional arguments:
  -h, --help            show this help message and exit
  --server SERVER       API Tower server (default: tower.nf)
  --node NODE           full address to your HPC login node (guessed: gadi-login-08.nci.org.au)
  -y, --yes             perform actions without confirming
  -v, --verbose         display all Tower API calls
  --platform PLATFORM   compute platform (guessed: altair-platform)
  --queue_options QUEUE_OPTIONS
                        arguments to add to head job submission e.g. resources
  --launchdir LAUNCHDIR
                        scratch directory used for launching workflows
  --pipelines PIPELINES [PIPELINES ...]
                        if provided, add/remove pipelines ending in cluster's "friendly" generated name to match
                        provided list
  --profiles PROFILES [PROFILES ...]
                        if provided, profiles assigned to any NEW/UPDATED pipelines
  --config CONFIG       nextflow config file/url assigned to NEW/UPDATED pipelines for when there is no organisation
                        profile
  --prerun PRERUN       script to prepare launch environment e.g load module
  -f, --force           force reload pipelines/compute if e.g. config has changed - does not break existing runs
  --days DAYS           days SSH key will be valid (default: 30)

Environment variables: TOWER_ACCESS_TOKEN, TOWER_WORKSPACE_ID (optional)
```
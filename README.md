# Nextflow Tower Auto-config

Configures Nextflow Tower credentials, compute env and top nf-core workflows automatically for any SSH accessible machine/HPC. Setup can take 2+ minutes for 10+ nf-core workflows since they are downloaded remotely during the API call. Tower pipelines currently can't exist without a compute env, which can't exist without credentials. This makes it quite slow to configure manually for a quick test.

### Usage:

```bash
export TOWER_WORKSPACE_ID=<your-workspace-id> # Optional, defaults to personal
export TOWER_ACCESS_TOKEN=<your-bearer-token>
python tower_autoconfig.py setup 
...
python tower_autoconfig.py clean # Remove everything auto configured

```

### Limitations:
- WIP, config extremely coupled to UNSW Sydney HPC
- GitHub credentials must be added using the Tower UI or adding many workflows starts to fail due to anonymous API limits
- Supplying a workplace ID will make your HPC account accessible by everyone in the workspace
- Pipeline names must be unique at an organization-level
  
### Ideas:
- Use PUT when possible
- Improve error handling
- Progress reporting, perhaps interactive for adding workflows as this could be considered optional
- python tower_autoconfig renew for token expiry
- Reconsider cost of mostly decorative labels


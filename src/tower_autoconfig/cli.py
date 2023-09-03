import os, asyncio, logging, argparse, string, sys

from io import StringIO

from tower_autoconfig.autoconfig import TowerAutoconfig
from tower_autoconfig.api import RECOGNIZED_PLATFORMS
from tower_autoconfig.agent import EXEC_PATH
from tower_autoconfig.utils import AUTH_KEY_PATH, guess_node, guess_platform, create_ssh_restriction, source_to_text, verify_external_server_is_me

def _get_name(host):
    return host.split('.', 1)[0].rstrip(string.digits) + 'auto'

# If used as a library, calculate defaults only once
EXTERNAL_IP = DEFAULT_NODE = DEFAULT_PLATFORM = DEFAULT_ERR = DEFAULT_NAME = None
try:
    EXTERNAL_IP, DEFAULT_NODE = guess_node()
    DEFAULT_PLATFORM = guess_platform()
except Exception as e:
    DEFAULT_ERR = e
if DEFAULT_NODE: 
    DEFAULT_NAME = _get_name(DEFAULT_NODE)

async def _validate(command, subcommand, server, workspace_id, user, node, compute_name, compute_id, credentials_name, credentials_id, pipelines_add, pipelines_remove, force):
    msg = StringIO()
    
    msg.write(f'Connected as "{user}" (https://{server} -- {workspace_id or "personal"})\n')
    msg.write(f'Configuring machine "{compute_name}" and pipelines with the suffix "_{compute_name}" ({node})\n')
    if command == 'clean' and not (compute_id or credentials_id or pipelines_remove):
        msg.write('Nothing to clean\n')
        return True, msg.getvalue()
    
    if command == 'setup' and compute_id and credentials_id and not (pipelines_add or pipelines_remove or force):
        msg.write('Already setup\n')
        return True, msg.getvalue()
        
    if command == 'setup':
        if not credentials_id or force: 
            msg.write(f'Autoconfig will {"UPDATE" if credentials_id else "install NEW"} {subcommand} credentials "{credentials_name}"\n')
            if subcommand == 'ssh':
                msg.write(f'Autoconfig will {"UPDATE" if credentials_id else "ADD"} key to {AUTH_KEY_PATH}\n')
        if not compute_id or force: 
            msg.write(f'Autoconfig will {"UPDATE" if compute_id else "install NEW"} compute environment "{compute_name}"\n')
    elif command == 'clean':
        if credentials_id: 
            msg.write(f'Autoconfig will DELETE credentials "{credentials_name}"\n')
            msg.write(f'Autoconfig will REMOVE created keys from {AUTH_KEY_PATH}\n')
        if compute_id: msg.write(f'Autoconfig will DELETE compute environment "{compute_name}"\n')

    if pipelines_add and len(pipelines_add): 
        msg.write(f'Pipelines to be INSTALLED{"/UPDATED" if force else ""}:\n')
        msg.write('\n'.join([f'    - {p}' for p in pipelines_add]) + '\n')
    if pipelines_remove and len(pipelines_remove): 
        msg.write(f'Pipelines to be REMOVED:\n')
        msg.write('\n'.join([f'    - {p}' for p in pipelines_remove]) + '\n')

    return False, msg.getvalue()
        
async def _ask(*args):
    is_already_valid, msg = await _validate(*args)
    print(msg)
    if is_already_valid: return is_already_valid, False
    while True:
        answer = input('continue (y/n)?\n')
        if answer.lower() in ['y', 'yes']: return is_already_valid, True
        elif answer.lower() in ['n', 'no']: return is_already_valid, False

async def _run(command=None, subcommand=None, server='tower.nf', node=DEFAULT_NODE, platform=DEFAULT_PLATFORM, queue_options=None, launchdir=None, pipelines=[], profiles=[], config='', prerun='', force=False, yes=False, verbose=False, days=30, bearer=None, workspace_id=None, ask_callback=None):    
    if launchdir: launchdir = os.path.abspath(os.path.expanduser(launchdir))

    # Trailing numbers are stripped to try to get "main" login node
    # Harmless if wrong, since we DON'T guess the address itself
    compute_name = _get_name(node)

    credentials_name = compute_name
    pipeline_suffix = '_' + compute_name
    compute_user = os.getlogin()

    async with TowerAutoconfig(server, bearer, workspace_id) as auto:
        print('Checking credentials...')
        (
            user, compute_id, compute_primary_id, credentials_id,
            pipelines_add, pipelines_remove, pipeline_name_to_id, remote_pipelines_dict,
            labels_add, labels_remove, label_name_to_id
        ) = await auto.prepare(compute_name, credentials_name, pipelines, pipeline_suffix, force)

        if ask_callback and not yes:
            is_already_valid, is_confirmed = await ask_callback(command, subcommand if command == 'setup' else None, server, workspace_id, user, node, compute_name, compute_id, credentials_name, credentials_id, pipelines_add, pipelines_remove, force)
            if is_already_valid or not is_confirmed:
                return is_already_valid
        
        remove_label_tasks = [] if not labels_remove else [auto.api.remove_label(label_name_to_id[l]) for l in labels_remove]
        remove_pipeline_tasks = [] if not pipelines_remove else [auto.api.remove_pipeline(pipeline_name_to_id[p]) for p in pipelines_remove]
        
        if command == 'clean':
            auto.clean_ssh()
            remove_credential_tasks = [] if not credentials_id else [auto.api.remove_credentials(credentials_id)]
            remove_compute_tasks = [] if not compute_id else [auto.api.remove_compute(compute_id)]
            await asyncio.gather(*remove_credential_tasks, *remove_compute_tasks, *remove_label_tasks, *remove_pipeline_tasks)

            print('Finished cleaning')

        if command == 'setup':
            if subcommand == 'ssh':
                ssh_restrictions = f'restrict,pty,{create_ssh_restriction(days)}'
                compute_id, credentials_id = await auto.setup_ssh(compute_name, '', platform, node, compute_user, queue_options, launchdir, credentials_name, '', compute_id, credentials_id, force, ssh_restrictions)
            elif subcommand == 'agent':
                agent_connection_id = compute_name
                compute_id, credentials_id = await auto.setup_agent(compute_name, '', platform, node, compute_user, queue_options, launchdir, credentials_name, '', compute_id, credentials_id, force, agent_connection_id, bearer=bearer)

            if pipelines: 
                # Group and run all independent tasks, including adding labels
                add_label_tasks = [auto.api.add_label(l) for l in labels_add]
                make_primary = [] if compute_id == compute_primary_id else [auto.api.make_compute_primary(compute_id)]
                new_label_ids = await asyncio.gather(*add_label_tasks, *remove_label_tasks, *remove_pipeline_tasks, *make_primary)

                # Add new label IDs to lookup in preparation for pipelines
                for i in range(len(add_label_tasks)): 
                    label_name_to_id[labels_add[i]] = new_label_ids[i]

                # Create new pipelines
                await auto.setup_pipelines(compute_id, pipelines_add, pipeline_name_to_id, label_name_to_id, remote_pipelines_dict, pipeline_suffix, source_to_text(config), source_to_text(prerun), launchdir, profiles or [])
            
            print('Finished setting up')
            
            if subcommand == 'agent':
                print(f'In future, start agent using either:')
                print(f'    a) tower-autoconfig-agent {agent_connection_id} {launchdir} {auto.endpoint} 43200')
                print(f'    b) tw-agent --work-dir {launchdir} --url {auto.endpoint} {agent_connection_id}')

async def run(argv):
    parser = argparse.ArgumentParser(description='Tower Autoconfig', epilog='Environment variables: TOWER_ACCESS_TOKEN, TOWER_WORKSPACE_ID (optional)')
    parent_all = argparse.ArgumentParser(add_help=False)
    parent_all.add_argument('--server', help='API Tower server (default: %(default)s)', default='tower.nf')
    parent_all.add_argument('--node', help='full address to your HPC login node (guessed: %(default)s)', required=not DEFAULT_NODE, default=DEFAULT_NODE)
    parent_all.add_argument('-y', '--yes', help='perform actions without confirming', action='store_true')
    parent_all.add_argument('-v', '--verbose', help='display all Tower API calls', action='store_true')

    setup_parent = argparse.ArgumentParser(add_help=False)
    setup_parent.add_argument('--platform', help='compute platform (guessed: %(default)s)', required=not DEFAULT_PLATFORM, default=DEFAULT_PLATFORM)
    setup_parent.add_argument('--queue_options', help='arguments to add to head job submission e.g. resources')
    setup_parent.add_argument('--launchdir', required=True, help='scratch directory used for launching workflows')
    setup_parent.add_argument('--pipelines', nargs='+', help=f'if provided, add/remove pipelines ending in cluster\'s "friendly" generated name to match provided list')
    setup_parent.add_argument('--profiles', nargs='+', help='if provided, profiles assigned to any NEW/UPDATED pipelines')
    setup_parent.add_argument('--config', help='nextflow config file/url assigned to NEW/UPDATED pipelines for when there is no organisation profile')
    setup_parent.add_argument('--prerun', help='script to prepare launch environment e.g load module')
    setup_parent.add_argument('-f', '--force', help=f'force reload pipelines/compute if e.g. config has changed - does not break existing runs', action='store_true')

    main_subparsers = parser.add_subparsers(title='commands', dest='command')
    main_subparsers.required = True

    setup_parser = main_subparsers.add_parser('setup', help='setup/update current machine as Tower compute environment and load/replace requested nf-core pipelines')
    cleanup_parser = main_subparsers.add_parser('clean', help='remove associated credentials, compute environments, pipelines and labels', parents=[parent_all])

    setup_subparsers = setup_parser.add_subparsers(title='subcommands', dest='subcommand')
    setup_subparsers.required = True

    setup_agent_parser = setup_subparsers.add_parser('agent', help='launch and configure Tower Agent', parents=[parent_all, setup_parent])
    setup_ssh_parser = setup_subparsers.add_parser('ssh', help='generate and configure SSH key', parents=[parent_all, setup_parent])
    setup_ssh_parser.add_argument('--days', type=int, default=30, help='days SSH key will be valid (default: %(default)s)')

    try:
        args, _ = parser.parse_known_args(args=argv)
    finally:
        # Footer
        missing = []
        if not DEFAULT_NODE: missing.append('--node')
        if not DEFAULT_PLATFORM: missing.append('--platform')
        if missing: 
            print(f'\nALERT: Could not guess default {" or ".join(missing)} - are you sure this is a HPC?\n')

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
        
    workspace_id = os.environ.get('TOWER_WORKSPACE_ID', None)
    bearer = os.environ.get('TOWER_ACCESS_TOKEN', None)
    if not bearer:
        parser.error('Environment variable TOWER_ACCESS_TOKEN is not set')

    if args.command == 'setup' and not os.path.exists(args.launchdir):
        parser.error(f'Launch directory "{args.launchdir}" does not exist')

    if args.command == 'setup' and args.platform not in RECOGNIZED_PLATFORMS:
        parser.error(f'Invalid compute platform - must be one of {", ".join(RECOGNIZED_PLATFORMS)}')

    if args.command == 'setup' and args.subcommand == 'ssh' and args.node == DEFAULT_NODE and not verify_external_server_is_me(EXTERNAL_IP):
        parser.error('Invalid node, manually specify')

    await _run(bearer=bearer, workspace_id=workspace_id, ask_callback=_ask, **vars(args))

def agent():
    os.execv(EXEC_PATH, sys.argv)

def main():
    if DEFAULT_ERR:
        print(DEFAULT_ERR)
        logging.critical('Failed to resolve host or connect to internet - are you connected to internet?')
        exit(1)
        
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(sys.argv[1:]))
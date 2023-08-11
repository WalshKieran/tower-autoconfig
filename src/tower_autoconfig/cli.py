import os, asyncio, logging, argparse, string

from tower_autoconfig.autoconfig import TowerAutoconfig
from tower_autoconfig.api import RECOGNIZED_PLATFORMS
from tower_autoconfig.utils import guess_node, guess_platform, create_ssh_restriction

async def _ask(command, subcommand, server, workspace_id, user, compute_host, compute_name, compute_id, credentials_name, credentials_id, pipelines_add, pipelines_remove, shouldForce):
    print(f'Connected as "{user}" (https://{server} -- {workspace_id or "personal"})')
    print(f'Configuring machine "{compute_name}" and pipelines with the suffix "_{compute_name}" ({compute_host})')
    if command == 'clean' and not (compute_id or credentials_id or pipelines_remove):
        print('Nothing to clean')
        return False 
    
    if command == 'setup' and compute_id and credentials_id and not (pipelines_add or pipelines_remove or shouldForce):
        print('Already setup')
        return False 
        
    if command == 'setup':
        if not credentials_id or shouldForce: 
            print(f'Autoconfig will {"UPDATE" if credentials_id else "install NEW"} {subcommand} credentials "{credentials_name}"')
        if not compute_id or shouldForce: 
            print(f'Autoconfig will {"UPDATE" if compute_id else "install NEW"} compute environment "{compute_name}"')
    elif command == 'clean':
        if credentials_id: print(f'Autoconfig will DELETE credentials "{credentials_name}"')
        if compute_id: print(f'Autoconfig will DELETE compute environment "{compute_name}"')

    if pipelines_add and len(pipelines_add): 
        print(f'Pipelines to be INSTALLED{"/UPDATED" if shouldForce else ""}:')
        print('\n'.join([f'    - {p}' for p in pipelines_add]))
    if pipelines_remove and len(pipelines_remove): 
        print(f'Pipelines to be REMOVED:')
        print('\n'.join([f'    - {p}' for p in pipelines_remove]))
        
    while True:
        answer = input('continue (y/n)?\n')
        if answer.lower() in ['y', 'yes']: return True
        elif answer.lower() in ['n', 'no']: return False


async def _run(args, bearer, workspace_id):
    new_pipelines = getattr(args, 'pipelines', [])
    compute_platform = getattr(args, 'platform', None)
    shouldForce = getattr(args, 'force', False)
    workdir = getattr(args, 'launchdir', False)
    
    skipPipe = (new_pipelines is None)

    compute_host = args.node

    # Trailing numbers are stripped to try to get "main" login node
    # Harmless if wrong, since we DON'T guess the address itself
    compute_name = compute_host.split('.', 1)[0].rstrip(string.digits) + 'auto'

    credentials_name = compute_name
    pipeline_suffix = '_' + compute_name
    compute_user = os.getlogin()

    async with TowerAutoconfig(args.server, bearer, workspace_id) as auto:
        print('Checking credentials...')
        (
            user, compute_id, compute_primary_id, credentials_id,
            pipelines_add, pipelines_remove, pipeline_name_to_id, remote_pipelines_dict,
            labels_add, labels_remove, label_name_to_id
        ) = await auto.prepare(compute_name, credentials_name, new_pipelines, pipeline_suffix, shouldForce)

        if not args.yes and not (await _ask(args.command, args.subcommand if args.command == 'setup' else None, args.server, workspace_id, user, compute_host, compute_name, compute_id, credentials_name, credentials_id, pipelines_add, pipelines_remove, shouldForce)):
            return
        
        remove_label_tasks = [] if not labels_remove else [auto.api.remove_label(label_name_to_id[l]) for l in labels_remove]
        remove_pipeline_tasks = [] if not pipelines_remove else [auto.api.remove_pipeline(pipeline_name_to_id[p]) for p in pipelines_remove]
        
        if args.command == 'clean':
            remove_credential_tasks = [] if not credentials_id else [auto.api.remove_credentials(credentials_id)]
            remove_compute_tasks = [] if not compute_id else [auto.api.remove_compute(compute_id)]
            await asyncio.gather(*remove_credential_tasks, *remove_compute_tasks, *remove_label_tasks, *remove_pipeline_tasks)

        if args.command == 'setup':
            if args.subcommand == 'ssh':
                ssh_restrictions = f'restrict,pty,{create_ssh_restriction(30)}'
                compute_id, credentials_id = await auto.setup_ssh(compute_name, '', compute_platform, compute_host, compute_user, workdir, credentials_name, '', compute_id, credentials_id, shouldForce, ssh_restrictions)
            elif args.subcommand == 'agent':
                agent_connection_id = compute_name
                compute_id, credentials_id = await auto.setup_agent(compute_name, '', compute_platform, compute_host, compute_user ,workdir, credentials_name, '', compute_id, credentials_id, shouldForce, agent_connection_id)

            if not skipPipe: 
                # Group and run all independent tasks, including adding labels
                add_label_tasks = [auto.api.add_label(l) for l in labels_add]
                make_primary = [] if compute_id == compute_primary_id else [auto.api.make_compute_primary(compute_id)]
                new_label_ids = await asyncio.gather(*add_label_tasks, *remove_label_tasks, *remove_pipeline_tasks, *make_primary)

                # Add new label IDs to lookup in preparation for pipelines
                for i in range(len(add_label_tasks)): 
                    label_name_to_id[labels_add[i]] = new_label_ids[i]

                # Create new pipelines
                config_text = prerun_text = ''
                if os.path.exists(args.prerun):
                    with open(args.prerun, 'r') as f: prerun_text = f.read()
                if os.path.exists(args.config):
                    with open(args.config, 'r') as f: config_text = f.read()
                else:
                    logging.warning(f'{args.config} does not exist')
                await auto.setup_pipelines(compute_id, pipelines_add, pipeline_name_to_id, label_name_to_id, remote_pipelines_dict, pipeline_suffix, config_text, prerun_text, workdir, args.profiles or [])

            if args.subcommand == 'agent':
                print(f'Agent configured - in future, launch using "tw-agent --url {auto.endpoint} --work-dir {workdir} {agent_connection_id}"')
        print('Done')

def main():
    default_node = default_platform = None
    try:
        default_node = guess_node()
        default_platform = guess_platform()
    except Exception:
        logging.critical('Failed to resolve host or connect to internet - are you connected to internet?')
        exit(1)

    parser = argparse.ArgumentParser(description='Tower Autoconfig', epilog='Environment variables: TOWER_ACCESS_TOKEN, TOWER_WORKSPACE_ID (optional)')
    parent_all = argparse.ArgumentParser(add_help=False)
    parent_all.add_argument('--server', help='API Tower server (default: %(default)s)', default='tower.nf')
    parent_all.add_argument('--node', help='full address to your HPC login node (guessed: %(default)s)', required=not default_node, default=default_node)
    parent_all.add_argument('-y', '--yes', help='perform actions without confirming', action='store_true')
    parent_all.add_argument('-v', '--verbose', help='display all Tower API calls', action='store_true')

    setup_parent = argparse.ArgumentParser(add_help=False)
    setup_parent.add_argument('--platform', help='compute platform (guessed: %(default)s)', required=not default_platform, default=default_platform)
    setup_parent.add_argument('--launchdir', required=True, help='scratch directory used for launching workflows')
    setup_parent.add_argument('--pipelines', nargs='+', help=f'if provided, add/remove pipelines ending in cluster\'s "friendly" generated name to match provided list')
    setup_parent.add_argument('--profiles', nargs='+', help='if provided, profiles assigned to any NEW/UPDATED pipelines')
    setup_parent.add_argument('--config', help='nextflow config assigned to NEW/UPDATED pipelines - executor must match platform (default: %(default)s)', default='./nextflow.config')
    setup_parent.add_argument('--prerun', help='script to prepare launch environment e.g load module', default='./prerun.sh')
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

    ex = None
    try:
        args, _ = parser.parse_known_args()
    except SystemExit as e:
        ex = e
    finally:
        # Footer
        missing = []
        if not default_node: missing.append('node address')
        if not default_platform: missing.append('platform')
        if missing: 
            print(f'\nALERT: Could not guess default {" or ".join(missing)} - are you sure this is an HPC?\n')
        if ex: raise ex

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

    loop = asyncio.get_event_loop()
    loop.run_until_complete(_run(args, bearer, workspace_id))

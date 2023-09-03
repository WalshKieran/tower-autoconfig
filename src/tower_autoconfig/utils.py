import os, subprocess, tempfile, contextlib, datetime, socket, urllib.request, logging, secrets
from typing import Callable

from dns import reversename, resolver

AUTH_KEY_PATH='~/.ssh/authorized_keys'
TMP_KEY_COMMENT = 'temporary:check'

def source_to_text(source):
    if source:
        try:
            if source.startswith('https://'):
                with urllib.request.urlopen(source) as r:
                    return r.read().decode()
            elif os.path.exists(source):
                with open(source, 'r') as f: 
                    return f.read()
        except Exception:
            logging.warning(f'{source} could not be resolved')
    return None

def guess_ip():
    my_resolver = resolver.Resolver(configure=False)
    my_resolver.nameservers = ['208.67.222.222', '208.67.220.220']
    query_result = my_resolver.query('myip.opendns.com', 'a')
    if query_result:
        return query_result[0].address
    return None

def guess_node():
    external_ip = guess_ip()
    if external_ip:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        ssh_open = s.connect_ex((external_ip, 22)) == 0
        if ssh_open:
            reversed_ip = reversename.from_address(external_ip)
            return external_ip, str(resolver.resolve(reversed_ip, 'PTR')[0]).rstrip('.')
    return external_ip, None

def guess_platform():
    known_hpcs = [
        ('/opt/slurm', 'slurm-platform'),
        ('/opt/pbs', 'altair-platform'),
        ('/opt/torque', 'moab-platform'),
        ('/opt/lsf', 'lsf-platform'),
        ('/opt/sge', 'uge-platform')
    ]
    for path, platform in known_hpcs:
        if os.path.exists(path):
            return platform
    return None

def _modify_file_lines_atomic(file_path: str, modifier: Callable, permissions: int=0o644):
    src_file_path = os.path.expanduser(file_path)
    tmp_file_path = None
    try:
        tmp_file = tempfile.NamedTemporaryFile(delete=False, prefix=os.path.basename(src_file_path) + '.autoconfig', dir=os.path.dirname(src_file_path))
        tmp_file_path = tmp_file.name
        tmp_file.close()
        lines = []
        if os.path.exists(src_file_path):
            with open(src_file_path, 'r') as f_in: 
                lines = f_in.readlines()

        lines = modifier(lines)
        
        emptyToEmpty = len(lines) == 0 and not os.path.exists(src_file_path)
        if not emptyToEmpty:
            with open(tmp_file_path, 'w') as f_out: 
                f_out.writelines(lines)
            os.chmod(tmp_file_path, permissions)
            os.replace(tmp_file_path, src_file_path)
    finally:
        if tmp_file_path:
            with contextlib.suppress(FileNotFoundError):
                os.remove(tmp_file_path)

def create_ssh_restriction(day_limit: int):
    return'expiry-time="%s"' % (datetime.datetime.today() + datetime.timedelta(days=day_limit)).strftime('%Y%m%d')

def delete_ssh_key(key_comment: str):
    def modifier(lines):
        return [l for l in lines if not l.strip().endswith(f' {key_comment}')]

    _modify_file_lines_atomic(AUTH_KEY_PATH, modifier)
    
def create_ssh_key(key_comment: str, ssh_restrictions: str):
    # Create new key pair
    public_key_comment = private_key = None
    with tempfile.TemporaryDirectory(dir=os.path.expanduser('~/.ssh')) as tmpdir:
        p = subprocess.run(['ssh-keygen', '-f', os.path.join(tmpdir, 'tower'), '-N', '', '-t', 'ed25519', '-C', key_comment], stdout=subprocess.DEVNULL)
        if p.returncode: 
            raise RuntimeError('Failed to create key pair')
        with open(os.path.join(tmpdir, 'tower.pub'), 'r') as pub:
            public_key_comment = pub.read().strip()
        with open(os.path.join(tmpdir, 'tower'), 'r') as priv: 
            private_key = priv.read().strip()
    
    # Atomically delete existing entry and append
    def modifier(lines):
        lines = [l for l in lines if not l.strip().endswith(f' {key_comment}')]
        lines.append(f'{ssh_restrictions} {public_key_comment}\n')
        return lines

    _modify_file_lines_atomic(AUTH_KEY_PATH, modifier)
    return private_key

'''
In case e.g. opendns spoofed your IP, instantly got a hold of your new public keys file and 
impersonated your HPC when Tower reached out - this confirms a ssh server is you, even if 
the only thing at risk is data Tower chooses to send to it, not the HPC.

Should probably just require --node to reduce complexity.
'''
def verify_external_server_is_me(remote_addr):
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Since the user-created ssh might have restrictions, make a separate one
            tmp_key = create_ssh_key(TMP_KEY_COMMENT, 'restrict,pty')
            with open(os.path.join(tmpdir, 'key'), 'w') as tmp_key_file:
                tmp_key_file.write(tmp_key + '\n')
            os.chmod(tmp_key_file.name, 0o600)

            # Create a secret to check on the "remote" server
            secret = secrets.token_hex(256)
            with open(os.path.join(tmpdir, 'secret'), 'w') as secret_file:
                secret_file.write(secret)

            # Perform check
            try:
                remote_secret = subprocess.check_output([
                    'ssh',
                    '-o', 'StrictHostKeyChecking=no',
                    '-o', 'UserKnownHostsFile=/dev/null',
                    '-i', tmp_key_file.name,
                    f'{os.getlogin()}@{remote_addr}',
                    f'cat "{secret_file.name}"'
                ], text=True, stderr=subprocess.DEVNULL, timeout=10)
                return secret == remote_secret
            
            except subprocess.CalledProcessError:
                logging.warning(f'Issue connecting to {remote_addr}')
    finally:
        delete_ssh_key(TMP_KEY_COMMENT)
    return False
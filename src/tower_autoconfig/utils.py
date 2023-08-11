import os, subprocess, tempfile, contextlib, datetime, socket, urllib.request
from typing import Callable

from dns import reversename, resolver

def guess_node():
    external_ip = urllib.request.urlopen('http://icanhazip.com').read().decode().rstrip()
    ssh_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssh_socket.settimeout(2)
    ssh_open = ssh_socket.connect_ex((external_ip, 22)) == 0
    if ssh_open:
        reversed_ip = reversename.from_address(external_ip)
        return str(resolver.resolve(reversed_ip, 'PTR')[0]).rstrip('.')
    return None

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

    _modify_file_lines_atomic('~/.ssh/authorized_keys', modifier)
    
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

    _modify_file_lines_atomic('~/.ssh/authorized_keys', modifier)
    return private_key

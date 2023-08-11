import asyncio, contextlib
import tower_autoconfig

class TowerAgentError(Exception):
    pass

'''
Provides a nice async context manager for the Tower Agent with error reporting
Also uses a lightweight bash wrapper to prevent the Agent accidentally running forever
If this feature is added to the official agent, we can still keep the context manager
'''
class TowerAgentTimeout:
    def __init__(self, agent_connection_id, workdir, server, timeout, leave_alive=False):
        self.agent_connection_id = agent_connection_id
        self.workdir = workdir
        self.server = server
        self.timeout = timeout
        self.leave_alive = leave_alive
        self.pid = None
        self.cmd = ''

    async def __aenter__(self):
        stdout = ''
        self.cmd = [f'{tower_autoconfig.__path__[0]}/bin/tw-agent-timeout', self.agent_connection_id, self.workdir, self.server, str(self.timeout)]
        self.p = await asyncio.create_subprocess_exec(*self.cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True)
        self.pid = self.p.pid
        async for line in self.p.stdout:
            line = line.decode()
            stdout += line
            if line.endswith('Connection to Tower established\n'): 
                break
        else:
            await self.p.wait()
            e = RuntimeError((await self.p.stderr.read()).decode() or stdout)
            raise TowerAgentError('Tower agent could not be started') from e
        return self
        
    async def __aexit__(self, exc_type, exc, tb):
        if not self.leave_alive:
            with contextlib.suppress(ProcessLookupError): 
                self.p.terminate()
            await self.p.wait()

import abc
import logging
import humanfriendly
from subprocess import Popen, PIPE
from typing import Union, List, Dict, Tuple, Any, Callable, IO


class RunnerBase(metaclass=abc.ABCMeta):
    def __init__(self, prog: str, sudo: str, really: bool):
        self._prog = prog
        self._sudo = sudo
        self._really = really
        self._log = logging.getLogger(self.__class__.__name__)

    @property
    def really(self): return self._really

    @property
    def log(self): return self._log

    def _cmdline(self, args: Union[str, List[str]], sudo=False):
        cmd = [self._sudo, self._prog] if sudo and self._sudo else [self._prog]
        return cmd + args if isinstance(args, list) else cmd + [args]

    def _run(self, args: List[str], sudo=False,
             parser: Callable = None, parser_args: Dict[str, Any] = None,
             stdin: IO = None, readonly=False) -> Tuple[int, Any]:
        cmd = self._cmdline(args, sudo=sudo)
        if not self._really and not readonly:
            self.log.info("Would run '%s'", " ".join(cmd))
            if parser:
                return (0, parser("", "", 0, **parser_args))
            return (0, ("", ""))
        else:
            self.log.debug("Running '%s'", " ".join(cmd))

        p = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
        (stdout, stderr) = p.communicate(stdin)
        stdout = stdout.decode("utf8").split("\n")
        stderr = stderr.decode("utf8").split("\n")
        if parser:
            return (p.returncode, parser(stdout, stderr, p.returncode,
                                         **parser_args))
        return (p.returncode, (stdout, stderr))

    def _parse_list(self, stdout: List[str], stderr: List[str],
                    returncode: int,
                    options: List[str]):
        if returncode != 0:
            return stderr
        ret = []
        for line in stdout:
            if not line:
                continue
            opts = line.split("\t")
            i = 0
            pool = {}
            for opt in options:
                try:
                    pool[opt] = humanfriendly.parse_size(
                        opts[i].replace(",", "."), binary=True)
                except humanfriendly.InvalidSize:
                    pool[opt] = opts[i]
                i += 1
            ret.append(pool)
        return ret

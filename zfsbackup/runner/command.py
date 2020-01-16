from typing import List

from .base import RunnerBase


class Command(RunnerBase):
    def __init__(self, name: str, cmd: str, args: List[str],
                 sudo: str, use_sudo: bool, readonly: bool, really: bool):
        self._cmd = cmd
        self._arguments = args
        self._use_sudo = use_sudo
        self._readonly = readonly
        super().__init__(prog=self._cmd, sudo=sudo,
                         really=really, name=name)

    def run(self):
        (retcode, _) = self._run(args=self._arguments,
                                 sudo=self._use_sudo,
                                 readonly=self._readonly)
        return retcode == 0

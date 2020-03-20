import json
import logging
import os
from subprocess import Popen, PIPE
from typing import Dict, Any


class EventRunner:
    def __init__(self, directory: str, really: bool):
        self._directory = directory
        self._really = really
        self._log = logging.getLogger("Event")

    @property
    def directory(self): return self._directory

    def run(self, event: str, args: Dict[str, Any]) -> int:
        log = self._log.getChild(event)

        cmd = os.path.join(self.directory, event)
        if not os.path.exists(cmd):
            log.debug("No file found, skipping event")
            return 0
        if not os.path.isfile(cmd):
            log.debug("%s is not a file, skipping event", cmd)
            return 0
        if not os.access(cmd, os.X_OK):
            log.debug("%s is not executable, skipping event", cmd)
            return 0

        env = os.environ.copy()
        env.update({"ZFSBACKUP_%s" % k.upper(): v for k, v in args.items()})
        env["ZFSBACKUP_REALLY"] = str(self._really)

        log.debug("Executing with environment: %s", json.dumps(env))
        p = Popen([os.path.join(self.directory, event)],
                  stdout=PIPE, stderr=PIPE, stdin=PIPE, env=env)
        (stdout, stderr) = p.communicate()

        if p.returncode != 0:
            log.error("Failed with returncode %d: %s", p.returncode,
                      stderr.decode("utf8").split("\n")[0]
                      if stderr else "")
            return p.returncode

        log.debug("Success: %s",
                  stdout.decode("utf8").split("\n")[0]
                  if stdout else "")
        return 0

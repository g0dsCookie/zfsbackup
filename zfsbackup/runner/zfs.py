import os
from subprocess import Popen, PIPE
from typing import List, Dict, Union

from zfsbackup.runner.base import RunnerBase


class ZFS(RunnerBase):
    def __init__(self, zfs="/usr/bin/zfs", sudo="/usr/bin/sudo", really=False):
        super().__init__(zfs, sudo, really)

    @staticmethod
    def join(*args):
        if not args or not args[0]:
            raise Exception("At least one name has to be given!")
        return "/".join([v for v in args if v])

    def datasets(self, dataset: str = None, recurse=False,
                 snapshot=False, options: List[str] = None,
                 sort: str = None,
                 sort_ascending=False) -> List[Dict[str, Union[str, int]]]:
        if not options:
            options = ["name", "used", "available", "referenced", "mountpoint"]
        args = [
            "list", "-H",
            "-o", ",".join(options),
            "-s" if sort_ascending else "-S", sort if sort else "name"
        ]
        if snapshot:
            args += ["-t", "snapshot"]
        if recurse:
            args += ["-r"]
        args.append(dataset)
        ret = self._run(args, parser=self._parse_list,
                        parser_args={"options": options},
                        readonly=True)
        return ret[1] if ret[0] == 0 else None

    def has_dataset(self, dataset: str):
        return self.datasets(dataset=dataset, options=["name"]) is not None

    def snapshot(self, dataset: str, snapshot: str,
                 recurse=False):
        args = ["snapshot"]
        if recurse:
            args.append("-r")
        args.append("%s@%s" % (dataset, snapshot))
        return self._run(args, sudo=True)[0] == 0

    def destroy(self, dataset: str, snapshot: str = None,
                recurse=False):
        args = ["destroy"]
        if recurse:
            args.append("-r")
        args.append(dataset if not snapshot else "%s@%s" % (dataset, snapshot))
        return self._run(args, sudo=True)[0] == 0

    def diff_snapshots(self, dataset: str, lsnap: str, rsnap: str):
        args = [
            "diff",
            "%s@%s" % (dataset, lsnap),
            "%s@%s" % (dataset, rsnap)
        ]
        (retcode, (stdout, stderr)) = self._run(args, readonly=True)
        return retcode == 0 and stdout

    def copy(self, source: str, snapshot: str, target: str,
             incremental: str = None, replicate=False, rollback=False,
             overwrites: Dict[str, str] = None, ignores: List[str] = None):
        send_args = ["send"]
        if replicate:
            send_args.append("-R")
        if incremental:
            send_args += ["-I", incremental]
        send_args.append("%s@%s" % (source, snapshot))

        recv_args = ["recv"]
        if rollback:
            recv_args.append("-F")
        if overwrites:
            for k, v in overwrites.items():
                recv_args.append("-o")
                recv_args.append("%s=%s" % (k, v))
        if ignores:
            for v in ignores:
                recv_args.append("-x")
                recv_args.append(v)
        recv_args.append(target)

        send_cmd = self._cmdline(send_args, sudo=True)
        recv_cmd = self._cmdline(recv_args, sudo=True)

        if not self._really:
            self.log.info("Would run '%s | %s'",
                          " ".join(send_cmd),
                          " ".join(recv_cmd))
            return
        else:
            self.log.debug("Running '%s | %s'",
                           " ".join(send_cmd),
                           " ".join(recv_cmd))

        with open(os.devnull) as devnull:
            sender = Popen(send_cmd, stdout=PIPE, stderr=PIPE)
            receiver = Popen(recv_cmd, stdin=sender.stdout,
                             stdout=devnull, stderr=PIPE)
            sender.stdout.close()
            sstderr = sender.stderr.read().decode("utf8").split("\n")
            sender.stderr.close()
            rstderr = receiver.stderr.read().decode("utf8").split("\n")
            receiver.stderr.close()
            ret = [sender.wait(), receiver.wait()]

        msg = "%s process failed with return code %d:\n%s"
        if ret[0] != 0:
            raise Exception(msg % ("Sender", ret[0], sstderr))
        if ret[1] != 0:
            raise Exception(msg % ("Receiver", ret[1], rstderr))

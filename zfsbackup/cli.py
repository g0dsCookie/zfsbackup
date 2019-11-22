import argparse
from datetime import datetime
import logging
import sys

from zfsbackup.config import Config
from zfsbackup.runner.zfs import ZFS
from zfsbackup.job.base import JobType


class ZfsBackupCli:
    def __init__(self):
        parser = argparse.ArgumentParser(description="ZFS backup utility.")
        parser.add_argument("-c", "--config", type=str,
                            default="zfsbackup.cfg",
                            help="Set path to config file. (%(default)s)")
        parser.add_argument("--debug", action="store_true",
                            help="Enable debug output.")
        parser.add_argument("-r", "--really", action="store_true",
                            help="Really execute critical commands.")

        actions = parser.add_subparsers(title="action",
                                        help="Action to execute.",
                                        dest="action")

        snapshot = actions.add_parser("snapshot",
                                      description="Take a snapshot of target.")
        snapshot.add_argument("--snap-only", action="store_true",
                              help="Only run snapshot and exit.")
        snapshot.add_argument("--clean-only", action="store_true",
                              help="Only run cleanup and exit.")
        snapshot.add_argument("jobs", metavar="JOB", type=str, nargs="+",
                              help="Snapshot specified jobs.")

        copy = actions.add_parser("copy",
                                  description="Copy dataset target.")
        copy.add_argument("jobs", metavar="JOB", type=str, nargs="+",
                          help="Copy specified target.")

        self._args = parser.parse_args()
        if not self._args.action:
            print("No action given.", file=sys.stderr)
            parser.print_help()
            exit(1)

        logging.basicConfig(
            format="%(asctime)-15s %(name)s [%(levelname)s]: %(message)s",
            level=logging.DEBUG if self._args.debug else logging.INFO
        )
        self._log = logging.getLogger("zfsbackup")

        self._cfg = Config()
        self._cfg.load(self._args.config)

        self._zfs = ZFS(zfs=self._cfg.zfs, sudo=self._cfg.sudo,
                        really=self._args.really)

    def snapshot(self):
        now = datetime.now().utcnow()

        for job in self._cfg.list_jobs(JobType.snapshot, self._args.jobs):
            print(job._pool, job._dataset)
            if not self._args.clean_only:
                job.snapshot(self._zfs, now)
            if not self._args.snap_only:
                job.clean(self._zfs, now)

    def copy(self):
        raise NotImplementedError()

    def run(self):
        getattr(self, self._args.action)()


def main():
    ZfsBackupCli().run()


if __name__ == "__main__":
    main()

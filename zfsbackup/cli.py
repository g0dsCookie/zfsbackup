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

        action_list = {
            "snapshot": "Take a snapshot of a target.",
            "clean": "Cleanup snapshots of a target.",
            "copy": "Copy specified target to it's destination.",
        }

        for actionname, description in action_list.items():
            action = actions.add_parser(actionname, description=description)
            action.add_argument("jobs", metavar="JOB", type=str, nargs="+",
                                help="Target(s) to run action on")

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
        self._cfg.load(self._args.config, self._args.really)

        with self._cfg.cache as cache:
            if cache.db_version == -1:
                self._log.info("Creating new cache file")
                cache.create_tables()

    def snapshot(self):
        now = datetime.now().utcnow()
        for job in self._cfg.list_jobs(JobType.snapshot, self._args.jobs):
            job.run(now)

    def clean(self):
        now = datetime.now().utcnow()
        for job in self._cfg.list_jobs(JobType.clean, self._args.jobs):
            job.run(now)

    def copy(self):
        for job in self._cfg.list_jobs(JobType.copy, self._args.jobs):
            job.run()

    def run(self):
        getattr(self, self._args.action)()


def main():
    ZfsBackupCli().run()


if __name__ == "__main__":
    main()

import argparse
from datetime import datetime
import logging
import os
import shutil
import sys

from zfsbackup.config import Config
from zfsbackup.job.base import JobType


class ZfsBackupCli:
    def __init__(self):
        parser = argparse.ArgumentParser(description="ZFS backup utility.")
        parser.add_argument("-c", "--config", type=str,
                            default="zfsbackup.xml",
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
            "jobset": "Run specified jobset(s)",
        }

        for actionname, description in action_list.items():
            action = actions.add_parser(actionname, description=description)
            action.add_argument("-l", "--list", action="store_true",
                                help="Only list jobs that would be executed")
            action.add_argument("jobs", metavar="JOB", type=str, nargs="+",
                                help="Target(s) to run action on")

        a_cache = actions.add_parser("cache",
                                     description="Cache maintenance actions")
        as_cache = a_cache.add_subparsers(title="action",
                                          help="Action to execute on cache",
                                          dest="cacheaction")

        a = as_cache.add_parser("update", description="Update cache")
        a.add_argument("--no-backup", action="store_true",
                       help="Do not backup old cache file.")
        as_cache.add_parser("list-snapshots",
                            description="List recorded snapshots")
        as_cache.add_parser("maint", description="Run maintenance jobs")

        action = actions.add_parser("list",
                                    description="List all defined jobs(ets).")
        action.add_argument("type", metavar="TYPE", type=str,
                            help="Type to list (jobs|jobsets)")

        self._args = parser.parse_args()
        if not self._args.action:
            print("No action given.", file=sys.stderr)
            parser.print_help()
            exit(1)
        if self._args.action == "cache" and not self._args.cacheaction:
            print("No cache action given.", file=sys.stderr)
            a_cache.print_help()
            exit(1)

        logging.basicConfig(
            format="%(asctime)-15s %(name)s [%(levelname)s]: %(message)s",
            level=logging.DEBUG if self._args.debug else logging.INFO
        )
        self._log = logging.getLogger("zfsbackup")

        self._cfg = Config()
        self._cfg.load(self._args.config, self._args.really)

        if not self._args.action == "updatedb":
            with self._cfg.cache as cache:
                if not cache.is_current:
                    self._log.critical("Cache update is needed!")
                    self._log.critical("Use 'zfsbackup updatedb'" +
                                       " to update cache")
                    exit(1)

    def run_job(self, typ: JobType):
        now = datetime.now().utcnow()
        for job in self._cfg.list_jobs(typ, self._args.jobs):
            if self._args.list:
                self._log.info("Would run %s.%s", job.type.name, job.name)
                continue
            job.run(now=now)

    def snapshot(self): self.run_job(JobType.snapshot)

    def clean(self): self.run_job(JobType.clean)

    def copy(self): self.run_job(JobType.copy)

    def jobset(self):
        now = datetime.now().utcnow()
        for job in self._cfg.list_jobsets(self._args.jobs):
            if self._args.list:
                self._log.info("Would run %s.%s", job.type.name, job.name)
                continue
            job.run(now=now)

    def list(self):
        typ = self._args.type.lower()
        if typ == "jobs":
            for job in self._cfg.jobs:
                self._log.info("%s.%s", job.type, job.name)
        elif typ == "jobsets":
            for name, jobs in self._cfg.jobsets:
                self._log.info("%s: %s", name, ", ".join(jobs))
        else:
            self._log.error("Unknown type to list: %s", self._args.type)
            exit(1)

    def cache(self):
        getattr(self, "cache_" + self._args.cacheaction.replace("-", "_"))()

    def cache_update(self):
        bak = self._cfg.cache_path + ".bak"
        if not self._args.no_backup:
            if os.path.exists(bak):
                self._log.info("%s already exists, removing old backup...",
                               bak)
                os.unlink(bak)

            self._log.info("Copying %s to %s", self._cfg.cache_path, bak)
            shutil.copyfile(self._cfg.cache_path, bak, follow_symlinks=True)

        self._log.info("Updating cache file...")
        with self._cfg.cache as cache:
            cache.update_tables()
        self._log.info("Done!")

    def cache_list_snapshots(self):
        with self._cfg.cache as cache:
            snapshots = cache.snapshots()
            for dataset in sorted(snapshots):
                for snapshot in sorted(snapshots[dataset]):
                    self._log.info("%s@%s: %s", dataset, snapshot,
                                   snapshots[dataset][snapshot])

    def cache_maint(self):
        with self._cfg.cache as cache:
            cache.snapshots_cleanup()

    def run(self):
        getattr(self, self._args.action.replace("-", "_"))()


def main():
    ZfsBackupCli().run()


if __name__ == "__main__":
    main()

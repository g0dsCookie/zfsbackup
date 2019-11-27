import logging
from typing import List, Dict, Union
import xml.etree.ElementTree as ET

from zfsbackup.cache import Cache
from zfsbackup.runner.zfs import ZFS
from zfsbackup.job import JobBase, JobType, get_constructor


class Config:
    def __init__(self):
        self._zfs: ZFS = None
        # self._zpool = "/usr/bin/zpool"
        self._really = False
        self._cache = "/var/cache/zfsbackup/zfsbackup.sqlite"
        self._lockdir = "/var/lock/zfsbackup"
        self._jobs: Dict[JobType, List[JobBase]] = {}
        self._jobsets: Dict[str, List[Union[JobBase, List[str]]]] = {}
        self._log = logging.getLogger("Config")

    @property
    def zfs(self): return self._zfs

    @property
    def really(self): return self._really

    @property
    def cache(self): return Cache(self._cache)

    @property
    def lockdir(self): return self._lockdir

    def list_jobs(self, typ: JobType, names: List[str],
                  no_all=False) -> List[JobBase]:
        if not no_all and "all" in names:
            return self._jobs[typ]

        # first we list all jobset within names
        for name, jobset in self._jobsets.items():
            if name in names:
                names.remove(name)
                for job in jobset:
                    if isinstance(job, str):
                        # list another jobset within this jobset
                        yield from self.list_jobsets([job], no_all=True,
                                                     typ=typ)
                        continue
                    if job.type == typ:
                        yield job

        # then list all single jobs
        for job in self._jobs[typ]:
            if job.name in names:
                names.remove(job.name)
                yield job

        if names:
            self._log.warn("Unmatched job(set)s for %s: %s",
                           typ.name, ", ".join(names))

    def list_jobsets(self, names: List[str],
                     no_all=False, typ: JobType = None) -> List[JobBase]:
        if not no_all and "all" in names:
            for name, jobset in self._jobsets.items():
                for job in jobset:
                    yield job
            return

        for name, jobset in self._jobsets.items():
            if name not in names:
                continue
            names.remove(name)
            for job in jobset:
                if isinstance(job, str):
                    # list another jobset within this jobset
                    yield from self.list_jobsets([job], no_all=True)
                    continue

                if typ is not None:
                    # list only jobs matching typ
                    # (used exclusivly by list_jobs)
                    if job.type == typ:
                        yield job
                    continue

                yield job

        if names:
            self._log.warn("Unmatched jobsets: %s", ", ".join(names))

    def _parse_jobtype(self, cfg: List[ET.Element], ctor):
        for v in cfg:
            name = v.attrib["name"]
            enabled = v.find("enabled")
            yield ctor(name, enabled is not None, self, v)

    def _parse_jobs(self, cfg: ET.Element):
        for typ in JobType:
            name = typ.name
            self._log.debug("Loading '%s' jobs...", name)
            configs = cfg.findall(name)
            jobs = list(self._parse_jobtype(configs, get_constructor(typ)))
            self._log.debug("Found %d jobs for %s", len(jobs), name)
            self._jobs[typ] = jobs

    def _parse_jobsets(self, cfg: ET.Element):
        for jobset in cfg.findall("jobset"):
            name = jobset.attrib["name"]
            self._log.debug("Loading jobset %s", name)
            jobs = []
            for jobcfg in jobset:
                jobname = jobcfg.text

                if jobcfg.tag == "jobset":
                    self._log.debug("Added jobset %s to jobset %s",
                                    jobname, name)
                    jobs.append(jobname)
                    continue

                try:
                    jobtyp = JobType[jobcfg.tag]
                except KeyError:
                    self._log.error("Invalid jobtype in jobset %s for %s: %s",
                                    name, jobname, jobcfg.tag)
                    continue

                found = False
                for job in self._jobs[jobtyp]:
                    if job.name == jobname:
                        self._log.debug("Added job %s.%s to jobset %s",
                                        job.type.name, job.name, name)
                        jobs.append(job)
                        found = True
                        break
                if not found:
                    self._log.error("Undefined job in jobset %s: %s.%s",
                                    name, jobtyp.name, jobname)
            self._log.debug("Loaded jobset %s with %d jobs", name, len(jobs))
            self._jobsets[name] = jobs

        for typ in JobType:
            typejobs = self._jobs[typ]
            for jobset in cfg.findall(typ.name):
                name = jobset.attrib["name"]
                jobs = []
                self._log.debug("Loading jobset %s.%s", typ.name, name)
                for jobcfg in jobset:
                    jobname = jobcfg.text
                    found = False
                    for job in typejobs:
                        if job.name == jobname:
                            self._log.debug("Added job %s.%s to jobset %s",
                                            job.type.name, job.name, name)
                            jobs.append(job)
                            found = True
                            break
                    if not found:
                        self._log.error("Undefined job in jobset %s: %s.%s",
                                        name, typ.name, jobname)
                self._jobsets[name] = jobs
                self._log.debug("Loaded jobset %s.%s with %d jobs",
                                typ.name, name, len(jobs))

    def load(self, file: str, really: bool):
        self._really = really
        cfg = ET.parse(file)
        root = cfg.getroot()

        cache = root.find("cache")
        lockdir = root.find("locks")
        commands = root.find("commands")
        jobs = root.find("jobs")
        jobsets = root.find("jobsets")

        zfs = "/usr/bin/zfs"
        sudo = "/usr/bin/sudo"

        if cache is not None:
            self._cache = cache.text

        if lockdir is not None:
            self._lockdir = lockdir.text

        if commands is not None:
            for cmd in commands:
                if cmd.tag == "zfs":
                    zfs = cmd.text
                elif cmd.tag == "sudo":
                    sudo = cmd.text
                else:
                    self._log.debug("Ignoring extra command: %s",
                                    cmd.attrib["name"])

        self._zfs = ZFS(zfs=zfs, sudo=sudo, really=really)

        if jobs is None:
            self._log.critical("No jobs defined.")
            exit(0)
        self._parse_jobs(jobs)

        if jobsets is not None:
            self._parse_jobsets(jobsets)

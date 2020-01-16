import glob
import logging
import os
from typing import List, Dict, Union, Tuple
import xml.etree.ElementTree as ET

from zfsbackup.cache import Cache
from zfsbackup.runner.command import Command
from zfsbackup.runner.zfs import ZFS
from zfsbackup.job import JobBase, JobType, get_constructor


class Config:
    def __init__(self):
        self._zfs: ZFS = None
        # self._zpool = "/usr/bin/zpool"
        self._really = False
        self._zfs = "/usr/bin/zfs"
        self._sudo = "/usr/bin/sudo"
        self._cache = "/var/cache/zfsbackup/zfsbackup.sqlite"
        self._lockdir = "/var/lock/zfsbackup"
        self._commands: Dict[str, Dict] = {}
        self._jobs: Dict[JobType, List[JobBase]] = {}
        self._jobsets: Dict[str, List[Union[JobBase, str]]] = {}
        self._log = logging.getLogger("Config")

    @property
    def jobs(self) -> List[JobBase]:
        for _, jobs in self._jobs.items():
            yield from jobs

    @property
    def jobsets(self) -> List[Tuple[str, str]]:
        for name, jobset in self._jobsets.items():
            yield (name, [("%s.%s" % (j.type.name, j.name)
                           if isinstance(j, JobBase) else "jobset.%s" % j)
                          for j in jobset])

    @property
    def zfs(self): return ZFS(zfs=self._zfs, sudo=self._sudo,
                              really=self._really)

    @property
    def really(self): return self._really

    @property
    def cache(self): return Cache(self._cache)

    @property
    def cache_path(self): return self._cache

    @property
    def lockdir(self): return self._lockdir

    def get_command(self, name):
        cmd = self._commands.get(name, None)
        if cmd is None:
            return None
        return Command(name=name, cmd=cmd["command"],
                       args=cmd.get("args", []),
                       sudo=self._sudo, use_sudo=cmd.get("use_sudo", False),
                       readonly=cmd.get("readonly", False),
                       really=self._really)

    def list_jobs(self, typ: JobType, names: List[str],
                  no_all=False) -> List[JobBase]:
        if not no_all:
            ret = False
            if "all" in names:
                yield from self._jobs[typ]
                ret = True
            if "all-js" in names or "all-jobsets" in names:
                yield from self.list_jobsets(["all"], typ=typ)
                ret = True
            if ret:
                return

        i = 0
        while i < len(names):
            if names[i] in self._jobsets:
                for job in self._jobsets[names[i]]:
                    if isinstance(job, str):
                        yield from self.list_jobsets([job], typ=typ)
                    elif job.type == typ:
                        yield job
                names.pop(i)
                continue
            i += 1

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
                    if isinstance(job, str):
                        yield from self.list_jobsets([job], no_all=True)
                        continue
                    if typ is not None:
                        if job.type == typ:
                            yield job
                        continue
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

    def _load_include(self, cfg: ET.ElementTree) -> str:
        include = cfg.find("include")
        return include.text if include is not None else ""

    def _load_cache(self, cfg: ET.ElementTree) -> str:
        cache = cfg.find("cache")
        return cache.text if cache is not None else ""

    def _load_lockdir(self, cfg: ET.ElementTree) -> str:
        lockdir = cfg.find("locks")
        return lockdir.text if lockdir is not None else ""

    def _load_commands(self, cfg: ET.ElementTree) \
            -> List[Tuple[str, Union[str, Dict]]]:
        commands = cfg.find("commands")
        if commands is None:
            return

        for cmd in commands:
            if cmd.tag == "zfs":
                yield ("zfs", cmd.text)
                continue
            if cmd.tag == "sudo":
                yield ("sudo", cmd.text)
                continue

            name = cmd.attrib["name"]
            command = cmd.find("command")
            if command is None:
                self._log.error("Could not load command %s: Missing <command>",
                                name)

            argcfg = cmd.find("arguments")
            arguments = ([] if argcfg is None
                         else list([a.text for a in argcfg.findall("arg")]))
            yield (name, {
                "command": command.text,
                "args": arguments,
                "use_sudo": cmd.find("sudo") is not None,
                "readonly": cmd.find("readonly") is not None,
            })

    def _load_jobs(self, file: str, cfg: ET.ElementTree) -> List[JobBase]:
        jobs = cfg.find("jobs")
        if jobs is None:
            return
        for job in jobs:
            name = job.attrib["name"]
            enabled = job.find("enabled") is not None
            try:
                typ = JobType[job.tag]
            except KeyError:
                self._log.error("Unknown JobType for %s: %s",
                                name, job.tag)
                continue
            ctor = get_constructor(typ)
            yield ctor(name, file, enabled, self, job)

    def _load_jobsets(self, cfg: ET.ElementTree) -> List[ET.Element]:
        jobsets = cfg.find("jobsets")
        if jobsets is None:
            return
        yield from jobsets

    def _load_file(self, file: str):
        root = ET.parse(file).getroot()
        return (self._load_include(root),
                self._load_cache(root),
                self._load_lockdir(root),
                self._load_commands(root),
                self._load_jobs(file, root),
                self._load_jobsets(root))

    def _append_jobs(self, jobs: List[JobBase]):
        for job in jobs:
            if job.type not in self._jobs:
                self._jobs[job.type] = []
            lst = self._jobs[job.type]
            i = 0
            while i < len(lst):
                if lst[i].name == job.name:
                    self._log.warn("Job %s already defined in %s, " +
                                   "overwriting from file %s",
                                   job.name, lst[i].file, job.file)
                    lst[i] = job
                    break
                i += 1
            else:
                self._log.debug("Adding new job %s from %s",
                                job.name, job.file)
                lst.append(job)

    def _append_generic_jobset(self, file: str, jobset: ET.Element):
        name = jobset.attrib["name"]

        if name in self._jobsets:
            self._log.warn("JobSet %s already defined in %s, " +
                           "overwriting from file %s",
                           name, self._jobset_files[name], file)

        jobs = []
        for jc in jobset:
            jn = jc.text
            if jc.tag == "jobset":
                jobs.append(jn)
                continue

            try:
                jt = JobType[jc.tag]
            except KeyError:
                self._log.error("Invalid JobType %s in JobSet %s for %s",
                                jc.tag, name, jn)
                exit(1)

            for job in self._jobs[jt]:
                if job.name == jn:
                    jobs.append(job)
                    break
            else:
                self._log.error("Undefined Job %s.%s in JobSet %s",
                                jt.name, jn, name)
                exit(1)

        self._jobsets[name] = jobs
        self._jobset_files[name] = file

    def _append_specific_jobset(self, file: str,
                                jobset: ET.Element):
        name = jobset.attrib["name"]
        try:
            typ = JobType[jobset.tag]
        except KeyError:
            self._log.error("Invalid JobType %s for JobSet %s",
                            jobset.tag, name)
            exit(1)

        if name in self._jobsets:
            self._log.warn("JobSet %s already defined in %s, " +
                           "overwriting from file %s",
                           name, self._jobset_files[name], file)

        jobs = []
        for jc in jobset:
            jn = jc.text
            for job in self._jobs[typ]:
                if job.name == jn:
                    jobs.append(job)
                    break
            else:
                self._log.error("Undefined Job %s.%s in JobSet %s",
                                typ.name, jn, name)
                exit(1)

        self._jobsets[name] = jobs
        self._jobset_files[name] = file

    def _append_jobsets(self, file: str, jobsets: List[ET.Element]):
        for jobset in jobsets:
            if jobset.tag == "jobset":
                self._append_generic_jobset(file, jobset)
                continue
            self._append_specific_jobset(file, jobset)

    def _append_commands(self, commands: List[Tuple[str, Union[str, Dict]]]):
        for name, command in commands:
            if name == "zfs":
                self._zfs = command
            elif name == "sudo":
                self._sudo = command
            else:
                self._commands[name] = command

    def load(self, file: str, really: bool):
        self._really = really

        # we defer jobset parsing till we loaded all jobs
        alljobsets = []

        files = [file]
        i = 0
        while i < len(files):
            (inc, cache, lockdir, cmds, jobs, js) = self._load_file(files[i])
            if inc:
                files.extend([f for f in glob.iglob(inc, recursive=True)
                              if os.path.isfile(f)])
            if cache:
                self._cache = cache
            if lockdir:
                self._lockdir = lockdir
            if cmds:
                self._append_commands(list(cmds))
            if jobs:
                self._append_jobs(jobs)
            if js:
                alljobsets.append((files[i], js))
            i += 1

        self._jobset_files: Dict[str, str] = {}
        for (file, jobsets) in alljobsets:
            self._append_jobsets(file, jobsets)
        del self._jobset_files

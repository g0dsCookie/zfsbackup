from zfsbackup.job.base import JobBase, JobType
from zfsbackup.job.clean import Clean
from zfsbackup.job.copy import Copy
from zfsbackup.job.snapshot import Snapshot


_ctors = dict({t: globals()[t.name.capitalize()] for t in JobType})


def get_constructor(typ: JobType):
    return _ctors[typ]

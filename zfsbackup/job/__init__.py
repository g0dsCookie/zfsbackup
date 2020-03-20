from .base import JobBase, JobType
from .clean import Clean
from .copy import Copy
from .snapshot import Snapshot


_ctors = dict({t: globals()[t.name.capitalize()] for t in JobType})


def get_constructor(typ: JobType):
    return _ctors[typ]

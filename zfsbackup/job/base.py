import abc
from enum import Enum
import logging


class JobType(Enum):
    copy = 0
    snapshot = 1


class JobBase(metaclass=abc.ABCMeta):
    def __init__(self, name: str, typ: JobType, enabled: bool):
        self._name = name
        self._type = typ
        self._enabled = enabled
        self._log = logging.getLogger("%s.%s" % (typ.name.capitalize(), name))

    @property
    def name(self): return self._name

    @property
    def type(self): return self._type

    @property
    def enabled(self): return self._enabled

    @property
    def log(self): return self._log

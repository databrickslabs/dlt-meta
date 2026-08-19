"""Data models shared by tag configuration, planning, SQL, and state."""

from dataclasses import dataclass, field
from typing import Optional

OWN_SCRIPT = "script"
OWN_EXTERNAL = "external"


@dataclass(frozen=True)
class Key:
    catalog: str
    schema: str
    table: str
    column: Optional[str]
    tag_key: str

    def fq_table(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{self.table}`"

    def label(self) -> str:
        column = f".{self.column}" if self.column else ""
        return f"{self.catalog}.{self.schema}.{self.table}{column} :: {self.tag_key}"


@dataclass
class Desired:
    value: str
    contributors: set = field(default_factory=set)


@dataclass
class Action:
    kind: str
    key: Key
    value: Optional[str] = None
    reason: str = ""
    idx: int = -1
    contributors: set = field(default_factory=set)
    ownership: Optional[str] = None

import abc
from dataclasses import dataclass
from pathlib import Path
import re

BUFFER_SIZE = 4 * 2**20  # 4MB


@dataclass
class QueryStr:
    s: str
    starts_with: str | None

    def clean(self) -> str:
        query = re.sub(r"\bnotnull\b", "is not null", self.s, flags=re.I)
        query = re.sub(r"\bisnull\b", "is null", query, flags=re.I)
        query = re.sub(r"\s~\*\s", " ilike ", query, flags=re.I)
        query = re.sub(r"\s~\s", " like ", query, flags=re.I)
        query = re.sub(
            r"(?P<x>\([^()]+\)|[^(),\s]+)::json *#>> *(?P<y>'[^']+')",
            lambda m: "json_path_lookup(%s, %s)" % (m.group("x"), m.group("y")),
            query,
            flags=re.I,
        )
        query = re.sub(r"\bcharacter varying\b", "varchar(100)", query, flags=re.I)
        query = re.sub(r"\) TO STDOUT.*$", "", query, flags=re.I)
        query = re.sub(r"\) TO [\"']?s3:.*$", "", query, flags=re.I)
        query = re.sub(
            r"substring\((?P<id>[^)]+)\bfrom\s+(?P<beg>[0-9]+)\s+for\s+(?P<len>[0-9]+)\s+\)",
            lambda m: f"substr({m.group('id')}, {m.group('beg')}, {m.group('len')})",
            query,
            flags=re.I,
        )
        query = re.sub(r"day\s*\(\s*[0-9]+\s*\)", "day", query, flags=re.I)
        if self.starts_with is not None:
            query = re.sub(r"\\", "", query)
            if query.endswith(self.starts_with):
                query = query[: -len(self.starts_with)]
        return query.strip()

    def replace(self, replace_map: dict[str, str]) -> None:
        # To make largest values match, first sort biggest to smallest
        for k, v in sorted(replace_map.items(), key=lambda x: x[0], reverse=True):
            self.s = self.s.replace(k, v)


class LogReader(abc.ABC):
    def __init__(self, log_path: Path) -> None:
        self._log_path: Path = log_path
        self._queries: list[str] = []

    @abc.abstractmethod
    def read(self, num=None) -> None:
        ...

    def get_queries(self) -> list[str]:
        return self._queries


class PostgresLogReader(LogReader):
    LINE_START = r"[0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\s+UTC"

    def read(self, num=None) -> None:
        with open(self._log_path) as f:
            text = f.read()

        pattern = re.compile(
            r"(?P<logline>%s.+?)((?=%s)|$)" % (self.LINE_START, self.LINE_START), re.S
        )

        query_strs = []
        for match in re.finditer(pattern, text):
            logline = match.group("logline")
            logline = re.sub(r"--\s+.*$", "", logline, flags=re.M)
            logline = logline.replace("\n", " ")

            if re.search(self.LINE_START + r"\s+LOG", logline) and re.search(
                r"\bselect\b", logline, flags=re.I
            ):
                if num is not None and len(query_strs) >= num:
                    break

                match = re.search(r"\bselect\b.*", logline, flags=re.I | re.S)
                if match is not None:
                    starts_with = (
                        logline[match.start() - 1] if match.start() >= 1 else None
                    )
                    query_strs.append(
                        QueryStr(s=match.group(), starts_with=starts_with)
                    )
            elif re.search(self.LINE_START + r"\s+DETAIL", logline) and re.search(
                r"\bparameters:", logline
            ):
                match = re.search(
                    r"\bparameters:(?P<map>.*)", logline, flags=re.I | re.S
                )
                if match is not None:
                    params = split_commas(match.group("map"))
                    param_map = {}
                    for param in params:
                        k, v = param.split("=", 1)
                        param_map[k.strip()] = v.strip()

                    query_strs[-1].replace(param_map)

        self._queries = [q.clean() for q in query_strs]


class SingleLogReader(LogReader):
    def read(self, num=None) -> None:
        with open(self._log_path, buffering=BUFFER_SIZE) as f:
            for line in f.readlines():
                query = QueryStr(line, None)
                self._queries.append(query.clean())

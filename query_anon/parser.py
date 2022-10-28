from dataclasses import dataclass
import logging

import sqlglot


class IdGen:
    def __init__(self, tag: str) -> None:
        self._tag = tag
        self._counter = 0

    def gen(self) -> str:
        ret = f"{self._tag}{self._counter}"
        self._counter += 1
        return ret


class IdMap:
    def __init__(self, tag: str) -> None:
        self._d = {}
        self._gen = IdGen(tag)

    def __getitem__(self, key: str) -> str:
        if key not in self._d:
            self._d[key] = self._gen.gen()
        return self._d[key]

    def __contains__(self, key: str) -> bool:
        return key in self._d


class QueryParser:
    def __init__(self) -> None:
        self._tables = IdMap("t")
        self._attrs = IdMap("a")
        self._funcs = IdMap("f")
        self._vars = IdMap("v")
        self._miscs = IdMap("m")

        # For each table,
        # they can have regular table name, or global table name
        # each column can have no table, refer to regular table name, or global table name

    def _anonymize_subquery(self, expr) -> None:
        """Helper function to anonymize subqueries to make sure aliases don't
        go across subqueries.
        """
        attr_aliases = IdMap("aa")
        table_aliases = IdMap("ta")

        local_table_map = {}

        replaced = set()

        for table_alias in expr.find_all(sqlglot.exp.TableAlias):
            if table_alias.name in replaced:
                continue

            alias_id = table_aliases[table_alias.name]
            table_alias.this.replace(sqlglot.exp.to_identifier(alias_id))
            replaced.add(alias_id)

            if table_alias.args.get("columns"):
                for col in table_alias.args["columns"]:
                    if not isinstance(col, sqlglot.exp.Identifier):
                        continue

                    alias_id = attr_aliases[col.name]
                    col.replace(sqlglot.exp.to_identifier(alias_id))
                    replaced.add(alias_id)

        for alias in expr.find_all(sqlglot.exp.Alias):
            if alias.alias in replaced:
                continue

            if not isinstance(alias.args.get("alias"), sqlglot.exp.TableAlias):
                alias_id = attr_aliases[alias.alias]
                alias.args["alias"].replace(sqlglot.exp.to_identifier(alias_id))
                replaced.add(alias_id)

        for table in expr.find_all(sqlglot.exp.Table):
            if table.name in replaced:
                continue

            if table.name in table_aliases:
                table_id = table_aliases[table.name]
                table.replace(sqlglot.exp.table_(table_id))
                replaced.add(table_id)
                continue

            if table.name not in local_table_map:
                # fqtn = fully-qualified table name
                table_id = self._tables[sqlglot.exp.table_name(table)]
                local_table_map[table.name] = table_id

            table_id = local_table_map[table.name]
            table.replace(sqlglot.exp.table_(table_id))
            replaced.add(table_id)

        for col in expr.find_all(sqlglot.exp.Column):
            if col.args.get("table"):
                if col.table in table_aliases:
                    col.args["table"].replace(
                        sqlglot.exp.to_identifier(table_aliases[col.table])
                    )
                elif col.table in local_table_map:
                    col.args["table"].replace(
                        sqlglot.exp.to_identifier(local_table_map[col.table])
                    )

            if col.name in replaced:
                continue

            if col.name in attr_aliases:
                attr_id = attr_aliases[col.name]
                col.this.replace(sqlglot.exp.to_identifier(attr_id))
                continue

            attr_id = self._attrs[col.name]

            col.this.replace(sqlglot.exp.to_identifier(attr_id))
            replaced.add(attr_id)

        for func in expr.find_all(sqlglot.exp.Anonymous):
            if func.name in replaced:
                continue

            func_id = self._funcs[func.name]
            func.set("this", func_id)
            replaced.add(func_id)

        for var in expr.find_all(sqlglot.exp.Var):
            if var.name in replaced:
                continue

            var_id = self._vars[var.name]
            var.set("this", var_id)
            replaced.add(var_id)

        for const in expr.find_all(sqlglot.exp.Literal):
            if (const.is_string and const.this == "x") or (
                not const.is_string and const.this == "1"
            ):
                continue

            const.replace(
                sqlglot.exp.Literal.string("x")
                if const.is_string
                else sqlglot.exp.Literal.number(1)
            )

        for misc in expr.find_all(sqlglot.exp.Identifier):
            if misc.name not in replaced:
                misc_id = self._miscs[misc.name]
                misc.set("this", misc_id)
                replaced.add(misc_id)

    def _anonymize(self, expr) -> str:
        for subquery in reversed(list(expr.find_all(sqlglot.exp.Subquery, bfs=False))):
            self._anonymize_subquery(subquery)
        self._anonymize_subquery(expr)
        return expr.sql()

    def parse_queries(self, query_strs: list[str]) -> list[str]:
        queries = []
        unparsed = []
        for query_str in query_strs:
            try:
                parsed = sqlglot.parse_one(query_str)
                queries.append(self._anonymize(parsed))
            except Exception as e:
                import pdb

                pdb.post_mortem()
                raise e

                unparsed.append(query_str)
                continue

        if unparsed:
            logging.warn(f"Was unable to parse {len(unparsed)} queries")

        return queries

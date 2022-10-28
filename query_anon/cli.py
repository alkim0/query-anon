import logging
from pathlib import Path
import sys

import click

from query_anon.log_reader import PostgresLogReader, SingleLogReader
from query_anon.parser import QueryParser


@click.command()
@click.option("-o", "--output", type=Path)
@click.option("-d", "--dialect")
@click.option("-v", "--verbose", is_flag=True)
@click.option(
    "--log-type",
    type=click.Choice(["postgres", "single"]),
    default="single",
    help="Log type to parse. 'single' is one query per line",
)
@click.argument("query-log", default=Path("/dev/stdin"))
def cli(output, dialect, verbose, log_type, query_log):
    if verbose:
        logging_level = logging.INFO
    else:
        logging_level = logging.WARNING
    logging.basicConfig(level=logging_level)

    if log_type == "postgres":
        log_reader = PostgresLogReader(query_log)
    else:
        log_reader = SingleLogReader(query_log)

    log_reader.read()
    query_strs = log_reader.get_queries()
    queries = QueryParser().parse_queries(query_strs)
    with (open(output, "w") if output is not None else sys.stdout) as f:
        for query in queries:
            f.write(query + "\n")


if __name__ == "__main__":
    cli()

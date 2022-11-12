from src.crawler import start_crawler
from src.flask import run_flask
from src.rankerer import calculate_ranks

import argparse

parser = argparse.ArgumentParser(
    description="Search system. Includes crawler, indexed DB, ranging module and module for accepting search queries")

parser.add_argument("command", metavar="<command [start_crawler, calc_ranks, run_flask]>", type=str,
                    help="Available commands: start_crawler, calc_ranks, run_flask", )

args = parser.parse_args()


COMMANDS_MAPPING = {
    "start_crawler": start_crawler,
    "run_flask": run_flask,
    "calc_ranks": calculate_ranks,
}

command = COMMANDS_MAPPING.get(args.command)

if not command:
    print(
        f"Available commands: start_crawler, calculate_ranks, run_flask.\nGot: {args.command}")
    exit(1)

command()

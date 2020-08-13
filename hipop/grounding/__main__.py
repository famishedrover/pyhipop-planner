import sys
import os
import argparse
import logging
import time
import itertools

import pddl
from .problem import Problem
from ..utils.profiling import start_profiling, stop_profiling
from ..utils.logger import setup_logging

LOGGER = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="HDDL Grounding")
    parser.add_argument("domain", help="PDDL domain file", type=str)
    parser.add_argument("problem", help="PDDL problem file", type=str)
    parser.add_argument("-d", "--debug", help="activate debug logs",
                        action='store_const', dest="loglevel",
                        const=logging.DEBUG, default=logging.WARNING)
    parser.add_argument("-v", "--verbose", help="activate verbose logs",
                        action='store_const', dest="loglevel",
                        const=logging.INFO, default=logging.WARNING)
    parser.add_argument("-o", "--output-graph", 
                        const='', default=None,
                        action='store', nargs='?',
                        help="generate output graphs for grounding steps")
    parser.add_argument("--trace-malloc", help="activate tracemalloc",
                        action='store_true')
    parser.add_argument("--profile", help="activate profiling",
                        action='store_true')

    def add_bool_arg(parser: argparse.ArgumentParser, name: str, dest: str, help: str, default: bool = False):
        group = parser.add_mutually_exclusive_group(required=False)
        group.add_argument('--' + name, dest=dest, action='store_true', help=help)
        group.add_argument('--no-' + name, dest=dest, action='store_false', help=f"do not {help}")
        parser.set_defaults(**{dest: default})
    add_bool_arg(parser, 'filter-rigid', 'rigid', "use rigid relations to filter groundings", True)
    add_bool_arg(parser, 'filter-relaxed', 'relaxed',
                 "use delete-relaxation to filter groundings", True)
    add_bool_arg(parser, 'htn', 'htn',
                 "use pure HTN decomposition", True)
    add_bool_arg(parser, 'mutex', 'mutex',
             "compute mutex on (motion) predicates", True)

    args = parser.parse_args()
    setup_logging(level=args.loglevel, without=['pddl', 'hipop.utils'])

    tic = time.process_time()
    LOGGER.info("Parsing PDDL domain %s", args.domain)
    pddl_domain = pddl.parse_domain(args.domain, file_stream=True)
    LOGGER.info("Parsing PDDL problem %s", args.problem)
    pddl_problem = pddl.parse_problem(args.problem, file_stream=True)
    toc = time.process_time()
    LOGGER.warning("parsing duration: %.3f", (toc - tic))

    profiler = start_profiling(args.trace_malloc, args.profile)

    tic = time.process_time()
    LOGGER.info("Building HiPOP problem")
    _ = Problem(pddl_problem, pddl_domain, args.output_graph, 
                args.rigid, args.relaxed, args.htn, args.mutex)
    toc = time.process_time()
    LOGGER.warning("grounding duration: %.3f", (toc - tic))

    stop_profiling(args.trace_malloc, profiler, "profile-grounding.stat")


if __name__ == '__main__':
    main()

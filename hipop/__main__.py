import sys
import os
import argparse
import logging
import time
import itertools

import pddl
from .problem.problem import Problem
from .utils.profiling import start_profiling, stop_profiling

LOGGER = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="HiPOP planner")
    parser.add_argument("domain", help="PDDL domain file", type=str)
    parser.add_argument("problem", help="PDDL problem file", type=str)
    parser.add_argument("-d", "--debug", help="Activate debug logs",
                        action='store_const', dest="loglevel",
                        const=logging.DEBUG, default=logging.WARNING)
    parser.add_argument("-v", "--verbose", help="Activate verbose logs",
                        action='store_const', dest="loglevel",
                        const=logging.INFO, default=logging.WARNING)
    parser.add_argument("--trace-malloc", help="Activate tracemalloc",
                        action='store_true')
    parser.add_argument("--profile", help="Activate profiling",
                        action='store_true')
    args = parser.parse_args()

    logformat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(stream=sys.stderr,
                        level=args.loglevel,
                        format=logformat)


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
    problem = Problem(pddl_problem, pddl_domain)
    toc = time.process_time()
    LOGGER.warning("building problem duration: %.3f", (toc - tic))
    LOGGER.info("nb actions: %d", len(problem.actions))
    LOGGER.info("nb tasks: %d", len(problem.tasks))
    LOGGER.info("nb methods: %d", sum(1 for task in problem.tasks for _ in task.methods))
    LOGGER.info("init state size: %d", len(problem.init))

    stop_profiling(args.trace_malloc, profiler)

if __name__ == '__main__':
    main()

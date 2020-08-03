import signal
import sys
import os
import argparse
import logging
import time
import itertools
import networkx
import io

import pddl
from .problem.problem import Problem
from .search.pop import POP
from .utils.profiling import start_profiling, stop_profiling
from .utils.logger import setup_logging
from .utils.io import output_ipc2020_flat, output_ipc2020_hierarchical

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
    parser.add_argument("--shoplike", help="SHOP-like search",
                        action='store_true')
    parser.add_argument("--dq", help="Double queue",
                        action='store_true')
    args = parser.parse_args()

    setup_logging(level=args.loglevel, without=['hipop.problem'])

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

    stop_profiling(args.trace_malloc, profiler, "profile-grounding.stat")
    profiler = start_profiling(args.trace_malloc, args.profile)

    LOGGER.info("Solving problem")
    tic = time.process_time()
    solver = POP(problem, args.shoplike, args.dq)

    def signal_handler(sig, frame):
        print('Stopping solver...')
        solver.stop()
    signal.signal(signal.SIGINT, signal_handler)

    plan = solver.solve(problem)
    toc = time.process_time()
    LOGGER.warning("solving duration: %.3f", (toc - tic))

    stop_profiling(args.trace_malloc, profiler, "profile-solving.stat")

    if plan is None:
        LOGGER.error("No plan found!")
        sys.exit(0)

    out_plan = io.StringIO()
    output_ipc2020_hierarchical(plan, out_plan)
    print(out_plan.getvalue())

if __name__ == '__main__':
    main()
